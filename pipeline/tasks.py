import itertools
import json
import time
import importlib
import yaml
import datetime
import csv
import random
import ast

import luigi
from luigi.parameter import ParameterVisibility
from luigi.contrib.simulate import RunAnywayTarget

import redis
import pandas as pd
import numpy as np

from descriptors import cachedproperty
from sqlalchemy import create_engine

from sklearn.model_selection import ParameterGrid
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder

from pipeline.luigi_ext import DBCredentialMixin, RowPostgresTarget, RedisConfig, NewRedisTarget
from pipeline.utils import filename_friendly_hash, scoped_session, redis_connect, encoding_cat_feats
from pipeline.results import Model, Evaluation
from pipeline.evaluations import Evaluations

def read_csv(path):
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = dict(row)
            new_row = {}
            for k, v in row.items():
                try:
                    new_row[k] = ast.literal_eval(v)
                except:
                    if v == '':
                        v = np.nan
                    new_row[k] = v
            yield new_row


def reservoir(it, k):
    it = iter(it)
    result = []
    for i, datum in enumerate(it):
        if i < k:
            result.append(datum)
        else:
            j = random.randint(0, i-1)
            if j < k:
                result[j] = datum
    while len(result) > 0:
        yield result.pop()


class RedisCleanUp(luigi.Task):
    pattern = luigi.Parameter(default="*")

    def run(self):
        r = redis_connect()
        for key in r.scan_iter(self.pattern):
            r.delete(key)


class LoadData(luigi.Task):
    id = luigi.Parameter()
    basic_config = luigi.DictParameter()

    @property
    def data_path(self):
        return self.basic_config['data_path']

    def run(self):
        # Read and sample with lazy evaluation
        reader = read_csv(self.data_path)
        if self.basic_config['random_sample']:
            df = pd.DataFrame(reservoir(reader, self.basic_config['sample_n']))
        df = pd.DataFrame(reader)
        # dfs = []
        # for df in pd.read_csv(path, chunksize=100):
        #     dfs.append(df)
        # df = df.concat(dfs)
        redis_conn = redis_connect()
        serialized_data = df.to_msgpack(compress='zlib')
        redis_conn.set('data', serialized_data)

    def output(self):
        return NewRedisTarget(
            host=RedisConfig().host,
            port=6379,
            db=0,
            update_id=f"data")


class BuildMatrix(luigi.Task):
    id = luigi.Parameter()
    cat_feats = luigi.ListParameter()
    label_column = luigi.Parameter()
    label_value = luigi.Parameter()
    drop_columns = luigi.ListParameter()
    basic_config = luigi.DictParameter()

    def run(self):
        redis_conn = redis_connect()
        df = pd.read_msgpack(redis_conn.get("data"))

        df.drop(list(self.drop_columns), axis=1, inplace=True)
        df_transformed = encoding_cat_feats(df, self.cat_feats)
        df_transformed[self.label_column] = (df_transformed[self.label_column] == self.label_value).astype(int)

        serialized_matrix = df_transformed.to_msgpack(compress='zlib')
        redis_conn.set('matrix', serialized_matrix)

    def output(self):
        return [NewRedisTarget(host=RedisConfig().host, port=6379, db=0, update_id=f"matrix")]

    def requires(self):
        return LoadData(
            id=self.id,
            basic_config=self.basic_config,
        )


class TrainTestSplit(luigi.Task):
    id = luigi.Parameter()
    cat_feats = luigi.ListParameter()
    label_column = luigi.Parameter()
    label_value = luigi.Parameter()
    drop_columns = luigi.ListParameter()
    basic_config = luigi.DictParameter()

    @property
    def k_fold(self):
        return self.basic_config['k_fold']

    def run(self):
        redis_conn = redis_connect()
        df = pd.read_msgpack(redis_conn.get("matrix"))
        cv = KFold(n_splits=self.k_fold, random_state=1, shuffle=True)
        for i, indices in enumerate(cv.split(df)):
            train_index, test_index = indices
            index_dict = {'train_index': train_index.tolist(), 'test_index': test_index.tolist()}
            redis_conn.set(f"fold_id:{i}", json.dumps(index_dict))

    def output(self):
        return [NewRedisTarget(host=RedisConfig().host, port=6379, db=0, update_id=f"fold_id:{x}") for x in range(self.k_fold)]

    def requires(self):
        return BuildMatrix(
            id=self.id,
            cat_feats=self.cat_feats,
            label_column=self.label_column,
            label_value=self.label_value,
            drop_columns=self.drop_columns,
            basic_config=self.basic_config,
        )

class Fold(luigi.WrapperTask):
    id = luigi.IntParameter()
    fold_id = luigi.Parameter()
    cat_feats = luigi.ListParameter()
    label_column = luigi.Parameter()
    label_value = luigi.Parameter()
    drop_columns = luigi.ListParameter()
    basic_config = luigi.DictParameter()

    def requires(self):
        return TrainTestSplit(
            id=self.id,
            cat_feats=self.cat_feats,
            basic_config=self.basic_config,
            label_column=self.label_column,
            label_value=self.label_value,
            drop_columns=self.drop_columns,
        )


class ModelAndEvaluate(DBCredentialMixin, luigi.Task):
    id = luigi.Parameter()
    class_path = luigi.Parameter()
    params = luigi.DictParameter()
    evaluation_config = luigi.DictParameter()
    label_column = luigi.Parameter()
    label_value = luigi.Parameter()
    fold_id = luigi.IntParameter()
    cat_feats = luigi.ListParameter()
    drop_columns = luigi.ListParameter()
    basic_config = luigi.DictParameter()

    evaluation_id_list = []

    def create_evaluation_id(self, metric, parameter):
        unique = {
            "model_uuid": self.model_uuid,
            "label_column": self.label_column,
            "metric": metric,
            "parameter": parameter,
            }
        return filename_friendly_hash(unique)

    @property
    def k_fold(self):
        return self.basic_config['k_fold']

    @property
    def db_engine(self):
        return create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")

    @property
    def model_uuid(self):
        unique = {
            "class_path": self.class_path,
            "parameters": dict(self.params),
            "fold_id": self.fold_id,
            "label_column": self.label_column,
            "k_fold": self.k_fold,
        }
        return filename_friendly_hash(unique)

    @property
    def model_group_id(self):
        unique = {
            "class_path": self.class_path,
            "parameters": dict(self.params),
            "label_column": self.label_column,
        }
        return filename_friendly_hash(unique)

    @cachedproperty
    def train_test_index_dict(self):
        redis_conn = redis_connect()
        index_dict = json.loads(redis_conn.get(f"fold_id:{self.fold_id}"))
        return index_dict

    @cachedproperty
    def matrix(self):
        redis_conn = redis_connect()
        df = pd.read_msgpack(redis_conn.get("matrix"))
        return df

    def filter_parameter(self, x):
        if '@' not in x[0] and x[1] != 'all':
            return False
        else:
            return True

    def run(self):
        df = self.matrix
        index_dict = self.train_test_index_dict

        train_index = index_dict['train_index']
        test_index = index_dict['test_index']
        train, test = df.loc[train_index], df.loc[test_index]

        train_labels = train[self.label_column]
        train_data = train.drop(self.label_column, axis=1)

        test_labels = test[self.label_column]
        test_data = test.drop(self.label_column, axis=1)

        module_name, class_name = self.class_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        instance = cls(**self.params)

        instance.fit(train_data, train_labels)

        train_base_number = sum(train_labels == 1)
        test_base_number = sum(test_labels == 1)

        ev = Evaluations(test_matrix=test, model=instance)
        result = {
                "model_uuid": self.model_uuid,
                "class_path": self.class_path,
                "parameters": dict(self.params),
                "feature_importance": ev.feature_importance.set_index('feature').to_dict(),
                "train_index": self.train_test_index_dict['train_index'],
                "test_index": self.train_test_index_dict['test_index'],
                "label_column": self.label_column,
                "train_base_number": int(train_base_number),
                "train_total_number": len(train),
                "test_base_number": int(test_base_number),
                "test_total_number": len(test),
                "model_group_id": self.model_group_id,
                "k_fold": self.k_fold,
                }

        with scoped_session(self.db_engine) as session:
            target = RowPostgresTarget(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table="results.models",
                column_name="model_uuid",
                update_id=self.model_uuid)

            if not target.exists():
                row_model = Model(
                    model_uuid=result['model_uuid'],
                    class_path=result['class_path'],
                    hyperparameters=result['parameters'],
                    feature_importance=result['feature_importance'],
                    train_index=result['train_index'],
                    label_column=result['label_column'],
                    train_base_number=result['train_base_number'],
                    train_total_number=result['train_total_number'],
                    model_group_id=result['model_group_id'],
                    k_fold=result['k_fold'],
                )
                session.add(row_model)

            metric_parameter_product = itertools.product(self.evaluation_config['metrics'], self.evaluation_config['parameters'])
            for metric, parameter in [x for x in filter(self.filter_parameter, metric_parameter_product)]:
                if parameter == 'all':
                    k = len(test)
                else:
                    k = int(parameter)

                if metric == 'precision@':
                    evaluate_value = ev.precision_at_topk(k)
                elif metric == 'recall@':
                    evaluate_value = ev.recall_at_topk(k)
                elif metric == 'roc_auc':
                    evaluate_value = ev.roc_auc_score()

                evaluation_id = self.create_evaluation_id(metric, parameter)
                target = RowPostgresTarget(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    table="results.evaluation",
                    column_name="evaluation_id",
                    update_id=evaluation_id)
                if not target.exists():
                    row_evaluation = Evaluation(
                        evaluation_id=evaluation_id,
                        model_uuid=result['model_uuid'],
                        model_group_id=result['model_group_id'],
                        label_column=result['label_column'],
                        test_base_number=result['test_base_number'],
                        test_total_number=result['test_total_number'],
                        test_index=result['test_index'],
                        metric=metric,
                        parameter=parameter,
                        value=evaluate_value,
                        k_fold=result['k_fold'],
                    )
                    self.evaluation_id_list.append(evaluation_id)
                    session.add(row_evaluation)


    def output(self):
        for evaluation_id in self.evaluation_id_list:
            yield RowPostgresTarget(
                      host=self.host,
                      database=self.database,
                      user=self.user,
                      password=self.password,
                      table="results.evaluation",
                      column_name="evaluation_id",
                      update_id=evaluation_id)
        yield RowPostgresTarget(
                  host=self.host,
                  database=self.database,
                  user=self.user,
                  password=self.password,
                  table="results.models",
                  column_name="model_uuid",
                  update_id=self.model_uuid)

    def requires(self):
        return Fold(
            id=self.id,
            fold_id=self.fold_id,
            basic_config=self.basic_config,
            cat_feats=self.cat_feats,
            label_column=self.label_column,
            label_value=self.label_value,
            drop_columns=self.drop_columns,
        )


class Experiment(luigi.WrapperTask):
    id = luigi.IntParameter(default='1')
    model_config = luigi.Parameter(description="model configuration file path")
    data_path = luigi.Parameter(default=100)

    @property
    def basic_config(self):
        return self.model_config_dict['basic_config']

    @property
    def cat_feats(self):
        return self.model_config_dict['feature_config']['cat_feats']

    @property
    def drop_columns(self):
        return self.model_config_dict['feature_config']['drop_columns']

    @property
    def model_config_dict(self):
        config = yaml.safe_load(open(self.model_config))
        return config

    @property
    def label_column(self):
        return self.model_config_dict['label_config']['label_column'][0]

    @property
    def label_value(self):
        return self.model_config_dict['label_config']['label_value'][0]

    @property
    def evaluation_config(self):
        return self.model_config_dict['evaluation_config']

    @cachedproperty
    def experiment_id(self):
        unique = str(datetime.datetime.now()).replace(' ', '-').replace('-', '_')
        return unique

    def flatten_grid(self):
        grid_config = self.model_config_dict['grid_config']
        for class_path, param_config in grid_config.items():
            for p in ParameterGrid(dict(param_config)):
                yield {'class_path': class_path, 'params': p}

    def kfold_grid(self):
        for i in range(self.basic_config['k_fold']):
            yield i

    def requires(self):
        for grid, fold_id in itertools.product(self.flatten_grid(), self.kfold_grid()):
            yield ModelAndEvaluate(
                id=self.id,
                class_path=grid['class_path'],
                params=grid['params'],
                evaluation_config=self.evaluation_config,
                label_column=self.label_column,
                label_value=self.label_value,
                fold_id=fold_id,
                basic_config=self.basic_config,
                cat_feats=self.cat_feats,
                drop_columns=self.drop_columns,
            )
