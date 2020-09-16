import itertools
import json
import time
import importlib
import yaml

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


# def redis_connect(host=RedisConfig().host, port=RedisConfig().port, db=0):
#     return redis.Redis(host=host, port=port, db=db)


class RedisCleanUp(luigi.Task):
    id = luigi.IntParameter()
    pattern = luigi.Parameter(default="*")

    def run(self):
        r = redis_connect()
        for key in r.scan_iter(self.pattern):
            r.delete(key)


class LoadData(luigi.Task):
    id = luigi.IntParameter()

    def run(self):
        df = pd.read_csv('placement_data_full_class.csv')
        redis_conn = redis_connect()
        serialized_data = df.to_msgpack(compress='zlib')
        redis_conn.set('data', serialized_data)

    def output(self):
        return [NewRedisTarget(host=RedisConfig().host, port=6379, db=0, update_id=f"data")]


class BuildMatrix(luigi.Task):
    id = luigi.IntParameter()

    def run(self):
        redis_conn = redis_connect()
        df = pd.read_msgpack(redis_conn.get("data"))
        df.drop('salary', axis=1, inplace=True)
        cat_feats = [
            'gender',
            'ssc_b',
            'hsc_b',
            'hsc_s',
            'degree_t',
            'workex',
            'specialisation'
        ]
        df_transformed = encoding_cat_feats(df, cat_feats)
        df_transformed['status'] = (df_transformed['status'] == 'Placed').astype(int)

        serialized_matrix = df_transformed.to_msgpack(compress='zlib')
        redis_conn.set('matrix', serialized_matrix)

    def output(self):
        return [NewRedisTarget(host=RedisConfig().host, port=6379, db=0, update_id=f"matrix")]

    def requires(self):
        return LoadData(
            id=self.id,
        )


class TrainTestSplit(luigi.Task):
    id = luigi.IntParameter()
    k_fold = luigi.IntParameter()

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
        return [BuildMatrix(id=self.id)]


class TrainTestModel(DBCredentialMixin, luigi.Task):
    id = luigi.IntParameter()
    class_path = luigi.Parameter()
    params = luigi.DictParameter()
    evaluation_config = luigi.DictParameter()
    label_column = luigi.Parameter()
    fold_id = luigi.IntParameter()
    k_fold = luigi.IntParameter()

    @property
    def evaluation_id_list(self):
        evaluation_id_list = []
        for metric, parameter in itertools.product(self.evaluation_config['metrics'], self.evaluation_config['parameters']):
            evaluation_id_list.append(self.create_evaluation_id(metric, parameter))
        return evaluation_id_list

    def create_evaluation_id(self, metric, parameter):
        unique = {
            "model_uuid": self.model_uuid,
            "label_column": self.label_column,
            "metric": metric,
            "parameter": parameter,
            }
        return filename_friendly_hash(unique)

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

            for metric, parameter in itertools.product(self.evaluation_config['metrics'], self.evaluation_config['parameters']):
                if metric == 'precision':
                    evaluate_fn = ev.precision_at_top
                elif metric == 'recall':
                    evaluate_fn = ev.recall_at_top

                if parameter == 'all':
                    evaluate_value = evaluate_fn(len(test))
                else:
                    evaluate_value = evaluate_fn(int(parameter))

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
                        metric=metric+'@',
                        parameter=parameter,
                        value=evaluate_value,
                        k_fold=result['k_fold'],
                    )
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
        return TrainTestSplit(
            id=self.id,
            k_fold=self.k_fold,
        )


class CreateKFolds(luigi.WrapperTask):
    id = luigi.IntParameter()
    model_config = luigi.DictParameter(visibility=ParameterVisibility.HIDDEN)
    fold_id = luigi.IntParameter()
    k_fold = luigi.IntParameter()

    @property
    def label_column(self):
        return self.model_config['label_config']['label_column'][0]

    @property
    def evaluation_config(self):
        return self.model_config['evaluation_config']

    def flatten_grid(self):
        grid_config = self.model_config['grid_config']
        for class_path, param_config in grid_config.items():
            for p in ParameterGrid(dict(param_config)):
                yield {'class_path': class_path, 'params': p}

    def requires(self):
        for grid in self.flatten_grid():
            yield TrainTestModel(
                id=self.id,
                class_path=grid['class_path'],
                params=grid['params'],
                evaluation_config=self.evaluation_config,
                label_column=self.label_column,
                fold_id=self.fold_id,
                k_fold=self.k_fold,
            )


class Experiment(luigi.WrapperTask):
    id = luigi.IntParameter()
    model_config = luigi.Parameter(description="model configuration file path")
    k_fold = luigi.IntParameter()

    @property
    def model_config_dict(self):
        config = yaml.safe_load(open(self.model_config))
        return config

    def requires(self):
        for i in range(self.k_fold):
            yield CreateKFolds(
                id=self.id,
                fold_id=i,
                model_config=self.model_config_dict,
                k_fold=self.k_fold,
            )

