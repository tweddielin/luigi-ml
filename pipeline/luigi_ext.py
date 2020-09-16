import luigi
import yaml
import json
import os
import re
import hashlib
import inspect
import time
from pathlib import Path
import psycopg2
import psycopg2.errorcodes

class ConfigParameter(luigi.Parameter):
    def parse(self, x):
        with open(x) as f:
            config = yaml.load(f)
        return config

    def serialize(self, x):
        return json.dumps(x)

    def normalize(self, x):
        if not isinstance(x, dict):
            try:
                return self.parse(x)
            except:
                return x
        return x


class ForcibleTask(luigi.Task):

    force = luigi.BoolParameter(significant=False, default=False)
    force_upstream = luigi.BoolParameter(significant=False, default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # To force execution, we just remove all outputs before `complete()` is called
        if self.force_upstream:
            self.force = True

        if self.force:
            print(f"force {self.__class__}")
            done = False
            tasks = [self]
            while not done:
                outputs = luigi.task.flatten(tasks[0].output())
                [os.remove(out.path) for out in outputs if out.exists()]
                if self.force_upstream is True:
                    tasks += luigi.task.flatten(tasks[0].requires())
                tasks.pop(0)

                if len(tasks) == 0:
                    done = True

    @property
    def get_task_path(self):
        return "_".join(re.sub(r'([A-Z])', r' \1', self.__class__.__name__).split()).lower()

    def complete(self):
        def to_list(obj):
            if type(obj) in (type(()), type([])):
                return obj
            else:
                return [obj]

        def mtime(path):
            return os.path.getmtime(path)

        if not all(os.path.exists(out.path) for out in luigi.task.flatten(self.output())):
            return False

        self_task_mtime = mtime(inspect.getfile(self.__class__))

        for el in luigi.task.flatten(self.requires()):
            if not el.complete():
                return False

            if mtime(inspect.getfile(el.__class__)) > self_task_mtime:
                [os.remove(out.path) for out in luigi.task.flatten(el.output())]
                return False
        return True


class RedisConfig(luigi.Config):
    host = luigi.Parameter(default="localhost")
    port = luigi.Parameter(default=6379)


class DatabaseConfig(luigi.Config):
    host = luigi.Parameter(default='')
    database = luigi.Parameter(default='')
    user = luigi.Parameter(default='')
    password = luigi.Parameter(default='')
    port = luigi.Parameter(default='')


class DBCredentialMixin(object):
    host = DatabaseConfig().host
    database = DatabaseConfig().database
    user = DatabaseConfig().user
    password = DatabaseConfig().password
    port = DatabaseConfig().port

    force = luigi.BoolParameter(significant=False, default=False)
    force_upstream = luigi.BoolParameter(significant=False, default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # To force execution, we just remove all outputs before `complete()` is called
        if self.force_upstream:
            self.force = True

        if self.force:
            print(f"force {self.__class__}")
            done = False
            tasks = [self]
            while not done:
                outputs = luigi.task.flatten(tasks[0].output())
                [out.remove() for out in outputs if out.exists()]
                if self.force_upstream:
                    tasks += luigi.task.flatten(tasks[0].requires())
                tasks.pop(0)
                if len(tasks) == 0:
                    done = True


class RowPostgresTarget(luigi.Target):

    DEFAULT_DB_PORT = 5432
    use_db_timestamps = True

    def __init__(self, host, database, user, password, table, column_name, update_id, port=None):
        if ':' in host:
            self.host, self.port = host.split(':')
        else:
            self.host = host
            self.port = port or self.DEFAULT_DB_PORT
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id
        self.column_name = column_name

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True

        cursor = connection.cursor()
        try:
            cursor.execute(f"""SELECT * FROM {self.table} WHERE {self.column_name}='{self.update_id}'""")
            row = cursor.fetchall()

        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                row = None
                raise Exception(f"table {self.table} doesn't exist. Please make sure the alembic has run before.")
            else:
                raise e
        return len(row) > 0

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password)
        connection.set_client_encoding('utf-8')
        return connection
