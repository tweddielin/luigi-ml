import datetime
import hashlib
import json
import redis
import pandas as pd

from descriptors import cachedproperty
from contextlib import contextmanager
from sqlalchemy.orm import Session
from sklearn.preprocessing import LabelEncoder
from pipeline.luigi_ext import RedisConfig

@contextmanager
def scoped_session(db_engine):
    """Provide a transactional scope around a series of operations."""
    session = Session(bind=db_engine)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def filename_friendly_hash(inputs):
    def dt_handler(x):
        if isinstance(x, datetime.datetime) or isinstance(x, datetime.date):
            return x.isoformat()
        raise TypeError("Unknown type")

    return hashlib.md5(
        json.dumps(inputs, default=dt_handler, sort_keys=True).encode("utf-8")
    ).hexdigest()


def encoding_cat_feats(df, list_of_cat):
    df = df.copy()
    encoder = LabelEncoder()
    for cat_feat in list_of_cat:
        df[cat_feat] = encoder.fit_transform(df[cat_feat])
    return df


def redis_connect(host=RedisConfig().host, port=RedisConfig().port, db=0):
    return redis.Redis(host=host, port=port, db=db)


