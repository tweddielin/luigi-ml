import datetime
import hashlib
import json
import pandas as pd
from descriptors import cachedproperty
from contextlib import contextmanager
from sqlalchemy.orm import Session

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
