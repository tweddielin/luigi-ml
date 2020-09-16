import datetime

from sqlalchemy import (
    Column,
    BigInteger,
    Boolean,
    Integer,
    String,
    Numeric,
    DateTime,
    JSON,
    ForeignKey,
)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import ARRAY, Enum
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()


class Model(Base):
    __tablename__ = "models"
    __table_args__ = {"schema": "results"}

    model_uuid = Column(String, primary_key=True)
    class_path = Column(String)
    hyperparameters = Column(JSON)
    feature_importance = Column(JSON)
    train_index = Column(ARRAY(Integer))
    train_base_number = Column(Integer)
    train_total_number = Column(Integer)
    label_column = Column(String)
    model_group_id = Column(String)
    k_fold = Column(Integer)

class Evaluation(Base):
    __tablename__ = "evaluation"
    __table_args__ = {"schema": "results"}

    evaluation_id = Column(String, primary_key=True)
    model_uuid = Column(String, ForeignKey("results.models.model_uuid"))
    model_group_id = Column(String)
    test_base_number = Column(Integer)
    test_total_number = Column(Integer)
    test_index = Column(ARRAY(Integer))
    label_column = Column(String)
    metric = Column(String)
    parameter = Column(String)
    value = Column(Numeric)
    k_fold = Column(Integer)

    model_rel = relationship("Model")
