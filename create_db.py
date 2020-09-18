from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import event, DDL

from alembic.config import Config
from alembic import command

from pipeline.results import Base
from pipeline.luigi_ext import DatabaseConfig


event.listen(Base.metadata, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS results"))

url = None

if not url:
    url = URL(
        "postgres",
        host=DatabaseConfig().host,
        username=DatabaseConfig().user,
        database=DatabaseConfig().database,
        password=DatabaseConfig().password,
        port=DatabaseConfig().port
    )

engine = create_engine(url, echo=True)
Base.metadata.create_all(engine)

alembic_cfg = Config("alembic.ini")
command.stamp(alembic_cfg, "head")


