"""create results schema

Revision ID: 2e7dac655911
Revises:
Create Date: 2020-09-15 21:06:39.157407

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2e7dac655911'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE SCHEMA IF NOT EXISTS results")


def downgrade():
    op.execute("DROP SCHEMA results")
