"""add basic scan progress fields

Revision ID: 20251231_01
Revises: 20251106_01
Create Date: 2025-12-31 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251231_01"
down_revision = "20251106_01"
branch_labels = None
depends_on = None


PROGRESS_COLUMNS = [
    sa.Column("progress_completed_units", sa.Integer(), nullable=True),
    sa.Column("progress_total_units", sa.Integer(), nullable=True),
    sa.Column("progress_pct", sa.Integer(), nullable=True),
    sa.Column("eta_ms", sa.Integer(), nullable=True),
    sa.Column("eta_human", sa.String(), nullable=True),
    sa.Column("last_progress_at", sa.DateTime(timezone=True), nullable=True),
]


def upgrade():
    for column in PROGRESS_COLUMNS:
        op.add_column("basic_scans", column)


def downgrade():
    for column in reversed(PROGRESS_COLUMNS):
        op.drop_column("basic_scans", column.name)
