"""add basic scan cancellation fields

Revision ID: 20260115_01
Revises: 20251231_01
Create Date: 2026-01-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260115_01"
down_revision = "20251231_01"
branch_labels = None
depends_on = None


NEW_COLUMNS = [
    sa.Column("last_event_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("error_reason", sa.String(), nullable=True),
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    ),
]


def upgrade():
    for column in NEW_COLUMNS:
        op.add_column("basic_scans", column)


def downgrade():
    for column in reversed(NEW_COLUMNS):
        op.drop_column("basic_scans", column.name)
