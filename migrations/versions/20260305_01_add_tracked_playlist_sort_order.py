"""add tracked playlist sort order

Revision ID: 20260305_01
Revises: 20260201_01
Create Date: 2026-03-05 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20260305_01"
down_revision = "20260201_01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tracked_playlists",
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
    )
    op.execute(
        """
        WITH ordered AS (
            SELECT id,
                   ROW_NUMBER() OVER (ORDER BY created_at ASC, id ASC) - 1 AS sort_order
            FROM tracked_playlists
        )
        UPDATE tracked_playlists
        SET sort_order = ordered.sort_order
        FROM ordered
        WHERE tracked_playlists.id = ordered.id
        """
    )


def downgrade():
    op.drop_column("tracked_playlists", "sort_order")
