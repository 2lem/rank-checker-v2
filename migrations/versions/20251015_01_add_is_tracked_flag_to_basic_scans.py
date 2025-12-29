"""add is_tracked_playlist flag to basic_scans

Revision ID: 20251015_01
Revises: 20251012_01
Create Date: 2025-10-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251015_01"
down_revision = "20251012_01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "basic_scans",
        sa.Column(
            "is_tracked_playlist",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.execute(
        sa.text(
            "UPDATE basic_scans SET is_tracked_playlist = tracked_playlist_id IS NOT NULL"
        )
    )


def downgrade():
    op.drop_column("basic_scans", "is_tracked_playlist")
