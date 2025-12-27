"""add tracked playlist refresh fields

Revision ID: 20251010_01
Revises: 20251002_01
Create Date: 2025-10-10 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251010_01"
down_revision = "20251002_01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("tracked_playlists", sa.Column("description", sa.Text(), nullable=True))
    op.add_column("tracked_playlists", sa.Column("cover_image_url_large", sa.String(), nullable=True))
    op.add_column(
        "tracked_playlists", sa.Column("last_meta_refresh_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade():
    op.drop_column("tracked_playlists", "last_meta_refresh_at")
    op.drop_column("tracked_playlists", "cover_image_url_large")
    op.drop_column("tracked_playlists", "description")
