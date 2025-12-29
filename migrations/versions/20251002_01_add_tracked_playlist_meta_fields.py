"""add tracked playlist meta fields

Revision ID: 20251002_01
Revises: 20240925_01
Create Date: 2025-10-02 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251002_01"
down_revision = "20240925_01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("tracked_playlists", sa.Column("cover_image_url_small", sa.String(), nullable=True))
    op.add_column("tracked_playlists", sa.Column("owner_name", sa.String(), nullable=True))
    op.add_column("tracked_playlists", sa.Column("followers_total", sa.Integer(), nullable=True))
    op.add_column("tracked_playlists", sa.Column("tracks_count", sa.Integer(), nullable=True))
    op.add_column("tracked_playlists", sa.Column("last_meta_scan_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "tracked_playlists", sa.Column("playlist_last_updated_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade():
    op.drop_column("tracked_playlists", "playlist_last_updated_at")
    op.drop_column("tracked_playlists", "last_meta_scan_at")
    op.drop_column("tracked_playlists", "tracks_count")
    op.drop_column("tracked_playlists", "followers_total")
    op.drop_column("tracked_playlists", "owner_name")
    op.drop_column("tracked_playlists", "cover_image_url_small")
