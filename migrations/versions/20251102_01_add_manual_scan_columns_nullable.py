"""add manual scan columns nullable

Revision ID: 20251102_01
Revises: 20251101_01
Create Date: 2025-11-02 00:00:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "20251102_01"
down_revision = "20251101_01"
branch_labels = None
depends_on = None


COLUMNS = [
    ("manual_playlist_url", "TEXT"),
    ("manual_playlist_id", "TEXT"),
    ("manual_playlist_name", "TEXT"),
    ("manual_playlist_owner", "TEXT"),
    ("manual_playlist_image_url", "TEXT"),
    ("manual_target_countries", "JSONB"),
    ("manual_target_keywords", "JSONB"),
]


def upgrade():
    for column, column_type in COLUMNS:
        op.execute(
            f"ALTER TABLE basic_scans ADD COLUMN IF NOT EXISTS {column} {column_type}"
        )


def downgrade():
    for column, _column_type in COLUMNS:
        op.execute(f"ALTER TABLE basic_scans DROP COLUMN IF EXISTS {column}")
