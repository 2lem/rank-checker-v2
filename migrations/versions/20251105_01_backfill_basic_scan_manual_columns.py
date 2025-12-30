"""backfill basic scan manual columns

Revision ID: 20251105_01
Revises: 20251102_01
Create Date: 2025-11-05 00:00:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "20251105_01"
down_revision = "20251102_01"
branch_labels = None
depends_on = None


TEXT_COLUMNS = [
    "manual_playlist_url",
    "manual_playlist_id",
    "manual_playlist_name",
    "manual_playlist_owner",
    "manual_playlist_image_url",
]

JSON_COLUMNS = [
    "manual_target_countries",
    "manual_target_keywords",
]


def upgrade():
    for column in TEXT_COLUMNS:
        op.execute(
            f"ALTER TABLE basic_scans ADD COLUMN IF NOT EXISTS {column} TEXT"
        )

    for column in JSON_COLUMNS:
        op.execute(
            "ALTER TABLE basic_scans "
            f"ADD COLUMN IF NOT EXISTS {column} JSONB DEFAULT '[]'::jsonb"
        )
        op.execute(
            "ALTER TABLE basic_scans "
            f"ALTER COLUMN {column} SET DEFAULT '[]'::jsonb"
        )
        op.execute(
            f"UPDATE basic_scans SET {column} = '[]'::jsonb WHERE {column} IS NULL"
        )


def downgrade():
    for column in JSON_COLUMNS:
        op.execute(
            "ALTER TABLE basic_scans "
            f"ALTER COLUMN {column} DROP DEFAULT"
        )
