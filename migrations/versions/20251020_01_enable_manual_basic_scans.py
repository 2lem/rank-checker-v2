"""enable manual basic scans

Revision ID: 20251020_01
Revises: 20251015_01
Create Date: 2025-10-20 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251020_01"
down_revision = "20251015_01"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("basic_scans", sa.Column("playlist_id", sa.String(), nullable=True))
    op.alter_column(
        "basic_scans",
        "tracked_playlist_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )

    op.execute(
        sa.text(
            """
            UPDATE basic_scans AS bs
            SET playlist_id = tp.playlist_id
            FROM tracked_playlists AS tp
            WHERE bs.tracked_playlist_id = tp.id
              AND bs.playlist_id IS NULL
            """
        )
    )


def downgrade():
    op.alter_column(
        "basic_scans",
        "tracked_playlist_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )
    op.drop_column("basic_scans", "playlist_id")
