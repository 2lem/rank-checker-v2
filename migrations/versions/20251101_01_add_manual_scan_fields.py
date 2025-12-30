"""add manual scan fields

Revision ID: 20251101_01
Revises: 20251015_01
Create Date: 2025-11-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251101_01"
down_revision = "20251015_01"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "basic_scans",
        "tracked_playlist_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )
    op.add_column("basic_scans", sa.Column("manual_playlist_url", sa.Text(), nullable=True))
    op.add_column("basic_scans", sa.Column("manual_playlist_id", sa.String(), nullable=True))
    op.add_column("basic_scans", sa.Column("manual_playlist_name", sa.String(), nullable=True))
    op.add_column("basic_scans", sa.Column("manual_playlist_owner", sa.String(), nullable=True))
    op.add_column("basic_scans", sa.Column("manual_playlist_image_url", sa.String(), nullable=True))
    op.add_column(
        "basic_scans",
        sa.Column(
            "manual_target_countries",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )
    op.add_column(
        "basic_scans",
        sa.Column(
            "manual_target_keywords",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column("basic_scans", "manual_target_keywords")
    op.drop_column("basic_scans", "manual_target_countries")
    op.drop_column("basic_scans", "manual_playlist_image_url")
    op.drop_column("basic_scans", "manual_playlist_owner")
    op.drop_column("basic_scans", "manual_playlist_name")
    op.drop_column("basic_scans", "manual_playlist_id")
    op.drop_column("basic_scans", "manual_playlist_url")
    op.alter_column(
        "basic_scans",
        "tracked_playlist_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )
