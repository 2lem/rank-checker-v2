"""add playlist insights tables

Revision ID: 20260201_01
Revises: 20260115_01
Create Date: 2026-02-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260201_01"
down_revision = "20260115_01"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "playlists",
        sa.Column("playlist_id", sa.String(), primary_key=True, nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_seen_source", sa.String(), nullable=False),
        sa.Column("first_seen_followers", sa.Integer(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_followers", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_table(
        "playlist_follower_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("playlist_id", sa.String(), nullable=False),
        sa.Column("snapshot_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("followers", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["playlist_id"],
            ["playlists.playlist_id"],
            ondelete="CASCADE",
            name="fk_playlist_follower_snapshots_playlist_id",
        ),
        sa.UniqueConstraint(
            "playlist_id",
            "snapshot_date",
            name="uq_playlist_follower_snapshots_playlist_date",
        ),
    )


def downgrade():
    op.drop_table("playlist_follower_snapshots")
    op.drop_table("playlists")
