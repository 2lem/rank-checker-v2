"""add basic scans and accounts

Revision ID: 20251012_01
Revises: 20251010_01
Create Date: 2025-10-12 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20251012_01"
down_revision = "20251010_01"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "accounts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.add_column("tracked_playlists", sa.Column("account_id", postgresql.UUID(as_uuid=True)))
    op.create_foreign_key(
        "tracked_playlists_account_id_fkey",
        "tracked_playlists",
        "accounts",
        ["account_id"],
        ["id"],
    )

    op.create_table(
        "basic_scans",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("account_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "tracked_playlist_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "scanned_countries",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "scanned_keywords",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("follower_snapshot", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"]),
        sa.ForeignKeyConstraint(
            ["tracked_playlist_id"],
            ["tracked_playlists.id"],
            ondelete="CASCADE",
        ),
    )

    op.create_table(
        "basic_scan_queries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "basic_scan_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("country_code", sa.String(), nullable=False),
        sa.Column("keyword", sa.String(), nullable=False),
        sa.Column("searched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("tracked_rank", sa.Integer(), nullable=True),
        sa.Column(
            "tracked_found_in_top20",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["basic_scan_id"],
            ["basic_scans.id"],
            ondelete="CASCADE",
        ),
    )

    op.create_table(
        "basic_scan_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "basic_scan_query_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("playlist_id", sa.String(), nullable=True),
        sa.Column("playlist_name", sa.String(), nullable=True),
        sa.Column("playlist_owner", sa.String(), nullable=True),
        sa.Column("playlist_followers", sa.Integer(), nullable=True),
        sa.Column("songs_count", sa.Integer(), nullable=True),
        sa.Column("playlist_last_added_track_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("playlist_description", sa.Text(), nullable=True),
        sa.Column("playlist_url", sa.String(), nullable=True),
        sa.Column(
            "is_tracked_playlist",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["basic_scan_query_id"],
            ["basic_scan_queries.id"],
            ondelete="CASCADE",
        ),
    )

    op.create_index(
        "idx_basic_scan_queries_scan",
        "basic_scan_queries",
        ["basic_scan_id"],
    )
    op.create_index(
        "idx_basic_scan_results_query",
        "basic_scan_results",
        ["basic_scan_query_id"],
    )


def downgrade():
    op.drop_index("idx_basic_scan_results_query", table_name="basic_scan_results")
    op.drop_index("idx_basic_scan_queries_scan", table_name="basic_scan_queries")
    op.drop_table("basic_scan_results")
    op.drop_table("basic_scan_queries")
    op.drop_table("basic_scans")
    op.drop_constraint("tracked_playlists_account_id_fkey", "tracked_playlists", type_="foreignkey")
    op.drop_column("tracked_playlists", "account_id")
    op.drop_table("accounts")
