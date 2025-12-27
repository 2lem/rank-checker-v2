"""create tracked playlists

Revision ID: 20240925_01
Revises: 
Create Date: 2024-09-25 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20240925_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "tracked_playlists",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("playlist_id", sa.String(), nullable=False),
        sa.Column("playlist_url", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column(
            "target_countries",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "target_keywords",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("playlist_id", name="uq_tracked_playlists_playlist_id"),
    )


def downgrade():
    op.drop_table("tracked_playlists")
