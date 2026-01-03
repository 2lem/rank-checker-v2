import uuid
from datetime import datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Playlist(Base):
    __tablename__ = "playlists"

    playlist_id: Mapped[str] = mapped_column(String, primary_key=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_seen_source: Mapped[str] = mapped_column(String, nullable=False)
    first_seen_followers: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    current_followers: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class PlaylistFollowerSnapshot(Base):
    __tablename__ = "playlist_follower_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "playlist_id",
            "snapshot_date",
            name="uq_playlist_follower_snapshots_playlist_date",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    playlist_id: Mapped[str] = mapped_column(
        String, ForeignKey("playlists.playlist_id", ondelete="CASCADE"), nullable=False
    )
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    snapshot_date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    followers: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
