import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class TrackedPlaylist(Base):
    __tablename__ = "tracked_playlists"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    playlist_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    playlist_url: Mapped[str | None] = mapped_column(String, nullable=True)
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    cover_image_url_small: Mapped[str | None] = mapped_column(String, nullable=True)
    owner_name: Mapped[str | None] = mapped_column(String, nullable=True)
    followers_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tracks_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_meta_scan_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    playlist_last_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    target_countries: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(JSONB),
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    target_keywords: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(JSONB),
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
