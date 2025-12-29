import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class BasicScan(Base):
    __tablename__ = "basic_scans"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    account_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=True
    )
    tracked_playlist_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tracked_playlists.id", ondelete="CASCADE"),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    scanned_countries: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    scanned_keywords: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    follower_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_tracked_playlist: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    queries: Mapped[list["BasicScanQuery"]] = relationship(
        "BasicScanQuery", back_populates="scan", cascade="all, delete-orphan"
    )


class BasicScanQuery(Base):
    __tablename__ = "basic_scan_queries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    basic_scan_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("basic_scans.id", ondelete="CASCADE"),
        nullable=False,
    )
    country_code: Mapped[str] = mapped_column(String, nullable=False)
    keyword: Mapped[str] = mapped_column(String, nullable=False)
    searched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    tracked_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tracked_found_in_top20: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    scan: Mapped["BasicScan"] = relationship("BasicScan", back_populates="queries")
    results: Mapped[list["BasicScanResult"]] = relationship(
        "BasicScanResult", back_populates="query", cascade="all, delete-orphan"
    )


class BasicScanResult(Base):
    __tablename__ = "basic_scan_results"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    basic_scan_query_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("basic_scan_queries.id", ondelete="CASCADE"),
        nullable=False,
    )
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    playlist_id: Mapped[str | None] = mapped_column(String, nullable=True)
    playlist_name: Mapped[str | None] = mapped_column(String, nullable=True)
    playlist_owner: Mapped[str | None] = mapped_column(String, nullable=True)
    playlist_followers: Mapped[int | None] = mapped_column(Integer, nullable=True)
    songs_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    playlist_last_added_track_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    playlist_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    playlist_url: Mapped[str | None] = mapped_column(String, nullable=True)
    is_tracked_playlist: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    query: Mapped["BasicScanQuery"] = relationship("BasicScanQuery", back_populates="results")
