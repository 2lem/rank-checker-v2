from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class TrackedPlaylistCreate(BaseModel):
    playlist_url: str = Field(..., examples=["https://open.spotify.com/playlist/abc123"])
    target_countries: list[str] | None = None
    target_keywords: list[str] | None = None


class TrackedPlaylistOut(BaseModel):
    id: UUID
    playlist_id: str
    playlist_url: str | None
    name: str | None
    description: str | None = None
    cover_image_url_small: str | None = None
    cover_image_url_large: str | None = None
    owner_name: str | None = None
    followers_total: int | None = None
    tracks_count: int | None = None
    last_meta_scan_at: datetime | None = None
    last_meta_refresh_at: datetime | None = None
    playlist_last_updated_at: datetime | None = None
    stats_updated_at: datetime | None = None
    target_countries: list[str]
    target_keywords: list[str]
    created_at: datetime

    class Config:
        from_attributes = True


class TrackedPlaylistTargetsUpdate(BaseModel):
    target_countries: list[str] | None = None
    target_keywords: list[str] | None = None


class RefreshPlaylistResponse(BaseModel):
    ok: bool
    refreshed_at: datetime
    playlist: TrackedPlaylistOut
