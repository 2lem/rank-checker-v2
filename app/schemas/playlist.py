from datetime import date, datetime
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


class TrackedPlaylistReorder(BaseModel):
    ordered_ids: list[str]


class RefreshPlaylistResponse(BaseModel):
    ok: bool
    job_id: str | None = None
    queued: bool | None = None
    queued_at: datetime | None = None
    ts: datetime | None = None
    refreshed_at: datetime | None = None
    playlist: TrackedPlaylistOut | None = None
    status: str | None = None


class FollowerTimeseriesEntry(BaseModel):
    date: date
    followers: int


class PlaylistInsightsDeltas(BaseModel):
    change_1d: int | None = None
    change_7d: int | None = None
    change_30d: int | None = None
    change_90d: int | None = None
    change_180d: int | None = None
    change_365d: int | None = None
    change_all_time: int | None = None


class PlaylistInsightsCompare(BaseModel):
    date_newer: date
    date_older: date
    followers_newer: int
    followers_older: int
    followers_change: int
    improved_positions: int
    declined_positions: int
    unchanged_positions: int


class PlaylistInsightsOut(BaseModel):
    playlist_id: str
    first_seen_at: datetime | None = None
    first_seen_followers: int | None = None
    current_followers: int | None = None
    follower_timeseries: list[FollowerTimeseriesEntry]
    computed_deltas: PlaylistInsightsDeltas
    daily_summary: list[dict[str, object]] = Field(default_factory=list)
    daily_compare: PlaylistInsightsCompare | None = None
    weekly_summary: list[dict[str, object]] = Field(default_factory=list)
    weekly_compare: PlaylistInsightsCompare | None = None
