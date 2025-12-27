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
    target_countries: list[str]
    target_keywords: list[str]
    created_at: datetime

    class Config:
        from_attributes = True
