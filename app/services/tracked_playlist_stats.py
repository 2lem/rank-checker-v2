from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.playlist import Playlist, PlaylistFollowerSnapshot
from app.models.tracked_playlist import TrackedPlaylist


@dataclass(frozen=True)
class ResolvedPlaylistStats:
    followers_total: int | None
    stats_updated_at: datetime | None


def _coerce_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def resolve_latest_playlist_stats(
    db: Session,
    tracked: TrackedPlaylist,
) -> ResolvedPlaylistStats:
    playlist = db.get(Playlist, tracked.playlist_id) if tracked.playlist_id else None
    latest_snapshot = (
        db.execute(
            select(PlaylistFollowerSnapshot)
            .where(PlaylistFollowerSnapshot.playlist_id == tracked.playlist_id)
            .order_by(PlaylistFollowerSnapshot.snapshot_at.desc())
            .limit(1)
        )
        .scalar_one_or_none()
        if tracked.playlist_id
        else None
    )

    candidates: list[tuple[datetime | None, int | None]] = []
    if latest_snapshot:
        candidates.append((latest_snapshot.snapshot_at, latest_snapshot.followers))
    if playlist and playlist.last_seen_at:
        candidates.append((playlist.last_seen_at, playlist.current_followers))
    if tracked.last_meta_refresh_at:
        candidates.append((tracked.last_meta_refresh_at, tracked.followers_total))
    if tracked.last_meta_scan_at:
        candidates.append((tracked.last_meta_scan_at, tracked.followers_total))

    resolved_at: datetime | None = None
    resolved_followers: int | None = tracked.followers_total

    if candidates:
        normalized = [
            (_coerce_utc(timestamp), followers) for timestamp, followers in candidates
        ]
        resolved_at, resolved_followers = max(
            normalized, key=lambda item: item[0] or datetime.min.replace(tzinfo=timezone.utc)
        )

    return ResolvedPlaylistStats(
        followers_total=resolved_followers,
        stats_updated_at=resolved_at,
    )
