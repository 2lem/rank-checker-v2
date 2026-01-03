from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.playlist import Playlist, PlaylistFollowerSnapshot


def _ensure_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def upsert_playlist_seen_and_snapshot(
    db: Session,
    *,
    playlist_id: str,
    followers: int | None,
    seen_at: datetime | None,
    source: str,
) -> Playlist | None:
    if not playlist_id:
        return None

    seen_at_utc = _ensure_utc(seen_at)
    playlist = db.get(Playlist, playlist_id)

    if playlist is None:
        playlist = Playlist(
            playlist_id=playlist_id,
            first_seen_at=seen_at_utc,
            first_seen_source=source,
            first_seen_followers=followers,
            last_seen_at=seen_at_utc,
            current_followers=followers,
        )
        db.add(playlist)
    else:
        playlist.last_seen_at = seen_at_utc
        if followers is not None:
            playlist.current_followers = followers
        db.add(playlist)

    if followers is None:
        return playlist

    snapshot_date = seen_at_utc.date()
    snapshot = db.execute(
        select(PlaylistFollowerSnapshot).where(
            PlaylistFollowerSnapshot.playlist_id == playlist_id,
            PlaylistFollowerSnapshot.snapshot_date == snapshot_date,
        )
    ).scalar_one_or_none()

    if snapshot:
        if snapshot.snapshot_at is None or seen_at_utc > snapshot.snapshot_at:
            snapshot.snapshot_at = seen_at_utc
            snapshot.followers = followers
            snapshot.source = source
            db.add(snapshot)
    else:
        db.add(
            PlaylistFollowerSnapshot(
                playlist_id=playlist_id,
                snapshot_at=seen_at_utc,
                snapshot_date=snapshot_date,
                followers=followers,
                source=source,
            )
        )

    return playlist


def _find_snapshot_on_or_before(
    snapshots: list[PlaylistFollowerSnapshot],
    target_date: date,
) -> PlaylistFollowerSnapshot | None:
    for snapshot in reversed(snapshots):
        if snapshot.snapshot_date <= target_date:
            return snapshot
    return None


def build_playlist_insights(db: Session, playlist_id: str) -> dict:
    playlist = db.get(Playlist, playlist_id)
    snapshots = (
        db.execute(
            select(PlaylistFollowerSnapshot)
            .where(PlaylistFollowerSnapshot.playlist_id == playlist_id)
            .order_by(PlaylistFollowerSnapshot.snapshot_date)
        )
        .scalars()
        .all()
    )

    timeseries = [
        {"date": snapshot.snapshot_date, "followers": snapshot.followers}
        for snapshot in snapshots
    ]

    latest_snapshot = snapshots[-1] if snapshots else None
    current_followers = (
        playlist.current_followers
        if playlist and playlist.current_followers is not None
        else latest_snapshot.followers if latest_snapshot else None
    )

    deltas: dict[str, int | None] = {
        "change_1d": None,
        "change_7d": None,
        "change_30d": None,
        "change_90d": None,
        "change_180d": None,
        "change_365d": None,
        "change_all_time": None,
    }

    if latest_snapshot and current_followers is not None:
        baseline_dates = {
            "change_1d": 1,
            "change_7d": 7,
            "change_30d": 30,
            "change_90d": 90,
            "change_180d": 180,
            "change_365d": 365,
        }
        for key, days in baseline_dates.items():
            target_date = latest_snapshot.snapshot_date - timedelta(days=days)
            baseline = _find_snapshot_on_or_before(snapshots, target_date)
            if baseline:
                deltas[key] = current_followers - baseline.followers
        if snapshots:
            deltas["change_all_time"] = current_followers - snapshots[0].followers

    return {
        "playlist_id": playlist_id,
        "first_seen_at": playlist.first_seen_at if playlist else None,
        "first_seen_followers": playlist.first_seen_followers if playlist else None,
        "current_followers": current_followers,
        "follower_timeseries": timeseries,
        "computed_deltas": deltas,
    }
