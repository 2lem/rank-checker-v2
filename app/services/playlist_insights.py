from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.basic_scan import BasicScan, BasicScanQuery
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


def _scan_timestamp(scan: BasicScan) -> datetime:
    return _ensure_utc(scan.finished_at or scan.created_at or scan.started_at)


def backfill_playlist_follower_snapshots_from_dedicated_scans(
    db: Session,
    playlist_id: str,
    tracked_playlist_id: str,
    *,
    days_back: int = 90,
) -> int:
    if not playlist_id or not tracked_playlist_id:
        return 0

    cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=days_back - 1)
    scans = (
        db.execute(
            select(BasicScan).where(
                BasicScan.tracked_playlist_id == tracked_playlist_id,
                BasicScan.status == "completed",
                BasicScan.follower_snapshot.isnot(None),
            )
        )
        .scalars()
        .all()
    )
    if not scans:
        return 0

    daily_scans: dict[date, dict[str, object]] = {}
    for scan in scans:
        timestamp = _scan_timestamp(scan)
        scan_date = timestamp.date()
        if scan_date < cutoff_date:
            continue
        existing = daily_scans.get(scan_date)
        if existing is None or timestamp > existing["timestamp"]:
            daily_scans[scan_date] = {
                "timestamp": timestamp,
                "followers": scan.follower_snapshot,
            }

    if not daily_scans:
        return 0

    existing_snapshots = (
        db.execute(
            select(PlaylistFollowerSnapshot).where(
                PlaylistFollowerSnapshot.playlist_id == playlist_id
            )
        )
        .scalars()
        .all()
    )
    existing_by_date = {
        snapshot.snapshot_date: snapshot for snapshot in existing_snapshots
    }

    updated = 0
    for snapshot_date, payload in daily_scans.items():
        followers = payload["followers"]
        if followers is None:
            continue
        snapshot_at = payload["timestamp"]
        snapshot = existing_by_date.get(snapshot_date)
        if snapshot:
            if snapshot.snapshot_at is None or snapshot_at > snapshot.snapshot_at:
                snapshot.snapshot_at = snapshot_at
                snapshot.followers = followers
                snapshot.source = "dedicated_scan_backfill"
                db.add(snapshot)
                updated += 1
            continue
        db.add(
            PlaylistFollowerSnapshot(
                playlist_id=playlist_id,
                snapshot_at=snapshot_at,
                snapshot_date=snapshot_date,
                followers=followers,
                source="dedicated_scan_backfill",
            )
        )
        updated += 1

    if updated:
        db.commit()
    return updated


@dataclass(frozen=True)
class DailyScanRep:
    date: date
    scan_id: object
    follower_snapshot: int | None
    rank_map: dict[tuple[str, str], int | None]


def _fetch_scan_rank_maps(
    db: Session,
    scan_ids: list[object],
) -> dict[object, dict[tuple[str, str], int | None]]:
    if not scan_ids:
        return {}
    rows = db.execute(
        select(
            BasicScanQuery.basic_scan_id,
            BasicScanQuery.country_code,
            BasicScanQuery.keyword,
            BasicScanQuery.tracked_rank,
        ).where(BasicScanQuery.basic_scan_id.in_(scan_ids))
    ).all()
    rank_maps: dict[object, dict[tuple[str, str], int | None]] = {}
    for scan_id, country, keyword, tracked_rank in rows:
        bucket = rank_maps.setdefault(scan_id, {})
        bucket[(country, keyword)] = tracked_rank
    return rank_maps


def build_daily_representative_scans_from_dedicated_scans(
    db: Session,
    tracked_playlist_id: str,
    *,
    days_back: int = 90,
) -> list[DailyScanRep]:
    if not tracked_playlist_id:
        return []
    cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=days_back - 1)
    scans = (
        db.execute(
            select(BasicScan).where(
                BasicScan.tracked_playlist_id == tracked_playlist_id,
                BasicScan.status == "completed",
            )
        )
        .scalars()
        .all()
    )
    if not scans:
        return []

    daily_latest: dict[date, dict[str, object]] = {}
    for scan in scans:
        timestamp = _scan_timestamp(scan)
        scan_date = timestamp.date()
        if scan_date < cutoff_date:
            continue
        existing = daily_latest.get(scan_date)
        if existing is None or timestamp > existing["timestamp"]:
            daily_latest[scan_date] = {"scan": scan, "timestamp": timestamp}

    if not daily_latest:
        return []

    daily_items = sorted(daily_latest.items(), key=lambda item: item[0])
    scan_ids = [item[1]["scan"].id for item in daily_items]
    rank_maps = _fetch_scan_rank_maps(db, scan_ids)

    daily_reps: list[DailyScanRep] = []
    for scan_date, payload in daily_items:
        scan = payload["scan"]
        daily_reps.append(
            DailyScanRep(
                date=scan_date,
                scan_id=scan.id,
                follower_snapshot=scan.follower_snapshot,
                rank_map=rank_maps.get(scan.id, {}),
            )
        )
    return daily_reps


def compute_position_counts(
    scan_a_map: dict[tuple[str, str], int | None],
    scan_b_map: dict[tuple[str, str], int | None],
) -> dict[str, int]:
    improved = declined = unchanged = 0
    shared_keys = scan_a_map.keys() & scan_b_map.keys()
    for key in shared_keys:
        rank_a = scan_a_map.get(key)
        rank_b = scan_b_map.get(key)
        # Ignore missing ranks to keep position deltas consistent.
        if rank_a is None or rank_b is None:
            continue
        delta = rank_b - rank_a
        if delta > 0:
            improved += 1
        elif delta < 0:
            declined += 1
        else:
            unchanged += 1
    return {
        "improved": improved,
        "declined": declined,
        "unchanged": unchanged,
    }


def _build_daily_summary(
    daily_reps: list[DailyScanRep],
    *,
    days: int = 7,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for index, rep in enumerate(daily_reps):
        previous = daily_reps[index - 1] if index > 0 else None
        follower_change = None
        improved = declined = unchanged = 0
        if previous:
            if rep.follower_snapshot is not None and previous.follower_snapshot is not None:
                follower_change = rep.follower_snapshot - previous.follower_snapshot
            if rep.rank_map and previous.rank_map:
                counts = compute_position_counts(rep.rank_map, previous.rank_map)
                improved = counts["improved"]
                declined = counts["declined"]
                unchanged = counts["unchanged"]

        entries.append(
            {
                "date": rep.date,
                "followers": rep.follower_snapshot,
                "follower_change": follower_change,
                "improved_positions": improved,
                "declined_positions": declined,
                "unchanged_positions": unchanged,
            }
        )

    if days <= 0:
        return entries
    return entries[-days:]


def _build_weekly_summary(daily_entries: list[dict[str, object]]) -> list[dict[str, object]]:
    weekly: dict[tuple[int, int], dict[str, object]] = {}
    for entry in daily_entries:
        entry_date = entry["date"]
        iso = entry_date.isocalendar()
        key = (iso.year, iso.week)
        bucket = weekly.setdefault(
            key,
            {
                "week": f"{iso.year}-W{iso.week:02d}",
                "follower_change": 0,
                "improved_positions": 0,
                "declined_positions": 0,
                "unchanged_positions": 0,
                "has_change": False,
            },
        )
        follower_change = entry.get("follower_change")
        if isinstance(follower_change, int):
            bucket["follower_change"] += follower_change
            bucket["has_change"] = True
        bucket["improved_positions"] += entry.get("improved_positions", 0)
        bucket["declined_positions"] += entry.get("declined_positions", 0)
        bucket["unchanged_positions"] += entry.get("unchanged_positions", 0)

    summary = []
    for key in sorted(weekly.keys()):
        bucket = weekly[key]
        summary.append(
            {
                "week": bucket["week"],
                "follower_change": bucket["follower_change"]
                if bucket["has_change"]
                else None,
                "improved_positions": bucket["improved_positions"],
                "declined_positions": bucket["declined_positions"],
                "unchanged_positions": bucket["unchanged_positions"],
            }
        )
    return summary


def build_playlist_insights(
    db: Session,
    playlist_id: str,
    *,
    tracked_playlist_id: str | None = None,
) -> dict:
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
    if len(snapshots) < 2 and tracked_playlist_id:
        backfill_playlist_follower_snapshots_from_dedicated_scans(
            db,
            playlist_id,
            tracked_playlist_id,
        )
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

    daily_reps = (
        build_daily_representative_scans_from_dedicated_scans(
            db,
            tracked_playlist_id,
        )
        if tracked_playlist_id
        else []
    )
    daily_entries_full = _build_daily_summary(daily_reps, days=0)
    daily_summary = _build_daily_summary(daily_reps, days=7)
    weekly_summary = _build_weekly_summary(daily_entries_full)

    return {
        "playlist_id": playlist_id,
        "first_seen_at": playlist.first_seen_at if playlist else None,
        "first_seen_followers": playlist.first_seen_followers if playlist else None,
        "current_followers": current_followers,
        "follower_timeseries": timeseries,
        "computed_deltas": deltas,
        "daily_summary": daily_summary,
        "weekly_summary": weekly_summary,
    }
