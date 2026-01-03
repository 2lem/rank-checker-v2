from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from app.models.basic_scan import BasicScan
from app.models.playlist import PlaylistFollowerSnapshot
from app.services.playlist_insights import (
    DailyScanRep,
    backfill_playlist_follower_snapshots_from_dedicated_scans,
    build_daily_compare,
    build_weekly_compare,
    compute_position_counts,
)


class DummyScalarsResult:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return self._items


class DummySession:
    def __init__(self, results):
        self._results = list(results)
        self.added = []
        self.committed = False

    def execute(self, _query):
        if not self._results:
            raise AssertionError("Unexpected query execution.")
        return DummyScalarsResult(self._results.pop(0))

    def add(self, item):
        self.added.append(item)

    def commit(self):
        self.committed = True


def _make_scan(scan_date: datetime, *, followers: int) -> BasicScan:
    return BasicScan(
        id=uuid4(),
        tracked_playlist_id=uuid4(),
        status="completed",
        started_at=scan_date,
        finished_at=scan_date,
        created_at=scan_date,
        scanned_countries=["US"],
        scanned_keywords=["pop"],
        follower_snapshot=followers,
        is_tracked_playlist=True,
    )


def test_backfill_snapshots_latest_per_day_upserts() -> None:
    now = datetime.now(timezone.utc)
    base = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) - timedelta(days=2)
    day_one = base + timedelta(hours=10)
    day_one_latest = base + timedelta(hours=12)
    day_two = base + timedelta(days=1, hours=9)

    scan_old = _make_scan(day_one, followers=110)
    scan_latest = _make_scan(day_one_latest, followers=120)
    scan_day_two = _make_scan(day_two, followers=130)

    existing_snapshot = PlaylistFollowerSnapshot(
        playlist_id="playlist_1",
        snapshot_at=day_one - timedelta(hours=1),
        snapshot_date=day_one.date(),
        followers=105,
        source="insights",
    )

    session = DummySession(
        results=[
            [scan_old, scan_latest, scan_day_two],
            [existing_snapshot],
        ]
    )

    updated = backfill_playlist_follower_snapshots_from_dedicated_scans(
        session,
        "playlist_1",
        str(uuid4()),
        days_back=5,
    )

    assert updated == 2
    assert existing_snapshot.followers == 120
    assert existing_snapshot.source == "dedicated_scan_backfill"
    added_dates = {item.snapshot_date for item in session.added if isinstance(item, PlaylistFollowerSnapshot)}
    assert day_two.date() in added_dates
    assert session.committed is True


def test_compute_position_counts_ignores_missing() -> None:
    scan_a = {("US", "pop"): 10, ("US", "rock"): None, ("GB", "indie"): 5}
    scan_b = {("US", "pop"): 12, ("US", "rock"): 18, ("GB", "indie"): 5}

    counts = compute_position_counts(scan_a, scan_b)

    assert counts["improved"] == 1
    assert counts["declined"] == 0
    assert counts["unchanged"] == 1


def test_build_daily_compare_uses_latest_two_days() -> None:
    base_date = datetime.now(timezone.utc).date()
    older = DailyScanRep(
        date=base_date - timedelta(days=2),
        scan_id="scan_old",
        follower_snapshot=100,
        rank_map={("US", "pop"): 12},
    )
    previous = DailyScanRep(
        date=base_date - timedelta(days=1),
        scan_id="scan_previous",
        follower_snapshot=110,
        rank_map={("US", "pop"): 10, ("US", "rock"): 5},
    )
    newest = DailyScanRep(
        date=base_date,
        scan_id="scan_new",
        follower_snapshot=125,
        rank_map={("US", "pop"): 8, ("US", "rock"): 7},
    )

    compare = build_daily_compare([older, newest, previous])

    assert compare is not None
    assert compare["date_newer"] == newest.date
    assert compare["date_older"] == previous.date
    assert compare["followers_newer"] == 125
    assert compare["followers_older"] == 110
    assert compare["followers_change"] == 15
    assert compare["improved_positions"] == 1
    assert compare["declined_positions"] == 1
    assert compare["unchanged_positions"] == 0


def test_build_daily_compare_returns_none_with_single_entry() -> None:
    base_date = datetime.now(timezone.utc).date()
    only = DailyScanRep(
        date=base_date,
        scan_id="scan_only",
        follower_snapshot=150,
        rank_map={("US", "pop"): 3},
    )

    compare = build_daily_compare([only])

    assert compare is None


def test_build_weekly_compare_uses_first_day_when_history_is_short() -> None:
    base_date = datetime.now(timezone.utc).date()
    first = DailyScanRep(
        date=base_date - timedelta(days=2),
        scan_id="scan_first",
        follower_snapshot=100,
        rank_map={("US", "pop"): 20},
    )
    middle = DailyScanRep(
        date=base_date - timedelta(days=1),
        scan_id="scan_middle",
        follower_snapshot=120,
        rank_map={("US", "pop"): 15},
    )
    newest = DailyScanRep(
        date=base_date,
        scan_id="scan_newest",
        follower_snapshot=140,
        rank_map={("US", "pop"): 10},
    )

    compare = build_weekly_compare([middle, newest, first])

    assert compare is not None
    assert compare["date_newer"] == newest.date
    assert compare["date_older"] == first.date
    assert compare["followers_change"] == 40
    assert compare["improved_positions"] == 1
    assert compare["declined_positions"] == 0
    assert compare["unchanged_positions"] == 0


def test_build_weekly_compare_chooses_closest_day_before_target() -> None:
    base_date = datetime(2024, 1, 15, tzinfo=timezone.utc).date()
    reps = [
        DailyScanRep(
            date=base_date - timedelta(days=14),
            scan_id="scan_oldest",
            follower_snapshot=90,
            rank_map={("US", "pop"): 30},
        ),
        DailyScanRep(
            date=base_date - timedelta(days=8),
            scan_id="scan_target_neighbor",
            follower_snapshot=100,
            rank_map={("US", "pop"): 25},
        ),
        DailyScanRep(
            date=base_date,
            scan_id="scan_latest",
            follower_snapshot=150,
            rank_map={("US", "pop"): 15},
        ),
    ]

    compare = build_weekly_compare(reps)

    assert compare is not None
    assert compare["date_newer"] == base_date
    assert compare["date_older"] == base_date - timedelta(days=8)
    assert compare["followers_change"] == 50
    assert compare["improved_positions"] == 1
    assert compare["declined_positions"] == 0


def test_build_weekly_compare_handles_exact_seven_day_gap() -> None:
    base_date = datetime(2024, 1, 15, tzinfo=timezone.utc).date()
    older = DailyScanRep(
        date=base_date - timedelta(days=7),
        scan_id="scan_week_before",
        follower_snapshot=200,
        rank_map={("US", "pop"): 5, ("GB", "rock"): 12},
    )
    newest = DailyScanRep(
        date=base_date,
        scan_id="scan_current",
        follower_snapshot=230,
        rank_map={("US", "pop"): 4, ("GB", "rock"): 15},
    )

    compare = build_weekly_compare([newest, older])

    assert compare is not None
    assert compare["date_newer"] == newest.date
    assert compare["date_older"] == older.date
    assert compare["followers_change"] == 30
    assert compare["improved_positions"] == 1
    assert compare["declined_positions"] == 1
    assert compare["unchanged_positions"] == 0
