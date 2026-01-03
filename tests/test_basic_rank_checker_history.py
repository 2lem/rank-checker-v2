from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.routes.basic_rank_checker import get_scan_history
from app.models.basic_scan import BasicScan


class DummyScalarResult:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one(self) -> int:
        return self._value


class DummyScalarsResult:
    def __init__(self, items: list[BasicScan]) -> None:
        self._items = items

    def scalars(self) -> "DummyScalarsResult":
        return self

    def all(self) -> list[BasicScan]:
        return self._items


class DummySession:
    def __init__(self, total: int, items: list[BasicScan]) -> None:
        self._total = total
        self._items = items
        self._calls = 0

    def execute(self, _query):
        self._calls += 1
        if self._calls == 1:
            return DummyScalarResult(self._total)
        return DummyScalarsResult(self._items)


def test_scan_history_limit_capped() -> None:
    tracked_playlist_id = uuid4()
    scan_id = uuid4()
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    scan = BasicScan(
        id=scan_id,
        tracked_playlist_id=tracked_playlist_id,
        status="completed",
        started_at=now,
        finished_at=now,
        created_at=now,
        scanned_countries=["US"],
        scanned_keywords=["pop"],
        follower_snapshot=123,
        is_tracked_playlist=True,
    )
    session = DummySession(total=1, items=[scan])

    payload = get_scan_history(
        tracked_playlist_id=str(tracked_playlist_id),
        limit=1000,
        offset=0,
        db=session,
    )
    assert payload["limit"] == 100
    assert payload["total"] == 1
    assert payload["items"][0]["scan_id"] == str(scan_id)


def test_scan_history_offset_negative_rejected() -> None:
    session = DummySession(total=0, items=[])

    with pytest.raises(HTTPException) as excinfo:
        get_scan_history(
            tracked_playlist_id=str(uuid4()),
            limit=20,
            offset=-1,
            db=session,
        )

    assert excinfo.value.status_code == 400


def test_scan_history_invalid_uuid() -> None:
    session = DummySession(total=0, items=[])

    with pytest.raises(HTTPException) as excinfo:
        get_scan_history(
            tracked_playlist_id="not-a-uuid",
            limit=20,
            offset=0,
            db=session,
        )

    assert excinfo.value.status_code == 400
