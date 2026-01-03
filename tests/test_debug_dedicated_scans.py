from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
import uuid

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api.routes import debug
from app.models.base import Base
from app.models.basic_scan import BasicScan
from app.models.tracked_playlist import TrackedPlaylist


def _make_session_factory(tmp_path) -> Callable[[], sessionmaker]:
    db_path = tmp_path / "debug_dedicated_scans.db"
    engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    for column_name in (
        "scanned_countries",
        "scanned_keywords",
        "manual_target_countries",
        "manual_target_keywords",
    ):
        BasicScan.__table__.c[column_name].server_default = None
    for column_name in ("target_countries", "target_keywords"):
        TrackedPlaylist.__table__.c[column_name].server_default = None
    Base.metadata.create_all(
        engine,
        tables=[
            BasicScan.__table__,
            TrackedPlaylist.__table__,
        ],
    )
    return sessionmaker(autocommit=False, autoflush=False, bind=engine, expire_on_commit=False)


def test_dedicated_scan_ordering_desc(tmp_path) -> None:
    session_factory = _make_session_factory(tmp_path)
    now = datetime.now(timezone.utc)

    tracked_playlist = TrackedPlaylist(
        id=uuid.uuid4(),
        playlist_id="playlist-1",
        name="Tracked Playlist",
        target_countries=["US"],
        target_keywords=["k1"],
    )
    dedicated_old = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=tracked_playlist.id,
        started_at=now - timedelta(hours=2),
        status="completed",
        created_at=now - timedelta(hours=2),
        scanned_countries=["US"],
        scanned_keywords=["k1"],
        manual_target_keywords=[],
        manual_target_countries=[],
        is_tracked_playlist=True,
    )
    dedicated_new = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=tracked_playlist.id,
        started_at=now - timedelta(hours=1),
        status="completed",
        created_at=now - timedelta(hours=1),
        scanned_countries=["US"],
        scanned_keywords=["k1"],
        manual_target_keywords=[],
        manual_target_countries=[],
        is_tracked_playlist=True,
    )
    manual_scan = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=None,
        started_at=now - timedelta(minutes=30),
        status="completed",
        created_at=now - timedelta(minutes=30),
        manual_target_keywords=["k2"],
        manual_target_countries=[],
        scanned_countries=[],
        scanned_keywords=[],
    )

    with session_factory() as session:
        session.add(tracked_playlist)
        session.add_all([dedicated_old, dedicated_new, manual_scan])
        session.commit()

    with session_factory() as session:
        limit, offset, total, scans = debug._query_dedicated_scans(
            session,
            limit=50,
            offset=0,
        )

    assert limit == 50
    assert offset == 0
    assert total == 2
    assert [scan.id for scan, _name in scans] == [dedicated_new.id, dedicated_old.id]


def test_dedicated_scan_limit_cap(tmp_path) -> None:
    session_factory = _make_session_factory(tmp_path)
    now = datetime.now(timezone.utc)

    tracked_playlist = TrackedPlaylist(
        id=uuid.uuid4(),
        playlist_id="playlist-1",
        name="Tracked Playlist",
        target_countries=["US"],
        target_keywords=["k1"],
    )
    dedicated_scan = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=tracked_playlist.id,
        started_at=now,
        status="completed",
        created_at=now,
        scanned_countries=["US"],
        scanned_keywords=["k1"],
        manual_target_keywords=[],
        manual_target_countries=[],
        is_tracked_playlist=True,
    )

    with session_factory() as session:
        session.add(tracked_playlist)
        session.add(dedicated_scan)
        session.commit()

    with session_factory() as session:
        limit, offset, total, scans = debug._query_dedicated_scans(
            session,
            limit=500,
            offset=0,
        )

    assert limit == 200
    assert offset == 0
    assert total == 1
    assert [scan.id for scan, _name in scans] == [dedicated_scan.id]


def test_dedicated_scan_not_found(tmp_path, monkeypatch) -> None:
    session_factory = _make_session_factory(tmp_path)
    monkeypatch.setattr(debug, "SessionLocal", session_factory)

    with pytest.raises(HTTPException) as excinfo:
        debug.get_dedicated_scan(str(uuid.uuid4()))

    assert excinfo.value.status_code == 404
