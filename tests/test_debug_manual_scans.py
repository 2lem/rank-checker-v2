from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api.routes import debug
from app.models.base import Base
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult


def _make_session_factory(tmp_path) -> Callable[[], sessionmaker]:
    db_path = tmp_path / "debug_manual_scans.db"
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
    Base.metadata.create_all(
        engine,
        tables=[
            BasicScan.__table__,
            BasicScanQuery.__table__,
            BasicScanResult.__table__,
        ],
    )
    return sessionmaker(autocommit=False, autoflush=False, bind=engine, expire_on_commit=False)


def test_manual_scan_filters_and_ordering(tmp_path) -> None:
    session_factory = _make_session_factory(tmp_path)
    now = datetime.now(timezone.utc)

    manual_old = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=None,
        started_at=now - timedelta(hours=3),
        status="completed",
        created_at=now - timedelta(hours=3),
        manual_target_keywords=["k1"],
        manual_target_countries=[],
        scanned_countries=[],
        scanned_keywords=[],
    )
    manual_new = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=None,
        started_at=now - timedelta(hours=1),
        status="completed",
        created_at=now - timedelta(hours=1),
        manual_target_keywords=[],
        manual_target_countries=["US"],
        scanned_countries=[],
        scanned_keywords=[],
    )
    tracked = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=uuid.uuid4(),
        started_at=now - timedelta(minutes=30),
        status="completed",
        created_at=now - timedelta(minutes=30),
        manual_target_keywords=[],
        manual_target_countries=[],
        scanned_countries=[],
        scanned_keywords=[],
        is_tracked_playlist=True,
    )

    with session_factory() as session:
        session.add_all([manual_old, manual_new, tracked])
        session.commit()

    assert debug._is_manual_scan(manual_old) is True
    assert debug._is_manual_scan(manual_new) is True
    assert debug._is_manual_scan(tracked) is False

    with session_factory() as session:
        limit, offset, total, scans = debug._query_manual_scans(
            session,
            limit=50,
            offset=0,
            status=None,
            from_ts=None,
            to_ts=None,
        )

    assert limit == 50
    assert offset == 0
    assert total == 2
    assert [scan.id for scan in scans] == [manual_new.id, manual_old.id]


def test_manual_scan_limit_cap(tmp_path) -> None:
    session_factory = _make_session_factory(tmp_path)
    now = datetime.now(timezone.utc)

    manual_scan = BasicScan(
        id=uuid.uuid4(),
        tracked_playlist_id=None,
        started_at=now,
        status="completed",
        created_at=now,
        manual_target_keywords=["k1"],
        manual_target_countries=[],
        scanned_countries=[],
        scanned_keywords=[],
    )

    with session_factory() as session:
        session.add(manual_scan)
        session.commit()

    with session_factory() as session:
        limit, offset, total, scans = debug._query_manual_scans(
            session,
            limit=500,
            offset=0,
            status=None,
            from_ts=None,
            to_ts=None,
        )

    assert limit == 200
    assert offset == 0
    assert total == 1
    assert [scan.id for scan in scans] == [manual_scan.id]
