from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.basic_rank_checker import manual_service
from app.core import spotify
from app.models.base import Base
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult


def _make_session_factory(tmp_path) -> Callable[[], sessionmaker]:
    db_path = tmp_path / "manual_scan.db"
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
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def test_manual_scan_processes_multiple_keywords(monkeypatch, tmp_path) -> None:
    session_factory = _make_session_factory(tmp_path)
    monkeypatch.setattr(manual_service, "SessionLocal", session_factory)
    monkeypatch.setattr(manual_service.scan_event_manager, "publish", lambda *_, **__: None)
    monkeypatch.setattr(manual_service, "start_scan_spotify_usage", lambda *_, **__: None)
    monkeypatch.setattr(manual_service, "log_scan_spotify_usage", lambda *_, **__: None)
    monkeypatch.setattr(manual_service, "get_access_token", lambda *_, **__: "token")
    monkeypatch.setattr(manual_service.basic_service, "fetch_scan_details", lambda *_: {})

    search_calls: list[tuple[str, str]] = []
    scan_id_holder: dict[str, object] = {}

    def fake_search_playlists(keyword: str, country: str, *_args, **_kwargs):
        search_calls.append((keyword, country))
        if len(search_calls) == 1:
            with session_factory() as session:
                scan = session.get(BasicScan, scan_id_holder["scan_id"])
                assert scan is not None
                assert scan.status == "running"
        return []

    monkeypatch.setattr(manual_service, "search_playlists", fake_search_playlists)

    with session_factory() as session:
        scan = manual_service.create_manual_scan(
            session,
            playlist_url=None,
            target_keywords=["lofi", "focus"],
            target_countries=["US"],
        )
        scan_id_holder["scan_id"] = scan.id

    manual_service.run_manual_scan(scan_id_holder["scan_id"])

    with session_factory() as session:
        scan = session.get(BasicScan, scan_id_holder["scan_id"])
        assert scan is not None
        assert scan.status == "completed"
        queries = session.execute(
            select(BasicScanQuery).where(
                BasicScanQuery.basic_scan_id == scan.id
            )
        ).scalars().all()

    assert search_calls == [("lofi", "US"), ("focus", "US")]
    assert sorted({query.keyword for query in queries}) == ["focus", "lofi"]


def test_manual_scan_uses_global_limiter(monkeypatch, tmp_path) -> None:
    session_factory = _make_session_factory(tmp_path)
    monkeypatch.setattr(manual_service, "SessionLocal", session_factory)
    monkeypatch.setattr(manual_service.scan_event_manager, "publish", lambda *_, **__: None)
    monkeypatch.setattr(manual_service, "start_scan_spotify_usage", lambda *_, **__: None)
    monkeypatch.setattr(manual_service, "log_scan_spotify_usage", lambda *_, **__: None)
    monkeypatch.setattr(manual_service, "get_access_token", lambda *_, **__: "token")
    monkeypatch.setattr(manual_service.basic_service, "fetch_scan_details", lambda *_: {})

    limiter_calls: list[float] = []

    def fake_acquire(rps: float) -> None:
        limiter_calls.append(rps)

    monkeypatch.setattr(spotify._spotify_global_rps_limiter, "acquire", fake_acquire)

    class FakeResponse:
        status_code = 200
        content = b"{}"
        headers: dict[str, str] = {}
        text = ""

        def json(self):
            return {"playlists": {"items": []}}

        def raise_for_status(self) -> None:
            return None

    def fake_request(*_args, **_kwargs):
        return FakeResponse()

    monkeypatch.setattr(spotify.requests, "request", fake_request)

    with session_factory() as session:
        scan = manual_service.create_manual_scan(
            session,
            playlist_url=None,
            target_keywords=["lofi", "focus"],
            target_countries=["US"],
        )
        scan_id = scan.id

    manual_service.run_manual_scan(scan_id)

    assert len(limiter_calls) >= 2
