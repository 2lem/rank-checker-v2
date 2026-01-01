from __future__ import annotations

import json
import logging
import os
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in os.sys.path:
    os.sys.path.insert(0, str(ROOT_DIR))


@dataclass
class SpotifyCall:
    ts: float
    path: str


class StubSpotifyResponder:
    def __init__(self, per_call_delay: float = 0.05) -> None:
        self.per_call_delay = per_call_delay
        self.call_log: list[SpotifyCall] = []

    def __call__(self, method: str, url: str, **_kwargs: Any) -> requests.Response:
        ts = time.time()
        path = urlparse(url).path
        self.call_log.append(SpotifyCall(ts=ts, path=path))
        time.sleep(self.per_call_delay)
        payload: dict[str, Any]
        if url.endswith("/api/token"):
            payload = {"access_token": "diagnostic-token"}
        elif "/search" in url:
            payload = {
                "playlists": {
                    "items": [
                        {
                            "id": f"playlist-{idx}",
                            "name": f"Playlist {idx}",
                            "external_urls": {"spotify": f"https://open.spotify.com/playlist/{idx}"},
                            "description": "",
                            "owner": {"display_name": f"owner-{idx}"},
                            "tracks": {"total": 10},
                            "placeholder": False,
                        }
                        for idx in range(2)
                    ]
                }
            }
        elif "/tracks" in url:
            payload = {"items": [], "total": 0}
        else:
            playlist_id = path.rsplit("/", 1)[-1]
            payload = {
                "name": f"Playlist {playlist_id}",
                "external_urls": {"spotify": f"https://open.spotify.com/playlist/{playlist_id}"},
                "followers": {"total": 100},
                "tracks": {"total": 10},
                "description": "",
                "images": [{"url": "https://images.example/test.jpg"}],
                "snapshot_id": None,
                "owner": {"display_name": "diagnostic-owner", "id": "diagnostic-owner"},
            }
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode("utf-8")
        response.headers["Content-Type"] = "application/json"
        response.url = url
        return response


def _reset_sqlite_db(db_url: str) -> None:
    if not db_url.startswith("sqlite:///"):
        return
    db_path = Path(db_url.replace("sqlite:///", ""))
    if db_path.exists():
        db_path.unlink()


def _seed_tracked_playlist(db_session):
    from app.models.tracked_playlist import TrackedPlaylist

    tracked = TrackedPlaylist(
        playlist_id="diagnostic-playlist",
        name="Diagnostics Playlist",
        target_countries=["US"],
        target_keywords=[f"keyword-{idx}" for idx in range(1, 11)],
    )
    db_session.add(tracked)
    db_session.commit()
    db_session.refresh(tracked)
    return tracked


def _compute_rps(call_log: list[SpotifyCall]) -> tuple[float, float, dict[int, int]]:
    if not call_log:
        return 0.0, 0.0, {}
    buckets: dict[int, int] = {}
    for entry in call_log:
        bucket = int(entry.ts)
        buckets[bucket] = buckets.get(bucket, 0) + 1
    peak_rps = max(buckets.values()) if buckets else 0
    span_seconds = max(buckets.keys()) - min(buckets.keys()) + 1 if buckets else 0
    avg_rps = sum(buckets.values()) / span_seconds if span_seconds else 0
    return float(peak_rps), round(avg_rps, 2), buckets


def run_diagnostics() -> dict[str, Any]:
    os.environ.setdefault("DATABASE_URL", "sqlite:///./diagnostics.db")
    os.environ.setdefault("SPOTIFY_CLIENT_ID", "diagnostic-client")
    os.environ.setdefault("SPOTIFY_CLIENT_SECRET", "diagnostic-secret")
    from app.core import db
    from app.models import Base
    from app.models.basic_scan import BasicScan
    from app.core import spotify

    db_url = os.environ["DATABASE_URL"]
    _reset_sqlite_db(db_url)
    Base.metadata.drop_all(bind=db.engine)
    Base.metadata.create_all(bind=db.engine)

    responder = StubSpotifyResponder()
    spotify.requests.request = responder

    with db.SessionLocal() as session:
        tracked = _seed_tracked_playlist(session)
        tracked_id = str(tracked.id)

    post_started = time.perf_counter()
    from app.api.routes.basic_rank_checker import start_basic_scan

    with db.SessionLocal() as session:
        response_payload = start_basic_scan({"tracked_playlist_id": tracked_id}, db=session)
    post_duration_ms = round((time.perf_counter() - post_started) * 1000, 2)
    scan_id = response_payload.get("scan_id")

    deadline = time.time() + 60
    final_status = None
    while time.time() < deadline:
        with db.SessionLocal() as session:
            scan = session.get(BasicScan, uuid.UUID(scan_id))
            final_status = scan.status if scan else None
        if final_status in {"completed", "completed_partial", "failed", "cancelled"}:
            break
        time.sleep(0.1)

    peak_rps, avg_rps, rps_buckets = _compute_rps(responder.call_log)
    endpoint_counts = Counter(call.path for call in responder.call_log)
    return {
        "post_duration_ms": post_duration_ms,
        "scan_id": scan_id,
        "final_status": final_status,
        "call_count": len(responder.call_log),
        "peak_rps": peak_rps,
        "avg_rps": avg_rps,
        "rps_buckets": endpoint_counts,
        "raw_rps_buckets": rps_buckets,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    summary = run_diagnostics()
    print(json.dumps(summary, indent=2, sort_keys=True))
