from __future__ import annotations

import time
from datetime import datetime, timezone
from threading import Lock

_lock = Lock()
_total_http_requests = 0
_total_spotify_requests = 0
_scan_start_times: dict[str, float] = {}


def log_basic_scan_start(*, scan_id: str, country: str | None, keyword: str | None) -> None:
    global _total_http_requests
    ts = datetime.now(timezone.utc).isoformat()
    with _lock:
        _total_http_requests += 1
        _scan_start_times[scan_id] = time.monotonic()
    print(f"[BASIC_SCAN_START] country={country} keyword={keyword} ts={ts}")


def log_basic_scan_end(*, scan_id: str) -> None:
    with _lock:
        total_http_requests = _total_http_requests
        total_spotify_requests = _total_spotify_requests
        start_time = _scan_start_times.pop(scan_id, None)
    duration_ms = round((time.monotonic() - start_time) * 1000, 2) if start_time else 0.0
    print("[BASIC_SCAN_END]")
    print(f"total_http_requests={total_http_requests}")
    print(f"total_spotify_requests={total_spotify_requests}")
    print(f"duration_ms={duration_ms}")


def log_spotify_call(endpoint: str) -> None:
    global _total_spotify_requests
    with _lock:
        _total_spotify_requests += 1
        total_spotify_requests = _total_spotify_requests
    print(f"[SPOTIFY_CALL] endpoint={endpoint} total_spotify_requests={total_spotify_requests}")
