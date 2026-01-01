import asyncio
import base64
import json
import logging
import os
import random
import re
import time
import uuid
from collections import Counter, deque
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from urllib.parse import urlparse

import requests

from app.core.basic_scan_visibility import log_spotify_call
from app.core.config import (
    MARKETS_URL,
    PLAYLIST_URL,
    RESULTS_LIMIT,
    SEARCH_URL,
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
    SPOTIFY_GLOBAL_RPS,
    SPOTIFY_MAX_CONCURRENCY,
    SPOTIFY_MAX_RETRY_AFTER,
    SPOTIFY_REQUEST_TIMEOUT,
    TOKEN_URL,
)

logger = logging.getLogger(__name__)
_spotify_semaphore = asyncio.Semaphore(SPOTIFY_MAX_CONCURRENCY)
_spotify_semaphore_loop: asyncio.AbstractEventLoop | None = None
_spotify_semaphore_thread: Thread | None = None
_spotify_semaphore_ready = Event()
_spotify_semaphore_lock = Lock()
_REDACT_KEYS = {"access_token", "refresh_token", "client_secret", "authorization", "token"}
_METRICS_WINDOW_SECONDS = 15 * 60
MAX_SPOTIFY_CALLS_PER_MINUTE = int(os.getenv("SPOTIFY_MAX_CALLS_PER_MINUTE", "600"))
MAX_SPOTIFY_CALLS_PER_SCAN = int(os.getenv("SPOTIFY_MAX_CALLS_PER_SCAN", "2000"))
SPOTIFY_BUDGET_PACING_THRESHOLD = float(os.getenv("SPOTIFY_BUDGET_PACING_THRESHOLD", "0.85"))
SPOTIFY_BUDGET_PACING_SLEEP_MS = int(os.getenv("SPOTIFY_BUDGET_PACING_SLEEP_MS", "250"))
_SPOTIFY_RPS_LOCK = Lock()
_SPOTIFY_RPS_NEXT_ALLOWED_TIME = 0.0


def _apply_spotify_rps_limit() -> None:
    global _SPOTIFY_RPS_NEXT_ALLOWED_TIME
    if SPOTIFY_GLOBAL_RPS <= 0:
        return
    min_interval = 1.0 / SPOTIFY_GLOBAL_RPS
    wait_seconds = 0.0
    with _SPOTIFY_RPS_LOCK:
        now = time.monotonic()
        if now < _SPOTIFY_RPS_NEXT_ALLOWED_TIME:
            wait_seconds = _SPOTIFY_RPS_NEXT_ALLOWED_TIME - now
        _SPOTIFY_RPS_NEXT_ALLOWED_TIME = max(_SPOTIFY_RPS_NEXT_ALLOWED_TIME, now) + min_interval
    if wait_seconds > 0:
        wait_ms = int(round(wait_seconds * 1000))
        if wait_ms >= 25:
            logger.info(
                "[RATE_LIMIT] waiting_ms=%s reason=rps rps=%s",
                wait_ms,
                SPOTIFY_GLOBAL_RPS,
            )
        time.sleep(wait_seconds)


def _ensure_spotify_semaphore_loop() -> asyncio.AbstractEventLoop:
    global _spotify_semaphore_loop, _spotify_semaphore_thread
    if _spotify_semaphore_loop and _spotify_semaphore_loop.is_running():
        return _spotify_semaphore_loop
    with _spotify_semaphore_lock:
        if _spotify_semaphore_loop and _spotify_semaphore_loop.is_running():
            return _spotify_semaphore_loop

        loop = asyncio.new_event_loop()
        _spotify_semaphore_loop = loop

        def _run_loop() -> None:
            asyncio.set_event_loop(loop)
            _spotify_semaphore_ready.set()
            loop.run_forever()

        _spotify_semaphore_thread = Thread(
            target=_run_loop,
            name="spotify-semaphore-loop",
            daemon=True,
        )
        _spotify_semaphore_thread.start()
        _spotify_semaphore_ready.wait()
        return loop


@contextmanager
def _spotify_concurrency_guard() -> Iterator[None]:
    loop = _ensure_spotify_semaphore_loop()
    acquire_future = asyncio.run_coroutine_threadsafe(_spotify_semaphore.acquire(), loop)
    acquire_future.result()
    try:
        yield
    finally:
        if _spotify_semaphore_loop and _spotify_semaphore_loop.is_running():
            _spotify_semaphore_loop.call_soon_threadsafe(_spotify_semaphore.release)


class SpotifyMetrics:
    def __init__(self, window_seconds: int) -> None:
        self._window_seconds = window_seconds
        self._entries: deque[tuple[float, int | None, float]] = deque()
        self._lock = Lock()

    def record(self, status_code: int | None, latency_ms: float) -> None:
        now = time.time()
        with self._lock:
            self._entries.append((now, status_code, latency_ms))
            self._prune_locked(now)

    def snapshot(self) -> dict:
        now = time.time()
        with self._lock:
            self._prune_locked(now)
            entries = list(self._entries)

        total_requests = len(entries)
        success_count = sum(1 for _, status, _ in entries if status and 200 <= status < 300)
        status_429_count = sum(1 for _, status, _ in entries if status == 429)
        status_5xx_count = sum(1 for _, status, _ in entries if status and status >= 500)
        latencies = [latency for _, _, latency in entries]
        average_latency_ms = round(sum(latencies) / total_requests, 2) if total_requests else 0.0
        max_latency_ms = round(max(latencies), 2) if latencies else 0.0

        return {
            "total_requests": total_requests,
            "success_count": success_count,
            "status_429_count": status_429_count,
            "status_5xx_count": status_5xx_count,
            "average_latency_ms": average_latency_ms,
            "max_latency_ms": max_latency_ms,
        }

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._entries and self._entries[0][0] < cutoff:
            self._entries.popleft()


_spotify_metrics = SpotifyMetrics(_METRICS_WINDOW_SECONDS)
_SCAN_USAGE_TTL_SECONDS = 60 * 60


class SpotifyScanUsageTracker:
    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._entries: dict[str, dict] = {}
        self._lock = Lock()

    def start(self, scan_id: str) -> None:
        now_wall = time.time()
        now_monotonic = time.monotonic()
        with self._lock:
            self._prune_locked(now_wall)
            entry = self._entries.get(scan_id)
            if entry is None:
                self._entries[scan_id] = {
                    "start_monotonic": now_monotonic,
                    "last_seen": now_wall,
                    "count": 0,
                    "endpoints": Counter(),
                }
            else:
                entry["last_seen"] = now_wall

    def record(self, scan_id: str | None, endpoint: str | None) -> None:
        if not scan_id:
            return
        now_wall = time.time()
        now_monotonic = time.monotonic()
        with self._lock:
            self._prune_locked(now_wall)
            entry = self._entries.get(scan_id)
            if entry is None:
                entry = {
                    "start_monotonic": now_monotonic,
                    "last_seen": now_wall,
                    "count": 0,
                    "endpoints": Counter(),
                }
                self._entries[scan_id] = entry
            entry["last_seen"] = now_wall
            entry["count"] += 1
            if endpoint:
                entry["endpoints"][endpoint] += 1

    def finalize(self, scan_id: str) -> dict | None:
        now_wall = time.time()
        now_monotonic = time.monotonic()
        with self._lock:
            self._prune_locked(now_wall)
            entry = self._entries.pop(scan_id, None)
        if entry is None:
            return None
        duration_ms = round((now_monotonic - entry["start_monotonic"]) * 1000, 2)
        return {
            "total": entry["count"],
            "duration_ms": duration_ms,
            "endpoints": entry["endpoints"],
        }

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self._ttl_seconds
        stale_ids = [scan_id for scan_id, entry in self._entries.items() if entry["last_seen"] < cutoff]
        for scan_id in stale_ids:
            self._entries.pop(scan_id, None)


_spotify_scan_usage = SpotifyScanUsageTracker(_SCAN_USAGE_TTL_SECONDS)


def get_spotify_metrics_snapshot() -> dict:
    return _spotify_metrics.snapshot()


def _log_event(event: str, **fields: object) -> None:
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, sort_keys=True, default=str))


def start_scan_spotify_usage(scan_id: str) -> None:
    _spotify_scan_usage.start(scan_id)


def log_scan_spotify_usage(
    *,
    scan_id: str,
    scan_kind: str,
    tracked_playlist_id: str | None,
    countries_count: int,
    keywords_count: int,
    ended_status: str,
) -> None:
    usage = _spotify_scan_usage.finalize(scan_id)
    total_calls = usage["total"] if usage else 0
    duration_ms = usage["duration_ms"] if usage else 0.0
    endpoints: Counter = usage["endpoints"] if usage else Counter()
    endpoints_summary = [
        {"endpoint": endpoint, "count": count} for endpoint, count in endpoints.most_common(5)
    ]
    requests_per_minute = None
    if duration_ms > 0:
        requests_per_minute = round(total_calls / (duration_ms / 60000), 2)
    payload = {
        "type": "scan_spotify_usage",
        "scan_id": scan_id,
        "scan_kind": scan_kind,
        "tracked_playlist_id": tracked_playlist_id,
        "countries_count": countries_count,
        "keywords_count": keywords_count,
        "spotify_calls_total": total_calls,
        "duration_ms": duration_ms,
        "requests_per_minute_est": requests_per_minute,
        "ended_status": ended_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if endpoints_summary:
        payload["spotify_calls_by_endpoint"] = endpoints_summary
    logger.info(json.dumps(payload, sort_keys=True, default=str))


def _spotify_client_id_suffix() -> str | None:
    if not SPOTIFY_CLIENT_ID:
        return None
    return SPOTIFY_CLIENT_ID[-4:]


def _log_spotify_call(
    *,
    phase: str,
    endpoint: str,
    method: str,
    status_code: int | None,
    duration_ms: float | None,
    retry_after_sec: int | str | None,
    scan_id: str | None,
    playlist_id: str | None,
    country: str | None,
    keyword: str | None,
) -> None:
    payload = {
        "type": "spotify_api_call",
        "phase": phase,
        "endpoint": endpoint,
        "method": method,
        "status_code": status_code,
        "duration_ms": duration_ms,
        "retry_after_sec": retry_after_sec,
        "scan_id": scan_id,
        "playlist_id": playlist_id,
        "country": country,
        "keyword": keyword,
        "app_client_id_suffix": _spotify_client_id_suffix(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(payload, sort_keys=True, default=str))


def _log_budget_warning(*, scope: str, limit: int, current: int, scan_id: str | None) -> None:
    payload = {
        "type": "spotify_budget_warning",
        "scope": scope,
        "limit": limit,
        "current": current,
        "scan_id": scan_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.warning(json.dumps(payload, sort_keys=True, default=str))


def _log_budget_pacing(*, scope: str, limit: int, current: int, sleep_ms: int, scan_id: str | None) -> None:
    payload = {
        "type": "spotify_budget_pacing",
        "scope": scope,
        "limit": limit,
        "current": current,
        "sleep_ms": sleep_ms,
        "scan_id": scan_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(payload, sort_keys=True, default=str))


def _log_spotify_token_fetch(
    *,
    phase: str,
    reason: str | None,
    status_code: int | None,
    duration_ms: float | None,
    retry_after_sec: int | str | None,
    body_keys: list[str] | None,
    body_length: int | None,
    request_id: str,
    attempt: int,
) -> None:
    payload = {
        "type": "spotify_token_fetch",
        "phase": phase,
        "reason": reason,
        "status_code": status_code,
        "duration_ms": duration_ms,
        "retry_after_sec": retry_after_sec,
        "body_keys": body_keys,
        "body_length": body_length,
        "request_id": request_id,
        "attempt": attempt,
        "app_client_id_suffix": _spotify_client_id_suffix(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(payload, sort_keys=True, default=str))


def _redact_value(value: object) -> object:
    if isinstance(value, dict):
        return {key: ("***" if key.lower() in _REDACT_KEYS else _redact_value(val)) for key, val in value.items()}
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value


def _redact_body_preview(body_text: str, limit: int = 400) -> str:
    if not body_text:
        return ""
    preview = body_text[:limit]
    try:
        parsed = json.loads(preview)
    except json.JSONDecodeError:
        return preview
    redacted = _redact_value(parsed)
    return json.dumps(redacted, sort_keys=True)[:limit]


def _endpoint_path(url: str) -> str:
    parsed = urlparse(url)
    return parsed.path or "/"


def _compute_backoff(
    attempt: int, base_seconds: float, cap_seconds: float, jitter_seconds: float
) -> float:
    exponent = max(attempt - 1, 0)
    backoff = base_seconds * (2**exponent)
    jitter = random.uniform(0, jitter_seconds)
    return min(backoff + jitter, cap_seconds)


class SpotifyTokenError(ValueError):
    pass


class SpotifyCallBudget:
    def __init__(self) -> None:
        self._global_calls: deque[float] = deque()
        self._scan_counts: dict[str, int] = {}
        self._lock = Lock()

    def record(self, scan_id: str | None) -> tuple[list[dict], list[dict]]:
        now = time.time()
        warnings: list[dict] = []
        pacings: list[dict] = []
        with self._lock:
            self._global_calls.append(now)
            self._prune_locked(now)
            global_count = len(self._global_calls)
            if global_count > MAX_SPOTIFY_CALLS_PER_MINUTE:
                warnings.append(
                    {
                        "scope": "global",
                        "limit": MAX_SPOTIFY_CALLS_PER_MINUTE,
                        "current": global_count,
                        "scan_id": None,
                    }
                )
            global_threshold = MAX_SPOTIFY_CALLS_PER_MINUTE * SPOTIFY_BUDGET_PACING_THRESHOLD
            if global_count >= global_threshold:
                pacings.append(
                    {
                        "scope": "global",
                        "limit": MAX_SPOTIFY_CALLS_PER_MINUTE,
                        "current": global_count,
                        "sleep_ms": SPOTIFY_BUDGET_PACING_SLEEP_MS,
                        "scan_id": None,
                    }
                )

            if scan_id:
                current = self._scan_counts.get(scan_id, 0) + 1
                self._scan_counts[scan_id] = current
                if current > MAX_SPOTIFY_CALLS_PER_SCAN:
                    warnings.append(
                        {
                            "scope": "scan",
                            "limit": MAX_SPOTIFY_CALLS_PER_SCAN,
                            "current": current,
                            "scan_id": scan_id,
                        }
                    )
                scan_threshold = MAX_SPOTIFY_CALLS_PER_SCAN * SPOTIFY_BUDGET_PACING_THRESHOLD
                if current >= scan_threshold:
                    sleep_ms = SPOTIFY_BUDGET_PACING_SLEEP_MS
                    pacings.append(
                        {
                            "scope": "scan",
                            "limit": MAX_SPOTIFY_CALLS_PER_SCAN,
                            "current": current,
                            "sleep_ms": sleep_ms,
                            "scan_id": scan_id,
                        }
                    )

        return warnings, pacings

    def _prune_locked(self, now: float) -> None:
        cutoff = now - 60
        while self._global_calls and self._global_calls[0] < cutoff:
            self._global_calls.popleft()


_spotify_budget = SpotifyCallBudget()


def _spotify_request(
    method: str,
    url: str,
    *,
    token: str | None = None,
    params: dict | None = None,
    data: dict | None = None,
    headers: dict | None = None,
    scan_id: str | None = None,
    job_id: str | None = None,
    playlist_id: str | None = None,
    country: str | None = None,
    keyword: str | None = None,
) -> dict:
    payload, _meta = _spotify_request_with_meta(
        method,
        url,
        token=token,
        params=params,
        data=data,
        headers=headers,
        scan_id=scan_id,
        job_id=job_id,
        playlist_id=playlist_id,
        country=country,
        keyword=keyword,
    )
    return payload


def _spotify_request_with_meta(
    method: str,
    url: str,
    *,
    token: str | None = None,
    params: dict | None = None,
    data: dict | None = None,
    headers: dict | None = None,
    scan_id: str | None = None,
    job_id: str | None = None,
    playlist_id: str | None = None,
    country: str | None = None,
    keyword: str | None = None,
) -> tuple[dict, dict]:
    request_id = str(uuid.uuid4())
    attempt = 0
    max_429_retries = 5
    max_transient_retries = 3
    retry_429_count = 0
    retry_transient_count = 0
    path = _endpoint_path(url)
    base_headers = dict(headers or {})
    if token:
        base_headers["Authorization"] = f"Bearer {token}"

    while True:
        attempt += 1
        _spotify_scan_usage.record(scan_id, path)
        warnings, pacings = _spotify_budget.record(scan_id)
        for warning in warnings:
            _log_budget_warning(
                scope=warning["scope"],
                limit=warning["limit"],
                current=warning["current"],
                scan_id=warning["scan_id"],
            )
        sleep_ms = 0
        for pacing in pacings:
            _log_budget_pacing(
                scope=pacing["scope"],
                limit=pacing["limit"],
                current=pacing["current"],
                sleep_ms=pacing["sleep_ms"],
                scan_id=pacing["scan_id"],
            )
            sleep_ms = max(sleep_ms, int(pacing["sleep_ms"]))
        if sleep_ms:
            pacing_detail = max(pacings, key=lambda item: int(item.get("sleep_ms", 0)))
            logger.info(
                "[RATE_LIMIT] waiting_ms=%s scope=%s current=%s limit=%s scan_id=%s",
                sleep_ms,
                pacing_detail.get("scope"),
                pacing_detail.get("current"),
                pacing_detail.get("limit"),
                pacing_detail.get("scan_id"),
            )
        started_at = datetime.now(timezone.utc).isoformat()
        start_monotonic = time.monotonic()
        log_spotify_call(path)
        _log_spotify_call(
            phase="start",
            endpoint=path,
            method=method,
            status_code=None,
            duration_ms=None,
            retry_after_sec=None,
            scan_id=scan_id,
            playlist_id=playlist_id,
            country=country,
            keyword=keyword,
        )
        _log_event(
            "spotify_api_request",
            request_id=request_id,
            scan_id=scan_id,
            job_id=job_id,
            method=method,
            path=path,
            started_at=started_at,
            attempt=attempt,
        )
        try:
            with _spotify_concurrency_guard():
                if sleep_ms:
                    time.sleep(sleep_ms / 1000)
                _apply_spotify_rps_limit()
                response = requests.request(
                    method,
                    url,
                    headers=base_headers,
                    params=params,
                    data=data,
                    timeout=SPOTIFY_REQUEST_TIMEOUT,
                )
        except requests.RequestException as exc:
            duration_ms = round((time.monotonic() - start_monotonic) * 1000, 2)
            _spotify_metrics.record(None, duration_ms)
            _log_spotify_call(
                phase="error",
                endpoint=path,
                method=method,
                status_code=None,
                duration_ms=duration_ms,
                retry_after_sec=None,
                scan_id=scan_id,
                playlist_id=playlist_id,
                country=country,
                keyword=keyword,
            )
            _log_event(
                "spotify_api_error",
                request_id=request_id,
                scan_id=scan_id,
                job_id=job_id,
                method=method,
                path=path,
                attempt=attempt,
                duration_ms=duration_ms,
                error=str(exc),
            )
            if retry_transient_count < max_transient_retries:
                retry_transient_count += 1
                wait_seconds = _compute_backoff(retry_transient_count, 0.5, 10.0, 0.25)
                _log_event(
                    "spotify_api_retry",
                    request_id=request_id,
                    scan_id=scan_id,
                    job_id=job_id,
                    method=method,
                    path=path,
                    attempt=attempt,
                    wait_seconds=wait_seconds,
                    reason="transient",
                )
                time.sleep(wait_seconds)
                continue
            raise

        duration_ms = round((time.monotonic() - start_monotonic) * 1000, 2)
        retry_after_header = response.headers.get("Retry-After")
        retry_after_sec: int | str | None = None
        if retry_after_header is not None:
            try:
                retry_after_sec = int(retry_after_header)
            except ValueError:
                retry_after_sec = retry_after_header
        body_preview = _redact_body_preview(response.text)
        response_size = len(response.content or b"")
        _spotify_metrics.record(response.status_code, duration_ms)
        phase = "success" if 200 <= response.status_code < 300 else "error"
        if response.status_code == 429:
            phase = "rate_limited"
        _log_spotify_call(
            phase=phase,
            endpoint=path,
            method=method,
            status_code=response.status_code,
            duration_ms=duration_ms,
            retry_after_sec=retry_after_sec,
            scan_id=scan_id,
            playlist_id=playlist_id,
            country=country,
            keyword=keyword,
        )
        _log_event(
            "spotify_api_response",
            request_id=request_id,
            scan_id=scan_id,
            job_id=job_id,
            method=method,
            path=path,
            status_code=response.status_code,
            duration_ms=duration_ms,
            retry_after=retry_after_header,
            response_size_bytes=response_size,
            response_preview=body_preview,
            attempt=attempt,
        )

        if response.status_code == 429:
            if retry_429_count >= max_429_retries:
                _log_event(
                    "spotify_api_error",
                    request_id=request_id,
                    scan_id=scan_id,
                    job_id=job_id,
                    method=method,
                    path=path,
                    attempt=attempt,
                    status_code=response.status_code,
                    response_preview=body_preview,
                )
                response.raise_for_status()
            retry_429_count += 1
            retry_after_seconds = 2
            if retry_after_header:
                try:
                    retry_after_seconds = int(retry_after_header)
                except ValueError:
                    retry_after_seconds = 2
            wait_seconds = min(retry_after_seconds, SPOTIFY_MAX_RETRY_AFTER)
            _log_event(
                "spotify_api_retry",
                request_id=request_id,
                scan_id=scan_id,
                job_id=job_id,
                method=method,
                path=path,
                attempt=attempt,
                wait_seconds=wait_seconds,
                retry_after=retry_after_header,
                reason="rate_limited",
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code >= 500 or response.status_code == 408:
            if retry_transient_count < max_transient_retries:
                retry_transient_count += 1
                wait_seconds = _compute_backoff(retry_transient_count, 0.5, 10.0, 0.25)
                _log_event(
                    "spotify_api_retry",
                    request_id=request_id,
                    scan_id=scan_id,
                    job_id=job_id,
                    method=method,
                    path=path,
                    attempt=attempt,
                    wait_seconds=wait_seconds,
                    reason="transient",
                )
                time.sleep(wait_seconds)
                continue
            _log_event(
                "spotify_api_error",
                request_id=request_id,
                scan_id=scan_id,
                job_id=job_id,
                method=method,
                path=path,
                attempt=attempt,
                status_code=response.status_code,
                response_preview=body_preview,
            )
            response.raise_for_status()

        if response.status_code >= 400:
            _log_event(
                "spotify_api_error",
                request_id=request_id,
                scan_id=scan_id,
                job_id=job_id,
                method=method,
                path=path,
                attempt=attempt,
                status_code=response.status_code,
                response_preview=body_preview,
            )
            response.raise_for_status()

        return response.json() or {}, {
            "request_id": request_id,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "retry_after_sec": retry_after_sec,
            "response_size_bytes": response_size,
            "budget_enforced": False,
        }


def extract_playlist_id(text: str):
    text = (text or "").strip()
    m = re.search(r"(?:open\.spotify\.com/playlist/|spotify:playlist:)([A-Za-z0-9]+)", text)
    return m.group(1) if m else None


def normalize_spotify_playlist_url(url: str) -> str:
    return (url or "").split("?", 1)[0].strip()


def get_access_token_payload() -> dict:
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        raise ValueError("SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET missing from environment.")

    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()

    headers = {"Authorization": f"Basic {b64_auth}"}
    data = {"grant_type": "client_credentials"}
    return _spotify_request("POST", TOKEN_URL, headers=headers, data=data)


def _token_body_keys(payload: dict | None) -> list[str] | None:
    if not isinstance(payload, dict):
        return None
    return sorted([str(key) for key in payload.keys()])


def get_access_token(*, scan_id: str | None = None) -> str:
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        raise ValueError("SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET missing from environment.")

    auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    headers = {"Authorization": f"Basic {b64_auth}"}
    data = {"grant_type": "client_credentials"}

    request_id = str(uuid.uuid4())
    missing_token_retries = 0
    max_missing_token_retries = 3
    attempt = 0

    while True:
        attempt += 1
        start_monotonic = time.monotonic()
        _log_spotify_token_fetch(
            phase="start",
            reason=None,
            status_code=None,
            duration_ms=None,
            retry_after_sec=None,
            body_keys=None,
            body_length=None,
            request_id=request_id,
            attempt=attempt,
        )
        try:
            payload, meta = _spotify_request_with_meta(
                "POST",
                TOKEN_URL,
                headers=headers,
                data=data,
                scan_id=scan_id,
            )
        except requests.HTTPError as exc:
            duration_ms = round((time.monotonic() - start_monotonic) * 1000, 2)
            response = exc.response
            status_code = response.status_code if response is not None else None
            retry_after_sec = response.headers.get("Retry-After") if response is not None else None
            body_length = len(response.content or b"") if response is not None else None
            body_keys = None
            if response is not None:
                try:
                    body_keys = _token_body_keys(response.json())
                except ValueError:
                    body_keys = None
            _log_spotify_token_fetch(
                phase="error",
                reason="http_error",
                status_code=status_code,
                duration_ms=duration_ms,
                retry_after_sec=retry_after_sec,
                body_keys=body_keys,
                body_length=body_length,
                request_id=request_id,
                attempt=attempt,
            )
            raise
        except ValueError:
            duration_ms = round((time.monotonic() - start_monotonic) * 1000, 2)
            _log_spotify_token_fetch(
                phase="error",
                reason="invalid_json",
                status_code=None,
                duration_ms=duration_ms,
                retry_after_sec=None,
                body_keys=None,
                body_length=None,
                request_id=request_id,
                attempt=attempt,
            )
            if missing_token_retries >= max_missing_token_retries:
                raise SpotifyTokenError("Spotify token response missing access_token.")
            missing_token_retries += 1
            wait_seconds = _compute_backoff(missing_token_retries, 0.5, 5.0, 0.25)
            time.sleep(wait_seconds)
            continue
        except requests.RequestException:
            duration_ms = round((time.monotonic() - start_monotonic) * 1000, 2)
            _log_spotify_token_fetch(
                phase="error",
                reason="request_exception",
                status_code=None,
                duration_ms=duration_ms,
                retry_after_sec=None,
                body_keys=None,
                body_length=None,
                request_id=request_id,
                attempt=attempt,
            )
            raise
        duration_ms = meta.get("duration_ms")
        status_code = meta.get("status_code")
        retry_after_sec = meta.get("retry_after_sec")
        body_length = meta.get("response_size_bytes")
        body_keys = _token_body_keys(payload)
        access_token = payload.get("access_token")
        if access_token:
            _log_spotify_token_fetch(
                phase="success",
                reason=None,
                status_code=status_code,
                duration_ms=duration_ms,
                retry_after_sec=retry_after_sec,
                body_keys=body_keys,
                body_length=body_length,
                request_id=request_id,
                attempt=attempt,
            )
            return access_token

        _log_spotify_token_fetch(
            phase="error",
            reason="missing_access_token",
            status_code=status_code,
            duration_ms=duration_ms,
            retry_after_sec=retry_after_sec,
            body_keys=body_keys,
            body_length=body_length,
            request_id=request_id,
            attempt=attempt,
        )
        if missing_token_retries >= max_missing_token_retries:
            raise SpotifyTokenError("Spotify token response missing access_token.")
        missing_token_retries += 1
        wait_seconds = _compute_backoff(missing_token_retries, 0.5, 5.0, 0.25)
        time.sleep(wait_seconds)


def spotify_get(
    url: str,
    token: str,
    params=None,
    *,
    scan_id: str | None = None,
    job_id: str | None = None,
    playlist_id: str | None = None,
    country: str | None = None,
    keyword: str | None = None,
):
    return _spotify_request(
        "GET",
        url,
        token=token,
        params=params,
        scan_id=scan_id,
        job_id=job_id,
        playlist_id=playlist_id,
        country=country,
        keyword=keyword,
    )


def get_spotify_markets():
    token = get_access_token()
    data = spotify_get(MARKETS_URL, token)
    markets = data.get("markets") or []
    return [market for market in markets if isinstance(market, str)]


def search_playlists(keyword: str, market: str, token: str, limit: int = 50, offset: int = 0):
    params = {
        "q": keyword,
        "type": "playlist",
        "market": market,
        "limit": limit,
        "offset": offset,
    }
    data = spotify_get(SEARCH_URL, token, params=params, country=market, keyword=keyword)
    items = ((data.get("playlists") or {}).get("items") or [])
    items = [item for item in items if isinstance(item, dict)]
    return items


def search_playlists_with_pagination(
    keyword: str,
    market: str,
    token: str,
    target_count: int = RESULTS_LIMIT,
):
    offsets = [0, 50, 100]
    collected = []
    seen_ids = set()

    for offset in offsets:
        items = search_playlists(keyword, market, token, limit=50, offset=offset)
        for item in items:
            playlist_id = item.get("id")
            if playlist_id and playlist_id in seen_ids:
                continue

            if playlist_id:
                seen_ids.add(playlist_id)
            collected.append(item)

            if len(collected) >= target_count:
                break

        if len(collected) >= target_count:
            break

    actual_count = len(collected)

    if actual_count < target_count:
        placeholder_count = target_count - actual_count
        for _ in range(placeholder_count):
            collected.append(
                {
                    "id": None,
                    "name": "N/A",
                    "external_urls": {"spotify": ""},
                    "description": "",
                    "placeholder": True,
                }
            )

    return collected, actual_count


def get_latest_track_added_at(playlist_id: str, snapshot_id: str, token: str) -> str | None:
    if not playlist_id or not snapshot_id:
        return None

    latest_dt: datetime | None = None
    limit = 50
    offset = 0

    while True:
        data = spotify_get(
            f"{PLAYLIST_URL.format(playlist_id)}/tracks",
            token,
            params={
                "fields": "items(added_at),total",
                "limit": limit,
                "offset": offset,
            },
            playlist_id=playlist_id,
        )

        items = data.get("items") or []
        total = data.get("total")
        total = total if isinstance(total, int) else offset + len(items)

        for item in items:
            added_at = item.get("added_at")
            if not added_at:
                continue
            try:
                added_dt = datetime.fromisoformat(added_at.replace("Z", "+00:00"))
            except ValueError:
                continue
            if added_dt.tzinfo is None:
                added_dt = added_dt.replace(tzinfo=timezone.utc)

            if latest_dt is None or added_dt > latest_dt:
                latest_dt = added_dt

        offset += limit
        if offset >= total or not items:
            break

    if latest_dt is None:
        return None

    return (
        latest_dt.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def fetch_playlist_details(playlist_ids, token: str, cache: dict):
    unique_ids = [pid for pid in dict.fromkeys(playlist_ids) if pid and pid not in cache]
    if not unique_ids:
        return

    def fetch_one(pid: str):
        detail = spotify_get(
            PLAYLIST_URL.format(pid),
            token,
            params={
                "fields": "name,external_urls.spotify,followers.total,tracks.total,description,images,snapshot_id,owner.display_name,owner.id",
            },
            playlist_id=pid,
        )
        followers = (detail.get("followers") or {}).get("total")
        tracks_total = (detail.get("tracks") or {}).get("total")
        snapshot_id = detail.get("snapshot_id")
        owner_info = detail.get("owner") or {}
        playlist_owner = owner_info.get("display_name") or owner_info.get("id")
        playlist_last_track_added_at = None
        if tracks_total and snapshot_id:
            playlist_last_track_added_at = get_latest_track_added_at(pid, snapshot_id, token)
        images = detail.get("images") or []
        playlist_image_url = images[0].get("url") if images else ""
        cache[pid] = {
            "playlist_name": detail.get("name", "-"),
            "playlist_url": (detail.get("external_urls") or {}).get("spotify", ""),
            "playlist_description": detail.get("description", ""),
            "playlist_followers": followers,
            "songs_count": tracks_total,
            "playlist_last_track_added_at": playlist_last_track_added_at,
            "playlist_image": playlist_image_url,
            "playlist_image_url": playlist_image_url,
            "playlist_snapshot_id": snapshot_id,
            "playlist_owner": playlist_owner,
        }
        time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_pid = {executor.submit(fetch_one, pid): pid for pid in unique_ids}
        for future in as_completed(future_to_pid):
            try:
                future.result()
            except Exception:
                failed_pid = future_to_pid.get(future)
                if failed_pid:
                    cache[failed_pid] = {
                        "playlist_name": "-",
                        "playlist_url": "",
                        "playlist_description": "",
                        "playlist_followers": None,
                        "songs_count": None,
                        "playlist_last_track_added_at": None,
                        "playlist_image": "",
                        "playlist_owner": None,
                    }


def fetch_spotify_playlist_metadata(playlist_id: str, token: str | None = None) -> dict:
    token = token or get_access_token()
    meta_cache: dict[str, dict] = {}
    fetch_playlist_details([playlist_id], token, meta_cache)
    base_meta = meta_cache.get(playlist_id) or {}
    playlist_url = base_meta.get("playlist_url") or f"https://open.spotify.com/playlist/{playlist_id}"
    base_meta["playlist_url"] = playlist_url
    base_meta["playlist_image_url"] = (
        base_meta.get("playlist_image") or base_meta.get("playlist_image_url") or ""
    )
    return base_meta
