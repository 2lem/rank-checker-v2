from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

DEFAULT_BASE_URL = "https://rank-checker-v2-production.up.railway.app"
DEFAULT_TRACKED_PLAYLIST_ID = "6bc10d07-d549-49c7-aa0e-bdf0a1c085bb"
TARGET_COUNTRIES = ["US"]
TARGET_KEYWORDS = [
    "k1",
    "k2",
    "k3",
    "k4",
    "k5",
    "k6",
    "k7",
    "k8",
    "k9",
    "k10",
]
STATUS_TERMINAL = {"completed", "completed_partial", "failed", "cancelled"}
ARTIFACT_PATH = Path("artifacts/prod_diagnose_manual_1x10.json")
TIMEOUT_SECONDS = 20 * 60


def _request_json(method: str, url: str, **kwargs: Any) -> Any:
    timeout = kwargs.pop("timeout", 30)
    response = requests.request(method, url, timeout=timeout, **kwargs)
    response.raise_for_status()
    if response.content:
        return response.json()
    return None


def _request_json_allow_404(method: str, url: str, **kwargs: Any) -> Any:
    timeout = kwargs.pop("timeout", 30)
    response = requests.request(method, url, timeout=timeout, **kwargs)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    if response.content:
        return response.json()
    return None


def _spotify_metrics(base_url: str, debug_token: str | None) -> dict[str, Any]:
    headers = {}
    if debug_token:
        headers["X-Debug-Token"] = debug_token
    return _request_json("GET", f"{base_url}/api/debug/spotify-metrics", headers=headers)


def _window_peak_rps(start_times: list[float]) -> float:
    if not start_times:
        return 0.0
    start_times.sort()
    peak = 1
    left = 0
    for right in range(len(start_times)):
        while start_times[right] - start_times[left] > 1.0:
            left += 1
        peak = max(peak, right - left + 1)
    return float(peak)


def _min_interval_ms(start_times: list[float]) -> float | None:
    if len(start_times) < 2:
        return None
    start_times.sort()
    min_interval = min(
        start_times[idx] - start_times[idx - 1] for idx in range(1, len(start_times))
    )
    return round(min_interval * 1000, 2)


def _throttled_wait_ms_total(start_times: list[float], expected_rps: float) -> float:
    if len(start_times) < 2 or expected_rps <= 0:
        return 0.0
    start_times.sort()
    min_interval = 1.0 / expected_rps
    total_wait = 0.0
    for idx in range(1, len(start_times)):
        interval = start_times[idx] - start_times[idx - 1]
        if interval > min_interval:
            total_wait += interval - min_interval
    return round(total_wait * 1000, 2)


def _finalize(payload: dict[str, Any], *, exit_code: int) -> int:
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True))

    print("FINAL VERDICT")
    print(f"Timeout fix: {payload.get('timeout_verdict')}")
    print(f"Limiter: {payload.get('limiter_verdict')}")
    print(f"Safety: {payload.get('safety_verdict')}")
    print(json.dumps(payload, indent=2, sort_keys=True))

    return exit_code


def main() -> int:
    base_url = os.getenv("BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    tracked_playlist_id = os.getenv(
        "DIAG_TRACKED_PLAYLIST_ID", DEFAULT_TRACKED_PLAYLIST_ID
    ).strip()
    debug_token = os.getenv("DEBUG_TOKEN")
    expected_rps = float(os.getenv("SPOTIFY_GLOBAL_RPS", "2.0"))

    payload: dict[str, Any] = {
        "base_url": base_url,
        "tracked_playlist_id": tracked_playlist_id,
        "scan_started_count": 0,
        "scan_id": None,
        "post_duration_ms": None,
        "scan_total_duration_s": None,
        "spotify_total_calls": None,
        "peak_rps": None,
        "avg_rps": None,
        "min_inter_start_s": None,
        "any_429_count": None,
        "limiter_verdict": None,
        "safety_verdict": None,
        "timeout_verdict": None,
    }

    try:
        playlist = _request_json("GET", f"{base_url}/api/playlists/{tracked_playlist_id}")
        playlist_url = playlist.get("playlist_url") if isinstance(playlist, dict) else None
        if not playlist_url:
            raise RuntimeError("Tracked playlist missing playlist_url")

        active_scan = _request_json_allow_404(
            "GET",
            f"{base_url}/api/basic-rank-checker/scans/active",
            params={"tracked_playlist_id": tracked_playlist_id},
        )
        if active_scan:
            payload["timeout_verdict"] = "NOT CONFIRMED"
            payload["limiter_verdict"] = "PARTIAL"
            payload["safety_verdict"] = "BORDERLINE"
            payload["any_429_count"] = 0
            return _finalize(payload, exit_code=1)

        try:
            metrics_snapshot = _spotify_metrics(base_url, debug_token)
        except Exception as exc:
            raise RuntimeError("spotify metrics unavailable; check DEBUG_TOKEN/DEBUG_TOOLS") from exc

        if not isinstance(metrics_snapshot, dict):
            raise RuntimeError("Unexpected spotify metrics payload")
        baseline_total = int(metrics_snapshot.get("total_requests", 0))
        baseline_429 = int(metrics_snapshot.get("status_429_count", 0))

        post_start = time.perf_counter()
        scan_response = _request_json(
            "POST",
            f"{base_url}/api/scans/manual",
            json={
                "playlist_url": playlist_url,
                "target_countries": TARGET_COUNTRIES,
                "target_keywords": TARGET_KEYWORDS,
            },
        )
        payload["post_duration_ms"] = round((time.perf_counter() - post_start) * 1000, 2)

        scan_id = scan_response.get("scan_id") if isinstance(scan_response, dict) else None
        if not scan_id:
            raise RuntimeError("Scan did not return scan_id")
        payload["scan_id"] = scan_id
        payload["scan_started_count"] = 1

        deadline = time.monotonic() + TIMEOUT_SECONDS
        poll_interval = 5.0
        metrics_interval = 1.0
        status = None
        next_status_poll = 0.0
        next_metrics_poll = 0.0
        samples: list[tuple[float, int, int]] = []
        started_at = time.monotonic()
        while time.monotonic() < deadline:
            now = time.monotonic()
            if now >= next_metrics_poll:
                metrics_snapshot = _spotify_metrics(base_url, debug_token)
                total_requests = int(metrics_snapshot.get("total_requests", 0))
                status_429 = int(metrics_snapshot.get("status_429_count", 0))
                samples.append((now, total_requests, status_429))
                next_metrics_poll = now + metrics_interval
            if now >= next_status_poll:
                scan_payload = _request_json(
                    "GET",
                    f"{base_url}/api/basic-rank-checker/scans/{scan_id}",
                    timeout=45,
                )
                status = scan_payload.get("status") if isinstance(scan_payload, dict) else None
                if status in STATUS_TERMINAL:
                    break
                next_status_poll = now + poll_interval
                poll_interval = min(poll_interval + 5, 20)

            sleep_for = min(next_metrics_poll, next_status_poll) - time.monotonic()
            if sleep_for > 0:
                time.sleep(min(sleep_for, 1.0))

        payload["scan_total_duration_s"] = round(time.monotonic() - started_at, 2)

        if status not in {"completed", "completed_partial"}:
            payload["timeout_verdict"] = "NOT CONFIRMED"
            payload["limiter_verdict"] = "NOT ACTIVE"
            payload["safety_verdict"] = "NOT SAFE"
            return _finalize(payload, exit_code=1)

        if not samples:
            raise RuntimeError("No spotify metrics samples collected")

        samples.sort(key=lambda entry: entry[0])
        last_total = samples[-1][1]
        last_429 = samples[-1][2]
        spotify_total_calls = max(0, last_total - baseline_total)
        any_429_count = max(0, last_429 - baseline_429)
        payload["spotify_total_calls"] = spotify_total_calls
        payload["any_429_count"] = any_429_count

        start_times: list[float] = []
        for idx in range(1, len(samples)):
            _, total_prev, _ = samples[idx - 1]
            current_time, total_current, _ = samples[idx]
            delta = max(0, total_current - total_prev)
            if delta:
                start_times.extend([current_time] * delta)

        if start_times:
            start_times.sort()
            duration_seconds = max(start_times[-1] - start_times[0], 0.001)
        else:
            duration_seconds = max(payload["scan_total_duration_s"] or 0.0, 0.001)

        peak_rps = _window_peak_rps(start_times)
        avg_rps = round(spotify_total_calls / duration_seconds, 3) if duration_seconds else 0.0
        payload["peak_rps"] = round(peak_rps, 3)
        payload["avg_rps"] = avg_rps

        min_interval_ms = _min_interval_ms(start_times)
        payload["min_inter_start_s"] = (
            round(min_interval_ms / 1000, 4) if min_interval_ms is not None else None
        )
        _throttled_wait_ms_total(start_times, expected_rps)

        if payload["peak_rps"] <= expected_rps and payload["avg_rps"] <= expected_rps:
            limiter_verdict = "ACTIVE"
        elif payload["peak_rps"] <= expected_rps * 1.5 and payload["avg_rps"] <= expected_rps * 1.2:
            limiter_verdict = "PARTIAL"
        else:
            limiter_verdict = "NOT ACTIVE"
        payload["limiter_verdict"] = limiter_verdict

        if payload["peak_rps"] <= 2.0 and payload["avg_rps"] <= 2.0 and any_429_count == 0:
            safety_verdict = "SAFE"
        elif payload["peak_rps"] <= 2.2 and payload["avg_rps"] <= 2.2 and any_429_count == 0:
            safety_verdict = "BORDERLINE"
        else:
            safety_verdict = "NOT SAFE"
        payload["safety_verdict"] = safety_verdict

        if payload["post_duration_ms"] is not None and payload["post_duration_ms"] < 1000:
            payload["timeout_verdict"] = "CONFIRMED"
        else:
            payload["timeout_verdict"] = "NOT CONFIRMED"

        if payload["post_duration_ms"] is None or payload["post_duration_ms"] >= 1000:
            return _finalize(payload, exit_code=1)
        if any_429_count > 0:
            return _finalize(payload, exit_code=1)
        if payload["peak_rps"] > 2.0 or payload["avg_rps"] > 2.0:
            return _finalize(payload, exit_code=1)

        return _finalize(payload, exit_code=0)
    except Exception as exc:
        payload["timeout_verdict"] = payload["timeout_verdict"] or "NOT CONFIRMED"
        payload["limiter_verdict"] = payload["limiter_verdict"] or "NOT ACTIVE"
        payload["safety_verdict"] = payload["safety_verdict"] or "NOT SAFE"
        payload["error"] = str(exc)
        return _finalize(payload, exit_code=1)


if __name__ == "__main__":
    sys.exit(main())
