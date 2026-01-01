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
        status = None
        next_status_poll = 0.0
        started_at = time.monotonic()
        scan_payload: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            now = time.monotonic()
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

            sleep_for = next_status_poll - time.monotonic()
            if sleep_for > 0:
                time.sleep(min(sleep_for, 1.0))

        payload["scan_total_duration_s"] = round(time.monotonic() - started_at, 2)

        if status not in {"completed", "completed_partial"}:
            payload["timeout_verdict"] = "NOT CONFIRMED"
            payload["limiter_verdict"] = "NOT ACTIVE"
            payload["safety_verdict"] = "NOT SAFE"
            return _finalize(payload, exit_code=1)

        if isinstance(scan_payload, dict):
            payload["spotify_total_calls"] = scan_payload.get("spotify_total_calls")
            payload["peak_rps"] = scan_payload.get("peak_rps")
            payload["avg_rps"] = scan_payload.get("avg_rps")
            payload["min_inter_start_s"] = scan_payload.get("min_inter_start_s")
            payload["any_429_count"] = scan_payload.get("any_429_count")

        peak_rps = payload.get("peak_rps")
        avg_rps = payload.get("avg_rps")
        min_inter_start_s = payload.get("min_inter_start_s")

        min_expected_interval = 1.0 / expected_rps if expected_rps > 0 else 0.0

        if peak_rps is None or min_inter_start_s is None:
            limiter_verdict = "PARTIAL"
        elif peak_rps <= expected_rps and min_inter_start_s >= min_expected_interval:
            limiter_verdict = "ACTIVE"
        elif peak_rps <= expected_rps * 1.5:
            limiter_verdict = "PARTIAL"
        else:
            limiter_verdict = "NOT ACTIVE"
        payload["limiter_verdict"] = limiter_verdict

        any_429_count = payload.get("any_429_count")
        if peak_rps is None or avg_rps is None or any_429_count is None:
            safety_verdict = "NOT SAFE"
        elif peak_rps <= 2.0 and avg_rps <= 2.0 and any_429_count == 0:
            safety_verdict = "SAFE"
        elif peak_rps <= 2.2 and avg_rps <= 2.2 and any_429_count == 0:
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
        if any_429_count and any_429_count > 0:
            return _finalize(payload, exit_code=1)
        if safety_verdict == "NOT SAFE":
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
