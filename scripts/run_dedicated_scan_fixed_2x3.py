from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

DEFAULT_BASE_URL = "https://rank-checker-v2-production.up.railway.app"
DEFAULT_TRACKED_PLAYLIST_ID = "6bc10d07-d549-49c7-aa0e-bdf0a1c085bb"
TARGET_COUNTRIES = ["US", "GB"]
TARGET_KEYWORDS = ["lofi", "workout", "chill"]
STATUS_TERMINAL = {"completed", "completed_partial", "failed", "cancelled"}
ARTIFACT_PATH = Path("artifacts/dedicated_fixed_2x3_result.json")
TIMEOUT_SECONDS = 20 * 60


@dataclass
class ScanResult:
    scan_id: str | None = None
    status: str | None = None
    completed_at: str | None = None
    total_duration_s: float | None = None


class DiagnosticsClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.any_429_count = 0

    def request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", 30)
        response = self.session.request(method, url, timeout=timeout, **kwargs)
        if response.status_code == 429:
            self.any_429_count += 1
        response.raise_for_status()
        if response.content:
            return response.json()
        return None

    def request_json_allow_404(self, method: str, path: str, **kwargs: Any) -> Any:
        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", 30)
        response = self.session.request(method, url, timeout=timeout, **kwargs)
        if response.status_code == 429:
            self.any_429_count += 1
        if response.status_code == 404:
            return None
        response.raise_for_status()
        if response.content:
            return response.json()
        return None


def _normalize_countries(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for entry in values:
        value = (entry or "").strip().upper()
        if not value or value in cleaned:
            continue
        cleaned.append(value)
    return cleaned


def _normalize_keywords(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for entry in values:
        value = (entry or "").strip().lower()
        if not value or value in cleaned:
            continue
        cleaned.append(value)
    return cleaned


def _assert_subset(existing: list[str], target: list[str], label: str) -> None:
    missing = [entry for entry in existing if entry not in target]
    if missing:
        raise RuntimeError(
            f"Existing {label} contain values outside target list: {missing}"
        )


def _build_targets(existing: list[str], target: list[str], label: str) -> list[str]:
    if len(existing) > len(target):
        raise RuntimeError(
            f"Cannot shrink {label} from {len(existing)} to {len(target)}"
        )
    _assert_subset(existing, target, label)
    return list(target)


def _poll_scan(
    client: DiagnosticsClient, scan_id: str, *, timeout_seconds: int
) -> ScanResult:
    started_at = time.monotonic()
    intervals = [2, 5, 10]
    poll_index = 0
    result = ScanResult(scan_id=scan_id)

    while True:
        elapsed = time.monotonic() - started_at
        if elapsed > timeout_seconds:
            raise TimeoutError("Scan polling exceeded timeout")

        payload = client.request_json("GET", f"/api/basic-rank-checker/scans/{scan_id}")
        status = payload.get("status") if isinstance(payload, dict) else None
        result.status = status
        result.completed_at = payload.get("finished_at") if isinstance(payload, dict) else None

        if status in STATUS_TERMINAL:
            result.total_duration_s = round(elapsed, 2)
            return result

        sleep_seconds = intervals[min(poll_index, len(intervals) - 1)]
        poll_index += 1
        time.sleep(sleep_seconds)


def main() -> int:
    base_url = os.getenv("BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    tracked_playlist_id = os.getenv(
        "DIAG_TRACKED_PLAYLIST_ID", DEFAULT_TRACKED_PLAYLIST_ID
    ).strip()
    if not tracked_playlist_id:
        raise RuntimeError("DIAG_TRACKED_PLAYLIST_ID is required")

    print("DIAG: dedicated fixed playlist scan starting")

    client = DiagnosticsClient(base_url)
    payload = {
        "base_url": base_url,
        "tracked_playlist_id": tracked_playlist_id,
        "countries": TARGET_COUNTRIES,
        "keywords": TARGET_KEYWORDS,
        "scan_id": None,
        "post_duration_ms": None,
        "completed_at": None,
        "total_duration_s": None,
        "any_429_count": 0,
        "limiter_evidence": {
            "peak_rps": None,
            "avg_rps": None,
            "note": "Limiter telemetry not exposed via API.",
        },
        "scan_success": False,
    }

    scan_started = False
    original_targets: dict[str, list[str]] | None = None
    targets_updated = False

    try:
        active_scan = client.request_json_allow_404(
            "GET",
            "/api/basic-rank-checker/scans/active",
            params={"tracked_playlist_id": tracked_playlist_id},
        )
        if active_scan and active_scan.get("scan_id"):
            message = (
                "DIAG: aborting — active scan exists for "
                f"tracked_playlist_id={tracked_playlist_id}"
            )
            print(message)
            return _finalize(payload, client, exit_code=1)

        playlist = client.request_json(
            "GET", f"/api/playlists/{tracked_playlist_id}"
        )
        if not isinstance(playlist, dict):
            raise RuntimeError("Unexpected playlist response")

        existing_countries = _normalize_countries(playlist.get("target_countries") or [])
        existing_keywords = _normalize_keywords(playlist.get("target_keywords") or [])

        final_countries = _build_targets(
            existing_countries, TARGET_COUNTRIES, "target countries"
        )
        final_keywords = _build_targets(
            existing_keywords, TARGET_KEYWORDS, "target keywords"
        )

        original_targets = {
            "target_countries": existing_countries,
            "target_keywords": existing_keywords,
        }

        if final_countries != existing_countries or final_keywords != existing_keywords:
            client.request_json(
                "PATCH",
                f"/api/playlists/{tracked_playlist_id}/targets",
                json={
                    "target_countries": final_countries,
                    "target_keywords": final_keywords,
                },
            )
            targets_updated = True

        idempotency_key = f"dedicated-fixed-2x3-{os.getenv('GITHUB_RUN_ID', 'local')}"
        started_at = time.perf_counter()
        scan_response = client.request_json(
            "POST",
            "/api/basic-rank-checker/scans",
            json={"tracked_playlist_id": tracked_playlist_id},
            headers={"X-Idempotency-Key": idempotency_key},
        )
        post_duration_ms = int(round((time.perf_counter() - started_at) * 1000))
        payload["post_duration_ms"] = post_duration_ms

        scan_id = scan_response.get("scan_id") if isinstance(scan_response, dict) else None
        if not scan_id:
            raise RuntimeError("Scan did not return scan_id")
        if scan_started:
            raise RuntimeError("Scan already started; refusing to start another")
        scan_started = True
        payload["scan_id"] = scan_id
        print(f"DIAG: scan_id={scan_id}")

        scan_result = _poll_scan(client, scan_id, timeout_seconds=TIMEOUT_SECONDS)
        payload["completed_at"] = scan_result.completed_at
        payload["total_duration_s"] = scan_result.total_duration_s

        total_pairs = len(TARGET_COUNTRIES) * len(TARGET_KEYWORDS)
        if scan_result.total_duration_s and scan_result.total_duration_s > 0:
            payload["limiter_evidence"]["avg_rps"] = round(
                total_pairs / scan_result.total_duration_s, 3
            )

        if scan_result.status in {"completed", "completed_partial"}:
            payload["scan_success"] = True
            print("DIAG: scan completed")
            return _finalize(payload, client, exit_code=0)

        print(f"DIAG: scan failed with status={scan_result.status}")
        return _finalize(payload, client, exit_code=1)
    except TimeoutError:
        print("DIAG: scan timed out")
        return _finalize(payload, client, exit_code=1)
    except Exception as exc:
        print(f"DIAG: scan failed: {exc}")
        return _finalize(payload, client, exit_code=1)
    finally:
        if targets_updated and original_targets:
            try:
                client.request_json(
                    "PATCH",
                    f"/api/playlists/{tracked_playlist_id}/targets",
                    json=original_targets,
                )
            except Exception as exc:
                print(f"DIAG: failed to restore targets: {exc}")


def _finalize(payload: dict[str, Any], client: DiagnosticsClient, *, exit_code: int) -> int:
    payload["any_429_count"] = client.any_429_count
    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
