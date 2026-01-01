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
STATUS_TERMINAL = {"completed", "completed_partial", "failed", "cancelled"}
ARTIFACT_PATH = Path("artifacts/prod_diagnose_dedicated_click.json")
TIMEOUT_SECONDS = 20 * 60


@dataclass
class ScanResult:
    scan_id: str | None = None
    status: str | None = None
    completed_at: str | None = None
    total_duration_s: float | None = None


class RequestMetrics:
    def __init__(self) -> None:
        self.start_times: list[float] = []

    def record(self) -> None:
        self.start_times.append(time.monotonic())

    def summary(self) -> dict[str, float | None]:
        if len(self.start_times) < 2:
            return {"peak_rps": None, "avg_rps": None, "min_inter_start_s": None}

        sorted_times = sorted(self.start_times)
        intervals = [
            max(0.0, current - previous)
            for previous, current in zip(sorted_times, sorted_times[1:])
        ]
        min_interval = min(intervals) if intervals else None
        peak_rps = None
        if min_interval and min_interval > 0:
            peak_rps = round(1.0 / min_interval, 3)

        total_window = sorted_times[-1] - sorted_times[0]
        avg_rps = None
        if total_window > 0:
            avg_rps = round(len(sorted_times) / total_window, 3)

        return {
            "peak_rps": peak_rps,
            "avg_rps": avg_rps,
            "min_inter_start_s": round(min_interval, 3) if min_interval else None,
        }


class DiagnosticsClient:
    def __init__(self, base_url: str, metrics: RequestMetrics) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.any_429_count = 0
        self.metrics = metrics

    def request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", 30)
        self.metrics.record()
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
        self.metrics.record()
        response = self.session.request(method, url, timeout=timeout, **kwargs)
        if response.status_code == 429:
            self.any_429_count += 1
        if response.status_code == 404:
            return None
        response.raise_for_status()
        if response.content:
            return response.json()
        return None


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


def _evaluate_limits(
    metrics: dict[str, float | None], *, any_429_count: int
) -> tuple[str, str]:
    peak = metrics.get("peak_rps")
    avg = metrics.get("avg_rps")
    if peak is None or avg is None:
        limiter = "PARTIAL"
        safety = "BORDERLINE"
    else:
        limiter = "ACTIVE" if peak <= 2.0 and avg <= 2.0 else "NOT ACTIVE"
        safety = "SAFE" if peak <= 2.0 and avg <= 2.0 else "NOT SAFE"
    if any_429_count > 0:
        safety = "NOT SAFE"
    return limiter, safety


def _finalize(
    payload: dict[str, Any],
    client: DiagnosticsClient,
    metrics: RequestMetrics,
    *,
    exit_code: int,
) -> int:
    limiter_metrics = metrics.summary()
    limiter_state, safety_state = _evaluate_limits(
        limiter_metrics, any_429_count=client.any_429_count
    )
    payload.update(
        {
            "any_429_count": client.any_429_count,
            "limiter_evidence": limiter_metrics,
        }
    )
    payload["verdict_one_scan_started"] = (
        "One scan started: PASS"
        if payload.get("scan_started_count") == 1
        else "One scan started: FAIL"
    )
    payload["verdict_limiter"] = f"Limiter: {limiter_state}"
    payload["verdict_safety"] = f"Safety: {safety_state}"

    ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True))

    print("FINAL VERDICT")
    print(payload["verdict_one_scan_started"])
    print(payload["verdict_limiter"])
    print(payload["verdict_safety"])

    return exit_code


def main() -> int:
    base_url = os.getenv("BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    tracked_playlist_id = os.getenv(
        "DIAG_TRACKED_PLAYLIST_ID", DEFAULT_TRACKED_PLAYLIST_ID
    ).strip()
    if not tracked_playlist_id:
        raise RuntimeError("DIAG_TRACKED_PLAYLIST_ID is required")

    print("DIAG: prod diagnose dedicated scan starting")

    metrics = RequestMetrics()
    client = DiagnosticsClient(base_url, metrics)
    payload: dict[str, Any] = {
        "github_run_id": os.getenv("GITHUB_RUN_ID"),
        "base_url": base_url,
        "tracked_playlist_id": tracked_playlist_id,
        "scan_started_count": 0,
        "scan_id": None,
        "post_duration_ms": None,
        "scan_total_duration_s": None,
        "any_429_count": 0,
        "limiter_evidence": {
            "peak_rps": None,
            "avg_rps": None,
            "min_inter_start_s": None,
        },
        "verdict_one_scan_started": None,
        "verdict_limiter": None,
        "verdict_safety": None,
    }

    scan_started = False

    try:
        active_scan = client.request_json_allow_404(
            "GET",
            "/api/basic-rank-checker/scans/active",
            params={"tracked_playlist_id": tracked_playlist_id},
        )
        if active_scan and active_scan.get("scan_id"):
            print(
                "DIAG: aborting — active scan exists for "
                f"tracked_playlist_id={tracked_playlist_id}"
            )
            return _finalize(payload, client, metrics, exit_code=1)

        start_time = time.perf_counter()
        scan_response = client.request_json(
            "POST",
            "/api/basic-rank-checker/scans",
            json={"tracked_playlist_id": tracked_playlist_id},
        )
        payload["post_duration_ms"] = int(
            round((time.perf_counter() - start_time) * 1000)
        )

        scan_id = scan_response.get("scan_id") if isinstance(scan_response, dict) else None
        if not scan_id:
            raise RuntimeError("Scan did not return scan_id")
        if scan_started:
            raise RuntimeError("Scan already started; refusing to start another")
        scan_started = True
        payload["scan_started_count"] = 1
        payload["scan_id"] = scan_id
        print(f"DIAG: scan_id={scan_id}")

        scan_result = _poll_scan(client, scan_id, timeout_seconds=TIMEOUT_SECONDS)
        payload["scan_total_duration_s"] = scan_result.total_duration_s

        if scan_result.status in {"completed", "completed_partial"}:
            print("DIAG: scan completed")
        else:
            print(f"DIAG: scan failed with status={scan_result.status}")
            return _finalize(payload, client, metrics, exit_code=1)

        limiter_metrics = metrics.summary()
        peak_rps = limiter_metrics.get("peak_rps")
        avg_rps = limiter_metrics.get("avg_rps")
        if peak_rps is not None and peak_rps > 2.0:
            print("DIAG: peak_rps exceeded limit")
            return _finalize(payload, client, metrics, exit_code=1)
        if avg_rps is not None and avg_rps > 2.0:
            print("DIAG: avg_rps exceeded limit")
            return _finalize(payload, client, metrics, exit_code=1)
        if client.any_429_count > 0:
            print("DIAG: received 429 responses")
            return _finalize(payload, client, metrics, exit_code=1)

        return _finalize(payload, client, metrics, exit_code=0)
    except TimeoutError:
        print("DIAG: scan timed out")
        return _finalize(payload, client, metrics, exit_code=1)
    except Exception as exc:
        print(f"DIAG: scan failed: {exc}")
        return _finalize(payload, client, metrics, exit_code=1)


if __name__ == "__main__":
    sys.exit(main())
