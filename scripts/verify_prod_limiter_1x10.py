from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

DEFAULT_BASE_URL = "https://rank-checker-v2-production.up.railway.app"
TARGET_COUNTRIES = ["US"]
TARGET_KEYWORDS = [
    "pop",
    "indie",
    "focus",
    "dance",
    "hip hop",
    "rock",
    "jazz",
    "classical",
    "chill",
    "workout",
]
STATUS_TERMINAL = {"completed", "completed_partial", "failed", "cancelled"}


def _request_json(method: str, url: str, **kwargs: Any) -> Any:
    timeout = kwargs.pop("timeout", 30)
    response = requests.request(method, url, timeout=timeout, **kwargs)
    response.raise_for_status()
    if response.content:
        return response.json()
    return None


def _unique_preserve(entries: list[str], *, key_fn, out_fn) -> list[str]:
    seen = set()
    output: list[str] = []
    for entry in entries:
        raw = (entry or "").strip()
        if not raw:
            continue
        key = key_fn(raw)
        if key in seen:
            continue
        seen.add(key)
        output.append(out_fn(raw))
    return output


def _build_targets(
    existing: list[str],
    desired: list[str],
    target_count: int,
    *,
    key_fn,
    out_fn,
    label: str,
) -> list[str]:
    current = _unique_preserve(existing, key_fn=key_fn, out_fn=out_fn)
    current_keys = {key_fn(item) for item in current}
    if len(current) > target_count:
        raise RuntimeError(
            f"Cannot shrink {label} to {target_count}; existing={current}"
        )
    desired_clean = _unique_preserve(desired, key_fn=key_fn, out_fn=out_fn)
    for entry in desired_clean:
        if len(current) >= target_count:
            break
        entry_key = key_fn(entry)
        if entry_key in current_keys:
            continue
        current.append(entry)
        current_keys.add(entry_key)
    if len(current) != target_count:
        raise RuntimeError(
            f"Unable to build {label} list of {target_count}; got {current}"
        )
    return current


def _find_candidate(playlists: list[dict[str, Any]]) -> dict[str, Any] | None:
    for playlist in playlists:
        playlist_id = playlist.get("id")
        if not playlist_id:
            continue
        countries = playlist.get("target_countries") or []
        keywords = playlist.get("target_keywords") or []
        if len(countries) <= len(TARGET_COUNTRIES) and len(keywords) <= len(TARGET_KEYWORDS):
            return playlist
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


def main() -> int:
    base_url = os.getenv("BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    max_runtime_minutes = float(os.getenv("VERIFY_TIMEOUT_MINUTES", "20"))
    expected_rps = float(os.getenv("SPOTIFY_GLOBAL_RPS", "2.0"))
    debug_token = os.getenv("DEBUG_TOKEN")

    started_at = time.monotonic()
    payload: dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_url": base_url,
        "countries": TARGET_COUNTRIES,
        "keywords": TARGET_KEYWORDS,
        "scan_id": None,
        "tracked_playlist_id": None,
        "post_duration_ms": None,
        "spotify_total_calls": None,
        "peak_rps": None,
        "avg_rps": None,
        "any_429_count": None,
        "limiter_verdict": None,
        "safety_verdict": None,
        "limiter_evidence": None,
        "duration_seconds": None,
        "pass": False,
        "reason": None,
    }

    try:
        playlists = _request_json("GET", f"{base_url}/api/playlists")
        if not isinstance(playlists, list):
            raise RuntimeError("Unexpected playlists payload")
        candidate = _find_candidate(playlists)
        if not candidate:
            raise RuntimeError("No tracked playlist found with <=1 country and <=10 keywords")

        tracked_playlist_id = candidate["id"]
        payload["tracked_playlist_id"] = tracked_playlist_id

        existing_countries = candidate.get("target_countries") or []
        existing_keywords = candidate.get("target_keywords") or []

        final_countries = _build_targets(
            existing_countries,
            TARGET_COUNTRIES,
            len(TARGET_COUNTRIES),
            key_fn=str.upper,
            out_fn=str.upper,
            label="target_countries",
        )
        final_keywords = _build_targets(
            existing_keywords,
            TARGET_KEYWORDS,
            len(TARGET_KEYWORDS),
            key_fn=str.lower,
            out_fn=lambda value: value.strip(),
            label="target_keywords",
        )

        if final_countries != existing_countries or final_keywords != existing_keywords:
            _request_json(
                "PATCH",
                f"{base_url}/api/playlists/{tracked_playlist_id}/targets",
                json={
                    "target_countries": final_countries,
                    "target_keywords": final_keywords,
                },
            )

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
            f"{base_url}/api/basic-rank-checker/scans",
            json={"tracked_playlist_id": tracked_playlist_id},
        )
        payload["post_duration_ms"] = round((time.perf_counter() - post_start) * 1000, 2)

        scan_id = scan_response.get("scan_id") if isinstance(scan_response, dict) else None
        if not scan_id:
            raise RuntimeError("Scan did not return scan_id")
        payload["scan_id"] = scan_id

        deadline = time.monotonic() + (max_runtime_minutes * 60)
        poll_interval = 5.0
        metrics_interval = 1.0
        status = None
        next_status_poll = 0.0
        next_metrics_poll = 0.0
        samples: list[tuple[float, int, int]] = []
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

        duration = time.monotonic() - started_at
        payload["duration_seconds"] = round(duration, 2)

        if status not in {"completed", "completed_partial"}:
            payload["reason"] = f"scan ended with status={status}"
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
            duration_seconds = max(duration, 0.001)

        peak_rps = _window_peak_rps(start_times)
        avg_rps = round(spotify_total_calls / duration_seconds, 3) if duration_seconds else 0.0
        payload["peak_rps"] = round(peak_rps, 3)
        payload["avg_rps"] = avg_rps

        min_interval_ms = _min_interval_ms(start_times)
        min_interval_s = round(min_interval_ms / 1000, 4) if min_interval_ms is not None else None
        throttled_wait_ms_total = _throttled_wait_ms_total(start_times, expected_rps)
        limiter_evidence = (
            "observed_inter_start_min_interval_s="
            f"{min_interval_s} throttled_wait_ms_total={throttled_wait_ms_total} "
            f"peak_rps={payload['peak_rps']}"
        )
        payload["min_interval_s"] = min_interval_s
        payload["throttled_wait_ms_total"] = throttled_wait_ms_total
        payload["limiter_evidence"] = limiter_evidence

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

        if safety_verdict != "SAFE":
            payload["reason"] = "unsafe or borderline pacing detected"
            return _finalize(payload, exit_code=1)

        if limiter_verdict == "NOT ACTIVE":
            payload["reason"] = "limiter did not appear active"
            return _finalize(payload, exit_code=1)

        if payload["post_duration_ms"] is None or payload["post_duration_ms"] >= 1000:
            payload["reason"] = "scan start exceeded 1s timeout threshold"
            return _finalize(payload, exit_code=1)

        payload["pass"] = True
        payload["reason"] = "scan completed; pacing within safety bounds"
        return _finalize(payload, exit_code=0)
    except Exception as exc:
        payload["reason"] = f"error: {exc}"
        return _finalize(payload, exit_code=1)


def _finalize(payload: dict[str, Any], *, exit_code: int) -> int:
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    output_path = artifacts_dir / "verify_prod_limiter_1x10.json"
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    print("=== Prod limiter human-sim verification (1x10) ===")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
