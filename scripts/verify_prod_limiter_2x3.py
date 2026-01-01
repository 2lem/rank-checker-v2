from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

DEFAULT_BASE_URL = "https://rank-checker-v2-production.up.railway.app"
TARGET_COUNTRIES = ["US", "GB"]
TARGET_KEYWORDS = ["pop", "indie", "focus"]
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


def main() -> int:
    base_url = os.getenv("BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    max_runtime_minutes = float(os.getenv("VERIFY_TIMEOUT_MINUTES", "15"))
    expected_rps = float(os.getenv("SPOTIFY_GLOBAL_RPS", "2.0"))

    started_at = time.monotonic()
    payload: dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_url": base_url,
        "countries": TARGET_COUNTRIES,
        "keywords": TARGET_KEYWORDS,
        "scan_id": None,
        "tracked_playlist_id": None,
        "duration_seconds": None,
        "estimated_pair_rps": None,
        "pass": False,
        "reason": None,
    }

    try:
        playlists = _request_json("GET", f"{base_url}/api/playlists")
        if not isinstance(playlists, list):
            raise RuntimeError("Unexpected playlists payload")
        candidate = _find_candidate(playlists)
        if not candidate:
            raise RuntimeError("No tracked playlist found with <=2 countries and <=3 keywords")

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

        scan_response = _request_json(
            "POST",
            f"{base_url}/api/basic-rank-checker/scans",
            json={"tracked_playlist_id": tracked_playlist_id},
        )
        scan_id = scan_response.get("scan_id") if isinstance(scan_response, dict) else None
        if not scan_id:
            raise RuntimeError("Scan did not return scan_id")
        payload["scan_id"] = scan_id

        deadline = time.monotonic() + (max_runtime_minutes * 60)
        poll_interval = 5
        status = None
        while time.monotonic() < deadline:
            scan_payload = _request_json(
                "GET",
                f"{base_url}/api/basic-rank-checker/scans/{scan_id}",
                timeout=45,
            )
            status = scan_payload.get("status") if isinstance(scan_payload, dict) else None
            if status in STATUS_TERMINAL:
                break
            time.sleep(poll_interval)
            poll_interval = min(poll_interval + 5, 20)

        duration = time.monotonic() - started_at
        payload["duration_seconds"] = round(duration, 2)

        total_pairs = len(final_countries) * len(final_keywords)
        estimated_pair_rps = (total_pairs / duration) if duration > 0 else None
        payload["estimated_pair_rps"] = round(estimated_pair_rps, 3) if estimated_pair_rps else None

        if status not in {"completed", "completed_partial"}:
            payload["reason"] = f"scan ended with status={status}"
            return _finalize(payload, exit_code=1)

        allowed_rps = expected_rps * 1.1
        if estimated_pair_rps and estimated_pair_rps > allowed_rps:
            payload["reason"] = (
                "estimated per-pair rps exceeds expected limiter "
                f"({estimated_pair_rps:.2f} > {allowed_rps:.2f})"
            )
            return _finalize(payload, exit_code=1)

        payload["pass"] = True
        payload["reason"] = "scan completed and pacing within expected bounds"
        return _finalize(payload, exit_code=0)
    except Exception as exc:
        payload["reason"] = f"error: {exc}"
        return _finalize(payload, exit_code=1)


def _finalize(payload: dict[str, Any], *, exit_code: int) -> int:
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    output_path = artifacts_dir / "verify_prod_limiter_2x3.json"
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    print("=== Prod limiter human-sim verification ===")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
