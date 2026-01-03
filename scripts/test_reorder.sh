#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

python - <<'PY'
import json
import os
import sys
import urllib.request

base_url = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


def request(method, path, payload=None):
    url = f"{base_url}{path}"
    headers = {}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as response:
        body = response.read()
        if not body:
            return None
        return json.loads(body)


playlists = request("GET", "/api/playlists") or []
playlist_ids = [entry["id"] for entry in playlists if "id" in entry]

if not playlist_ids:
    print("No tracked playlists found; skipping reorder test.")
    sys.exit(0)

reversed_ids = list(reversed(playlist_ids))
request("PATCH", "/api/playlists/reorder", {"ordered_ids": reversed_ids})

updated = request("GET", "/api/playlists") or []
updated_ids = [entry["id"] for entry in updated if "id" in entry]

if updated_ids != reversed_ids:
    print("Reorder failed: playlist order did not match expected result.")
    print(f"Expected: {reversed_ids}")
    print(f"Actual:   {updated_ids}")
    sys.exit(1)

print("Reorder test passed.")
PY
