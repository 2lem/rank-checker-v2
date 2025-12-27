import base64
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from app.core.config import (
    MARKETS_URL,
    PLAYLIST_URL,
    RESULTS_LIMIT,
    SEARCH_URL,
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
    SPOTIFY_REQUEST_TIMEOUT,
    TOKEN_URL,
)


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

    response = requests.post(
        TOKEN_URL,
        headers=headers,
        data=data,
        timeout=SPOTIFY_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json() or {}


def get_access_token() -> str:
    payload = get_access_token_payload()
    access_token = payload.get("access_token")
    if not access_token:
        raise ValueError("Spotify token response missing access_token.")
    return access_token


def spotify_get(url: str, token: str, params=None):
    headers = {"Authorization": f"Bearer {token}"}

    while True:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=SPOTIFY_REQUEST_TIMEOUT,
        )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "2")
            try:
                retry_after_seconds = int(retry_after)
            except ValueError:
                retry_after_seconds = 2
            time.sleep(retry_after_seconds)
            continue
        response.raise_for_status()
        return response.json() or {}


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
    data = spotify_get(SEARCH_URL, token, params=params)
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
