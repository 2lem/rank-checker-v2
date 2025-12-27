import logging
from datetime import datetime, timezone

import requests
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.spotify import PLAYLIST_URL, get_access_token, get_latest_track_added_at, spotify_get
from app.repositories.tracked_playlists import get_tracked_playlist_by_id

logger = logging.getLogger(__name__)


def select_smallest_image_url(images: list[dict]) -> str | None:
    if not images:
        return None
    sorted_images = sorted(
        images,
        key=lambda image: (
            image.get("width") or image.get("height") or float("inf"),
            image.get("height") or image.get("width") or float("inf"),
        ),
    )
    return (sorted_images[0].get("url") or "").strip() or None


def select_largest_image_url(images: list[dict]) -> str | None:
    if not images:
        return None
    sorted_images = sorted(
        images,
        key=lambda image: (
            image.get("width") or image.get("height") or 0,
            image.get("height") or image.get("width") or 0,
        ),
        reverse=True,
    )
    return (sorted_images[0].get("url") or "").strip() or None


def parse_spotify_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_spotify_error_details(exc: Exception) -> tuple[str | None, str]:
    response = getattr(exc, "response", None)
    if response is None:
        return None, ""
    status_code = getattr(response, "status_code", None)
    body_text = (getattr(response, "text", "") or "").strip()
    if len(body_text) > 300:
        body_text = body_text[:300]
    return status_code, body_text


def build_spotify_url(url: str, params: dict | None) -> str:
    request = requests.Request("GET", url, params=params).prepare()
    return request.url or url


def raise_spotify_request_error(playlist_id: str, exc: Exception) -> None:
    status_code, body_snippet = _extract_spotify_error_details(exc)
    status_label = status_code if status_code is not None else "unknown"
    logger.warning(
        "Spotify request failed for playlist %s: status=%s, body=%s",
        playlist_id,
        status_label,
        body_snippet or "<empty>",
    )
    detail = f"Spotify request failed: status={status_label}, body={body_snippet}"
    raise HTTPException(status_code=502, detail=detail) from exc


def _resolve_playlist_last_updated_at(
    playlist_id: str, *, snapshot_id: str | None, tracks_count: int | None, token: str
) -> datetime | None:
    if not (tracks_count and snapshot_id):
        return None
    try:
        last_track_added_at = get_latest_track_added_at(playlist_id, snapshot_id, token)
    except Exception as exc:
        logger.warning("Failed to fetch latest track added for %s: %s", playlist_id, exc)
        return None
    return parse_spotify_timestamp(last_track_added_at)


def refresh_playlist_metadata(db: Session, tracked_playlist_id: str):
    tracked = get_tracked_playlist_by_id(db, tracked_playlist_id)
    if not tracked:
        raise HTTPException(status_code=404, detail="Tracked playlist not found.")

    token = get_access_token()
    playlist_api_url = PLAYLIST_URL.format(tracked.playlist_id)
    default_params = {
        "fields": (
            "name,description,images,owner.display_name,followers.total,tracks.total,"
            "external_urls.spotify,snapshot_id"
        ),
    }
    fallback_market = (tracked.target_countries or [None])[0] or "US"
    fallback_params = {
        "fields": (
            "name,description,images,owner.display_name,followers.total,tracks.total,"
            "external_urls.spotify,snapshot_id"
        ),
        "market": fallback_market,
    }
    attempted_urls: list[str] = []

    try:
        attempted_urls.append(build_spotify_url(playlist_api_url, default_params))
        detail = spotify_get(playlist_api_url, token, params=default_params)
    except requests.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code == 404:
            try:
                attempted_urls.append(build_spotify_url(playlist_api_url, fallback_params))
                detail = spotify_get(playlist_api_url, token, params=fallback_params)
            except requests.HTTPError as fallback_exc:
                fallback_status = getattr(
                    getattr(fallback_exc, "response", None), "status_code", None
                )
                if fallback_status == 404:
                    message = (
                        "Playlist not accessible via Spotify API (often editorial/region restriction). "
                        "Try a user playlist or a different market."
                    )
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "message": message,
                            "playlist_id": tracked.playlist_id,
                            "attempted_urls": attempted_urls,
                        },
                    ) from fallback_exc
                raise_spotify_request_error(tracked.playlist_id, fallback_exc)
            except Exception as fallback_exc:
                raise_spotify_request_error(tracked.playlist_id, fallback_exc)
        else:
            raise_spotify_request_error(tracked.playlist_id, exc)
    except Exception as exc:
        raise_spotify_request_error(tracked.playlist_id, exc)

    images = detail.get("images") or []
    owner_info = detail.get("owner") or {}
    snapshot_id = detail.get("snapshot_id")
    followers_total = (detail.get("followers") or {}).get("total")
    tracks_count = (detail.get("tracks") or {}).get("total")

    tracked.name = detail.get("name")
    tracked.description = detail.get("description")
    tracked.playlist_url = (detail.get("external_urls") or {}).get("spotify") or tracked.playlist_url
    tracked.cover_image_url_small = select_smallest_image_url(images)
    tracked.cover_image_url_large = select_largest_image_url(images)
    tracked.owner_name = owner_info.get("display_name") or owner_info.get("id")
    tracked.followers_total = followers_total
    tracked.tracks_count = tracks_count
    tracked.playlist_last_updated_at = _resolve_playlist_last_updated_at(
        tracked.playlist_id,
        snapshot_id=snapshot_id,
        tracks_count=tracks_count,
        token=token,
    )
    tracked.last_meta_refresh_at = datetime.now(timezone.utc)

    db.add(tracked)
    db.commit()
    db.refresh(tracked)
    return tracked
