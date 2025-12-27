import logging
from datetime import datetime, timezone
from uuid import UUID

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.spotify import (
    PLAYLIST_URL,
    extract_playlist_id,
    get_access_token,
    get_latest_track_added_at,
    normalize_spotify_playlist_url,
    spotify_get,
)
from app.core.db import get_db
from app.repositories.tracked_playlists import (
    create_tracked_playlist,
    get_tracked_playlist_by_id,
    get_tracked_playlist_by_playlist_id,
    list_tracked_playlists,
    update_tracked_playlist_targets,
)
from app.schemas.playlist import (
    TrackedPlaylistCreate,
    TrackedPlaylistOut,
    TrackedPlaylistTargetsUpdate,
)

router = APIRouter(tags=["playlists"])
logger = logging.getLogger(__name__)


def _select_smallest_image_url(images: list[dict]) -> str | None:
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


def _parse_spotify_timestamp(value: str | None) -> datetime | None:
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


def _build_spotify_url(url: str, params: dict | None) -> str:
    request = requests.Request("GET", url, params=params).prepare()
    return request.url or url


def _raise_spotify_request_error(playlist_id: str, exc: Exception) -> None:
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


def _normalize_list(values: list[str] | None, *, upper: bool = False) -> list[str]:
    if values is None:
        return []
    cleaned: list[str] = []
    for entry in values:
        value = (entry or "").strip()
        if not value:
            continue
        if upper:
            value = value.upper()
        if value not in cleaned:
            cleaned.append(value)
    return cleaned


def _ensure_no_removals(existing: list[str], incoming: list[str], label: str) -> None:
    missing = [entry for entry in existing if entry not in incoming]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Existing {label} cannot be removed. Please contact support.",
        )


@router.get("", response_model=list[TrackedPlaylistOut])
def get_playlists(db: Session = Depends(get_db)):
    return list_tracked_playlists(db)


@router.post("", response_model=TrackedPlaylistOut, status_code=status.HTTP_201_CREATED)
def add_playlist(payload: TrackedPlaylistCreate, db: Session = Depends(get_db)):
    playlist_url = normalize_spotify_playlist_url(payload.playlist_url)
    playlist_id = extract_playlist_id(playlist_url)
    if not playlist_id:
        raise HTTPException(status_code=400, detail="Invalid Spotify playlist URL.")

    existing = get_tracked_playlist_by_playlist_id(db, playlist_id)
    if existing:
        raise HTTPException(status_code=409, detail="Playlist already tracked.")

    token = get_access_token()
    playlist_api_url = PLAYLIST_URL.format(playlist_id)
    default_params = {
        "fields": "name,external_urls.spotify,followers.total,tracks.total,images,snapshot_id,owner.display_name,owner.id",
    }
    fallback_market = (payload.target_countries or [None])[0] or "US"
    fallback_params = {
        "fields": "name,external_urls.spotify,followers.total,tracks.total,images,snapshot_id,owner.display_name,owner.id",
        "market": fallback_market,
    }
    attempted_urls = []

    try:
        attempted_urls.append(_build_spotify_url(playlist_api_url, default_params))
        detail = spotify_get(playlist_api_url, token, params=default_params)
    except requests.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code == 404:
            try:
                attempted_urls.append(_build_spotify_url(playlist_api_url, fallback_params))
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
                            "playlist_id": playlist_id,
                            "attempted_urls": attempted_urls,
                        },
                    ) from fallback_exc
                _raise_spotify_request_error(playlist_id, fallback_exc)
            except Exception as fallback_exc:
                _raise_spotify_request_error(playlist_id, fallback_exc)
        else:
            _raise_spotify_request_error(playlist_id, exc)
    except Exception as exc:
        _raise_spotify_request_error(playlist_id, exc)

    name = detail.get("name")
    resolved_url = (detail.get("external_urls") or {}).get("spotify") or playlist_url
    images = detail.get("images") or []
    cover_image_url_small = _select_smallest_image_url(images)
    owner_info = detail.get("owner") or {}
    owner_name = owner_info.get("display_name") or owner_info.get("id")
    followers_total = (detail.get("followers") or {}).get("total")
    tracks_count = (detail.get("tracks") or {}).get("total")
    snapshot_id = detail.get("snapshot_id")
    playlist_last_updated_at = None
    if tracks_count and snapshot_id:
        try:
            last_track_added_at = get_latest_track_added_at(playlist_id, snapshot_id, token)
            playlist_last_updated_at = _parse_spotify_timestamp(last_track_added_at)
        except Exception as exc:
            logger.warning("Failed to fetch latest track added for %s: %s", playlist_id, exc)
    last_meta_scan_at = datetime.now(timezone.utc)

    return create_tracked_playlist(
        db,
        playlist_id=playlist_id,
        playlist_url=resolved_url,
        name=name,
        cover_image_url_small=cover_image_url_small,
        owner_name=owner_name,
        followers_total=followers_total,
        tracks_count=tracks_count,
        last_meta_scan_at=last_meta_scan_at,
        playlist_last_updated_at=playlist_last_updated_at,
        target_countries=payload.target_countries,
        target_keywords=payload.target_keywords,
    )


@router.patch("/{tracked_playlist_id}/targets", response_model=TrackedPlaylistOut)
def update_playlist_targets(
    tracked_playlist_id: UUID,
    payload: TrackedPlaylistTargetsUpdate,
    db: Session = Depends(get_db),
):
    tracked = get_tracked_playlist_by_id(db, tracked_playlist_id)
    if not tracked:
        raise HTTPException(status_code=404, detail="Tracked playlist not found.")

    existing_countries = _normalize_list(tracked.target_countries, upper=True)
    existing_keywords = _normalize_list(tracked.target_keywords)

    incoming_countries = (
        _normalize_list(payload.target_countries, upper=True)
        if payload.target_countries is not None
        else existing_countries
    )
    incoming_keywords = (
        _normalize_list(payload.target_keywords)
        if payload.target_keywords is not None
        else existing_keywords
    )

    _ensure_no_removals(existing_countries, incoming_countries, "target countries")
    _ensure_no_removals(existing_keywords, incoming_keywords, "target keywords")

    if len(incoming_countries) > 5:
        raise HTTPException(status_code=400, detail="You can select up to 5 target countries.")
    if len(incoming_keywords) > 5:
        raise HTTPException(status_code=400, detail="You can select up to 5 target keywords.")

    return update_tracked_playlist_targets(
        db,
        tracked,
        target_countries=incoming_countries,
        target_keywords=incoming_keywords,
    )
