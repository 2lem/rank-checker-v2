import logging

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.spotify import (
    PLAYLIST_URL,
    extract_playlist_id,
    get_access_token,
    normalize_spotify_playlist_url,
    spotify_get,
)
from app.core.db import get_db
from app.repositories.tracked_playlists import (
    create_tracked_playlist,
    get_tracked_playlist_by_playlist_id,
    list_tracked_playlists,
)
from app.schemas.playlist import TrackedPlaylistCreate, TrackedPlaylistOut

router = APIRouter(tags=["playlists"])
logger = logging.getLogger(__name__)


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
    default_params = {"fields": "name,external_urls.spotify"}
    fallback_market = (payload.target_countries or [None])[0] or "US"
    fallback_params = {
        "fields": "name,external_urls.spotify",
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

    return create_tracked_playlist(
        db,
        playlist_id=playlist_id,
        playlist_url=resolved_url,
        name=name,
        target_countries=payload.target_countries,
        target_keywords=payload.target_keywords,
    )
