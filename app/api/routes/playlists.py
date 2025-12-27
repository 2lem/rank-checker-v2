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

    try:
        token = get_access_token()
        detail = spotify_get(
            PLAYLIST_URL.format(playlist_id),
            token,
            params={"fields": "name,external_urls.spotify"},
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Spotify request failed.") from exc

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
