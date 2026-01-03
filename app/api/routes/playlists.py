import logging
from datetime import datetime, timezone
from uuid import UUID

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, update
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
    get_tracked_playlist_by_id,
    get_tracked_playlist_by_playlist_id,
    list_tracked_playlists,
    update_tracked_playlist_targets,
)
from app.schemas.playlist import (
    RefreshPlaylistResponse,
    TrackedPlaylistCreate,
    TrackedPlaylistOut,
    TrackedPlaylistReorder,
    TrackedPlaylistTargetsUpdate,
)
from app.models.tracked_playlist import TrackedPlaylist
from app.services.playlist_metadata import (
    build_spotify_url,
    raise_spotify_request_error,
    select_largest_image_url,
    select_smallest_image_url,
)
from app.services.playlist_insights import upsert_playlist_seen_and_snapshot
from app.services.playlist_refresh_jobs import enqueue_refresh
from app.services.tracked_playlist_stats import resolve_latest_playlist_stats

router = APIRouter(tags=["playlists"])
logger = logging.getLogger(__name__)


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


def _serialize_tracked_playlist(db: Session, tracked) -> TrackedPlaylistOut:
    stats = resolve_latest_playlist_stats(db, tracked)
    payload = TrackedPlaylistOut.model_validate(tracked).model_dump()
    payload["followers_total"] = stats.followers_total
    payload["stats_updated_at"] = stats.stats_updated_at
    return TrackedPlaylistOut.model_validate(payload)


@router.get("", response_model=list[TrackedPlaylistOut])
def get_playlists(db: Session = Depends(get_db)):
    playlists = list_tracked_playlists(db)
    return [_serialize_tracked_playlist(db, item) for item in playlists]


@router.get("/{tracked_playlist_id}", response_model=TrackedPlaylistOut)
def get_playlist(tracked_playlist_id: UUID, db: Session = Depends(get_db)):
    tracked = get_tracked_playlist_by_id(db, tracked_playlist_id)
    if not tracked:
        raise HTTPException(status_code=404, detail="Tracked playlist not found.")
    return _serialize_tracked_playlist(db, tracked)


@router.patch("/reorder")
def reorder_playlists(payload: TrackedPlaylistReorder, db: Session = Depends(get_db)):
    ordered_ids = [str(entry) for entry in (payload.ordered_ids or [])]
    if len(set(ordered_ids)) != len(ordered_ids):
        raise HTTPException(status_code=400, detail="ordered_ids must be unique.")

    try:
        ordered_uuid = [UUID(str(entry)) for entry in ordered_ids]
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid playlist id.") from exc

    existing_ids = db.execute(select(TrackedPlaylist.id)).scalars().all()
    existing_id_strings = {str(entry) for entry in existing_ids}
    if set(ordered_ids) != existing_id_strings:
        raise HTTPException(status_code=400, detail="ordered_ids must match tracked playlists.")

    with db.begin():
        for index, playlist_id in enumerate(ordered_uuid):
            db.execute(
                update(TrackedPlaylist)
                .where(TrackedPlaylist.id == playlist_id)
                .values(sort_order=index)
            )

    return {"ok": True}


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
        "fields": (
            "name,description,external_urls.spotify,followers.total,tracks.total,images,"
            "snapshot_id,owner.display_name,owner.id"
        ),
    }
    fallback_market = (payload.target_countries or [None])[0] or "US"
    fallback_params = {
        "fields": (
            "name,description,external_urls.spotify,followers.total,tracks.total,images,"
            "snapshot_id,owner.display_name,owner.id"
        ),
        "market": fallback_market,
    }
    attempted_urls = []

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
                            "playlist_id": playlist_id,
                            "attempted_urls": attempted_urls,
                        },
                    ) from fallback_exc
                raise_spotify_request_error(playlist_id, fallback_exc)
            except Exception as fallback_exc:
                raise_spotify_request_error(playlist_id, fallback_exc)
        else:
            raise_spotify_request_error(playlist_id, exc)
    except Exception as exc:
        raise_spotify_request_error(playlist_id, exc)

    name = detail.get("name")
    description = detail.get("description")
    resolved_url = (detail.get("external_urls") or {}).get("spotify") or playlist_url
    images = detail.get("images") or []
    cover_image_url_small = select_smallest_image_url(images)
    cover_image_url_large = select_largest_image_url(images)
    owner_info = detail.get("owner") or {}
    owner_name = owner_info.get("display_name") or owner_info.get("id")
    followers_total = (detail.get("followers") or {}).get("total")
    tracks_count = (detail.get("tracks") or {}).get("total")
    playlist_last_updated_at = None
    last_meta_refresh_at = datetime.now(timezone.utc)

    tracked = create_tracked_playlist(
        db,
        playlist_id=playlist_id,
        playlist_url=resolved_url,
        name=name,
        description=description,
        cover_image_url_small=cover_image_url_small,
        cover_image_url_large=cover_image_url_large,
        owner_name=owner_name,
        followers_total=followers_total,
        tracks_count=tracks_count,
        last_meta_refresh_at=last_meta_refresh_at,
        playlist_last_updated_at=playlist_last_updated_at,
        target_countries=payload.target_countries,
        target_keywords=payload.target_keywords,
    )
    upsert_playlist_seen_and_snapshot(
        db,
        playlist_id=playlist_id,
        followers=followers_total,
        seen_at=last_meta_refresh_at,
        source="tracked_manual",
    )
    db.commit()
    return tracked


@router.post(
    "/refresh/{tracked_playlist_id}",
    response_model=RefreshPlaylistResponse,
)
@router.post(
    "/{tracked_playlist_id}/refresh-stats",
    response_model=RefreshPlaylistResponse,
)
def refresh_playlist_stats(
    tracked_playlist_id: UUID,
):
    start_time = datetime.now(timezone.utc)
    job_id, started = enqueue_refresh(str(tracked_playlist_id))
    queued_at = datetime.now(timezone.utc)
    status_label = "queued" if started else "already_running"
    logger.info(
        "Refresh stats request queued tracked_playlist_id=%s job_id=%s status=%s duration_ms=%.2f",
        tracked_playlist_id,
        job_id,
        status_label,
        (queued_at - start_time).total_seconds() * 1000,
    )
    return {
        "ok": True,
        "job_id": job_id,
        "queued": started,
        "queued_at": queued_at,
        "ts": queued_at,
        "status": status_label,
    }


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
