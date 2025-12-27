from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.repositories.tracked_playlists import (
    get_tracked_playlist_by_id,
    list_tracked_playlists,
)

router = APIRouter(tags=["pages"])

WEB_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = WEB_DIR / "templates"

_templates = Jinja2Templates(directory=str(TEMPLATES_DIR)) if TEMPLATES_DIR.exists() else None


def _format_datetime(value: datetime | None) -> str:
    if not value:
        return "—"
    return value.strftime("%b %d, %Y")


def _playlist_to_view_model(playlist) -> dict:
    playlist_url = playlist.playlist_url or f"https://open.spotify.com/playlist/{playlist.playlist_id}"
    return {
        "id": str(playlist.id),
        "playlist_id": playlist.playlist_id,
        "playlist_url": playlist_url,
        "name": playlist.name or "Tracked Playlist",
        "target_countries": playlist.target_countries or [],
        "target_keywords": playlist.target_keywords or [],
        "owner": "—",
        "followers": "—",
        "songs_count": "—",
        "scanned_display": "—",
        "last_updated": _format_datetime(playlist.created_at),
    }


def _render_template(request: Request, template_name: str, context: dict) -> HTMLResponse:
    if _templates is None:
        return HTMLResponse("Templates are not available.", status_code=500)
    return _templates.TemplateResponse(template_name, {"request": request, **context})


@router.get("/", response_class=HTMLResponse)
def tracked_playlists_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    tracked_playlists = list_tracked_playlists(db)
    playlists = [_playlist_to_view_model(item) for item in tracked_playlists]
    return _render_template(
        request,
        "tracked_playlists.html",
        {
            "playlists": playlists,
        },
    )


@router.get("/playlists/{tracked_playlist_id}", response_class=HTMLResponse)
def tracked_playlist_detail_page(
    tracked_playlist_id: str, request: Request, db: Session = Depends(get_db)
) -> HTMLResponse:
    try:
        UUID(tracked_playlist_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Tracked playlist not found") from exc

    playlist = get_tracked_playlist_by_id(db, tracked_playlist_id)
    if not playlist:
        raise HTTPException(status_code=404, detail="Tracked playlist not found")

    return _render_template(
        request,
        "tracked_playlist_detail.html",
        {
            "playlist": _playlist_to_view_model(playlist),
        },
    )
