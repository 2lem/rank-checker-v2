from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pycountry
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.version import get_build_time, get_git_sha
from app.repositories.tracked_playlists import (
    get_tracked_playlist_by_id,
    list_tracked_playlists,
)

router = APIRouter(tags=["pages"])

WEB_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = WEB_DIR / "templates"

_templates = Jinja2Templates(directory=str(TEMPLATES_DIR)) if TEMPLATES_DIR.exists() else None
_build_id = get_git_sha() or get_build_time() or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
if _templates is not None:
    _templates.env.globals["build_id"] = _build_id
DASHBOARD_HEADER_PREFIX = "Dashboard "
DASHBOARD_HEADER_MAX_LENGTH = 30
DASHBOARD_HEADER_PREFIX = "Dashboard "
DASHBOARD_HEADER_MAX_LENGTH = 30

AVAILABLE_MARKETS = [
    "AD",
    "AE",
    "AG",
    "AL",
    "AM",
    "AO",
    "AR",
    "AT",
    "AU",
    "AZ",
    "BA",
    "BB",
    "BD",
    "BE",
    "BF",
    "BG",
    "BH",
    "BI",
    "BJ",
    "BN",
    "BO",
    "BR",
    "BS",
    "BT",
    "BW",
    "BY",
    "BZ",
    "CA",
    "CD",
    "CG",
    "CH",
    "CI",
    "CL",
    "CM",
    "CO",
    "CR",
    "CV",
    "CW",
    "CY",
    "CZ",
    "DE",
    "DJ",
    "DK",
    "DM",
    "DO",
    "DZ",
    "EC",
    "EE",
    "EG",
    "ES",
    "FI",
    "FJ",
    "FM",
    "FR",
    "GA",
    "GB",
    "GD",
    "GE",
    "GH",
    "GM",
    "GN",
    "GQ",
    "GR",
    "GT",
    "GW",
    "GY",
    "HK",
    "HN",
    "HR",
    "HT",
    "HU",
    "ID",
    "IE",
    "IL",
    "IN",
    "IQ",
    "IS",
    "IT",
    "JM",
    "JO",
    "JP",
    "KE",
    "KG",
    "KH",
    "KI",
    "KM",
    "KN",
    "KR",
    "KW",
    "KZ",
    "LA",
    "LB",
    "LC",
    "LI",
    "LK",
    "LR",
    "LS",
    "LT",
    "LU",
    "LV",
    "LY",
    "MA",
    "MC",
    "MD",
    "ME",
    "MG",
    "MH",
    "MK",
    "ML",
    "MN",
    "MO",
    "MR",
    "MT",
    "MU",
    "MV",
    "MW",
    "MX",
    "MY",
    "MZ",
    "NA",
    "NE",
    "NG",
    "NI",
    "NL",
    "NO",
    "NP",
    "NR",
    "NZ",
    "OM",
    "PA",
    "PE",
    "PG",
    "PH",
    "PK",
    "PL",
    "PS",
    "PT",
    "PW",
    "PY",
    "QA",
    "RO",
    "RS",
    "RW",
    "SA",
    "SB",
    "SC",
    "SE",
    "SG",
    "SI",
    "SK",
    "SL",
    "SM",
    "SN",
    "SR",
    "ST",
    "SV",
    "SZ",
    "TD",
    "TG",
    "TH",
    "TJ",
    "TL",
    "TN",
    "TO",
    "TR",
    "TT",
    "TV",
    "TW",
    "TZ",
    "UA",
    "UG",
    "US",
    "UY",
    "UZ",
    "VC",
    "VE",
    "VN",
    "VU",
    "WS",
    "XK",
    "ZA",
    "ZM",
    "ZW",
]

_MARKET_OVERRIDES = {
    "XK": "Kosovo",
}


def _market_label(code: str) -> str:
    normalized = (code or "").strip().upper()
    if not normalized:
        return code
    if normalized in _MARKET_OVERRIDES:
        return _MARKET_OVERRIDES[normalized]
    country = pycountry.countries.get(alpha_2=normalized)
    return country.name if country else normalized


def _available_markets_with_labels() -> list[dict[str, str]]:
    markets = []
    for code in AVAILABLE_MARKETS:
        markets.append({"code": code, "label": _market_label(code)})

    # Sorted by English name (not code).
    markets.sort(key=lambda market: market["label"].casefold())
    return markets


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_relative_time(value: datetime | None, now: datetime | None = None) -> str:
    if not value:
        return "—"
    now = _ensure_utc(now or datetime.now(timezone.utc))
    value = _ensure_utc(value)
    if not now or not value:
        return "—"
    delta_seconds = int((now - value).total_seconds())
    if delta_seconds < 0:
        delta_seconds = 0
    if delta_seconds < 60:
        return "just now"
    minutes = delta_seconds // 60
    if minutes < 60:
        unit = "minute" if minutes == 1 else "minutes"
        return f"{minutes} {unit} ago"
    hours = minutes // 60
    if hours < 24:
        unit = "hour" if hours == 1 else "hours"
        return f"{hours} {unit} ago"
    days = hours // 24
    unit = "day" if days == 1 else "days"
    return f"{days} {unit} ago"


def _format_count(value: int | None) -> str:
    if value is None:
        return "—"
    return f"{value:,}"


def _build_dashboard_header_labels(name: str | None) -> tuple[str, str, str]:
    base_name = name or "Tracked Playlist"
    available = max(DASHBOARD_HEADER_MAX_LENGTH - len(DASHBOARD_HEADER_PREFIX), 0)
    playlist_label = base_name
    if available and len(playlist_label) > available:
        if available == 1:
            playlist_label = "…"
        else:
            playlist_label = f"{playlist_label[: available - 1]}…"
    header_text = f"{DASHBOARD_HEADER_PREFIX}{playlist_label}"
    header_text = header_text[:DASHBOARD_HEADER_MAX_LENGTH]
    return header_text, playlist_label, base_name


def _playlist_to_view_model(playlist) -> dict:
    playlist_url = playlist.playlist_url or f"https://open.spotify.com/playlist/{playlist.playlist_id}"
    header_text, playlist_label, base_name = _build_dashboard_header_labels(playlist.name)
    return {
        "id": str(playlist.id),
        "playlist_id": playlist.playlist_id,
        "playlist_url": playlist_url,
        "name": base_name,
        "image_url": playlist.cover_image_url_small or "",
        "owner_name": playlist.owner_name or "—",
        "followers_total": _format_count(playlist.followers_total),
        "tracks_count": _format_count(playlist.tracks_count),
        "scanned_display": _format_relative_time(playlist.stats_updated_at),
        "last_updated_display": _format_relative_time(playlist.playlist_last_updated_at),
        "target_countries": playlist.target_countries or [],
        "target_country_labels": {
            (code or "").upper(): _market_label(code) for code in (playlist.target_countries or [])
        },
        "target_keywords": playlist.target_keywords or [],
        "dashboard_header_title": header_text,
        "dashboard_playlist_label": playlist_label,
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
            "available_markets": _available_markets_with_labels(),
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
            "available_markets": _available_markets_with_labels(),
        },
    )
