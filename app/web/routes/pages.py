from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pycountry
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.version import get_build_time, get_git_sha
from app.models.playlist import PlaylistFollowerSnapshot
from app.repositories.tracked_playlists import (
    get_tracked_playlist_by_id,
    list_tracked_playlists,
)
from app.services.playlist_insights import (
    backfill_playlist_follower_snapshots_from_dedicated_scans,
    build_compare_entry,
    build_daily_representative_scans_from_dedicated_scans,
    build_daily_representative_snapshots,
    resolve_daily_compare_reps,
    resolve_weekly_compare_reps,
)
from app.services.tracked_playlist_stats import resolve_latest_playlist_stats

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


def _format_date_label(value: datetime | date | None) -> str:
    if not value:
        return "—"
    return value.strftime("%d %b %Y")


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


def _playlist_to_view_model(playlist, *, stats=None) -> dict:
    followers_total = stats.followers_total if stats else playlist.followers_total
    stats_updated_at = stats.stats_updated_at if stats else playlist.stats_updated_at
    playlist_url = playlist.playlist_url or f"https://open.spotify.com/playlist/{playlist.playlist_id}"
    header_text, playlist_label, base_name = _build_dashboard_header_labels(playlist.name)
    return {
        "id": str(playlist.id),
        "playlist_id": playlist.playlist_id,
        "playlist_url": playlist_url,
        "name": base_name,
        "image_url": playlist.cover_image_url_small or "",
        "owner_name": playlist.owner_name or "—",
        "followers_total": _format_count(followers_total),
        "tracks_count": _format_count(playlist.tracks_count),
        "scanned_display": _format_relative_time(stats_updated_at),
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
    playlists = [
        _playlist_to_view_model(item, stats=resolve_latest_playlist_stats(db, item))
        for item in tracked_playlists
    ]
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
            "playlist": _playlist_to_view_model(
                playlist, stats=resolve_latest_playlist_stats(db, playlist)
            ),
            "available_markets": _available_markets_with_labels(),
        },
    )


@router.get(
    "/playlists/{tracked_playlist_id}/insights/{period}/drilldown",
    response_class=HTMLResponse,
)
def tracked_playlist_insights_drilldown(
    tracked_playlist_id: str,
    period: str,
    metric: str,
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    try:
        UUID(tracked_playlist_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Tracked playlist not found") from exc

    if period not in {"daily", "weekly"}:
        raise HTTPException(status_code=404, detail="Insight period not supported")

    metric_key = (metric or "").lower()
    metric_labels = {
        "followers": "Followers",
        "improved": "Improved Positions",
        "declined": "Declined Positions",
        "unchanged": "Unchanged",
    }
    if metric_key not in metric_labels:
        raise HTTPException(status_code=404, detail="Insight metric not supported")

    playlist = get_tracked_playlist_by_id(db, tracked_playlist_id)
    if not playlist:
        raise HTTPException(status_code=404, detail="Tracked playlist not found")

    snapshots = (
        db.execute(
            select(PlaylistFollowerSnapshot)
            .where(PlaylistFollowerSnapshot.playlist_id == playlist.playlist_id)
            .order_by(PlaylistFollowerSnapshot.snapshot_date)
        )
        .scalars()
        .all()
    )
    if len(snapshots) < 2:
        backfill_playlist_follower_snapshots_from_dedicated_scans(
            db,
            playlist.playlist_id,
            tracked_playlist_id,
        )
        snapshots = (
            db.execute(
                select(PlaylistFollowerSnapshot)
                .where(PlaylistFollowerSnapshot.playlist_id == playlist.playlist_id)
                .order_by(PlaylistFollowerSnapshot.snapshot_date)
            )
            .scalars()
            .all()
        )

    scan_reps = build_daily_representative_scans_from_dedicated_scans(
        db,
        tracked_playlist_id,
    )
    daily_reps = build_daily_representative_snapshots(snapshots, scan_reps)

    compare_reps = (
        resolve_daily_compare_reps(daily_reps)
        if period == "daily"
        else resolve_weekly_compare_reps(daily_reps)
    )
    compare_entry = (
        build_compare_entry(*compare_reps) if compare_reps is not None else None
    )

    summary_label = "Daily" if period == "daily" else "Weekly"
    metric_label = metric_labels[metric_key]
    follower_row = None
    rows: list[dict[str, object]] = []

    if metric_key == "followers" and compare_entry:
        follower_change = compare_entry.get("followers_change")
        change_class = "delta-neutral"
        change_label = "—"
        if isinstance(follower_change, int):
            if follower_change > 0:
                change_class = "delta-up"
                change_label = f"+{follower_change:,}"
            elif follower_change < 0:
                change_class = "delta-down"
                change_label = f"{follower_change:,}"
            else:
                change_label = "0"
        follower_row = {
            "from_date": _format_date_label(compare_entry.get("date_older")),
            "to_date": _format_date_label(compare_entry.get("date_newer")),
            "followers_from": _format_count(compare_entry.get("followers_older")),
            "followers_to": _format_count(compare_entry.get("followers_newer")),
            "change_label": change_label,
            "change_class": change_class,
        }

    if metric_key != "followers" and compare_reps:
        newer, older = compare_reps
        shared_keys = newer.rank_map.keys() & older.rank_map.keys()
        for country_code, keyword in shared_keys:
            rank_newer = newer.rank_map.get((country_code, keyword))
            rank_older = older.rank_map.get((country_code, keyword))
            if rank_newer is None or rank_older is None:
                continue
            change = rank_newer - rank_older
            if metric_key == "improved" and change >= 0:
                continue
            if metric_key == "declined" and change <= 0:
                continue
            if metric_key == "unchanged" and change != 0:
                continue

            if change < 0:
                change_class = "delta-up"
            elif change > 0:
                change_class = "delta-down"
            else:
                change_class = "delta-neutral"

            if change == 0:
                change_label = "0"
            else:
                change_label = f"{change:+d}"

            rows.append(
                {
                    "country": _market_label(country_code) or country_code or "—",
                    "keyword": keyword or "—",
                    "previous_rank": rank_older,
                    "current_rank": rank_newer,
                    "change_label": change_label,
                    "change_class": change_class,
                }
            )

        rows.sort(
            key=lambda row: (
                (row.get("country") or "").casefold(),
                (row.get("keyword") or "").casefold(),
            )
        )

    return _render_template(
        request,
        "_components/insights_drilldown.html",
        {
            "summary_label": summary_label,
            "metric": metric_key,
            "metric_label": metric_label,
            "follower_row": follower_row,
            "rows": rows,
        },
    )
