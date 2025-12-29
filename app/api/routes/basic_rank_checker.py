from __future__ import annotations

import csv
import io
import threading
import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.basic_rank_checker.events import scan_event_manager
from app.basic_rank_checker.service import create_basic_scan, fetch_scan_details, run_basic_scan
from app.core.db import SessionLocal, get_db
from app.core.spotify import extract_playlist_id, normalize_spotify_playlist_url
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult
from app.repositories.tracked_playlists import get_tracked_playlist_by_id

router = APIRouter(tags=["basic-rank-checker"])
logger = logging.getLogger(__name__)


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.isoformat()
    return value.isoformat()


def _resolve_timezone(name: Optional[str]) -> ZoneInfo:
    if not name:
        return ZoneInfo("UTC")
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("UTC")


def _format_csv_datetime(value: datetime | None, tz: ZoneInfo) -> str | None:
    if value is None:
        return None
    localized = value
    if localized.tzinfo is None:
        localized = localized.replace(tzinfo=timezone.utc)
    localized = localized.astimezone(tz)
    return localized.strftime("%d-%m-%Y_%H-%M")


def _format_scan_timestamp(scan: BasicScan, tz: ZoneInfo) -> str:
    timestamp_source = scan.started_at or scan.created_at or datetime.now(timezone.utc)
    formatted = _format_csv_datetime(timestamp_source, tz)
    return formatted or "scan"


def _csv_response(filename: str, headers: list[str], rows: list[list[object | None]]) -> Response:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\r\n")
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)
    csv_text = output.getvalue()
    csv_bytes = ("\ufeff" + csv_text).encode("utf-8")
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/scans")
def start_basic_scan(payload: dict, db: Session = Depends(get_db)):
    logger.info("TEMP HOTFIX start_basic_scan payload_keys=%s", sorted((payload or {}).keys()))
    payload = payload or {}
    tracked_playlist_id = payload.get("tracked_playlist_id")
    raw_playlist_id = payload.get("playlist_id")
    playlist_url = payload.get("playlist_url")
    target_countries = payload.get("target_countries") or []
    target_keywords = payload.get("target_keywords") or []
    is_tracked_playlist = bool(payload.get("is_tracked_playlist"))

    tracked = None
    playlist_id: str | None = None

    if tracked_playlist_id:
        try:
            UUID(str(tracked_playlist_id))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid tracked_playlist_id.") from exc

        tracked = get_tracked_playlist_by_id(db, str(tracked_playlist_id))
        if not tracked:
            raise HTTPException(status_code=404, detail="Tracked playlist not found.")
        if not tracked.target_countries or not tracked.target_keywords:
            raise HTTPException(
                status_code=400,
                detail="Tracked playlist must have target countries and keywords.",
            )
        playlist_id = tracked.playlist_id
        is_tracked_playlist = True
    else:
        normalized_url = normalize_spotify_playlist_url(playlist_url or raw_playlist_id or "")
        playlist_id = extract_playlist_id(normalized_url) or raw_playlist_id
        if not playlist_id:
            raise HTTPException(status_code=400, detail="playlist_id is required.")
        if len(target_countries) == 0 or len(target_keywords) == 0:
            raise HTTPException(
                status_code=400,
                detail="Target countries and keywords are required for manual scans.",
            )
        if len(target_countries) > 10 or len(target_keywords) > 10:
            raise HTTPException(
                status_code=400,
                detail="You can scan up to 10 target countries and 10 keywords.",
            )
        tracked_playlist_id = None
        is_tracked_playlist = False

    scan = create_basic_scan(
        db,
        tracked_playlist=tracked,
        playlist_id=playlist_id,
        scanned_countries=target_countries,
        scanned_keywords=target_keywords,
        is_tracked_playlist=is_tracked_playlist,
    )
    scan_event_manager.create_queue(str(scan.id))
    thread = threading.Thread(target=run_basic_scan, args=(str(scan.id),), daemon=True)
    thread.start()
    return {"scan_id": str(scan.id)}


@router.get("/scans/{scan_id}/events")
def stream_scan_events(scan_id: str):
    logger.info("TEMP HOTFIX stream_scan_events scan_id=%s", scan_id)
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    if SessionLocal is None:
        raise RuntimeError("DATABASE_URL not configured")

    logger.info("TEMP HOTFIX stream_scan_events open session scan_id=%s", scan_id)
    with SessionLocal() as session:
        scan = session.get(BasicScan, scan_id)
        if scan is None:
            raise HTTPException(status_code=404, detail="Scan not found.")
        scan_status = scan.status
        scan_error_message = scan.error_message
    logger.info("TEMP HOTFIX stream_scan_events closed session scan_id=%s", scan_id)

    queue = scan_event_manager.get_queue(scan_id)
    if queue is None:
        queue = scan_event_manager.create_queue(scan_id)
        if scan_status == "completed":
            scan_event_manager.publish(scan_id, {"type": "done", "scan_id": scan_id})
        elif scan_status == "failed":
            scan_event_manager.publish(
                scan_id,
                {"type": "error", "message": scan_error_message or "Scan failed."},
            )

    def _stream():
        logger.info("TEMP HOTFIX scan_events_stream_start scan_id=%s", scan_id)
        try:
            yield from scan_event_manager.stream(scan_id)
        finally:
            logger.info("TEMP HOTFIX scan_events_stream_end scan_id=%s", scan_id)

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.get("/scans/{scan_id}")
def get_scan(scan_id: str, db: Session = Depends(get_db)):
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    payload = fetch_scan_details(db, scan_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Scan not found.")
    payload["started_at"] = _format_dt(payload.get("started_at"))
    payload["finished_at"] = _format_dt(payload.get("finished_at"))
    for entry in payload.get("summary") or []:
        entry["searched_at"] = _format_dt(entry.get("searched_at"))
    for country_data in payload.get("detailed", {}).values():
        for keyword_data in country_data.get("keywords", {}).values():
            keyword_data["searched_at"] = _format_dt(keyword_data.get("searched_at"))
            for result in keyword_data.get("results", []):
                result["playlist_last_added_track_at"] = _format_dt(
                    result.get("playlist_last_added_track_at")
                )
    return payload


@router.get("/scans/{scan_id}/export/summary.csv")
def export_summary_csv(
    scan_id: str,
    timezone_name: str | None = Query(default=None, alias="timezone"),
    db: Session = Depends(get_db),
):
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    scan = db.get(BasicScan, scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail="Scan not found.")

    tracked = get_tracked_playlist_by_id(db, str(scan.tracked_playlist_id)) if scan.tracked_playlist_id else None
    tz = _resolve_timezone(timezone_name)
    rows = []
    queries = (
        db.execute(
            select(BasicScanQuery)
            .where(BasicScanQuery.basic_scan_id == scan.id)
            .order_by(BasicScanQuery.searched_at)
        )
        .scalars()
        .all()
    )
    for query in queries:
        rows.append(
            [
                _format_csv_datetime(query.searched_at, tz),
                query.keyword,
                query.country_code,
                query.tracked_rank,
                (tracked.name if tracked else None) or scan.playlist_id,
                scan.follower_snapshot,
            ]
        )

    timestamp = _format_scan_timestamp(scan, tz)
    filename = f"{timestamp}_{scan.id}_summary.csv"
    return _csv_response(
        filename,
        ["searched_at", "keyword", "country", "rank", "playlist_name", "playlist_followers"],
        rows,
    )


@router.get("/scans/{scan_id}/export/detailed.csv")
def export_detailed_csv(
    scan_id: str,
    country: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    timezone_name: str | None = Query(default=None, alias="timezone"),
    db: Session = Depends(get_db),
):
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    scan = db.get(BasicScan, scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail="Scan not found.")

    tz = _resolve_timezone(timezone_name)
    query = (
        select(
            BasicScanQuery.searched_at,
            BasicScanQuery.keyword,
            BasicScanQuery.country_code,
            BasicScanResult.rank,
            BasicScanResult.playlist_name,
            BasicScanResult.playlist_owner,
            BasicScanResult.playlist_followers,
            BasicScanResult.songs_count,
            BasicScanResult.playlist_last_added_track_at,
            BasicScanResult.playlist_description,
            BasicScanResult.playlist_url,
        )
        .join(BasicScanResult, BasicScanResult.basic_scan_query_id == BasicScanQuery.id)
        .where(BasicScanQuery.basic_scan_id == scan.id)
        .order_by(
            BasicScanQuery.searched_at,
            BasicScanQuery.keyword,
            BasicScanQuery.country_code,
            BasicScanResult.rank,
        )
    )
    if country:
        query = query.where(BasicScanQuery.country_code == country)
    if keyword:
        query = query.where(BasicScanQuery.keyword == keyword)

    rows = []
    for row in db.execute(query).all():
        rows.append(
            [
                _format_csv_datetime(row.searched_at, tz),
                row.keyword,
                row.country_code,
                row.rank,
                row.playlist_name,
                row.playlist_owner,
                row.playlist_followers,
                row.songs_count,
                _format_csv_datetime(row.playlist_last_added_track_at, tz),
                row.playlist_description,
                row.playlist_url,
            ]
        )

    timestamp = _format_scan_timestamp(scan, tz)
    filename = f"{timestamp}_{scan.id}_detailed.csv"
    return _csv_response(
        filename,
        [
            "searched_at",
            "keyword",
            "country",
            "rank",
            "playlist_name",
            "playlist_owner",
            "playlist_followers",
            "songs_count",
            "playlist_last_added_track_at",
            "playlist_description",
            "playlist_url",
        ],
        rows,
    )
