from __future__ import annotations

import csv
import io
import threading
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.basic_rank_checker.events import scan_event_manager
from app.basic_rank_checker.service import create_basic_scan, fetch_scan_details, run_basic_scan
from app.core.db import get_db
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult
from app.repositories.tracked_playlists import get_tracked_playlist_by_id

router = APIRouter(tags=["basic-rank-checker"])


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.isoformat()
    return value.isoformat()


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
    tracked_playlist_id = (payload or {}).get("tracked_playlist_id")
    if not tracked_playlist_id:
        raise HTTPException(status_code=400, detail="tracked_playlist_id is required.")
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

    scan = create_basic_scan(db, tracked)
    scan_event_manager.create_queue(str(scan.id))
    thread = threading.Thread(target=run_basic_scan, args=(str(scan.id),), daemon=True)
    thread.start()
    return {"scan_id": str(scan.id)}


@router.get("/scans/{scan_id}/events")
def stream_scan_events(scan_id: str, db: Session = Depends(get_db)):
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    scan = db.get(BasicScan, scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail="Scan not found.")

    queue = scan_event_manager.get_queue(scan_id)
    if queue is None:
        queue = scan_event_manager.create_queue(scan_id)
        if scan.status == "completed":
            scan_event_manager.publish(scan_id, {"type": "done", "scan_id": scan_id})
        elif scan.status == "failed":
            scan_event_manager.publish(
                scan_id,
                {"type": "error", "message": scan.error_message or "Scan failed."},
            )

    return StreamingResponse(scan_event_manager.stream(scan_id), media_type="text/event-stream")


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
def export_summary_csv(scan_id: str, db: Session = Depends(get_db)):
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    scan = db.get(BasicScan, scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail="Scan not found.")

    tracked = get_tracked_playlist_by_id(db, str(scan.tracked_playlist_id))
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
                _format_dt(query.searched_at),
                query.keyword,
                query.country_code,
                query.tracked_rank,
                tracked.name if tracked else None,
                scan.follower_snapshot,
            ]
        )

    return _csv_response(
        f"basic_scan_{scan_id}_summary.csv",
        ["searched_at", "keyword", "country", "rank", "playlist_name", "playlist_followers"],
        rows,
    )


@router.get("/scans/{scan_id}/export/detailed.csv")
def export_detailed_csv(
    scan_id: str,
    country: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    scan = db.get(BasicScan, scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail="Scan not found.")

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
                _format_dt(row.searched_at),
                row.keyword,
                row.country_code,
                row.rank,
                row.playlist_name,
                row.playlist_owner,
                row.playlist_followers,
                row.songs_count,
                _format_dt(row.playlist_last_added_track_at),
                row.playlist_description,
                row.playlist_url,
            ]
        )

    return _csv_response(
        f"basic_scan_{scan_id}_detailed.csv",
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
