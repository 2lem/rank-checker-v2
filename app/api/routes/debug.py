import csv
import io
import os
from datetime import datetime, timezone
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, Response
from sqlalchemy import func, or_, select, text

from app.basic_rank_checker.events import scan_event_manager
from app.basic_rank_checker.service import fetch_scan_details
from app.core.db import SessionLocal, engine
from app.core.debug_tools import require_debug_tools, require_debug_tools_enabled
from app.core.spotify import get_spotify_metrics_snapshot
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult

router = APIRouter(
    prefix="/api/debug",
    tags=["debug"],
    dependencies=[Depends(require_debug_tools)],
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.isoformat()
    return value.isoformat()


def _normalize_limit(limit: int) -> int:
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1.")
    return min(limit, 200)


def _normalize_offset(offset: int) -> int:
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0.")
    return offset


def _parse_iso_datetime(value: str | None, label: str) -> datetime | None:
    if value is None:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {label}.") from exc


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


def _is_manual_scan(scan: BasicScan) -> bool:
    if scan.tracked_playlist_id is not None:
        return False
    if scan.manual_playlist_url:
        return True
    if scan.manual_target_keywords:
        return True
    if scan.manual_target_countries:
        return True
    return False


def _serialize_manual_scan_item(scan: BasicScan) -> dict:
    return {
        "scan_id": str(scan.id),
        "status": scan.status,
        "created_at": _format_dt(scan.created_at),
        "started_at": _format_dt(scan.started_at),
        "finished_at": _format_dt(scan.finished_at),
        "manual_playlist_url": scan.manual_playlist_url,
        "manual_playlist_id": scan.manual_playlist_id,
        "manual_playlist_name": scan.manual_playlist_name,
        "manual_playlist_owner": scan.manual_playlist_owner,
        "manual_target_countries": scan.manual_target_countries or [],
        "manual_target_keywords": scan.manual_target_keywords or [],
    }


def _query_manual_scans(
    session,
    limit: int,
    offset: int,
    status: str | None,
    from_ts: datetime | None,
    to_ts: datetime | None,
) -> tuple[int, int, int, list[BasicScan]]:
    normalized_limit = _normalize_limit(limit)
    normalized_offset = _normalize_offset(offset)
    filters = [
        BasicScan.tracked_playlist_id.is_(None),
        or_(
            BasicScan.manual_playlist_url.is_not(None),
            BasicScan.manual_target_keywords != [],
            BasicScan.manual_target_countries != [],
        ),
    ]
    if status:
        filters.append(BasicScan.status == status)
    if from_ts is not None:
        filters.append(BasicScan.created_at >= from_ts)
    if to_ts is not None:
        filters.append(BasicScan.created_at <= to_ts)

    total = (
        session.execute(select(func.count(BasicScan.id)).where(*filters)).scalar_one() or 0
    )
    scans = (
        session.execute(
            select(BasicScan)
            .where(*filters)
            .order_by(BasicScan.created_at.desc())
            .limit(normalized_limit)
            .offset(normalized_offset)
        )
        .scalars()
        .all()
    )
    return normalized_limit, normalized_offset, total, scans


@router.get("/version")
def version():
    return {
        "ok": True,
        "git_sha": os.getenv("RAILWAY_GIT_COMMIT_SHA") or None,
        "ts": _now_iso(),
    }


@router.get("/db-ping")
def db_ping():
    if SessionLocal is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database session not configured"},
        )

    session = SessionLocal()
    try:
        session.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - best effort defensive response
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": str(exc)},
        )
    finally:
        session.close()

    return {"ok": True, "ts": _now_iso()}


@router.get("/db-pool")
def db_pool():
    if engine is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database engine not configured"},
        )

    try:
        pool_status = engine.pool.status()
    except Exception as exc:  # pragma: no cover - best effort defensive response
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": str(exc)},
        )

    checked_out = None
    if hasattr(engine.pool, "checkedout"):
        checked_out = engine.pool.checkedout()
    elif hasattr(engine.pool, "checked_out"):
        checked_out = engine.pool.checked_out

    return {"ok": True, "pool_status": pool_status, "checked_out": checked_out, "ts": _now_iso()}


@router.get("/db-activity")
def db_activity():
    if engine is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database engine not configured"},
        )

    try:
        pool_status = engine.pool.status()
    except Exception as exc:  # pragma: no cover - best effort defensive response
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": str(exc)},
        )

    checked_out = None
    if hasattr(engine.pool, "checkedout"):
        checked_out = engine.pool.checkedout()
    elif hasattr(engine.pool, "checked_out"):
        checked_out = engine.pool.checked_out

    return {"ok": True, "pool_status": pool_status, "checked_out": checked_out, "ts": _now_iso()}


@router.get("/routes")
def routes(request: Request):
    route_entries = []
    for route in request.app.router.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        if not methods or path is None:
            continue
        route_entries.append({"path": path, "methods": sorted(methods)})
    return {"ok": True, "routes": route_entries, "ts": _now_iso()}


@router.get("/schema-version")
def schema_version(request: Request):
    if SessionLocal is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database session not configured"},
        )

    session = SessionLocal()
    try:
        version_row = session.execute(text("SELECT version_num FROM alembic_version")).one()
        manual_columns = session.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'basic_scans'
                  AND column_name IN (
                    'manual_playlist_url',
                    'manual_playlist_id',
                    'manual_playlist_name',
                    'manual_playlist_owner',
                    'manual_playlist_image_url',
                    'manual_target_countries',
                    'manual_target_keywords'
                  )
                """
            )
        ).all()
    except Exception as exc:  # pragma: no cover - best effort defensive response
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": str(exc)},
        )
    finally:
        session.close()

    return {
        "ok": True,
        "alembic_version": version_row[0],
        "manual_columns": sorted([row[0] for row in manual_columns]),
        "ts": _now_iso(),
    }


@router.get("/sse-state")
def sse_state(request: Request):
    state = scan_event_manager.snapshot()
    return {"ok": True, **state, "ts": _now_iso()}


@router.get("/scan/{scan_id}", dependencies=[Depends(require_debug_tools_enabled)])
def scan_state(scan_id: str):
    if SessionLocal is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database session not configured"},
        )

    session = SessionLocal()
    try:
        scan = session.get(BasicScan, scan_id)
    except Exception as exc:  # pragma: no cover - best effort defensive response
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": str(exc)},
        )
    finally:
        session.close()

    if scan is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"ok": False, "error": "Scan not found."},
        )

    state = scan_event_manager.get_state(scan_id)
    return {
        "ok": True,
        "scan_id": scan_id,
        "status": scan.status,
        **state,
        "ts": _now_iso(),
    }


@router.get("/manual-scans", dependencies=[Depends(require_debug_tools_enabled)])
def list_manual_scans(
    limit: int = Query(default=50),
    offset: int = Query(default=0),
    status_filter: str | None = Query(default=None, alias="status"),
    from_ts: str | None = Query(default=None),
    to_ts: str | None = Query(default=None),
):
    if SessionLocal is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database session not configured"},
        )

    parsed_from_ts = _parse_iso_datetime(from_ts, "from_ts")
    parsed_to_ts = _parse_iso_datetime(to_ts, "to_ts")

    session = SessionLocal()
    try:
        normalized_limit, normalized_offset, total, scans = _query_manual_scans(
            session,
            limit=limit,
            offset=offset,
            status=status_filter,
            from_ts=parsed_from_ts,
            to_ts=parsed_to_ts,
        )
    except HTTPException as exc:
        raise exc
    except Exception as exc:  # pragma: no cover - best effort defensive response
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": str(exc)},
        )
    finally:
        session.close()

    return {
        "limit": normalized_limit,
        "offset": normalized_offset,
        "total": total,
        "items": [_serialize_manual_scan_item(scan) for scan in scans],
    }


@router.get("/manual-scans/{scan_id}", dependencies=[Depends(require_debug_tools_enabled)])
def get_manual_scan(scan_id: str):
    if SessionLocal is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database session not configured"},
        )
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    session = SessionLocal()
    try:
        scan = session.get(BasicScan, scan_id)
        if scan is None or not _is_manual_scan(scan):
            raise HTTPException(status_code=404, detail="Scan not found.")
        detail = fetch_scan_details(session, scan_id)
    finally:
        session.close()

    if detail is None:
        raise HTTPException(status_code=404, detail="Scan not found.")

    return {"scan_id": str(scan.id), "type": "manual", "detail": detail}


@router.get("/manual-scans/{scan_id}/export.csv", dependencies=[Depends(require_debug_tools_enabled)])
def export_manual_scan_csv(scan_id: str):
    if SessionLocal is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database session not configured"},
        )
    try:
        UUID(scan_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Scan not found.") from exc

    session = SessionLocal()
    try:
        scan = session.get(BasicScan, scan_id)
        if scan is None or not _is_manual_scan(scan):
            raise HTTPException(status_code=404, detail="Scan not found.")

        query = (
            select(
                BasicScanQuery.searched_at,
                BasicScanQuery.country_code,
                BasicScanQuery.keyword,
                BasicScanResult.rank,
                BasicScanResult.playlist_id,
                BasicScanResult.playlist_name,
                BasicScanResult.playlist_owner,
                BasicScanResult.playlist_followers,
                BasicScanResult.songs_count,
                BasicScanResult.playlist_url,
                BasicScanResult.is_tracked_playlist,
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
        rows = []
        tz = ZoneInfo("UTC")
        for row in session.execute(query).all():
            rows.append(
                [
                    _format_csv_datetime(row.searched_at, tz),
                    row.country_code,
                    row.keyword,
                    row.rank,
                    row.playlist_id,
                    row.playlist_name,
                    row.playlist_owner,
                    row.playlist_followers,
                    row.songs_count,
                    row.playlist_url,
                    row.is_tracked_playlist,
                ]
            )
        timestamp = _format_scan_timestamp(scan, tz)
        filename = f"{timestamp}_{scan.id}_manual.csv"
        return _csv_response(
            filename,
            [
                "searched_at",
                "country_code",
                "keyword",
                "rank",
                "playlist_id",
                "playlist_name",
                "playlist_owner",
                "playlist_followers",
                "songs_count",
                "playlist_url",
                "is_tracked_playlist",
            ],
            rows,
        )
    finally:
        session.close()


@router.get("/spotify-metrics", dependencies=[Depends(require_debug_tools_enabled)])
def spotify_metrics():
    return {"ok": True, **get_spotify_metrics_snapshot(), "ts": _now_iso()}
