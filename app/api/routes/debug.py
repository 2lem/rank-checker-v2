import logging
from uuid import UUID

from sqlalchemy import Table, func, inspect, select, text
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.debug_tools import require_debug_tools
from app.core.db import engine, get_database_url, get_db
from app.core.spotify import get_access_token_payload
from app.models.base import Base
from app.models.basic_scan import BasicScan
from app.services.playlist_metadata import refresh_playlist_metadata

router = APIRouter(dependencies=[Depends(require_debug_tools)])
logger = logging.getLogger(__name__)


def _format_dt(value):
    if value is None:
        return None
    try:
        return value.isoformat()
    except AttributeError:
        return None


def _get_events_table(db: Session) -> Table | None:
    bind = db.get_bind()
    inspector = inspect(bind)
    for table_name in ("basic_scan_events", "scan_events"):
        if inspector.has_table(table_name):
            return Table(table_name, Base.metadata, autoload_with=bind)
    return None


def _get_column(table: Table, *names: str):
    for name in names:
        column = table.c.get(name)
        if column is not None:
            return column
    return None


# TEMP DEBUG: Trigger refresh without browser call to confirm handler logging.
@router.get("/trigger-refresh/{tracked_playlist_id}")
def trigger_refresh(tracked_playlist_id: UUID, db: Session = Depends(get_db)):
    logger.info("DEBUG trigger-refresh %s", tracked_playlist_id)
    try:
        refresh_playlist_metadata(db, str(tracked_playlist_id))
    except Exception as exc:  # pragma: no cover - best effort debug endpoint
        return {"ok": False, "error": str(exc)}
    return {"ok": True}


# TEMP DEBUG: Inspect Postgres connection state
@router.get("/db-activity")
def db_activity():
    if engine is None:
        return {"ok": False, "error": "Database engine not configured"}

    try:
        with engine.connect() as conn:
            activity_rows = conn.execute(
                text("SELECT now(), count(*) AS total, state FROM pg_stat_activity GROUP BY state ORDER BY total DESC;")
            ).mappings()
            idle_in_transaction = conn.execute(
                text("SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction';")
            ).scalar()
            idle_in_transaction_details = conn.execute(
                text(
                    """
                    SELECT
                        pid,
                        usename,
                        application_name,
                        client_addr,
                        state,
                        xact_start,
                        query_start,
                        wait_event_type,
                        wait_event,
                        left(query, 200) AS query
                    FROM pg_stat_activity
                    WHERE state = 'idle in transaction'
                    ORDER BY xact_start ASC NULLS LAST;
                    """
                )
            ).mappings()
    except Exception:
        return {"ok": False, "error": "Database activity query failed"}

    return {
        "ok": True,
        "activity": list(activity_rows),
        "idle_in_transaction": idle_in_transaction,
        "idle_in_transaction_details": list(idle_in_transaction_details),
    }


@router.get("/db-ping")
def db_ping():
    if not get_database_url():
        return {"ok": False, "error": "DATABASE_URL not set"}

    try:
        if engine is None:
            raise RuntimeError("Database engine not configured")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        return {"ok": False, "error": "Database connection failed"}

    return {"ok": True}


@router.get("/db-terminate-idle-in-txn")
def db_terminate_idle_in_txn_get():
    # TEMP DEBUG: Allow GET to trigger termination from mobile browsers.
    return db_terminate_idle_in_txn()


@router.post("/db-terminate-idle-in-txn")
def db_terminate_idle_in_txn():
    # TEMP DEBUG: Terminate "idle in transaction" sessions for this service.
    if engine is None:
        return {"ok": False, "error": "Database engine not configured"}

    terminated_pids = []
    try:
        with engine.connect() as conn:
            idle_pids = conn.execute(
                text(
                    """
                    SELECT pid
                    FROM pg_stat_activity
                    WHERE state = 'idle in transaction'
                      AND application_name = 'rank-checker-v2-fastapi'
                      AND datname = current_database();
                    """
                )
            ).scalars()

            for pid in idle_pids:
                conn.execute(text("SELECT pg_terminate_backend(:pid);"), {"pid": pid})
                terminated_pids.append(pid)
    except Exception:
        return {"ok": False, "error": "Terminate idle transaction query failed"}

    return {"ok": True, "terminated_pids": terminated_pids}


@router.get("/latest-scans")
def latest_basic_scans(limit: int = Query(default=10, ge=1, le=100), db: Session = Depends(get_db)):
    """TEMP DEBUG: Inspect the latest BasicScan records without modifying them."""

    order_column = getattr(BasicScan, "created_at", None) or BasicScan.id
    scans = db.execute(select(BasicScan).order_by(order_column.desc()).limit(limit)).scalars().all()

    payload: list[dict] = []
    for scan in scans:
        entry = {
            "id": str(scan.id),
            "tracked_playlist_id": str(scan.tracked_playlist_id)
            if scan.tracked_playlist_id
            else None,
            "status": getattr(scan, "status", None),
            "created_at": _format_dt(getattr(scan, "created_at", None)),
            "started_at": _format_dt(getattr(scan, "started_at", None)),
            "finished_at": _format_dt(getattr(scan, "finished_at", None)),
        }

        if hasattr(scan, "state"):
            entry["state"] = getattr(scan, "state", None)
        if getattr(scan, "error_message", None):
            entry["error_message"] = scan.error_message

        payload.append(entry)

    return payload


@router.get("/scan/{scan_id}")
def scan_details(scan_id: UUID, db: Session = Depends(get_db)):
    """TEMP DEBUG: Inspect a BasicScan row and its recent events (if available)."""

    scan = db.get(BasicScan, scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail="Scan not found.")

    response: dict[str, object] = {
        "id": str(scan.id),
        "tracked_playlist_id": str(scan.tracked_playlist_id),
        "status": getattr(scan, "status", None),
        "created_at": _format_dt(getattr(scan, "created_at", None)),
        "started_at": _format_dt(getattr(scan, "started_at", None)),
        "finished_at": _format_dt(getattr(scan, "finished_at", None)),
    }
    if getattr(scan, "error_message", None):
        response["error_message"] = scan.error_message

    events_table = _get_events_table(db)
    if events_table is None:
        response.update(
            {
                "events_supported": False,
                "events_count": 0,
                "note": "basic_scan_events or scan_events table not detected.",
            }
        )
        return response

    scan_id_column = _get_column(events_table, "basic_scan_id", "scan_id")
    if scan_id_column is None:
        response.update(
            {
                "events_supported": False,
                "events_count": 0,
                "note": f"{events_table.name} table is missing a scan reference column.",
            }
        )
        return response

    created_column = _get_column(events_table, "created_at", "timestamp")
    type_column = _get_column(events_table, "event_type", "level", "type")
    message_column = _get_column(events_table, "message", "details", "payload")
    order_column = created_column or _get_column(events_table, "id") or scan_id_column

    events_query = (
        select(events_table)
        .where(scan_id_column == scan_id)
        .order_by(order_column.desc())
        .limit(50)
    )
    events = db.execute(events_query).mappings().all()

    events_count = db.execute(
        select(func.count()).select_from(events_table).where(scan_id_column == scan_id)
    ).scalar() or 0

    formatted_events = []
    for row in events:
        entry: dict[str, object | None] = {}
        if created_column is not None:
            created_value = row.get(created_column.key)
            entry["created_at"] = _format_dt(created_value) or created_value
        if type_column is not None:
            entry[type_column.key] = row.get(type_column.key)
        if message_column is not None:
            entry[message_column.key] = row.get(message_column.key)
        if not entry:
            entry = dict(row)
        formatted_events.append(entry)

    response.update(
        {
            "events_supported": True,
            "events_count": events_count,
            "events": formatted_events,
        }
    )
    return response


@router.get("/spotify-token")
def spotify_token():
    try:
        payload = get_access_token_payload()
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except Exception:
        return {"ok": False, "error": "Spotify token request failed"}

    expires_in = payload.get("expires_in")
    if not isinstance(expires_in, int):
        return {"ok": False, "error": "Spotify token response invalid"}

    response = {"ok": True, "expires_in": expires_in}
    access_token = payload.get("access_token")
    if isinstance(access_token, str) and access_token:
        response["token_preview"] = f"...{access_token[-8:]}"

    return response
