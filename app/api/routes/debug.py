import logging
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, Query

from app.core.db import engine, get_database_url, get_db
from app.core.spotify import get_access_token_payload
from app.models.basic_scan import BasicScan
from app.services.playlist_metadata import refresh_playlist_metadata

router = APIRouter()
logger = logging.getLogger(__name__)


def _format_dt(value):
    if value is None:
        return None
    try:
        return value.isoformat()
    except AttributeError:
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
