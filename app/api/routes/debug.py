from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends

from app.core.db import engine, get_database_url, get_db
from app.core.spotify import get_access_token_payload
from app.services.playlist_metadata import refresh_playlist_metadata

router = APIRouter()


# TEMP DEBUG: Trigger refresh without browser call to confirm handler logging.
@router.get("/trigger-refresh/{tracked_playlist_id}")
def trigger_refresh(tracked_playlist_id: UUID, db: Session = Depends(get_db)):
    refresh_playlist_metadata(db, str(tracked_playlist_id))
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
