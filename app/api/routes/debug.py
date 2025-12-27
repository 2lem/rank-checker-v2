from sqlalchemy import text
from fastapi import APIRouter

from app.core.db import engine, get_database_url
from app.core.spotify import get_access_token_payload

router = APIRouter()


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
