import os

import psycopg2
from fastapi import APIRouter

from app.core.spotify import get_access_token_payload

router = APIRouter()


@router.get("/db-ping")
def db_ping():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"ok": False, "error": "DATABASE_URL not set"}

    try:
        conn = psycopg2.connect(database_url, connect_timeout=5)
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
        finally:
            conn.close()
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
