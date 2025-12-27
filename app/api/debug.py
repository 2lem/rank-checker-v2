import os

import psycopg2
import requests
from fastapi import APIRouter
from requests.auth import HTTPBasicAuth

router = APIRouter()


@router.get("/db-ping")
def db_ping():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"ok": False, "error": "DATABASE_URL not set"}

    try:
        conn = psycopg2.connect(database_url)
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
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    if not client_id or not client_secret:
        return {"ok": False, "error": "Spotify credentials not set"}

    try:
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            auth=HTTPBasicAuth(client_id, client_secret),
            timeout=(5, 20),
        )
    except requests.RequestException:
        return {"ok": False, "error": "Spotify token request failed"}

    if not response.ok:
        return {
            "ok": False,
            "error": f"Spotify token request failed (status {response.status_code})",
        }

    payload = response.json()
    expires_in = payload.get("expires_in")
    if not isinstance(expires_in, int):
        return {"ok": False, "error": "Spotify token response invalid"}

    return {"ok": True, "expires_in": expires_in}
