from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.api import debug_router, playlists_router, scans_router

app = FastAPI(title="Rank Checker v2")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "web" / "static"

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.include_router(debug_router, prefix="/api/debug")
app.include_router(playlists_router, prefix="/api/playlists")
app.include_router(scans_router, prefix="/api/scans")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Rank Checker v2</title>
      </head>
      <body style="font-family: system-ui; padding: 24px;">
        <h1>Rank Checker v2</h1>
        <p>FastAPI is running ✅</p>
        <p>Next step: /tracked</p>
      </body>
    </html>
    """
