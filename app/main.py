from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api import basic_rank_checker_router, debug_router, playlists_router, scans_router
from app.web.routes import pages_router

app = FastAPI(title="Rank Checker v2")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "web" / "static"

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.include_router(debug_router, prefix="/api/debug")
app.include_router(basic_rank_checker_router, prefix="/api/basic-rank-checker")
app.include_router(playlists_router, prefix="/api/playlists")
app.include_router(scans_router, prefix="/api/scans")
app.include_router(pages_router)


@app.get("/health")
def health():
    return {"ok": True}
