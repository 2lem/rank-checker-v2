import logging
import os
from pathlib import Path

from contextvars import Token

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from app.api import basic_rank_checker_router, debug_router, playlists_router, scans_router
from app.core.debug_tools import debug_tools_enabled
from app.core.db import request_path_var
from app.core.version import get_git_sha
from app.web.routes import pages_router

app = FastAPI(title="Rank Checker v2")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "web" / "static"

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

logger = logging.getLogger(__name__)

if os.getenv("DEBUG_STABILITY") == "1":
    logger.info("DEBUG_STABILITY git_sha=%s", get_git_sha())


def _set_request_path(path: str) -> Token[str | None]:
    """TEMP DEBUG: store the current request path for transaction logging."""
    return request_path_var.set(path)


@app.middleware("http")
async def request_path_context_middleware(request: Request, call_next):
    # TEMP DEBUG: Track request path in a context var for SQLAlchemy transaction logging.
    token = _set_request_path(request.url.path)
    try:
        response = await call_next(request)
    finally:
        request_path_var.reset(token)
    return response

if debug_tools_enabled():
    app.include_router(debug_router, prefix="/api/debug")
app.include_router(basic_rank_checker_router, prefix="/api/basic-rank-checker")
app.include_router(playlists_router, prefix="/api/playlists")
app.include_router(scans_router, prefix="/api/scans")
app.include_router(pages_router)


@app.get("/health")
def health():
    return {"ok": True}
