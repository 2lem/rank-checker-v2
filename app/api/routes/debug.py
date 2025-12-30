import os
from datetime import datetime, timezone

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.core.db import SessionLocal, engine

router = APIRouter(prefix="/api/debug", tags=["debug"])


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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

    return {"ok": True, "pool_status": pool_status, "ts": _now_iso()}
