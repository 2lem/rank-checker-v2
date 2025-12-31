import os
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.basic_rank_checker.events import scan_event_manager
from app.core.db import SessionLocal, engine
from app.core.debug_tools import require_debug_tools

router = APIRouter(
    prefix="/api/debug",
    tags=["debug"],
    dependencies=[Depends(require_debug_tools)],
)


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

    checked_out = None
    if hasattr(engine.pool, "checkedout"):
        checked_out = engine.pool.checkedout()
    elif hasattr(engine.pool, "checked_out"):
        checked_out = engine.pool.checked_out

    return {"ok": True, "pool_status": pool_status, "checked_out": checked_out, "ts": _now_iso()}


@router.get("/db-activity")
def db_activity():
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

    checked_out = None
    if hasattr(engine.pool, "checkedout"):
        checked_out = engine.pool.checkedout()
    elif hasattr(engine.pool, "checked_out"):
        checked_out = engine.pool.checked_out

    return {"ok": True, "pool_status": pool_status, "checked_out": checked_out, "ts": _now_iso()}


@router.get("/routes")
def routes(request: Request):
    route_entries = []
    for route in request.app.router.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        if not methods or path is None:
            continue
        route_entries.append({"path": path, "methods": sorted(methods)})
    return {"ok": True, "routes": route_entries, "ts": _now_iso()}


@router.get("/schema-version")
def schema_version(request: Request):
    if SessionLocal is None:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": "Database session not configured"},
        )

    session = SessionLocal()
    try:
        version_row = session.execute(text("SELECT version_num FROM alembic_version")).one()
        manual_columns = session.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'basic_scans'
                  AND column_name IN (
                    'manual_playlist_url',
                    'manual_playlist_id',
                    'manual_playlist_name',
                    'manual_playlist_owner',
                    'manual_playlist_image_url',
                    'manual_target_countries',
                    'manual_target_keywords'
                  )
                """
            )
        ).all()
    except Exception as exc:  # pragma: no cover - best effort defensive response
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"ok": False, "error": str(exc)},
        )
    finally:
        session.close()

    return {
        "ok": True,
        "alembic_version": version_row[0],
        "manual_columns": sorted([row[0] for row in manual_columns]),
        "ts": _now_iso(),
    }


@router.get("/sse-state")
def sse_state(request: Request):
    state = scan_event_manager.snapshot()
    return {"ok": True, **state, "ts": _now_iso()}
