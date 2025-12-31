import logging
import threading
import uuid

from app.core.db import SessionLocal
from app.services.playlist_metadata import refresh_playlist_metadata

logger = logging.getLogger(__name__)

_refresh_lock = threading.Lock()
_inflight_refreshes: set[str] = set()


def enqueue_refresh(tracked_playlist_id: str) -> tuple[str, bool]:
    job_id = uuid.uuid4().hex
    with _refresh_lock:
        if tracked_playlist_id in _inflight_refreshes:
            return job_id, False
        _inflight_refreshes.add(tracked_playlist_id)

    thread = threading.Thread(
        target=_run_refresh,
        args=(tracked_playlist_id, job_id),
        daemon=True,
    )
    thread.start()
    return job_id, True


def _run_refresh(tracked_playlist_id: str, job_id: str) -> None:
    logger.info(
        "Refresh stats job started tracked_playlist_id=%s job_id=%s",
        tracked_playlist_id,
        job_id,
    )
    if SessionLocal is None:
        logger.error(
            "Refresh stats job failed (no database) tracked_playlist_id=%s job_id=%s",
            tracked_playlist_id,
            job_id,
        )
        _mark_complete(tracked_playlist_id)
        return

    db = SessionLocal()
    try:
        refresh_playlist_metadata(db, tracked_playlist_id)
        logger.info(
            "Refresh stats job completed tracked_playlist_id=%s job_id=%s",
            tracked_playlist_id,
            job_id,
        )
    except Exception as exc:  # pragma: no cover - best effort background logging
        logger.exception(
            "Refresh stats job failed tracked_playlist_id=%s job_id=%s error=%s",
            tracked_playlist_id,
            job_id,
            exc,
        )
    finally:
        db.close()
        _mark_complete(tracked_playlist_id)


def _mark_complete(tracked_playlist_id: str) -> None:
    with _refresh_lock:
        _inflight_refreshes.discard(tracked_playlist_id)
