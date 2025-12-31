from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate_message(message: str | None, max_length: int = 200) -> str | None:
    if message is None:
        return None
    return message[:max_length]


def log_scan_lifecycle(phase: str, scan_id: str, **fields: object) -> None:
    payload = {
        "type": "scan_lifecycle",
        "phase": phase,
        "scan_id": scan_id,
        "ts": _now_iso(),
        **fields,
    }
    logger.info(json.dumps(payload, sort_keys=True, default=str))


def log_scan_failure(scan_id: str, exc: Exception) -> None:
    log_scan_lifecycle(
        "failed",
        scan_id,
        exc_type=type(exc).__name__,
        exc_message_trunc=_truncate_message(str(exc)),
    )


def log_scan_heartbeat(scan_id: str, elapsed_sec: int) -> None:
    payload = {
        "type": "scan_heartbeat",
        "scan_id": scan_id,
        "elapsed_sec": elapsed_sec,
        "ts": _now_iso(),
    }
    logger.info(json.dumps(payload, sort_keys=True, default=str))


def log_scan_start(
    *, scan_id: str, playlist_id: str | None, countries: list[str], keywords: list[str]
) -> None:
    logger.info(
        "[SCAN_START] scan_id=%s playlist_id=%s countries=%s keywords=%s",
        scan_id,
        playlist_id,
        countries,
        keywords,
    )


def log_scan_progress(
    *, scan_id: str, completed_units: int, total_units: int, eta_ms: int | None
) -> None:
    logger.info(
        "[SCAN_PROGRESS] scan_id=%s completed_units=%s total_units=%s eta_ms=%s",
        scan_id,
        completed_units,
        total_units,
        eta_ms,
    )


def log_scan_cancel_requested(*, scan_id: str, source: str) -> None:
    logger.info("[SCAN_CANCEL_REQUESTED] scan_id=%s source=%s", scan_id, source)


def log_scan_cancelled(*, scan_id: str) -> None:
    logger.info("[SCAN_CANCELLED] scan_id=%s", scan_id)


def log_scan_end(*, scan_id: str, status: str, duration_ms: int | None) -> None:
    logger.info(
        "[SCAN_END] scan_id=%s status=%s duration_ms=%s",
        scan_id,
        status,
        duration_ms,
    )


def log_scan_watchdog(
    *,
    scan_id: str,
    last_event_at: str | None,
    minutes_since_last_event: int,
    action: str,
) -> None:
    logger.info(
        "[SCAN_WATCHDOG] scan_id=%s last_event_at=%s minutes_since_last_event=%s action=%s",
        scan_id,
        last_event_at,
        minutes_since_last_event,
        action,
    )
