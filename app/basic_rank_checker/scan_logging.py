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
