from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.basic_rank_checker.events import scan_event_manager
from app.basic_rank_checker.scan_logging import log_scan_watchdog
from app.core.db import SessionLocal
from app.models.basic_scan import BasicScan

logger = logging.getLogger(__name__)


def _resolve_stuck_minutes() -> int:
    raw_value = os.getenv("STUCK_SCAN_MINUTES", "10")
    try:
        minutes = int(raw_value)
    except (TypeError, ValueError):
        minutes = 10
    return min(max(minutes, 1), 180)


def _resolve_interval_seconds() -> int:
    raw_value = os.getenv("SCAN_WATCHDOG_INTERVAL_SECONDS", "60")
    try:
        interval = int(raw_value)
    except (TypeError, ValueError):
        interval = 60
    return min(max(interval, 30), 300)


def _run_watchdog_loop() -> None:
    if SessionLocal is None:
        return
    interval_seconds = _resolve_interval_seconds()
    stuck_minutes = _resolve_stuck_minutes()

    while True:
        try:
            _check_for_stuck_scans(stuck_minutes)
        except Exception:
            logger.exception("basic_scan_watchdog_failed")
        time.sleep(interval_seconds)


def _check_for_stuck_scans(stuck_minutes: int) -> None:
    if SessionLocal is None:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stuck_minutes)
    db = SessionLocal()
    try:
        last_event_expr = func.coalesce(
            BasicScan.last_event_at,
            BasicScan.updated_at,
            BasicScan.created_at,
        )
        query = (
            select(BasicScan)
            .where(BasicScan.status.in_(["queued", "running"]))
            .where(last_event_expr < cutoff)
            .order_by(last_event_expr.asc())
        )
        scans = db.execute(query).scalars().all()
        for scan in scans:
            last_event_at = scan.last_event_at or scan.updated_at or scan.created_at
            minutes_since = int(
                max((datetime.now(timezone.utc) - last_event_at).total_seconds() / 60, 0)
            )
            scan.status = "failed"
            scan.error_reason = "stuck_no_progress"
            scan.error_message = (
                f"Scan marked failed due to no progress for {minutes_since} minutes."
            )
            scan.finished_at = datetime.now(timezone.utc)
            scan.last_event_at = scan.finished_at
            db.add(scan)
            db.commit()
            scan_event_manager.publish(
                str(scan.id),
                {
                    "type": "failed",
                    "message": scan.error_message,
                },
            )
            log_scan_watchdog(
                scan_id=str(scan.id),
                last_event_at=last_event_at.isoformat() if last_event_at else None,
                minutes_since_last_event=minutes_since,
                action="failed",
            )
    finally:
        db.close()


def start_scan_watchdog() -> None:
    thread = threading.Thread(target=_run_watchdog_loop, daemon=True)
    thread.start()
