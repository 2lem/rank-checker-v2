from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from queue import Empty, Queue
from typing import Iterator

from app.basic_rank_checker.scan_logging import log_scan_heartbeat

logger = logging.getLogger(__name__)


class ScanEventManager:
    def __init__(self) -> None:
        self._queues: dict[str, Queue] = {}
        self._last_event_ts: dict[str, str] = {}
        self._last_progress: dict[str, dict] = {}
        self._stream_timeout_seconds = self._resolve_timeout()
        self._heartbeat_interval_seconds = self._resolve_heartbeat_interval()

    @staticmethod
    def _resolve_heartbeat_interval() -> int:
        raw_value = os.getenv("SCAN_SSE_HEARTBEAT_SECONDS", "3")
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = 3
        return min(max(parsed, 3), 5)

    @staticmethod
    def _resolve_timeout() -> int:
        raw_value = os.getenv("SCAN_SSE_TIMEOUT_SECONDS", "900")
        try:
            parsed = int(raw_value)
        except (TypeError, ValueError):
            parsed = 900
        return max(parsed, 60)

    def create_queue(self, scan_id: str) -> Queue:
        queue = Queue()
        self._queues[scan_id] = queue
        return queue

    def get_queue(self, scan_id: str) -> Queue | None:
        return self._queues.get(scan_id)

    def publish(self, scan_id: str, payload: dict) -> None:
        queue = self._queues.get(scan_id)
        if queue is None:
            return
        self._last_event_ts[scan_id] = datetime.now(timezone.utc).isoformat()
        if payload.get("type") == "progress":
            self._last_progress[scan_id] = {
                "message": payload.get("message"),
                "step": payload.get("step"),
                "total": payload.get("total"),
                "country": payload.get("country"),
                "keyword": payload.get("keyword"),
            }
        queue.put(payload)

    def stream(self, scan_id: str) -> Iterator[str]:
        queue = self._queues.get(scan_id)
        if queue is None:
            return iter(())

        def _generator() -> Iterator[str]:
            start_time = time.monotonic()
            last_heartbeat_at = start_time
            while True:
                try:
                    event = queue.get(timeout=1)
                except Empty:
                    elapsed = time.monotonic() - start_time
                    if elapsed >= self._stream_timeout_seconds:
                        timeout_event = {
                            "type": "error",
                            "status": "failed",
                            "message": "Scan timed out.",
                        }
                        yield f"data: {json.dumps(timeout_event)}\n\n"
                        break
                    if time.monotonic() - last_heartbeat_at >= self._heartbeat_interval_seconds:
                        heartbeat_event = {"type": "heartbeat", "status": "running"}
                        self._last_event_ts[scan_id] = datetime.now(timezone.utc).isoformat()
                        log_scan_heartbeat(scan_id, int(elapsed))
                        yield f"data: {json.dumps(heartbeat_event)}\n\n"
                        last_heartbeat_at = time.monotonic()
                    continue

                payload = dict(event)
                if payload.get("type") == "done":
                    payload["type"] = "completed"
                    payload.setdefault("status", "completed")
                elif payload.get("type") == "completed_partial":
                    payload.setdefault("status", "completed_partial")
                elif payload.get("type") == "error":
                    payload.setdefault("status", "error")

                yield f"data: {json.dumps(payload)}\n\n"
                last_heartbeat_at = time.monotonic()
                if payload.get("type") in {"completed", "completed_partial", "partial", "error"}:
                    break

        return _generator()

    def snapshot(self) -> dict[str, int | list[str]]:
        active_scan_ids = sorted(self._queues.keys())
        return {
            "queue_count": len(active_scan_ids),
            "active_scan_ids": active_scan_ids,
        }

    def get_state(self, scan_id: str) -> dict[str, object | None]:
        return {
            "last_event_ts": self._last_event_ts.get(scan_id),
            "last_progress": self._last_progress.get(scan_id),
        }


scan_event_manager = ScanEventManager()
