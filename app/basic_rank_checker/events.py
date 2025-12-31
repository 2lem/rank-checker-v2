from __future__ import annotations

import json
import os
import time
from queue import Empty, Queue
from typing import Iterator


class ScanEventManager:
    def __init__(self) -> None:
        self._queues: dict[str, Queue] = {}
        self._stream_timeout_seconds = self._resolve_timeout()

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
        queue.put(payload)

    def stream(self, scan_id: str) -> Iterator[str]:
        queue = self._queues.get(scan_id)
        if queue is None:
            return iter(())

        def _generator() -> Iterator[str]:
            start_time = time.monotonic()
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
                    continue

                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in {"done", "error"}:
                    break

        return _generator()

    def snapshot(self) -> dict[str, int | list[str]]:
        active_scan_ids = sorted(self._queues.keys())
        return {
            "queue_count": len(active_scan_ids),
            "active_scan_ids": active_scan_ids,
        }


scan_event_manager = ScanEventManager()
