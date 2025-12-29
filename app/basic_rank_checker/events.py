from __future__ import annotations

import json
from queue import Queue
from typing import Iterator


class ScanEventManager:
    def __init__(self) -> None:
        self._queues: dict[str, Queue] = {}

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
            while True:
                event = queue.get()
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in {"done", "error"}:
                    break

        return _generator()


scan_event_manager = ScanEventManager()
