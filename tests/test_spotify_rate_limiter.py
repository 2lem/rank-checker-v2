import logging
import threading
from unittest.mock import Mock

import pytest

from app.core import spotify


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self._lock = threading.Lock()
        self.sleep_calls: list[float] = []

    def monotonic(self) -> float:
        with self._lock:
            return self._now

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleep_calls.append(seconds)
            self._now += seconds


def test_spotify_rps_limiter_paces_requests() -> None:
    clock = FakeClock()
    limiter = spotify.SpotifyGlobalRpsLimiter(now=clock.monotonic, sleep=clock.sleep)

    start_times = []
    for _ in range(5):
        limiter.acquire(2.0)
        start_times.append(clock.monotonic())

    intervals = [b - a for a, b in zip(start_times, start_times[1:])]

    assert pytest.approx(start_times[0]) == 0.0
    assert start_times[-1] >= 2.0
    assert min(intervals) >= 0.5


def test_spotify_rps_limiter_concurrent_requests() -> None:
    clock = FakeClock()
    limiter = spotify.SpotifyGlobalRpsLimiter(now=clock.monotonic, sleep=clock.sleep)
    barrier = threading.Barrier(10)
    recorded_times: list[float] = []
    record_lock = threading.Lock()

    def _worker() -> None:
        barrier.wait()
        limiter.acquire(2.0)
        with record_lock:
            recorded_times.append(clock.monotonic())

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(recorded_times) == 10
    recorded_times.sort()
    intervals = [b - a for a, b in zip(recorded_times, recorded_times[1:])]

    assert recorded_times[-1] - recorded_times[0] >= 4.5
    assert min(intervals) >= 0.5


def test_spotify_rps_limiter_disabled() -> None:
    clock = FakeClock()
    limiter = spotify.SpotifyGlobalRpsLimiter(now=clock.monotonic, sleep=clock.sleep)

    limiter.acquire(0.0)

    assert clock.sleep_calls == []
    assert clock.monotonic() == 0.0


def test_spotify_rps_limiter_logs_only_when_waiting(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    limiter = spotify.SpotifyGlobalRpsLimiter(now=clock.monotonic, sleep=clock.sleep)

    caplog.set_level(logging.INFO, logger="app.core.spotify")

    limiter.acquire(1.0)
    limiter.acquire(1.0)

    log_messages = [record.message for record in caplog.records]
    assert any("[RATE_LIMIT]" in message for message in log_messages)
    assert any("global_wait_ms=" in message for message in log_messages)


def test_spotify_request_wrapper_uses_global_limiter(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_request = Mock()
    mock_request.return_value.status_code = 200
    mock_request.return_value.headers = {}
    mock_request.return_value.content = b"{}"
    mock_request.return_value.text = "{}"
    mock_request.return_value.json.return_value = {}
    mock_request.return_value.url = "https://api.spotify.com/v1/playlists/test"

    limiter_mock = Mock()
    monkeypatch.setattr(spotify, "_spotify_global_rps_limiter", limiter_mock)
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify, "SPOTIFY_GLOBAL_RPS", 2.0)

    spotify.spotify_get("https://api.spotify.com/v1/playlists/test", token="token")

    limiter_mock.acquire.assert_called_once_with(2.0)
