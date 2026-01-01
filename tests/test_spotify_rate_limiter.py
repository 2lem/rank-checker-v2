import logging
from unittest.mock import Mock

import pytest

from app.core import spotify


def test_spotify_rps_limiter_paces_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_mock = Mock()
    monotonic_mock = Mock(side_effect=[100.0, 100.0])

    monkeypatch.setattr(spotify, "SPOTIFY_GLOBAL_RPS", 2.0)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.time, "monotonic", monotonic_mock)

    spotify._apply_spotify_rps_limit()
    spotify._apply_spotify_rps_limit()

    assert sleep_mock.call_count == 1
    assert sleep_mock.call_args_list[0].args[0] == pytest.approx(0.5)


def test_spotify_rps_limiter_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_mock = Mock()
    monotonic_mock = Mock()

    monkeypatch.setattr(spotify, "SPOTIFY_GLOBAL_RPS", 0.0)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.time, "monotonic", monotonic_mock)

    spotify._apply_spotify_rps_limit()

    assert sleep_mock.call_count == 0
    assert monotonic_mock.call_count == 0


def test_spotify_rps_limiter_logs_only_for_large_waits(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    sleep_mock = Mock()
    monotonic_mock = Mock(side_effect=[200.0, 200.0])

    monkeypatch.setattr(spotify, "SPOTIFY_GLOBAL_RPS", 100.0)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.time, "monotonic", monotonic_mock)

    caplog.set_level(logging.INFO, logger="app.core.spotify")

    spotify._apply_spotify_rps_limit()
    spotify._apply_spotify_rps_limit()

    assert sleep_mock.call_count == 1
    assert sleep_mock.call_args_list[0].args[0] == pytest.approx(0.01)
    assert not any("reason=rps" in record.message for record in caplog.records)
