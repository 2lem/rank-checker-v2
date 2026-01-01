import json
import logging
from unittest.mock import Mock

import pytest
import requests

from app.core import spotify
from app.core.config import SPOTIFY_MAX_RETRY_AFTER


def _response(status_code: int, payload: dict | None = None, headers: dict | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(payload or {}).encode("utf-8")
    response.headers.update(headers or {})
    response.url = "https://api.spotify.com/v1/playlists/test"
    return response


def _event_names(caplog: pytest.LogCaptureFixture) -> list[str]:
    events = []
    for record in caplog.records:
        try:
            data = json.loads(record.message)
        except json.JSONDecodeError:
            continue
        events.append(data.get("event"))
    return events


def _scan_usage_payloads(caplog: pytest.LogCaptureFixture) -> list[dict]:
    payloads: list[dict] = []
    for record in caplog.records:
        if record.name != "app.core.spotify":
            continue
        try:
            data = json.loads(record.message)
        except json.JSONDecodeError:
            continue
        if data.get("type") == "scan_spotify_usage":
            payloads.append(data)
    return payloads


def test_spotify_logs_successful_request(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    mock_request = Mock(return_value=_response(200, {"ok": True}))
    monkeypatch.setattr(spotify.requests, "request", mock_request)

    caplog.set_level(logging.INFO, logger="app.core.spotify")

    payload = spotify.spotify_get("https://api.spotify.com/v1/playlists/test", token="token")
    assert payload == {"ok": True}

    events = _event_names(caplog)
    assert "spotify_api_request" in events
    assert "spotify_api_response" in events


def test_spotify_retries_on_429(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    responses = [
        _response(429, {"error": "rate limit"}, headers={"Retry-After": "3"}),
        _response(200, {"ok": True}),
    ]
    mock_request = Mock(side_effect=responses)
    sleep_mock = Mock()
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.random, "uniform", lambda *_args, **_kwargs: 0)

    caplog.set_level(logging.INFO, logger="app.core.spotify")

    payload = spotify.spotify_get("https://api.spotify.com/v1/playlists/test", token="token")
    assert payload == {"ok": True}
    assert mock_request.call_count == 2
    assert sleep_mock.call_count == 1
    assert sleep_mock.call_args_list[0].args[0] == 3

    events = _event_names(caplog)
    assert "spotify_api_retry" in events


def test_spotify_caps_429_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        _response(429, {"error": "rate limit"}, headers={"Retry-After": "120"})
        for _ in range(6)
    ]
    mock_request = Mock(side_effect=responses)
    sleep_mock = Mock()
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.random, "uniform", lambda *_args, **_kwargs: 0)

    with pytest.raises(requests.HTTPError):
        spotify.spotify_get("https://api.spotify.com/v1/playlists/test", token="token")

    assert mock_request.call_count == 6
    assert sleep_mock.call_count == 5
    assert all(call.args[0] <= SPOTIFY_MAX_RETRY_AFTER for call in sleep_mock.call_args_list)


def test_spotify_retries_on_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [_response(500, {"error": "server"}), _response(200, {"ok": True})]
    mock_request = Mock(side_effect=responses)
    sleep_mock = Mock()
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.random, "uniform", lambda *_args, **_kwargs: 0)

    payload = spotify.spotify_get("https://api.spotify.com/v1/playlists/test", token="token")
    assert payload == {"ok": True}
    assert mock_request.call_count == 2
    assert sleep_mock.call_count == 1
    assert sleep_mock.call_args_list[0].args[0] == 0.5


def test_spotify_paces_scan_budget(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    spotify_budget = spotify.SpotifyCallBudget()
    mock_request = Mock(return_value=_response(200, {"ok": True}))
    sleep_mock = Mock()

    monkeypatch.setattr(spotify, "_spotify_budget", spotify_budget)
    monkeypatch.setattr(spotify, "MAX_SPOTIFY_CALLS_PER_SCAN", 0)
    monkeypatch.setattr(spotify, "SPOTIFY_BUDGET_PACING_SLEEP_MS", 100)
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)

    caplog.set_level(logging.INFO, logger="app.core.spotify")

    payload = spotify.spotify_get(
        "https://api.spotify.com/v1/playlists/test",
        token="token",
        scan_id="scan-1",
    )

    assert payload == {"ok": True}
    assert mock_request.call_count == 1
    assert sleep_mock.call_count == 1
    assert sleep_mock.call_args_list[0].args[0] == 0.1

    pacing_logs = []
    for record in caplog.records:
        if record.name != "app.core.spotify":
            continue
        try:
            pacing_logs.append(json.loads(record.message).get("type"))
        except json.JSONDecodeError:
            continue
    assert "spotify_budget_pacing" in pacing_logs


def test_spotify_token_retries_missing_access_token(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [_response(200, {}) for _ in range(4)]
    mock_request = Mock(side_effect=responses)
    sleep_mock = Mock()
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.random, "uniform", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(spotify, "SPOTIFY_CLIENT_ID", "client-id")
    monkeypatch.setattr(spotify, "SPOTIFY_CLIENT_SECRET", "client-secret")

    with pytest.raises(spotify.SpotifyTokenError):
        spotify.get_access_token()

    assert mock_request.call_count == 4
    assert sleep_mock.call_count == 3
    assert [call.args[0] for call in sleep_mock.call_args_list] == [0.5, 1.0, 2.0]


def test_spotify_token_recovers_after_missing_access_token(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [_response(200, {}), _response(200, {"access_token": "token"})]
    mock_request = Mock(side_effect=responses)
    sleep_mock = Mock()
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify.random, "uniform", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(spotify, "SPOTIFY_CLIENT_ID", "client-id")
    monkeypatch.setattr(spotify, "SPOTIFY_CLIENT_SECRET", "client-secret")

    token = spotify.get_access_token()

    assert token == "token"
    assert mock_request.call_count == 2
    assert sleep_mock.call_count == 1
    assert sleep_mock.call_args_list[0].args[0] == 0.5


def test_spotify_token_retries_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        _response(429, {"error": "rate limit"}, headers={"Retry-After": "2"}),
        _response(200, {"access_token": "token"}),
    ]
    mock_request = Mock(side_effect=responses)
    sleep_mock = Mock()
    monkeypatch.setattr(spotify.requests, "request", mock_request)
    monkeypatch.setattr(spotify.time, "sleep", sleep_mock)
    monkeypatch.setattr(spotify, "SPOTIFY_CLIENT_ID", "client-id")
    monkeypatch.setattr(spotify, "SPOTIFY_CLIENT_SECRET", "client-secret")

    token = spotify.get_access_token()

    assert token == "token"
    assert mock_request.call_count == 2
    assert sleep_mock.call_count == 1
    assert sleep_mock.call_args_list[0].args[0] == 2


def test_scan_spotify_usage_summary_counts_calls(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    mock_request = Mock(return_value=_response(200, {"ok": True}))
    monkeypatch.setattr(spotify.requests, "request", mock_request)

    caplog.set_level(logging.INFO, logger="app.core.spotify")

    scan_id = "scan-usage-1"
    spotify.start_scan_spotify_usage(scan_id)
    for _ in range(3):
        spotify.spotify_get(
            "https://api.spotify.com/v1/playlists/test",
            token="token",
            scan_id=scan_id,
        )

    spotify.log_scan_spotify_usage(
        scan_id=scan_id,
        scan_kind="basic",
        tracked_playlist_id="playlist-1",
        countries_count=1,
        keywords_count=1,
        ended_status="completed",
    )

    payloads = _scan_usage_payloads(caplog)
    assert len(payloads) == 1
    assert payloads[0]["spotify_calls_total"] == 3


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_scan_spotify_usage_metrics_snapshot_tracks_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    tracker = spotify.SpotifyScanUsageTracker(ttl_seconds=60)
    monkeypatch.setattr(spotify.time, "time", clock.time)
    monkeypatch.setattr(spotify.time, "monotonic", clock.monotonic)

    scan_id = "scan-usage-2"
    tracker.start(scan_id)
    tracker.record_start(scan_id, "/v1/playlists/test")
    clock.advance(0.6)
    tracker.record_start(scan_id, "/v1/playlists/test")
    clock.advance(0.4)
    tracker.record_start(scan_id, "/v1/playlists/test")
    tracker.record_response(scan_id, 429)

    snapshot = tracker.snapshot(scan_id)
    assert snapshot is not None
    assert snapshot["spotify_total_calls"] == 3
    assert snapshot["min_inter_start_s"] == 0.4
    assert snapshot["peak_rps"] == 3.0
    assert snapshot["avg_rps"] == 3.0
    assert snapshot["any_429_count"] == 1


def test_scan_spotify_usage_metrics_snapshot_guards_short_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    tracker = spotify.SpotifyScanUsageTracker(ttl_seconds=60)
    monkeypatch.setattr(spotify.time, "time", clock.time)
    monkeypatch.setattr(spotify.time, "monotonic", clock.monotonic)

    scan_id = "scan-usage-3"
    tracker.start(scan_id)
    tracker.record_start(scan_id, "/v1/search")
    clock.advance(0.2)
    tracker.record_start(scan_id, "/v1/search")

    snapshot = tracker.snapshot(scan_id)
    assert snapshot is not None
    assert snapshot["spotify_total_calls"] == 2
    assert snapshot["avg_rps"] == 10.0


def test_scan_spotify_usage_metrics_peak_rps_uses_sliding_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    tracker = spotify.SpotifyScanUsageTracker(ttl_seconds=60)
    monkeypatch.setattr(spotify.time, "time", clock.time)
    monkeypatch.setattr(spotify.time, "monotonic", clock.monotonic)

    scan_id = "scan-usage-4"
    tracker.start(scan_id)
    tracker.record_start(scan_id, "/v1/search")
    clock.advance(0.2)
    tracker.record_start(scan_id, "/v1/search")
    clock.advance(0.2)
    tracker.record_start(scan_id, "/v1/search")
    clock.advance(1.1)
    tracker.record_start(scan_id, "/v1/search")

    snapshot = tracker.snapshot(scan_id)
    assert snapshot is not None
    assert snapshot["peak_rps"] == 3.0
    assert snapshot["min_inter_start_s"] == 0.2


def test_spotify_request_wrapper_increments_scan_call_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = spotify.SpotifyScanUsageTracker(ttl_seconds=60)
    monkeypatch.setattr(spotify, "_spotify_scan_usage", tracker)

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.text = "{}"
    mock_response.content = b"{}"
    mock_response.json.return_value = {}
    mock_response.url = "https://api.spotify.com/v1/search"
    monkeypatch.setattr(spotify.requests, "request", Mock(return_value=mock_response))

    scan_id = "scan-wrapper-1"
    spotify.start_scan_spotify_usage(scan_id)
    spotify.spotify_get("https://api.spotify.com/v1/search", token="token", scan_id=scan_id)
    spotify.spotify_get("https://api.spotify.com/v1/search", token="token", scan_id=scan_id)

    snapshot = tracker.snapshot(scan_id)
    assert snapshot is not None
    assert snapshot["spotify_total_calls"] == 2


def test_spotify_request_wrapper_tracks_429_responses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = spotify.SpotifyScanUsageTracker(ttl_seconds=60)
    monkeypatch.setattr(spotify, "_spotify_scan_usage", tracker)
    monkeypatch.setattr(spotify.random, "uniform", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(spotify.time, "sleep", Mock())

    response_429 = Mock()
    response_429.status_code = 429
    response_429.headers = {"Retry-After": "0"}
    response_429.text = "{}"
    response_429.content = b"{}"
    response_429.json.return_value = {}
    response_429.url = "https://api.spotify.com/v1/search"

    response_ok = Mock()
    response_ok.status_code = 200
    response_ok.headers = {}
    response_ok.text = "{}"
    response_ok.content = b"{}"
    response_ok.json.return_value = {}
    response_ok.url = "https://api.spotify.com/v1/search"

    monkeypatch.setattr(
        spotify.requests,
        "request",
        Mock(side_effect=[response_429, response_ok]),
    )

    scan_id = "scan-wrapper-429"
    spotify.start_scan_spotify_usage(scan_id)
    spotify.spotify_get("https://api.spotify.com/v1/search", token="token", scan_id=scan_id)

    snapshot = tracker.snapshot(scan_id)
    assert snapshot is not None
    assert snapshot["any_429_count"] == 1
