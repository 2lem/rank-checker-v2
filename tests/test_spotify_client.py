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
    assert sleep_mock.call_args_list[0].args[0] == 0.2

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
