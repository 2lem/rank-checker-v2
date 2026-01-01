import pytest

from app.core import spotify


@pytest.fixture(autouse=True)
def reset_spotify_rps_limiter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(spotify, "SPOTIFY_GLOBAL_RPS", 0.0)
    spotify._spotify_global_rps_limiter._next_allowed_ts = 0.0
