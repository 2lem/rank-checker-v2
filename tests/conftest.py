import pytest

from app.core import spotify


@pytest.fixture(autouse=True)
def reset_spotify_rps_limiter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(spotify, "SPOTIFY_GLOBAL_RPS", 0.0)
    monkeypatch.setattr(spotify, "_SPOTIFY_RPS_NEXT_ALLOWED_TIME", 0.0)
