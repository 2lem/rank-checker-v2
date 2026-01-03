from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
import uuid

from app.basic_rank_checker import service as basic_service
from app.services import playlist_metadata


class _FakeSession:
    def __init__(self):
        self.added = []
        self.commits = 0
        self.rollbacks = 0

    def get(self, _model, _key):
        return None

    def execute(self, _query):
        class _Result:
            def scalar_one_or_none(self):
                return None

        return _Result()

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def refresh(self, obj):
        return None

    def rollback(self):
        self.rollbacks += 1


def test_scan_prefetch_metadata_does_not_call_latest_track_added():
    detail = {
        "name": "Test Playlist",
        "external_urls": {"spotify": "https://open.spotify.com/playlist/playlist123"},
        "followers": {"total": 100},
        "tracks": {"total": 10},
        "description": "Desc",
        "images": [{"url": "https://example.com/image.jpg"}],
        "snapshot_id": "snapshot123",
        "owner": {"display_name": "Owner", "id": "owner-id"},
    }
    cache = {}
    with patch("app.core.spotify.spotify_get", return_value=detail), patch(
        "app.core.spotify.get_latest_track_added_at"
    ) as get_latest_track_added_at:
        basic_service._prefetch_playlist_metadata(["playlist123"], "token", cache)

    get_latest_track_added_at.assert_not_called()
    assert cache["playlist123"]["playlist_last_track_added_at"] is None


def test_refresh_playlist_metadata_calls_latest_track_added():
    tracked_playlist_id = uuid.uuid4()
    tracked = SimpleNamespace(
        id=tracked_playlist_id,
        playlist_id="playlist123",
        playlist_url="https://open.spotify.com/playlist/playlist123",
        target_countries=["US"],
        name="Old Name",
        description="Old Desc",
        cover_image_url_small=None,
        cover_image_url_large=None,
        owner_name=None,
        followers_total=None,
        tracks_count=None,
        playlist_last_updated_at=None,
        last_meta_refresh_at=None,
    )
    detail = {
        "name": "New Name",
        "description": "New Desc",
        "images": [{"url": "https://example.com/image.jpg", "width": 300, "height": 300}],
        "owner": {"display_name": "New Owner", "id": "owner-id"},
        "followers": {"total": 123},
        "tracks": {"total": 10},
        "external_urls": {"spotify": "https://open.spotify.com/playlist/playlist123"},
        "snapshot_id": "snapshot123",
    }
    db = _FakeSession()
    last_track_added = "2024-02-20T10:00:00Z"

    with patch(
        "app.services.playlist_metadata.get_tracked_playlist_by_id", return_value=tracked
    ), patch(
        "app.services.playlist_metadata.get_access_token", return_value="token"
    ), patch(
        "app.services.playlist_metadata.spotify_get", return_value=detail
    ), patch(
        "app.services.playlist_metadata.get_latest_track_added_at",
        return_value=last_track_added,
    ) as get_latest_track_added_at:
        refreshed = playlist_metadata.refresh_playlist_metadata(db, str(tracked_playlist_id))

    get_latest_track_added_at.assert_called_once_with("playlist123", "snapshot123", "token")
    assert refreshed.playlist_last_updated_at == datetime(2024, 2, 20, 10, 0, tzinfo=timezone.utc)
