import unittest
import uuid

from app.basic_rank_checker.service import create_basic_scan
from app.models.tracked_playlist import TrackedPlaylist


class _FakeSession:
    def __init__(self):
        self.added = []
        self.commits = 0

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def refresh(self, obj):
        if getattr(obj, "id", None) is None:
            obj.id = uuid.uuid4()


class CreateBasicScanTest(unittest.TestCase):
    def test_scan_from_tracked_playlist_is_marked_tracked(self):
        tracked_playlist = TrackedPlaylist(
            id=uuid.uuid4(),
            playlist_id="playlist123",
            target_countries=["US"],
            target_keywords=["pop"],
        )
        session = _FakeSession()

        scan = create_basic_scan(session, tracked_playlist)

        self.assertTrue(scan.is_tracked_playlist)
        self.assertEqual(scan.tracked_playlist_id, tracked_playlist.id)
        self.assertIsNone(scan.account_id)


if __name__ == "__main__":
    unittest.main()
