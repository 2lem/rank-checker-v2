from datetime import datetime, timezone
import unittest
import uuid
from zoneinfo import ZoneInfo

from app.api.routes.basic_rank_checker import (
    _format_csv_datetime,
    _format_scan_timestamp,
    _resolve_timezone,
)
from app.models.basic_scan import BasicScan


class CsvDateFormattingTest(unittest.TestCase):
    def test_format_csv_datetime_converts_to_istanbul_from_utc(self):
        utc_time = datetime(2025, 12, 28, 22, 49, tzinfo=timezone.utc)
        ist_tz = ZoneInfo("Europe/Istanbul")
        self.assertEqual(_format_csv_datetime(utc_time, ist_tz), "29-12-2025_01-49")

    def test_format_csv_datetime_handles_naive_as_utc(self):
        naive_time = datetime(2025, 12, 28, 22, 49)
        est_tz = ZoneInfo("America/New_York")
        self.assertEqual(_format_csv_datetime(naive_time, est_tz), "28-12-2025_17-49")

    def test_format_scan_timestamp_prefers_started_at(self):
        started_at = datetime(2024, 5, 1, 10, 15, tzinfo=timezone.utc)
        tz = ZoneInfo("Europe/Istanbul")
        scan = BasicScan(
            id=uuid.uuid4(),
            tracked_playlist_id=uuid.uuid4(),
            started_at=started_at,
            status="completed",
        )
        self.assertEqual(_format_scan_timestamp(scan, tz), "01-05-2024_13-15")

    def test_resolve_timezone_falls_back_on_invalid(self):
        tz = _resolve_timezone("Invalid/Zone")
        self.assertEqual(tz.key, "UTC")


if __name__ == "__main__":
    unittest.main()
