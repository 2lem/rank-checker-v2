from datetime import datetime, timezone
import unittest
import uuid

from app.api.routes.basic_rank_checker import _format_csv_datetime, _format_scan_timestamp
from app.models.basic_scan import BasicScan


class CsvDateFormattingTest(unittest.TestCase):
    def test_format_csv_datetime_converts_to_istanbul_from_utc(self):
        utc_time = datetime(2025, 12, 28, 22, 49, tzinfo=timezone.utc)
        self.assertEqual(_format_csv_datetime(utc_time), "29-12-2025_01-49")

    def test_format_csv_datetime_handles_naive_as_utc(self):
        naive_time = datetime(2025, 12, 28, 22, 49)
        self.assertEqual(_format_csv_datetime(naive_time), "29-12-2025_01-49")

    def test_format_scan_timestamp_prefers_started_at(self):
        started_at = datetime(2024, 5, 1, 10, 15, tzinfo=timezone.utc)
        scan = BasicScan(
            id=uuid.uuid4(),
            tracked_playlist_id=uuid.uuid4(),
            started_at=started_at,
            status="completed",
        )
        self.assertEqual(_format_scan_timestamp(scan), "01-05-2024_13-15")


if __name__ == "__main__":
    unittest.main()
