from datetime import datetime, timezone

from app.api.routes.basic_rank_checker import _serialize_scan_payload


def test_serialize_scan_payload_includes_spotify_metrics() -> None:
    payload = {
        "scan_id": "scan-1",
        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "started_at": None,
        "finished_at": None,
        "summary": [],
        "detailed": {},
        "spotify_total_calls": 12,
        "peak_rps": 1.5,
        "avg_rps": 1.2,
        "min_inter_start_s": 0.5,
        "any_429_count": 0,
    }

    result = _serialize_scan_payload(payload)

    assert result["spotify_total_calls"] == 12
    assert result["peak_rps"] == 1.5
    assert result["avg_rps"] == 1.2
    assert result["min_inter_start_s"] == 0.5
    assert result["any_429_count"] == 0
