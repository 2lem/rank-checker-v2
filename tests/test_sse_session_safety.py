import inspect

from fastapi.params import Depends as DependsParam

from app.api.routes.basic_rank_checker import stream_scan_events
import app.api.scans as scans_module


def test_stream_scan_events_has_no_db_dependency() -> None:
    signature = inspect.signature(stream_scan_events)
    defaults = [param.default for param in signature.parameters.values()]

    assert all(not isinstance(default, DependsParam) for default in defaults)


def test_manual_scan_endpoint_does_not_import_db_helpers() -> None:
    source = inspect.getsource(scans_module)
    assert "get_db" not in source
    assert "SessionLocal" not in source
