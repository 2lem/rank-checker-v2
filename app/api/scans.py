import threading

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.basic_rank_checker.events import scan_event_manager
from app.basic_rank_checker.manual_service import create_manual_scan, run_manual_scan
from app.core.db import provide_db_session
from app.schemas.manual_scan import ManualScanCreate

router = APIRouter(tags=["scans"])


@router.post("/manual")
def manual_scan(payload: ManualScanCreate, db: Session = Depends(provide_db_session)):
    scan = create_manual_scan(
        db,
        playlist_url=payload.playlist_url,
        target_keywords=payload.target_keywords,
        target_countries=payload.target_countries,
    )
    scan_event_manager.create_queue(str(scan.id))
    thread = threading.Thread(target=run_manual_scan, args=(str(scan.id),), daemon=True)
    thread.start()
    return {"ok": True, "scan_id": str(scan.id)}
