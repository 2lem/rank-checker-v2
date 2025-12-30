from fastapi import APIRouter, Depends, HTTPException

from app.api.routes.basic_rank_checker import start_basic_scan

router = APIRouter(tags=["scans"])


@router.post("/manual")
def manual_scan(start_response: dict = Depends(start_basic_scan)):
    scan_id = (start_response or {}).get("scan_id")
    if not scan_id:
        raise HTTPException(status_code=500, detail="Failed to start manual scan.")

    return {"ok": True, "scan_id": scan_id}
