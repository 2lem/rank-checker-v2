from fastapi import APIRouter, Body

router = APIRouter(tags=["scans"])


@router.post("/manual")
def manual_scan(payload: dict | None = Body(default=None)):
    return {"status": "not_implemented"}
