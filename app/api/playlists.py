from fastapi import APIRouter, Body

router = APIRouter(tags=["playlists"])


@router.post("/validate")
def validate_playlist(payload: dict | None = Body(default=None)):
    return {"status": "not_implemented"}


@router.post("/add")
def add_playlist(payload: dict | None = Body(default=None)):
    return {"status": "not_implemented"}


@router.get("")
def list_playlists():
    return {"status": "not_implemented"}
