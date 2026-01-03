from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.repositories.tracked_playlists import get_tracked_playlist_by_id
from app.schemas.playlist import PlaylistInsightsOut
from app.services.playlist_insights import build_playlist_insights

router = APIRouter(tags=["tracked-playlists"])


@router.get("/{tracked_playlist_id}/insights", response_model=PlaylistInsightsOut)
def get_tracked_playlist_insights(
    tracked_playlist_id: UUID,
    db: Session = Depends(get_db),
):
    tracked = get_tracked_playlist_by_id(db, tracked_playlist_id)
    if not tracked:
        raise HTTPException(status_code=404, detail="Tracked playlist not found.")
    return build_playlist_insights(
        db,
        tracked.playlist_id,
        tracked_playlist_id=str(tracked.id),
    )
