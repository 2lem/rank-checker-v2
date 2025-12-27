from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.tracked_playlist import TrackedPlaylist


def get_tracked_playlist_by_playlist_id(db: Session, playlist_id: str) -> TrackedPlaylist | None:
    return db.execute(
        select(TrackedPlaylist).where(TrackedPlaylist.playlist_id == playlist_id)
    ).scalar_one_or_none()


def get_tracked_playlist_by_id(db: Session, tracked_playlist_id: str) -> TrackedPlaylist | None:
    return db.execute(
        select(TrackedPlaylist).where(TrackedPlaylist.id == tracked_playlist_id)
    ).scalar_one_or_none()


def list_tracked_playlists(db: Session) -> list[TrackedPlaylist]:
    return db.execute(select(TrackedPlaylist).order_by(TrackedPlaylist.created_at.desc())).scalars().all()


def create_tracked_playlist(
    db: Session,
    *,
    playlist_id: str,
    playlist_url: str | None,
    name: str | None,
    cover_image_url_small: str | None = None,
    owner_name: str | None = None,
    followers_total: int | None = None,
    tracks_count: int | None = None,
    last_meta_scan_at=None,
    playlist_last_updated_at=None,
    target_countries: list[str] | None,
    target_keywords: list[str] | None,
) -> TrackedPlaylist:
    tracked = TrackedPlaylist(
        playlist_id=playlist_id,
        playlist_url=playlist_url,
        name=name,
        cover_image_url_small=cover_image_url_small,
        owner_name=owner_name,
        followers_total=followers_total,
        tracks_count=tracks_count,
        last_meta_scan_at=last_meta_scan_at,
        playlist_last_updated_at=playlist_last_updated_at,
        target_countries=target_countries or [],
        target_keywords=target_keywords or [],
    )
    db.add(tracked)
    db.commit()
    db.refresh(tracked)
    return tracked
