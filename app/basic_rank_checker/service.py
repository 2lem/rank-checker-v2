from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests
import pycountry
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.basic_rank_checker.events import scan_event_manager
from app.core.db import SessionLocal
from app.core.spotify import (
    PLAYLIST_URL,
    fetch_playlist_details,
    get_access_token,
    search_playlists,
    spotify_get,
)
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult
from app.models.tracked_playlist import TrackedPlaylist

logger = logging.getLogger(__name__)
_MARKET_OVERRIDES = {"XK": "Kosovo"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _market_label(code: str) -> str:
    normalized = (code or "").strip().upper()
    if not normalized:
        return code
    if normalized in _MARKET_OVERRIDES:
        return _MARKET_OVERRIDES[normalized]
    country = pycountry.countries.get(alpha_2=normalized)
    return country.name if country else normalized


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def create_basic_scan(db: Session, tracked_playlist: TrackedPlaylist) -> BasicScan:
    is_tracked_playlist = tracked_playlist.id is not None
    scan = BasicScan(
        account_id=tracked_playlist.account_id,
        tracked_playlist_id=tracked_playlist.id,
        is_tracked_playlist=is_tracked_playlist,
        started_at=_now_utc(),
        status="running",
        scanned_countries=tracked_playlist.target_countries or [],
        scanned_keywords=tracked_playlist.target_keywords or [],
    )
    db.add(scan)
    db.commit()
    db.refresh(scan)
    return scan


def _resolve_follower_snapshot(
    playlist_id: str | None,
    followers_total: int | None,
    token: str,
) -> int | None:
    if followers_total is not None:
        return followers_total

    if not playlist_id:
        return None

    playlist_api_url = PLAYLIST_URL.format(playlist_id)
    try:
        detail = spotify_get(
            playlist_api_url,
            token,
            params={"fields": "followers.total"},
        )
    except requests.RequestException as exc:
        logger.warning("Unable to fetch playlist followers for %s: %s", playlist_id, exc)
        return None
    return (detail.get("followers") or {}).get("total")


def _extract_owner(item: dict) -> str | None:
    owner = item.get("owner") or {}
    return owner.get("display_name") or owner.get("id")


def _prefetch_playlist_metadata(
    playlist_ids: list[str], token: str, playlist_meta_cache: dict[str, dict]
) -> int:
    previous_cache_size = len(playlist_meta_cache)
    fetch_playlist_details(playlist_ids, token, playlist_meta_cache)
    return len(playlist_meta_cache) - previous_cache_size


def run_basic_scan(scan_id: str) -> None:
    if SessionLocal is None:
        return

    db = SessionLocal()
    try:
        scan = db.get(BasicScan, scan_id)
        if scan is None:
            return
        tracked_playlist = db.get(TrackedPlaylist, scan.tracked_playlist_id)
        if tracked_playlist is None:
            scan.status = "failed"
            scan.error_message = "Tracked playlist not found."
            scan.finished_at = _now_utc()
            db.add(scan)
            db.commit()
            scan_event_manager.publish(scan_id, {"type": "error", "message": scan.error_message})
            return

        scan_id_value = scan.id
        tracked_playlist_playlist_id = tracked_playlist.playlist_id
        tracked_playlist_followers_total = tracked_playlist.followers_total
        countries = list(scan.scanned_countries or [])
        keywords = list(scan.scanned_keywords or [])
        total_steps = max(len(countries) * len(keywords), 1)

        # Release the initial SELECT transaction before long-running Spotify requests.
        db.rollback()

        token = get_access_token()
        follower_snapshot = _resolve_follower_snapshot(
            tracked_playlist_playlist_id,
            tracked_playlist_followers_total,
            token,
        )
        scan = db.get(BasicScan, scan_id_value)
        if scan is None:
            return
        scan.follower_snapshot = follower_snapshot
        db.add(scan)
        db.commit()

        playlist_meta_cache: dict[str, dict] = {}
        total_playlist_occurrences = 0
        unique_playlists_fetched = 0

        step = 0
        for country in countries:
            for keyword in keywords:
                step += 1
                scan_event_manager.publish(
                    scan_id,
                    {
                        "type": "progress",
                        "country": country,
                        "keyword": keyword,
                        "message": f"Scanning {_market_label(country)} for '{keyword}'...",
                        "step": step,
                        "total": total_steps,
                    },
                )
                searched_at = _now_utc()
                items = search_playlists(keyword, country, token, limit=35, offset=0)[:20]

                playlist_ids_to_fetch = [
                    item.get("id")
                    for item in items
                    if item.get("id") and not item.get("placeholder")
                ]
                total_playlist_occurrences += len(playlist_ids_to_fetch)
                unique_playlists_fetched += _prefetch_playlist_metadata(
                    playlist_ids_to_fetch, token, playlist_meta_cache
                )

                tracked_rank = None
                query = BasicScanQuery(
                    basic_scan_id=scan_id_value,
                    country_code=country,
                    keyword=keyword,
                    searched_at=searched_at,
                    tracked_rank=None,
                    tracked_found_in_top20=False,
                )
                db.add(query)
                db.flush()

                results: list[BasicScanResult] = []
                for index, item in enumerate(items, start=1):
                    playlist_id = item.get("id")
                    is_placeholder = item.get("placeholder")
                    meta = playlist_meta_cache.get(playlist_id or "") if playlist_id else {}
                    playlist_name = meta.get("playlist_name") or item.get("name")
                    playlist_owner = meta.get("playlist_owner") or _extract_owner(item)
                    playlist_followers = None if is_placeholder else meta.get("playlist_followers")
                    tracks_total = (item.get("tracks") or {}).get("total")
                    songs_count = (
                        meta.get("songs_count")
                        if meta.get("songs_count") is not None
                        else tracks_total
                    )
                    playlist_last_added_track_at_raw = meta.get("playlist_last_track_added_at")
                    playlist_last_added_track_at = (
                        _parse_iso_datetime(playlist_last_added_track_at_raw)
                        if isinstance(playlist_last_added_track_at_raw, str)
                        else playlist_last_added_track_at_raw
                    )
                    playlist_description = meta.get("playlist_description") or item.get("description")
                    playlist_url = meta.get("playlist_url") or (item.get("external_urls") or {}).get(
                        "spotify"
                    )

                    is_tracked = playlist_id == tracked_playlist_playlist_id
                    if is_tracked and tracked_rank is None:
                        tracked_rank = index
                    results.append(
                        BasicScanResult(
                            basic_scan_query_id=query.id,
                            rank=index,
                            playlist_id=playlist_id,
                            playlist_name=playlist_name,
                            playlist_owner=playlist_owner,
                            playlist_followers=playlist_followers,
                            songs_count=songs_count,
                            playlist_last_added_track_at=playlist_last_added_track_at,
                            playlist_description=playlist_description,
                            playlist_url=playlist_url,
                            is_tracked_playlist=is_tracked,
                        )
                    )

                query.tracked_rank = tracked_rank
                query.tracked_found_in_top20 = tracked_rank is not None
                db.add_all(results)
                db.add(query)
                db.commit()

        scan.status = "completed"
        scan.finished_at = _now_utc()
        db.add(scan)
        db.commit()
        logger.info(
            "Basic scan playlist metadata fetch stats",
            extra={
                "scan_id": scan_id,
                "unique_playlists_fetched": unique_playlists_fetched,
                "total_playlist_occurrences": total_playlist_occurrences,
                "playlist_meta_cache_size": len(playlist_meta_cache),
            },
        )
        scan_event_manager.publish(scan_id, {"type": "done", "scan_id": scan_id})
    except Exception as exc:
        logger.exception("Basic scan failed")
        try:
            scan = db.get(BasicScan, scan_id)
            if scan:
                scan.status = "failed"
                scan.error_message = str(exc)
                scan.finished_at = _now_utc()
                db.add(scan)
                db.commit()
        except Exception:
            db.rollback()
        scan_event_manager.publish(scan_id, {"type": "error", "message": str(exc)})
    finally:
        db.close()


def fetch_scan_details(db: Session, scan_id: str) -> dict | None:
    scan = db.get(BasicScan, scan_id)
    if scan is None:
        return None

    tracked_playlist = db.get(TrackedPlaylist, scan.tracked_playlist_id)
    queries = (
        db.execute(
            select(BasicScanQuery).where(BasicScanQuery.basic_scan_id == scan.id).order_by(
                BasicScanQuery.searched_at.asc()
            )
        )
        .scalars()
        .all()
    )
    results = (
        db.execute(
            select(BasicScanResult)
            .join(BasicScanQuery, BasicScanResult.basic_scan_query_id == BasicScanQuery.id)
            .where(BasicScanQuery.basic_scan_id == scan.id)
            .order_by(BasicScanQuery.searched_at, BasicScanQuery.keyword, BasicScanQuery.country_code, BasicScanResult.rank)
        )
        .scalars()
        .all()
    )

    results_by_query: dict[str, list[BasicScanResult]] = {}
    for result in results:
        key = str(result.basic_scan_query_id)
        results_by_query.setdefault(key, []).append(result)

    summary = []
    detailed: dict[str, dict] = {}
    for query in queries:
        query_results = results_by_query.get(str(query.id), [])
        summary.append(
            {
                "searched_at": query.searched_at,
                "country": query.country_code,
                "keyword": query.keyword,
                "tracked_rank": query.tracked_rank,
                "tracked_found_in_top20": query.tracked_found_in_top20,
            }
        )

        country_bucket = detailed.setdefault(
            query.country_code,
            {"country": query.country_code, "keywords": {}},
        )
        country_bucket["keywords"][query.keyword] = {
            "searched_at": query.searched_at,
            "results": [
                {
                    "rank": result.rank,
                    "playlist_id": result.playlist_id,
                    "playlist_name": result.playlist_name,
                    "playlist_owner": result.playlist_owner,
                    "playlist_followers": result.playlist_followers,
                    "songs_count": result.songs_count,
                    "playlist_last_added_track_at": result.playlist_last_added_track_at,
                    "playlist_description": result.playlist_description,
                    "playlist_url": result.playlist_url,
                    "is_tracked_playlist": result.is_tracked_playlist,
                }
                for result in query_results
            ],
        }

    return {
        "scan_id": scan.id,
        "tracked_playlist_id": scan.tracked_playlist_id,
        "status": scan.status,
        "started_at": scan.started_at,
        "finished_at": scan.finished_at,
        "follower_snapshot": scan.follower_snapshot,
        "scanned_countries": scan.scanned_countries,
        "scanned_keywords": scan.scanned_keywords,
        "tracked_playlist_name": tracked_playlist.name if tracked_playlist else None,
        "summary": summary,
        "detailed": detailed,
    }
