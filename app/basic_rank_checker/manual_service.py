from __future__ import annotations

import logging
from datetime import datetime

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.basic_rank_checker import service as basic_service
from app.basic_rank_checker.events import scan_event_manager
from app.basic_rank_checker.scan_logging import (
    log_scan_end,
    log_scan_failure,
    log_scan_lifecycle,
    log_scan_start,
)
from app.core.config import SEARCH_URL
from app.core.db import SessionLocal
from app.core.spotify import (
    extract_playlist_id,
    fetch_spotify_playlist_metadata,
    get_access_token,
    log_scan_spotify_usage,
    normalize_spotify_playlist_url,
    search_playlists,
    start_scan_spotify_usage,
)
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult

logger = logging.getLogger(__name__)


def create_manual_scan(
    db: Session,
    playlist_url: str | None,
    target_keywords: list[str],
    target_countries: list[str],
) -> BasicScan:
    now = basic_service._now_utc()
    scan = BasicScan(
        account_id=None,
        tracked_playlist_id=None,
        is_tracked_playlist=False,
        started_at=now,
        status="queued",
        scanned_countries=target_countries or [],
        scanned_keywords=target_keywords or [],
        manual_playlist_url=playlist_url,
        manual_target_keywords=target_keywords or [],
        manual_target_countries=target_countries or [],
        last_event_at=now,
    )
    db.add(scan)
    db.commit()
    db.refresh(scan)
    log_scan_lifecycle("created", str(scan.id), kind="manual")
    return scan


def _fail_scan(db: Session, scan: BasicScan, message: str) -> None:
    log_scan_lifecycle(
        "failed",
        str(scan.id),
        exc_type="manual_scan_validation",
        exc_message_trunc=message,
    )
    scan.status = "failed"
    scan.error_message = message
    scan.error_reason = "validation"
    scan.finished_at = basic_service._now_utc()
    scan.last_event_at = scan.finished_at
    db.add(scan)
    db.commit()
    scan_event_manager.publish(str(scan.id), {"type": "error", "message": message})


def run_manual_scan(scan_id: str) -> None:
    if SessionLocal is None:
        return

    db = SessionLocal()
    scan_kind = "manual"
    tracked_playlist_id: str | None = None
    countries_count = 0
    keywords_count = 0
    ended_status = "error"
    scan_started_at: datetime | None = None
    start_scan_spotify_usage(scan_id)
    try:
        log_scan_lifecycle("task_started", scan_id)
        scan = db.get(BasicScan, scan_id)
        if scan is None:
            return
        scan_started_at = scan.started_at
        if scan.status != "running":
            scan.status = "running"
            scan.last_event_at = basic_service._now_utc()
            db.add(scan)
            db.commit()
        if basic_service._check_cancel_requested(db, scan_id):
            ended_status = "cancelled"
            return
        tracked_playlist_id = scan.tracked_playlist_id

        playlist_url = (scan.manual_playlist_url or "").strip()
        playlist_id = None
        normalized_playlist_url = None
        if playlist_url:
            normalized_playlist_url = normalize_spotify_playlist_url(playlist_url)
            playlist_id = extract_playlist_id(normalized_playlist_url)
            if not playlist_id:
                _fail_scan(db, scan, "Invalid Spotify playlist URL.")
                return

            scan.manual_playlist_url = normalized_playlist_url
            scan.manual_playlist_id = playlist_id
            db.add(scan)
            db.commit()

        scan_id_value = scan.id
        countries = list(scan.scanned_countries or [])
        keywords = list(scan.scanned_keywords or [])
        countries_count = len(countries)
        keywords_count = len(keywords)
        total_steps = max(len(countries) * len(keywords), 1)
        log_scan_start(
            scan_id=scan_id,
            playlist_id=playlist_id,
            countries=countries,
            keywords=keywords,
        )
        basic_service._persist_scan_progress(
            db,
            scan_id,
            completed_units=0,
            total_units=total_steps,
            started_at=scan.started_at,
        )

        if basic_service._check_cancel_requested(db, scan_id):
            ended_status = "cancelled"
            return

        # Release the initial SELECT transaction before long-running Spotify requests.
        db.rollback()

        token = get_access_token(scan_id=scan_id)
        manual_meta: dict = {}
        follower_snapshot = None
        if playlist_id:
            manual_meta = fetch_spotify_playlist_metadata(playlist_id, token)
            follower_snapshot = basic_service._resolve_follower_snapshot(
                playlist_id,
                manual_meta.get("playlist_followers"),
                token,
            )

        scan = db.get(BasicScan, scan_id_value)
        if scan is None:
            return

        scan.follower_snapshot = follower_snapshot
        if playlist_id:
            scan.manual_playlist_id = playlist_id
            scan.manual_playlist_url = manual_meta.get("playlist_url") or normalized_playlist_url
            scan.manual_playlist_name = manual_meta.get("playlist_name")
            scan.manual_playlist_owner = manual_meta.get("playlist_owner")
            scan.manual_playlist_image_url = manual_meta.get("playlist_image_url") or manual_meta.get(
                "playlist_image"
            )
        scan.last_event_at = basic_service._now_utc()
        db.add(scan)
        db.commit()

        playlist_meta_cache: dict[str, dict] = {}
        total_playlist_occurrences = 0
        unique_playlists_fetched = 0
        total_results_count = 0
        skipped_iterations = 0
        step = 0
        first_spotify_call_logged = False
        first_sse_event_logged = False

        for country in countries:
            for keyword in keywords:
                if basic_service._check_cancel_requested(db, scan_id):
                    ended_status = "cancelled"
                    return
                step += 1
                if not first_sse_event_logged:
                    log_scan_lifecycle("first_sse_event", scan_id)
                    first_sse_event_logged = True
                progress_payload = basic_service._persist_scan_progress(
                    db,
                    scan_id,
                    completed_units=step,
                    total_units=total_steps,
                    started_at=scan.started_at,
                )
                scan_event_manager.publish(
                    scan_id,
                    {
                        "type": "progress",
                        "country": country,
                        "keyword": keyword,
                        "message": f"Scanning {basic_service._market_label(country)} for '{keyword}'...",
                        "step": step,
                        "total": total_steps,
                        "progress_pct": progress_payload.get("progress_pct"),
                        "eta_ms": progress_payload.get("eta_ms"),
                        "eta_human": progress_payload.get("eta_human"),
                    },
                )
                basic_service._log_iteration_event(
                    "spotify_iteration_start",
                    scan_id=scan_id,
                    country=country,
                    keyword=keyword,
                )
                try:
                    searched_at = basic_service._now_utc()
                    if not first_spotify_call_logged:
                        log_scan_lifecycle(
                            "first_spotify_call",
                            scan_id,
                            endpoint=SEARCH_URL,
                        )
                        first_spotify_call_logged = True
                    items = search_playlists(keyword, country, token, limit=35, offset=0)[:20]
                    if basic_service._check_cancel_requested(db, scan_id):
                        ended_status = "cancelled"
                        return

                    playlist_ids_to_fetch = [
                        item.get("id")
                        for item in items
                        if item.get("id") and not item.get("placeholder")
                    ]
                    total_playlist_occurrences += len(playlist_ids_to_fetch)
                    unique_playlists_fetched += basic_service._prefetch_playlist_metadata(
                        playlist_ids_to_fetch, token, playlist_meta_cache
                    )
                    if basic_service._check_cancel_requested(db, scan_id):
                        ended_status = "cancelled"
                        return

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
                        result_playlist_id = item.get("id")
                        is_placeholder = item.get("placeholder")
                        meta = (
                            playlist_meta_cache.get(result_playlist_id or "")
                            if result_playlist_id
                            else {}
                        )
                        playlist_name = meta.get("playlist_name") or item.get("name")
                        playlist_owner = meta.get("playlist_owner") or basic_service._extract_owner(
                            item
                        )
                        playlist_followers = (
                            None if is_placeholder else meta.get("playlist_followers")
                        )
                        tracks_total = (item.get("tracks") or {}).get("total")
                        songs_count = (
                            meta.get("songs_count")
                            if meta.get("songs_count") is not None
                            else tracks_total
                        )
                        playlist_last_added_track_at_raw = meta.get(
                            "playlist_last_track_added_at"
                        )
                        playlist_last_added_track_at = (
                            basic_service._parse_iso_datetime(playlist_last_added_track_at_raw)
                            if isinstance(playlist_last_added_track_at_raw, str)
                            else playlist_last_added_track_at_raw
                        )
                        playlist_description = meta.get("playlist_description") or item.get(
                            "description"
                        )
                        playlist_url = meta.get("playlist_url") or (
                            (item.get("external_urls") or {}).get("spotify")
                        )

                        is_manual_playlist = (
                            playlist_id is not None and result_playlist_id == playlist_id
                        )
                        if is_manual_playlist and tracked_rank is None:
                            tracked_rank = index

                        results.append(
                            BasicScanResult(
                                basic_scan_query_id=query.id,
                                rank=index,
                                playlist_id=result_playlist_id,
                                playlist_name=playlist_name,
                                playlist_owner=playlist_owner,
                                playlist_followers=playlist_followers,
                                songs_count=songs_count,
                                playlist_last_added_track_at=playlist_last_added_track_at,
                                playlist_description=playlist_description,
                                playlist_url=playlist_url,
                                is_tracked_playlist=is_manual_playlist,
                            )
                        )

                    query.tracked_rank = tracked_rank
                    query.tracked_found_in_top20 = tracked_rank is not None
                    db.add_all(results)
                    db.add(query)
                    db.commit()
                    total_results_count += len(results)
                    basic_service._log_iteration_event(
                        "spotify_iteration_success",
                        scan_id=scan_id,
                        country=country,
                        keyword=keyword,
                    )
                except requests.RequestException as exc:
                    db.rollback()
                    skipped_iterations += 1
                    reason = basic_service._classify_spotify_error(exc)
                    basic_service._log_iteration_event(
                        "spotify_iteration_skipped",
                        scan_id=scan_id,
                        country=country,
                        keyword=keyword,
                        reason=reason,
                    )
                    continue

        scan.status = "completed"
        scan.finished_at = basic_service._now_utc()
        scan.last_event_at = scan.finished_at
        db.add(scan)
        db.commit()
        log_scan_lifecycle("completed", scan_id, results_count=total_results_count)
        ended_status = (
            "completed_partial" if skipped_iterations and total_results_count else "completed"
        )
        if skipped_iterations and total_results_count:
            logger.info(
                "scan_completed_partial",
                extra={
                    "scan_id": scan_id,
                    "skipped_iterations": skipped_iterations,
                    "total_results": total_results_count,
                },
            )
        logger.info(
            "Manual scan playlist metadata fetch stats",
            extra={
                "scan_id": scan_id,
                "unique_playlists_fetched": unique_playlists_fetched,
                "total_playlist_occurrences": total_playlist_occurrences,
                "playlist_meta_cache_size": len(playlist_meta_cache),
            },
        )
        completion_type = "completed_partial" if skipped_iterations and total_results_count else "done"
        scan_event_manager.publish(
            scan_id,
            {
                "type": completion_type,
                "scan_id": scan_id,
                "results": basic_service.fetch_scan_details(db, scan_id),
            },
        )
    except Exception as exc:
        log_scan_failure(scan_id, exc)
        logger.exception("Manual scan failed")
        ended_status = "error"
        try:
            scan = db.get(BasicScan, scan_id)
            if scan:
                scan.status = "failed"
                scan.error_message = str(exc)
                scan.error_reason = "exception"
                scan.finished_at = basic_service._now_utc()
                scan.last_event_at = scan.finished_at
                db.add(scan)
                db.commit()
        except Exception:
            db.rollback()
        scan_event_manager.publish(scan_id, {"type": "error", "message": str(exc)})
    finally:
        ended_at = basic_service._now_utc()
        log_scan_spotify_usage(
            scan_id=scan_id,
            scan_kind=scan_kind,
            tracked_playlist_id=tracked_playlist_id,
            countries_count=countries_count,
            keywords_count=keywords_count,
            ended_status=ended_status,
        )
        log_scan_end(
            scan_id=scan_id,
            status=ended_status,
            duration_ms=basic_service._duration_ms(scan_started_at, ended_at),
        )
        db.close()


def fetch_manual_scan_details(db: Session, scan_id: str) -> dict | None:
    scan = db.get(BasicScan, scan_id)
    if scan is None:
        return None

    queries = (
        db.execute(
            select(BasicScanQuery)
            .where(BasicScanQuery.basic_scan_id == scan.id)
            .order_by(BasicScanQuery.searched_at.asc())
        )
        .scalars()
        .all()
    )
    results = (
        db.execute(
            select(BasicScanResult)
            .join(BasicScanQuery, BasicScanResult.basic_scan_query_id == BasicScanQuery.id)
            .where(BasicScanQuery.basic_scan_id == scan.id)
            .order_by(
                BasicScanQuery.searched_at,
                BasicScanQuery.keyword,
                BasicScanQuery.country_code,
                BasicScanResult.rank,
            )
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
        "manual_playlist": {
            "playlist_id": scan.manual_playlist_id,
            "playlist_url": scan.manual_playlist_url,
            "playlist_name": scan.manual_playlist_name,
            "playlist_owner": scan.manual_playlist_owner,
            "playlist_image_url": scan.manual_playlist_image_url,
        },
        "summary": summary,
        "detailed": detailed,
    }
