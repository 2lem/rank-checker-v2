from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests
import pycountry
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.basic_rank_checker.events import scan_event_manager
from app.basic_rank_checker.scan_logging import (
    log_scan_cancelled,
    log_scan_end,
    log_scan_failure,
    log_scan_lifecycle,
    log_scan_progress,
    log_scan_start,
)
from app.core.basic_scan_visibility import log_basic_scan_end
from app.core.config import BASIC_SCAN_KEYWORD_TIMEOUT_SECONDS, SEARCH_URL
from app.core.db import SessionLocal
from app.core.spotify import (
    PLAYLIST_URL,
    fetch_playlist_details,
    get_access_token,
    log_scan_spotify_usage,
    search_playlists,
    start_scan_spotify_usage,
    spotify_get,
)
from app.models.basic_scan import BasicScan, BasicScanQuery, BasicScanResult
from app.models.tracked_playlist import TrackedPlaylist

logger = logging.getLogger(__name__)
_MARKET_OVERRIDES = {"XK": "Kosovo"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _format_eta_human(seconds_remaining: float) -> str:
    total_seconds = max(int(round(seconds_remaining)), 0)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _calculate_eta(
    *, started_at: datetime | None, completed_units: int, total_units: int, now: datetime
) -> tuple[int | None, str | None]:
    if not started_at or completed_units <= 0 or total_units <= 0:
        return None, None
    elapsed_seconds = (now - started_at).total_seconds()
    if elapsed_seconds <= 0:
        return None, None
    avg_seconds = elapsed_seconds / max(completed_units, 1)
    remaining_units = max(total_units - completed_units, 0)
    remaining_seconds = avg_seconds * remaining_units
    eta_ms = int(max(remaining_seconds * 1000, 0))
    return eta_ms, _format_eta_human(remaining_seconds)


def _persist_scan_progress(
    db: Session,
    scan_id: str,
    *,
    completed_units: int,
    total_units: int,
    started_at: datetime | None,
) -> dict[str, int | str | None]:
    scan = db.get(BasicScan, scan_id)
    if scan is None:
        return {
            "completed_units": completed_units,
            "total_units": total_units,
            "progress_pct": None,
            "eta_ms": None,
            "eta_human": None,
        }
    now = _now_utc()
    eta_ms, eta_human = _calculate_eta(
        started_at=started_at,
        completed_units=completed_units,
        total_units=total_units,
        now=now,
    )
    progress_pct = int(round((completed_units / total_units) * 100)) if total_units else 0
    scan.progress_completed_units = completed_units
    scan.progress_total_units = total_units
    scan.progress_pct = progress_pct
    scan.eta_ms = eta_ms
    scan.eta_human = eta_human
    scan.last_progress_at = now
    scan.last_event_at = now
    db.add(scan)
    db.commit()
    log_scan_progress(
        scan_id=scan_id,
        completed_units=completed_units,
        total_units=total_units,
        eta_ms=eta_ms,
    )
    return {
        "completed_units": completed_units,
        "total_units": total_units,
        "progress_pct": progress_pct,
        "eta_ms": eta_ms,
        "eta_human": eta_human,
    }


def _progress_payload(scan: BasicScan, completed_units: int, total_units: int) -> dict:
    stored_completed = scan.progress_completed_units
    stored_total = scan.progress_total_units
    stored_pct = scan.progress_pct
    completed = stored_completed if stored_completed is not None else completed_units
    total = stored_total if stored_total is not None else total_units
    progress_pct = stored_pct
    if progress_pct is None:
        progress_pct = int(round((completed / total) * 100)) if total else 0
    return {
        "completed_units": completed,
        "total_units": total,
        "progress_pct": progress_pct,
        "eta_ms": scan.eta_ms,
        "eta_human": scan.eta_human,
    }


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


def _classify_spotify_error(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError):
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code == 429:
            return "rate_limit"
        if status_code == 408 or (isinstance(status_code, int) and status_code >= 500):
            return "transient"
        return "transient"
    if isinstance(exc, requests.RequestException):
        return "transient"
    return "transient"


def _duration_ms(started_at: datetime | None, ended_at: datetime | None) -> int | None:
    if not started_at or not ended_at:
        return None
    return int(max((ended_at - started_at).total_seconds() * 1000, 0))


def _check_cancel_requested(db: Session, scan_id: str) -> bool:
    db.expire_all()
    scan = db.get(BasicScan, scan_id)
    if scan is None:
        return True
    if scan.status == "cancelled":
        return True
    if scan.cancel_requested_at is None:
        return False
    now = _now_utc()
    scan.status = "cancelled"
    scan.cancelled_at = scan.cancelled_at or now
    scan.finished_at = scan.finished_at or now
    scan.error_message = scan.error_message or "Scan cancelled."
    scan.error_reason = scan.error_reason or "cancelled"
    scan.last_event_at = now
    db.add(scan)
    db.commit()
    scan_event_manager.publish(scan_id, {"type": "cancelled", "message": "Scan cancelled."})
    log_scan_cancelled(scan_id=scan_id)
    return True


def _log_iteration_event(
    event: str, *, scan_id: str, country: str, keyword: str, reason: str | None = None
) -> None:
    payload = {"scan_id": scan_id, "country": country, "keyword": keyword}
    if reason:
        payload["reason"] = reason
    logger.info(event, extra=payload)


def _enforce_keyword_timeout(
    *,
    started_monotonic: float,
    scan_id: str,
    country: str,
    keyword: str,
) -> None:
    if BASIC_SCAN_KEYWORD_TIMEOUT_SECONDS <= 0:
        return
    elapsed = time.monotonic() - started_monotonic
    if elapsed <= BASIC_SCAN_KEYWORD_TIMEOUT_SECONDS:
        return
    message = (
        "Basic scan exceeded per-keyword time limit "
        f"({BASIC_SCAN_KEYWORD_TIMEOUT_SECONDS}s) for '{keyword}' in {country}."
    )
    logger.warning(
        "basic_scan_keyword_timeout",
        extra={"scan_id": scan_id, "country": country, "keyword": keyword, "elapsed": elapsed},
    )
    raise TimeoutError(message)


def create_basic_scan(db: Session, tracked_playlist: TrackedPlaylist) -> BasicScan:
    is_tracked_playlist = tracked_playlist.id is not None
    now = _now_utc()
    scan = BasicScan(
        account_id=tracked_playlist.account_id,
        tracked_playlist_id=tracked_playlist.id,
        is_tracked_playlist=is_tracked_playlist,
        started_at=now,
        status="queued",
        scanned_countries=tracked_playlist.target_countries or [],
        scanned_keywords=tracked_playlist.target_keywords or [],
        last_event_at=now,
    )
    db.add(scan)
    db.commit()
    db.refresh(scan)
    log_scan_lifecycle("created", str(scan.id), kind="basic")
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
    scan_kind = "basic"
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
            scan.last_event_at = _now_utc()
            db.add(scan)
            db.commit()
        if _check_cancel_requested(db, scan_id):
            ended_status = "cancelled"
            return
        tracked_playlist_id = scan.tracked_playlist_id
        tracked_playlist = db.get(TrackedPlaylist, scan.tracked_playlist_id)
        if tracked_playlist is None:
            log_scan_lifecycle(
                "failed",
                scan_id,
                exc_type="tracked_playlist_missing",
                exc_message_trunc="Tracked playlist not found.",
            )
            scan.status = "failed"
            scan.error_message = "Tracked playlist not found."
            scan.error_reason = "tracked_playlist_missing"
            scan.finished_at = _now_utc()
            scan.last_event_at = scan.finished_at
            db.add(scan)
            db.commit()
            scan_event_manager.publish(scan_id, {"type": "error", "message": scan.error_message})
            return

        scan_id_value = scan.id
        tracked_playlist_playlist_id = tracked_playlist.playlist_id
        tracked_playlist_followers_total = tracked_playlist.followers_total
        countries = list(scan.scanned_countries or [])
        keywords = list(scan.scanned_keywords or [])
        countries_count = len(countries)
        keywords_count = len(keywords)
        total_steps = max(len(countries) * len(keywords), 1)
        log_scan_start(
            scan_id=scan_id,
            playlist_id=str(tracked_playlist_id) if tracked_playlist_id else None,
            countries=countries,
            keywords=keywords,
        )
        _persist_scan_progress(
            db,
            scan_id,
            completed_units=0,
            total_units=total_steps,
            started_at=scan.started_at,
        )

        if _check_cancel_requested(db, scan_id):
            ended_status = "cancelled"
            return

        # Release the initial SELECT transaction before long-running Spotify requests.
        db.rollback()

        token = get_access_token(scan_id=scan_id)
        follower_snapshot = _resolve_follower_snapshot(
            tracked_playlist_playlist_id,
            tracked_playlist_followers_total,
            token,
        )
        scan = db.get(BasicScan, scan_id_value)
        if scan is None:
            return
        scan.follower_snapshot = follower_snapshot
        scan.last_event_at = _now_utc()
        db.add(scan)
        db.commit()

        playlist_meta_cache: dict[str, dict] = {}
        total_playlist_occurrences = 0
        unique_playlists_fetched = 0
        total_results_count = 0
        skipped_iterations = 0
        first_spotify_call_logged = False
        first_sse_event_logged = False

        step = 0
        for country in countries:
            for keyword in keywords:
                if _check_cancel_requested(db, scan_id):
                    ended_status = "cancelled"
                    return
                step += 1
                iteration_started = time.monotonic()
                if not first_sse_event_logged:
                    log_scan_lifecycle("first_sse_event", scan_id)
                    first_sse_event_logged = True
                progress_payload = _persist_scan_progress(
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
                        "message": f"Scanning {_market_label(country)} for '{keyword}'...",
                        "step": step,
                        "total": total_steps,
                        "completed_units": step,
                        "total_units": total_steps,
                        "progress_pct": progress_payload.get("progress_pct"),
                        "eta_ms": progress_payload.get("eta_ms"),
                        "eta_human": progress_payload.get("eta_human"),
                    },
                )
                _log_iteration_event(
                    "spotify_iteration_start",
                    scan_id=scan_id,
                    country=country,
                    keyword=keyword,
                )
                try:
                    searched_at = _now_utc()
                    if not first_spotify_call_logged:
                        log_scan_lifecycle(
                            "first_spotify_call",
                            scan_id,
                            endpoint=SEARCH_URL,
                        )
                        first_spotify_call_logged = True
                    items = search_playlists(keyword, country, token, limit=35, offset=0)[:20]
                    if _check_cancel_requested(db, scan_id):
                        ended_status = "cancelled"
                        return
                    _enforce_keyword_timeout(
                        started_monotonic=iteration_started,
                        scan_id=scan_id,
                        country=country,
                        keyword=keyword,
                    )

                    playlist_ids_to_fetch = [
                        item.get("id")
                        for item in items
                        if item.get("id") and not item.get("placeholder")
                    ]
                    total_playlist_occurrences += len(playlist_ids_to_fetch)
                    unique_playlists_fetched += _prefetch_playlist_metadata(
                        playlist_ids_to_fetch, token, playlist_meta_cache
                    )
                    if _check_cancel_requested(db, scan_id):
                        ended_status = "cancelled"
                        return
                    _enforce_keyword_timeout(
                        started_monotonic=iteration_started,
                        scan_id=scan_id,
                        country=country,
                        keyword=keyword,
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
                            _parse_iso_datetime(playlist_last_added_track_at_raw)
                            if isinstance(playlist_last_added_track_at_raw, str)
                            else playlist_last_added_track_at_raw
                        )
                        playlist_description = meta.get("playlist_description") or item.get(
                            "description"
                        )
                        playlist_url = meta.get("playlist_url") or (
                            item.get("external_urls") or {}
                        ).get("spotify")

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
                    _enforce_keyword_timeout(
                        started_monotonic=iteration_started,
                        scan_id=scan_id,
                        country=country,
                        keyword=keyword,
                    )
                    total_results_count += len(results)
                    _log_iteration_event(
                        "spotify_iteration_success",
                        scan_id=scan_id,
                        country=country,
                        keyword=keyword,
                    )
                except requests.RequestException as exc:
                    db.rollback()
                    skipped_iterations += 1
                    reason = _classify_spotify_error(exc)
                    _log_iteration_event(
                        "spotify_iteration_skipped",
                        scan_id=scan_id,
                        country=country,
                        keyword=keyword,
                        reason=reason,
                    )
                    continue

        scan.status = "completed"
        scan.finished_at = _now_utc()
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
            "Basic scan playlist metadata fetch stats",
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
                "results": fetch_scan_details(db, scan_id),
            },
        )
    except Exception as exc:
        log_scan_failure(scan_id, exc)
        logger.exception("Basic scan failed")
        ended_status = "error"
        try:
            scan = db.get(BasicScan, scan_id)
            if scan:
                scan.status = "failed"
                scan.error_message = str(exc)
                scan.error_reason = "exception"
                scan.finished_at = _now_utc()
                scan.last_event_at = scan.finished_at
                db.add(scan)
                db.commit()
        except Exception:
            db.rollback()
        scan_event_manager.publish(scan_id, {"type": "error", "message": str(exc)})
    finally:
        ended_at = _now_utc()
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
            duration_ms=_duration_ms(scan_started_at, ended_at),
        )
        log_basic_scan_end(scan_id=scan_id)
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

    manual_playlist = None
    tracked_playlist_name = tracked_playlist.name if tracked_playlist else None
    if tracked_playlist is None and scan.tracked_playlist_id is None:
        manual_playlist = {
            "playlist_id": scan.manual_playlist_id,
            "playlist_url": scan.manual_playlist_url,
            "playlist_name": scan.manual_playlist_name,
            "playlist_owner": scan.manual_playlist_owner,
            "playlist_image_url": scan.manual_playlist_image_url,
        }
        tracked_playlist_name = scan.manual_playlist_name

    total_units = max(len(scan.scanned_countries or []) * len(scan.scanned_keywords or []), 1)
    progress_payload = _progress_payload(scan, len(queries), total_units)

    return {
        "scan_id": scan.id,
        "tracked_playlist_id": scan.tracked_playlist_id,
        "status": scan.status,
        "created_at": scan.created_at,
        "started_at": scan.started_at,
        "finished_at": scan.finished_at,
        "follower_snapshot": scan.follower_snapshot,
        "scanned_countries": scan.scanned_countries,
        "scanned_keywords": scan.scanned_keywords,
        "progress": progress_payload,
        "tracked_playlist_name": tracked_playlist_name,
        "manual_playlist": manual_playlist,
        "summary": summary,
        "detailed": detailed,
    }
