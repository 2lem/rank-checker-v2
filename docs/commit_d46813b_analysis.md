# Commit d46813b analysis vs current HEAD

## What d46813b did

**app/core/spotify.py**
- Added `[RATE_LIMIT]` logging around budget pacing and introduced a sleep change in the budget pacing branch that used `time.sleep(sleep_ms / 500)` (a regression from the expected millisecond-to-second divisor of `/1000`).
- Continued to rely on the per-scan budget pacing and concurrency guard for throttling.

**app/api/routes/basic_rank_checker.py**
- Wrapped scan start with timing + status logging (`[SCAN_START_REQUEST] duration_ms=... status=...`).
- Normalized request handling to validate `tracked_playlist_id` via `UUID(...)` parsing before DB fetches.

**app/api/scans.py**
- Switched manual scan dependency injection to use the `provide_db_session` alias so routing code does not reference the internal `get_db` helper directly.

**app/core/db.py**
- Added `json_array_default_clause()` for SQLite vs Postgres JSON defaults (so JSON arrays default correctly on both dialects).
- Introduced `provide_db_session()` as the public dependency alias.

**app/basic_rank_checker/service.py**
- Added UUID normalization helpers to safely handle `scan_id` values and to keep DB lookups consistent.
- Improved ETA and duration calculations with timezone-aware handling.

**app/models/basic_scan.py**
- Switched JSON array defaults to use `json_array_default_clause()` (SQLite-safe defaults).

**app/models/tracked_playlist.py**
- Switched JSON array defaults to use `json_array_default_clause()` (SQLite-safe defaults).

**scripts/diagnose_spotify_limits.py**
- Added a dedicated diagnostic script to simulate Spotify calls, measure RPS, and assert pacing behavior.

## What we changed after (current HEAD)

**Global RPS limiter**
- `app/core/spotify.py` now enforces global pacing via `_apply_spotify_rps_limit()` using `SPOTIFY_GLOBAL_RPS` and logs `[RATE_LIMIT]` waits for RPS-driven delays.
- Concurrency guard + budget pacing remain in place, so throttling is layered: global RPS → concurrency semaphore → budget pacing sleep.

**Budget pacing sleep divisor**
- Current HEAD uses `time.sleep(sleep_ms / 1000)` for budget pacing; the `/500` regression is not present.
- Verified by searching the repository (`rg -n "/500" app scripts`).

## Risk items
- **Regression risk**: The `/500` divisor in d46813b halves the intended sleep duration and can effectively double request rate. This must remain corrected at `/1000`.
- **Rate-limit logging**: `[RATE_LIMIT]` events are now emitted for both global RPS waits and budget pacing. These logs are the best signal for runtime pacing (though they are not exposed via an API).

## Current status
- [PASS] `/500` divisor not found in repo (`rg -n "/500" app scripts`).
- [PASS] Global RPS limiter present in `app/core/spotify.py` (`_apply_spotify_rps_limit`, `SPOTIFY_GLOBAL_RPS`).
- [PASS] Concurrency guard still present (`_spotify_concurrency_guard`).
- [PASS] Budget pacing sleep uses `/1000` (in `app/core/spotify.py`).
