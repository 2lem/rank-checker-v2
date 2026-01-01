# Spotify safe-zone diagnostics (1 country × 10 keywords) — AFTER limiter

## Scope
- Scenario: 1 country + 10 keywords
- Environment: PROD (`https://rank-checker-v2-production.up.railway.app`)
- Run method: GitHub Actions `Prod Diagnose` workflow (human-sim) using `scripts/verify_prod_limiter_1x10.py`

## BEFORE vs AFTER (post-limiter)

| Metric | BEFORE (pre-limiter) | AFTER (post-limiter) |
| --- | --- | --- |
| POST `/api/basic-rank-checker/scans` duration_ms | ~652 ms | **Pending workflow run** (see `artifacts/verify_prod_limiter_1x10.json`) |
| Peak Spotify start RPS | ~8.0 | **Pending workflow run** |
| Avg Spotify start RPS | ~7.0 | **Pending workflow run** |
| Any 429s | Not recorded | **Pending workflow run** |
| Throttling evidence | None | **Pending workflow run** (`limiter_evidence` field) |

## Final verdicts (explicit)

- Timeout fix: **NOT CONFIRMED** (no post-limiter run yet)
- Global rate limiter: **NOT ACTIVE** (no post-limiter evidence captured yet)
- Spotify safety (1 country + 10 keywords): **NOT SAFE** (no post-limiter evidence captured yet)

## Required follow-up to confirm SAFE zone
1. Run the `Prod Diagnose` workflow (workflow_dispatch) once.
2. Download the artifact `verify-prod-limiter-1x10` and open `artifacts/verify_prod_limiter_1x10.json`.
3. Update the AFTER column with:
   - `post_duration_ms`
   - `peak_rps`
   - `avg_rps`
   - `any_429_count`
   - `limiter_evidence`
4. Replace the verdicts above with the computed outcomes:
   - Timeout fix: **CONFIRMED** if `post_duration_ms < 1000`
   - Global rate limiter: **ACTIVE** if `peak_rps <= 2` and `avg_rps <= 2`
   - Safety: **SAFE** if `peak_rps <= 2`, `avg_rps <= 2`, and `any_429_count == 0`

## Notes
- The diagnostics script will exit non-zero if the scan fails, if pacing exceeds the safe threshold, or if debug metrics are unavailable.
- Ensure `DEBUG_TOOLS=1` and `DEBUG_TOKEN` are configured in the prod environment for `/api/debug/spotify-metrics` access.
