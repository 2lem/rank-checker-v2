# Spotify global limiter enforcement

## Summary
- Goal: enforce process-wide request-start pacing for Spotify API calls.
- Config: `SPOTIFY_GLOBAL_RPS` (default `1.5`).

## Prod diagnose verification (pending)
This environment cannot trigger or observe the production GitHub Actions workflow runs. Update the values below after running the `Prod Diagnose` workflow on `main` post-merge.

- **Before**: peak_rps = _unknown_, avg_rps = _unknown_
- **After**: peak_rps = _unknown_, avg_rps = _unknown_
- **SPOTIFY_GLOBAL_RPS used**: `1.5`
- **limiter_verdict**: _unknown_
- **safety_verdict**: _unknown_
