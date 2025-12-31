# Rank Checker v2

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

Then open <http://localhost:8080>.

## CI functional tests

The GitHub Actions workflow `.github/workflows/ci-functional.yml` deploys to Railway, runs API smoke tests,
executes Playwright E2E tests against the deployed URL, and posts a PR comment summarizing results.

Required GitHub secrets:

- `BASE_URL` (e.g., `https://<your-app>.railway.app`)
- `TEST_TRACKED_PLAYLIST_ID`
- `TEST_PLAYLIST_URL_OR_ID`
- `BOT_GITHUB_TOKEN`

Optional secrets:

- `BASIC_SCAN_PATH`
- `ADD_PLAYLIST_PATH`

Run locally:

```bash
BASE_URL=http://localhost:8080 TEST_TRACKED_PLAYLIST_ID=123 npm run test:smoke
```

```bash
BASE_URL=http://localhost:8080 npx playwright test
```

Railway deploy is triggered by merging to main (GitHub auto-deploy).

## Production diagnose workflow

The GitHub Actions workflow `.github/workflows/prod-diagnose.yml` runs manual, production-facing diagnostics
against the deployed URL using stored secrets.

How to run:

1. Open **Actions** in GitHub.
2. Select **Prod Diagnose**.
3. Click **Run workflow**.

Artifacts are uploaded under `prod-diagnose` and include per-endpoint metrics, headers, and response bodies.

How to interpret:

- `*.metrics.txt` files contain `CURL_HTTP_CODE`, `CURL_TIME_TOTAL`, `CURL_TIME_STARTTRANSFER` (TTFB),
  `CURL_TIME_CONNECT`, and `CURL_EXIT_CODE` (28 indicates a timeout).
- `*.body.txt` files contain the first 5KB of response body for each endpoint.
- Debug endpoint responses (best-effort) are stored in `debug-*.body.txt` and can show pool saturation or
  thread/scan stalls when available.

## Health check

```bash
curl http://localhost:8080/health
```

## Safety checks

```bash
python scripts/check_sse_db_safety.py
```

## Debug tools

Set `DEBUG_TOOLS=1` and a `DEBUG_TOKEN` in your Railway environment to enable the protected debug routes.

Example request:

```bash
curl -H "X-Debug-Token: $DEBUG_TOKEN" https://<your-app>.railway.app/api/debug/db-activity
```
