# Rank Checker v2

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

Then open <http://localhost:8080>.

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
