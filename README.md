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
