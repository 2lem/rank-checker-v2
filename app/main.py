from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Rank Checker v2")

app.mount("/static", StaticFiles(directory="app/web/static"), name="static")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Rank Checker v2</title>
      </head>
      <body style="font-family: system-ui; padding: 24px;">
        <h1>Rank Checker v2</h1>
        <p>FastAPI is running ✅</p>
        <p>Next step: /tracked</p>
      </body>
    </html>
    """
