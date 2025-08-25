# api/main.py
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import hmac, hashlib, time, json
from .config import settings
from .db import init_db, insert_event, poll_events
from .schemas import PublishIn, PollOut, HealthOut

app = FastAPI(title="Multifronts API")

# CORS: permite a tu Streamlit Cloud
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS or ["*"],  # puedes restringir a tu dominio de Streamlit
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicializar tablas
init_db()

@app.get("/health", response_model=HealthOut)
def health():
    return {"ok": True}

@app.post("/events/publish")
def events_publish(body: PublishIn):
    try:
        ev = insert_event(org_id=body.org_id, type_=body.type, payload=body.payload)
        return {"ok": True, "event": ev}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/poll", response_model=PollOut)
def events_poll(org_id: str, after: int = 0, limit: int = 200):
    try:
        evs, cursor = poll_events(org_id=org_id, after=after, limit=limit)
        return {"events": evs, "cursor": cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---- Slack Events (opcional) ----
def _verify_slack(req: Request, body_bytes: bytes) -> bool:
    if not settings.SLACK_SIGNING_SECRET:
        return False
    timestamp = req.headers.get("X-Slack-Request-Timestamp")
    sig = req.headers.get("X-Slack-Signature")
    if not timestamp or not sig:
        return False
    # Previene replay (> 5 min)
    if abs(time.time() - int(timestamp)) > 60 * 5:
        return False
    basestring = f"v0:{timestamp}:{body_bytes.decode('utf-8')}".encode("utf-8")
    mysig = "v0=" + hmac.new(settings.SLACK_SIGNING_SECRET.encode("utf-8"), basestring, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mysig, sig)

@app.post("/slack/events")
async def slack_events(req: Request):
    body = await req.body()
    try:
        j = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # URL Verification
    if j.get("type") == "url_verification" and "challenge" in j:
        return {"challenge": j.get("challenge")}

    # Verificación de firma
    if not _verify_slack(req, body):
        raise HTTPException(status_code=401, detail="Bad signature")

    # Procesa evento mínimo (ejemplo)
    ev = j.get("event", {}) or {}
    org_id = ev.get("team") or "global"
    insert_event(org_id=org_id, type_="slack_event", payload={"raw": ev})
    return {"ok": True}
