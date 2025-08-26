# api/main.py
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, PlainTextResponse
import hmac, hashlib, time, json
from .config import settings
from .db import init_db, insert_event, poll_events
from .schemas import PublishIn, PollOut, HealthOut
import os, urllib.parse, httpx

app = FastAPI(title="Multifronts API")

SLACK_CLIENT_ID = os.getenv("SLACK_CLIENT_ID", "")
SLACK_CLIENT_SECRET = os.getenv("SLACK_CLIENT_SECRET", "")
OAUTH_REDIRECT_URL = os.getenv("OAUTH_REDIRECT_URL", "")

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

def _slack_authorize_url(state: str, scopes: list[str]) -> str:
    params = {
        "client_id": SLACK_CLIENT_ID,
        "scope": " ".join(scopes),  # p.ej. ["incoming-webhook","chat:write"]
        "redirect_uri": OAUTH_REDIRECT_URL,
        "state": state,
    }
    return "https://slack.com/oauth/v2/authorize?" + urllib.parse.urlencode(params)

@app.get("/slack/install")
def slack_install(org_id: str, return_url: str | None = None):
    """
    Inicia OAuth. 'state' lleva org_id y return_url codificados.
    """
    if not SLACK_CLIENT_ID or not OAUTH_REDIRECT_URL:
        raise HTTPException(status_code=500, detail="OAuth no configurado")
    state = json.dumps({"org_id": org_id, "return_url": return_url or ""})
    url = _slack_authorize_url(state, scopes=["incoming-webhook","chat:write"])
    return RedirectResponse(url)

@app.get("/slack/oauth_redirect")
async def slack_oauth_redirect(code: str | None = None, state: str | None = None):
    if not code or not state:
        raise HTTPException(status_code=400, detail="Faltan parámetros")
    try:
        st_obj = json.loads(state)
        org_id = st_obj.get("org_id")
        return_url = st_obj.get("return_url") or ""
    except Exception:
        raise HTTPException(status_code=400, detail="State inválido")

    # Intercambio de código por token (OAuth v2)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://slack.com/api/oauth.v2.access",
            data={
                "code": code,
                "client_id": SLACK_CLIENT_ID,
                "client_secret": SLACK_CLIENT_SECRET,
                "redirect_uri": OAUTH_REDIRECT_URL,
            },
        )
    data = r.json()
    if not data.get("ok"):
        raise HTTPException(status_code=400, detail=str(data))

    # Datos importantes
    team = data.get("team", {}) or {}
    team_id = team.get("id")
    bot = data.get("access_token") or data.get("bot_access_token")  # v2 pone bot primero
    # Incoming webhook (si pediste scope incoming-webhook)
    incoming = data.get("incoming_webhook") or {}
    webhook_url = incoming.get("url")
    webhook_channel = incoming.get("channel")

    from .db import save_slack_install
    save_slack_install(
        org_id=org_id, team_id=team_id, bot_token=bot,
        webhook_url=webhook_url, webhook_channel=webhook_channel,
        installed_by=None
    )

    # Redirige a Streamlit o muestra un OK simple
    if return_url:
        # agrega bandera ?slack=connected
        u = urllib.parse.urlparse(return_url)
        q = urllib.parse.parse_qs(u.query)
        q["slack"] = ["connected"]
        new_q = urllib.parse.urlencode({k:v[0] if len(v)==1 else v for k,v in q.items()}, doseq=True)
        dest = urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
        return RedirectResponse(dest)
    return PlainTextResponse("Slack conectado para org: " + str(org_id))

@app.get("/slack/status")
def slack_status(org_id: str):
    from .db import get_slack_status
    return get_slack_status(org_id)