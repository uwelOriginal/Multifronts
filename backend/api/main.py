# backend/api/main.py
from __future__ import annotations
import os, re, json, time, hmac, hashlib, urllib.parse
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, PlainTextResponse
from sqlalchemy import create_engine, text

from .config import settings
from .db import init_db, insert_event, poll_events  # usa tus funciones existentes
from .schemas import PublishIn, PollOut, HealthOut

app = FastAPI(title="Multifronts API")

# ---------- Config ----------
SLACK_CLIENT_ID     = os.getenv("SLACK_CLIENT_ID", "")
SLACK_CLIENT_SECRET = os.getenv("SLACK_CLIENT_SECRET", "")
OAUTH_REDIRECT_URL  = os.getenv("OAUTH_REDIRECT_URL", "")
SLACK_HQ_BOT_TOKEN  = os.getenv("SLACK_HQ_BOT_TOKEN", "").strip()
SLACK_API           = "https://slack.com/api"

# CORS para Streamlit Cloud
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- DB Init ----------
init_db()
_engine = create_engine(os.getenv("DATABASE_URL", ""), pool_pre_ping=True, future=True)

# ---------- Helpers Slack ----------
def _slug_org(org_id: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9\-_]", "-", str(org_id).strip())
    s = re.sub(r"-{2,}", "-", s).strip("-").lower()
    return s[:70]

def _ensure_slack_tables(conn) -> None:
    # Asegura tablas Slack (por si aún no corriste la migración full)
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS slack_installations (
      org_id               TEXT PRIMARY KEY,
      team_id              TEXT,
      team_name            TEXT,
      bot_user_id          TEXT,
      bot_token            TEXT,
      incoming_webhook_url TEXT,
      default_channel_id   TEXT,
      installed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
    );""")
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS slack_channels (
      org_id         TEXT PRIMARY KEY,
      channel_id     TEXT NOT NULL,
      channel_name   TEXT NOT NULL,
      created_by_bot BOOLEAN NOT NULL DEFAULT FALSE,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    );""")

def _get_installation(conn, org_id: str):
    r = conn.exec_driver_sql("""
        SELECT org_id, team_id, team_name, bot_user_id, bot_token, incoming_webhook_url, default_channel_id
          FROM slack_installations WHERE org_id = :o
    """, {"o": org_id}).fetchone()
    if not r: return None
    keys = ["org_id","team_id","team_name","bot_user_id","bot_token","incoming_webhook_url","default_channel_id"]
    return dict(zip(keys, r))

def _get_hq_channel(conn, org_id: str):
    r = conn.exec_driver_sql("""
        SELECT org_id, channel_id, channel_name, created_by_bot
          FROM slack_channels WHERE org_id = :o
    """, {"o": org_id}).fetchone()
    if not r: return None
    keys = ["org_id","channel_id","channel_name","created_by_bot"]
    return dict(zip(keys, r))

def _ensure_hq_channel(conn, org_id: str) -> Optional[str]:
    """Crea/asegura canal #mf-{org_id} en TU workspace (usa SLACK_HQ_BOT_TOKEN)."""
    if not SLACK_HQ_BOT_TOKEN:
        return None

    _ensure_slack_tables(conn)
    cur = _get_hq_channel(conn, org_id)
    if cur and cur.get("channel_id"):
        return cur["channel_id"]

    chan_name = f"mf-{_slug_org(org_id)}"
    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    with httpx.Client(timeout=8.0) as client:
        # intenta crear
        r = client.post(f"{SLACK_API}/conversations.create", data={"name": chan_name, "is_private": "false"}, headers=headers)
        data = r.json()
        chan_id = None
        if data.get("ok"):
            chan_id = data["channel"]["id"]
        elif data.get("error") == "name_taken":
            # listar y buscar
            r2 = client.get(f"{SLACK_API}/conversations.list", params={"exclude_archived": "true", "limit": "1000"}, headers=headers)
            d2 = r2.json()
            if d2.get("ok"):
                for c in d2.get("channels", []):
                    if c.get("name") == chan_name:
                        chan_id = c.get("id"); break
        if not chan_id:
            return None
        # join por si acaso
        client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)

    conn.exec_driver_sql("""
        INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot)
        VALUES (:o, :c, :n, true)
        ON CONFLICT (org_id) DO UPDATE SET channel_id = EXCLUDED.channel_id, channel_name = EXCLUDED.channel_name
    """, {"o": org_id, "c": chan_id, "n": chan_name})
    return chan_id

def _post_to_org(conn, org_id: str, message: str, blocks: list | None = None) -> bool:
    """
    Enrutado por organización:
      1) incoming_webhook_url → POST webhook
      2) bot_token + default_channel_id → chat.postMessage
      3) canal HQ #mf-{org} (creado con SLACK_HQ_BOT_TOKEN)
    """
    _ensure_slack_tables(conn)
    inst = _get_installation(conn, org_id)

    # 1) Webhook directo por org
    if inst and inst.get("incoming_webhook_url"):
        with httpx.Client(timeout=6.0) as client:
            payload = {"text": message}
            if blocks: payload["blocks"] = blocks
            r = client.post(inst["incoming_webhook_url"], json=payload)
            return r.status_code < 300

    # 2) Bot token de la org + canal por defecto
    if inst and inst.get("bot_token") and inst.get("default_channel_id"):
        headers = {"Authorization": f"Bearer {inst['bot_token']}"}
        with httpx.Client(timeout=6.0) as client:
            r = client.post(f"{SLACK_API}/chat.postMessage",
                            headers=headers,
                            json={"channel": inst["default_channel_id"], "text": message, **({"blocks": blocks} if blocks else {})})
            data = r.json()
            return bool(data.get("ok", False))

    # 3) Auto-canal en tu workspace
    chan_id = _ensure_hq_channel(conn, org_id)
    if not chan_id or not SLACK_HQ_BOT_TOKEN:
        return False
    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    with httpx.Client(timeout=6.0) as client:
        r = client.post(f"{SLACK_API}/chat.postMessage",
                        headers=headers,
                        json={"channel": chan_id, "text": message, **({"blocks": blocks} if blocks else {})})
        data = r.json()
        return bool(data.get("ok", False))

# ---------- Endpoints ----------
@app.get("/health", response_model=HealthOut)
def health():
    return {"ok": True}

@app.post("/events/publish")
def events_publish(body: PublishIn):
    try:
        ev = insert_event(org_id=body.org_id, type_=body.type, payload=body.payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    etype   = body.type
    org_id  = body.org_id
    payload = body.payload or {}

    if etype in ("org_created", "orders_approved", "transfers_approved"):
        with _engine.begin() as conn:
            try:
                if etype == "org_created":
                    _ensure_hq_channel(conn, org_id)
                else:
                    if etype == "orders_approved":
                        msg = f"✅ {etype} por {payload.get('approved_by','?')}: nuevas={payload.get('count_new',0)} dup={payload.get('count_dup',0)}"
                    else:
                        msg = f"✅ {etype} por {payload.get('approved_by','?')}: aplicadas={payload.get('count_applied',0)} dup={payload.get('count_dup',0)} sin_stock={payload.get('count_insufficient',0)}"
                    blocks = [
                        {"type":"section","text":{"type":"mrkdwn","text": f"*{etype}* — org `{org_id}`"}},
                        {"type":"section","text":{"type":"mrkdwn","text": msg}},
                    ]
                    _post_to_org(conn, org_id, msg, blocks)
            except Exception as e:
                print("Slack notify error:", e)

    return {"ok": True, "event": ev}

@app.get("/events/poll", response_model=PollOut)
def events_poll(org_id: str, after: int = 0, limit: int = 200):
    try:
        evs, cursor = poll_events(org_id=org_id, after=after, limit=limit)
        return {"events": evs, "cursor": cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/slack/reconcile")
def slack_reconcile():
    """Crea/asegura canales HQ para TODAS las orgs."""
    created_or_verified = 0
    with _engine.begin() as conn:
        _ensure_slack_tables(conn)
        orgs = conn.exec_driver_sql("SELECT org_id FROM orgs").fetchall()
        for (org_id,) in orgs:
            try:
                if _ensure_hq_channel(conn, org_id):
                    created_or_verified += 1
            except Exception as e:
                print("reconcile error:", org_id, e)
    return {"ok": True, "created_or_verified": created_or_verified}

@app.get("/slack/status")
def slack_status(org_id: str):
    try:
        with _engine.begin() as conn:
            _ensure_slack_tables(conn)
            inst = _get_installation(conn, org_id)
            hq   = _get_hq_channel(conn, org_id)
            return {
                "ok": True,
                "installation": {
                    "has_webhook": bool(inst and inst.get("incoming_webhook_url")),
                    "has_bot": bool(inst and inst.get("bot_token")),
                    "default_channel_id": inst.get("default_channel_id") if inst else None,
                },
                "hq_channel": hq or {},
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Slack OAuth (opcional) ----------
def _slack_authorize_url(state: str, scopes: list[str]) -> str:
    params = {
        "client_id": SLACK_CLIENT_ID,
        "scope": " ".join(scopes),
        "redirect_uri": OAUTH_REDIRECT_URL,
        "state": state,
    }
    return "https://slack.com/oauth/v2/authorize?" + urllib.parse.urlencode(params)

@app.get("/slack/install")
def slack_install(org_id: str, return_url: str | None = None):
    if not SLACK_CLIENT_ID or not OAUTH_REDIRECT_URL:
        raise HTTPException(status_code=500, detail="OAuth no configurado")
    state = json.dumps({"org_id": org_id, "return_url": return_url or ""})
    url = _slack_authorize_url(state, scopes=["incoming-webhook","chat:write","conversations:read","conversations:write"])
    return RedirectResponse(url)

@app.get("/slack/oauth_redirect")
def slack_oauth_redirect(code: str | None = None, state: str | None = None):
    if not code or not state:
        raise HTTPException(status_code=400, detail="Faltan parámetros")
    try:
        st_obj = json.loads(state)
        org_id = st_obj.get("org_id")
        return_url = st_obj.get("return_url") or ""
    except Exception:
        raise HTTPException(status_code=400, detail="State inválido")

    with httpx.Client(timeout=10) as client:
        r = client.post(
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

    team = data.get("team", {}) or {}
    team_id = team.get("id")
    bot_token = data.get("access_token") or data.get("bot_access_token")
    incoming = data.get("incoming_webhook") or {}
    webhook_url = incoming.get("url")
    webhook_channel = incoming.get("channel")

    # Guardado simple en slack_installations
    with _engine.begin() as conn:
        _ensure_slack_tables(conn)
        conn.exec_driver_sql("""
            INSERT INTO slack_installations(org_id, team_id, bot_token, incoming_webhook_url, default_channel_id)
            VALUES (:o, :t, :bt, :wh, :dc)
            ON CONFLICT (org_id) DO UPDATE
            SET team_id = COALESCE(EXCLUDED.team_id, slack_installations.team_id),
                bot_token = COALESCE(EXCLUDED.bot_token, slack_installations.bot_token),
                incoming_webhook_url = COALESCE(EXCLUDED.incoming_webhook_url, slack_installations.incoming_webhook_url),
                default_channel_id = COALESCE(EXCLUDED.default_channel_id, slack_installations.default_channel_id)
        """, {"o": org_id, "t": team_id, "bt": bot_token, "wh": webhook_url, "dc": webhook_channel})

        # asegura canal HQ también (útil como fallback)
        _ensure_hq_channel(conn, org_id)

    if return_url:
        u = urllib.parse.urlparse(return_url)
        q = urllib.parse.parse_qs(u.query)
        q["slack"] = ["connected"]
        new_q = urllib.parse.urlencode({k:(v[0] if isinstance(v, list) and len(v)==1 else v) for k,v in q.items()}, doseq=True)
        dest = urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
        return RedirectResponse(dest)
    return PlainTextResponse("Slack conectado para org: " + str(org_id))

@app.post("/admin/slack/reconcile")
def slack_reconcile():
    """Crea/asegura canales HQ para TODAS las orgs."""
    created_or_verified = 0
    with _engine.begin() as conn:
        _ensure_slack_tables(conn)
        orgs = conn.exec_driver_sql("SELECT org_id FROM orgs").fetchall()
        for (org_id,) in orgs:
            try:
                if _ensure_hq_channel(conn, org_id):
                    created_or_verified += 1
            except Exception as e:
                print("reconcile error:", org_id, e)
    return {"ok": True, "created_or_verified": created_or_verified}

@app.get("/debug/dbinfo")
def debug_dbinfo():
    url = os.getenv("DATABASE_URL","")
    masked = url
    if "@" in url and ":" in url.split("@")[0]:
        creds, hostpart = url.split("@", 1)
        user = creds.split("//",1)[-1].split(":")[0]
        masked = url.replace(creds, f"//{user}:*****")
    with _engine.begin() as conn:
        ver = conn.exec_driver_sql("SELECT version()").scalar_one()
        orgs = conn.exec_driver_sql("SELECT COUNT(*) FROM orgs").scalar_one()
    return {
        "db_url": masked,
        "db_has_orgs": orgs,
        "pg_version": ver,
        "has_slack_token": bool(SLACK_HQ_BOT_TOKEN and SLACK_HQ_BOT_TOKEN.startswith("xoxb-"))
    }
