# backend/api/routes_slack.py
import json, urllib.parse, httpx, os
from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse, PlainTextResponse
from sqlalchemy import text
from .dbconn import engine
from .slack_utils import (
    ensure_slack_tables, get_installation, get_hq_channel,
    ensure_hq_channel, ensure_hq_channel_verbose
)

SLACK_CLIENT_ID     = os.getenv("SLACK_CLIENT_ID","")
SLACK_CLIENT_SECRET = os.getenv("SLACK_CLIENT_SECRET","")
OAUTH_REDIRECT_URL  = os.getenv("OAUTH_REDIRECT_URL","")
SLACK_API           = "https://slack.com/api"

router = APIRouter()

@router.get("/slack/status")
def slack_status(org_id: str):
    with engine.begin() as conn:
        ensure_slack_tables(conn)
        inst = get_installation(conn, org_id)
        hq   = get_hq_channel(conn, org_id)
        return {
            "ok": True,
            "installation": {
                "has_webhook": bool(inst and inst.get("incoming_webhook_url")),
                "has_bot": bool(inst and inst.get("bot_token")),
                "default_channel_id": inst.get("default_channel_id") if inst else None,
            },
            "hq_channel": hq or {},
        }

@router.post("/admin/slack/ensure")
def slack_ensure(org_id: str):
    with engine.begin() as conn:
        ensure_slack_tables(conn)
        res = ensure_hq_channel_verbose(conn, org_id)
        return res

@router.post("/admin/slack/reconcile")
def slack_reconcile():
    results = []
    created_or_verified = 0
    with engine.begin() as conn:
        ensure_slack_tables(conn)
        rows = conn.execute(text("SELECT org_id FROM orgs")).all()
        for row in rows:
            org_id = row[0]
            res = ensure_hq_channel_verbose(conn, org_id)
            if res.get("ok"):
                created_or_verified += 1
            results.append({"org_id": org_id, **res})
    return {"ok": True, "created_or_verified": created_or_verified, "results": results}

def _slack_authorize_url(state: str, scopes: list[str]) -> str:
    params = {"client_id": SLACK_CLIENT_ID, "scope": " ".join(scopes), "redirect_uri": OAUTH_REDIRECT_URL, "state": state}
    return "https://slack.com/oauth/v2/authorize?" + urllib.parse.urlencode(params)

@router.get("/slack/install")
def slack_install(org_id: str, return_url: str | None = None):
    if not SLACK_CLIENT_ID or not OAUTH_REDIRECT_URL:
        raise HTTPException(status_code=500, detail="OAuth no configurado")
    state = json.dumps({"org_id": org_id, "return_url": return_url or ""})
    url = _slack_authorize_url(state, scopes=["incoming-webhook","chat:write","channels:read","conversations:read","conversations:write","groups:write"])
    return RedirectResponse(url)

@router.get("/slack/oauth_redirect")
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
        r = client.post("https://slack.com/api/oauth.v2.access",
                        data={"code": code, "client_id": SLACK_CLIENT_ID, "client_secret": SLACK_CLIENT_SECRET,
                              "redirect_uri": OAUTH_REDIRECT_URL})
    data = r.json()
    if not data.get("ok"):
        raise HTTPException(status_code=400, detail=str(data))
    team = data.get("team", {}) or {}
    team_id = team.get("id")
    bot_token = data.get("access_token") or data.get("bot_access_token")
    incoming = data.get("incoming_webhook") or {}
    webhook_url = incoming.get("url")
    webhook_channel = incoming.get("channel")

    with engine.begin() as conn:
        ensure_slack_tables(conn)
        conn.execute(text("""
            INSERT INTO slack_installations(org_id, team_id, bot_token, incoming_webhook_url, default_channel_id)
            VALUES (:o, :t, :bt, :wh, :dc)
            ON CONFLICT (org_id) DO UPDATE
              SET team_id = COALESCE(EXCLUDED.team_id, slack_installations.team_id),
                  bot_token = COALESCE(EXCLUDED.bot_token, slack_installations.bot_token),
                  incoming_webhook_url = COALESCE(EXCLUDED.incoming_webhook_url, slack_installations.incoming_webhook_url),
                  default_channel_id = COALESCE(EXCLUDED.default_channel_id, slack_installations.default_channel_id)
        """), {"o": org_id, "t": team_id, "bt": bot_token, "wh": webhook_url, "dc": webhook_channel})
        # Asegura canal HQ como fallback
        ensure_hq_channel(conn, org_id)

    if return_url:
        u = urllib.parse.urlparse(return_url)
        q = urllib.parse.parse_qs(u.query)
        q["slack"] = ["connected"]
        new_q = urllib.parse.urlencode({k:(v[0] if isinstance(v, list) and len(v)==1 else v) for k,v in q.items()}, doseq=True)
        dest = urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
        return RedirectResponse(dest)
    return PlainTextResponse("Slack conectado para org: " + str(org_id))

@router.get("/debug/slack/check")
def slack_check():
    """Valida que SLACK_HQ_BOT_TOKEN existe y es válido contra auth.test."""
    token = os.getenv("SLACK_HQ_BOT_TOKEN", "").strip()
    has_token = bool(token and token.startswith("xoxb-"))
    auth_ok = False
    team = None
    bot_user_id = None
    scopes = None
    error = None
    if has_token:
        try:
            with httpx.Client(timeout=6.0) as client:
                r = client.post(f"{SLACK_API}/auth.test",
                                headers={"Authorization": f"Bearer {token}"})
                jd = r.json()
                auth_ok = bool(jd.get("ok"))
                team = jd.get("team_id") or jd.get("team")
                bot_user_id = jd.get("user_id") or jd.get("bot_id")
                # algunos tokens exponen scopes en response_metadata
                scopes = (jd.get("response_metadata") or {}).get("scopes")
                if not auth_ok:
                    error = jd.get("error")
        except Exception as e:
            error = str(e)
    return {
        "has_token": has_token,
        "auth_ok": auth_ok,
        "team": team,
        "bot_user_id": bot_user_id,
        "scopes": scopes,
        "error": error,
    }

@router.get("/debug/slack/scopes")
def slack_scopes():
    """Alias simple que sólo devuelve los scopes (si Slack los incluye en auth.test)."""
    token = os.getenv("SLACK_HQ_BOT_TOKEN", "").strip()
    with httpx.Client(timeout=6.0) as client:
        r = client.post(f"{SLACK_API}/auth.test",
                        headers={"Authorization": f"Bearer {token}"})
        jd = r.json()
    return {"ok": jd.get("ok"), "scopes": (jd.get("response_metadata") or {}).get("scopes")}

@router.get("/debug/dbinfo")
def debug_dbinfo():
    url = os.getenv("DATABASE_URL","")
    masked = url
    if "@" in url and ":" in url.split("@")[0]:
        creds, hostpart = url.split("@", 1)
        user = creds.split("//",1)[-1].split(":")[0]
        masked = url.replace(creds, f"//{user}:*****")
    with engine.begin() as conn:
        ver = conn.execute(text("SELECT version()")).scalar_one()
        orgs = conn.execute(text("SELECT COUNT(*) FROM orgs")).scalar_one()
    return {"db_url": masked, "db_has_orgs": orgs, "pg_version": ver}
