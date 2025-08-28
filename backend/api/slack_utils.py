# backend/api/slack_utils.py
import os, re
from typing import Optional, Dict, Any, Tuple, List
import httpx
from sqlalchemy import text

SLACK_API = "https://slack.com/api"
SLACK_HQ_BOT_TOKEN = os.getenv("SLACK_HQ_BOT_TOKEN", "").strip()

# ---- Cliente HTTPX global con keep-alive y HTTP/2 ----
_client: httpx.Client | None = None
def _httpx() -> httpx.Client:
    global _client
    if _client is None:
        try:
            _client = httpx.Client(timeout=8.0, http2=True,
                                   limits=httpx.Limits(max_connections=50, max_keepalive_connections=10))
        except ImportError:
            # Loggea un warning y baja a HTTP/1.1
            # logger.warning("HTTP/2 not available; falling back to HTTP/1.1")
            _client = httpx.Client(timeout=8.0, http2=False,
                                   limits=httpx.Limits(max_connections=50, max_keepalive_connections=10))
    return _client

def _slug_org(org_id: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9\-_]", "-", str(org_id).strip())
    s = re.sub(r"-{2,}", "-", s).strip("-").lower()
    return s[:70]

def ensure_slack_tables(conn) -> None:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS slack_installations (
      org_id               TEXT PRIMARY KEY,
      team_id              TEXT,
      team_name            TEXT,
      bot_user_id          TEXT,
      bot_token            TEXT,
      incoming_webhook_url TEXT,
      default_channel_id   TEXT,
      installed_at         TIMESTAMPTZ NOT NULL DEFAULT now()
    );"""))
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS slack_channels (
      org_id         TEXT PRIMARY KEY,
      channel_id     TEXT NOT NULL,
      channel_name   TEXT NOT NULL,
      created_by_bot BOOLEAN NOT NULL DEFAULT FALSE,
      is_private     BOOLEAN,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    );"""))
    conn.execute(text("ALTER TABLE slack_channels ADD COLUMN IF NOT EXISTS is_private BOOLEAN;"))

def get_installation(conn, org_id: str) -> Optional[Dict[str, Any]]:
    res = conn.execute(text("""
        SELECT org_id, team_id, team_name, bot_user_id, bot_token, incoming_webhook_url, default_channel_id
        FROM slack_installations
        WHERE org_id = :o
    """), {"o": org_id})
    row = res.mappings().first()
    return dict(row) if row else None

def get_hq_channel(conn, org_id: str) -> Optional[Dict[str, Any]]:
    res = conn.execute(text("""
        SELECT org_id, channel_id, channel_name, created_by_bot, is_private
        FROM slack_channels
        WHERE org_id = :o
    """), {"o": org_id})
    row = res.mappings().first()
    return dict(row) if row else None

def auth_test() -> Dict[str, Any]:
    token = SLACK_HQ_BOT_TOKEN
    if not token:
        return {"ok": False, "error": "no_token"}
    r = _httpx().post(f"{SLACK_API}/auth.test",
                      headers={"Authorization": f"Bearer {token}"})
    return r.json()

def _find_channel_by_list(client: httpx.Client, headers: Dict[str,str], name: str) -> Tuple[Optional[str], Dict[str,Any]]:
    r2 = client.get(f"{SLACK_API}/conversations.list",
                    params={"exclude_archived":"true","limit":"1000","types":"public_channel,private_channel"},
                    headers=headers)
    d2 = r2.json()
    if d2.get("ok"):
        for c in d2.get("channels", []):
            if c.get("name") == name:
                return c.get("id"), d2
    return None, d2

def ensure_hq_channel_verbose(conn, org_id: str) -> Dict[str, Any]:
    ensure_slack_tables(conn)
    if not SLACK_HQ_BOT_TOKEN:
        return {"ok": False, "channel_id": None, "channel_name": None,
                "step": "precheck", "visibility": None, "slack_error": "no_token", "slack_response": {}}

    cur = get_hq_channel(conn, org_id)
    chan_name = f"mf-{_slug_org(org_id)}"
    if cur and cur.get("channel_id"):
        return {"ok": True, "channel_id": cur["channel_id"], "channel_name": chan_name,
                "step": "exists", "visibility": ("private" if cur.get("is_private") else "public"),
                "slack_error": None, "slack_response": {}}

    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    client = _httpx()

    # 1) Crear público
    r = client.post(f"{SLACK_API}/conversations.create",
                    data={"name": chan_name, "is_private": "false"},
                    headers=headers)
    data_pub = r.json()
    if data_pub.get("ok"):
        chan_id = data_pub["channel"]["id"]
        client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)
        conn.execute(text("""
            INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot, is_private)
            VALUES (:o, :c, :n, true, false)
            ON CONFLICT (org_id) DO UPDATE
               SET channel_id = EXCLUDED.channel_id,
                   channel_name = EXCLUDED.channel_name,
                   is_private = EXCLUDED.is_private
        """), {"o": org_id, "c": chan_id, "n": chan_name})
        return {"ok": True, "channel_id": chan_id, "channel_name": chan_name,
                "step": "create_public", "visibility": "public",
                "slack_error": None, "slack_response": data_pub}

    # tomado → list
    if data_pub.get("error") == "name_taken":
        chan_id, list_resp = _find_channel_by_list(client, headers, chan_name)
        if chan_id:
            client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)
            info = client.get(f"{SLACK_API}/conversations.info",
                              params={"channel": chan_id}, headers=headers).json()
            is_priv = bool(((info.get("channel") or {}).get("is_private")))
            conn.execute(text("""
                INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot, is_private)
                VALUES (:o, :c, :n, false, :p)
                ON CONFLICT (org_id) DO UPDATE
                   SET channel_id = EXCLUDED.channel_id,
                       channel_name = EXCLUDED.channel_name,
                       is_private = EXCLUDED.is_private
            """), {"o": org_id, "c": chan_id, "n": chan_name, "p": is_priv})
            return {"ok": True, "channel_id": chan_id, "channel_name": chan_name,
                    "step": "found_by_list", "visibility": ("private" if is_priv else "public"),
                    "slack_error": None, "slack_response": {"create": data_pub, "list": list_resp, "info": info}}

    # 2) Fallback privado
    r2 = client.post(f"{SLACK_API}/conversations.create",
                     data={"name": chan_name, "is_private": "true"},
                     headers=headers)
    data_priv = r2.json()
    if data_priv.get("ok"):
        chan_id = data_priv["channel"]["id"]
        client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)
        conn.execute(text("""
            INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot, is_private)
            VALUES (:o, :c, :n, true, true)
            ON CONFLICT (org_id) DO UPDATE
               SET channel_id = EXCLUDED.channel_id,
                   channel_name = EXCLUDED.channel_name,
                   is_private = EXCLUDED.is_private
        """), {"o": org_id, "c": chan_id, "n": chan_name})
        return {"ok": True, "channel_id": chan_id, "channel_name": chan_name,
                "step": "create_private", "visibility": "private",
                "slack_error": None, "slack_response": {"create_public": data_pub, "create_private": data_priv}}

    # 3) último: list
    chan_id, list_resp = _find_channel_by_list(client, headers, chan_name)
    return {"ok": bool(chan_id), "channel_id": chan_id, "channel_name": chan_name,
            "step": "list_fallback", "visibility": None,
            "slack_error": (data_pub.get("error") if isinstance(data_pub, dict) else None) or (data_priv.get("error") if isinstance(data_priv, dict) else None),
            "slack_response": {"create_public": data_pub, "create_private": data_priv, "list": list_resp}}

def ensure_hq_channel(conn, org_id: str) -> Optional[str]:
    res = ensure_hq_channel_verbose(conn, org_id)
    return res["channel_id"] if res.get("ok") and res.get("channel_id") else None

def post_to_org(conn, org_id: str, message: str, blocks: Optional[list]=None) -> bool:
    """
    PRIORIDAD:
      1) Canal HQ (mf-{org}) con SLACK_HQ_BOT_TOKEN.
      2) Bot de la instalación + default_channel_id.
      3) Fallback: incoming_webhook_url (puede ser canal personal).
    """
    ensure_slack_tables(conn)
    inst = get_installation(conn, org_id)

    # 1) Canal HQ con token HQ (crea/verifica si no existe)
    cid = ensure_hq_channel(conn, org_id)
    if cid and SLACK_HQ_BOT_TOKEN:
        headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
        d = _httpx().post(
            f"{SLACK_API}/chat.postMessage",
            headers=headers,
            json={"channel": cid, "text": message, **({"blocks": blocks} if blocks else {})}
        ).json()
        if d.get("ok"):
            return True

    # 2) Bot de la instalación (si viene con canal por defecto)
    if inst and inst.get("bot_token") and inst.get("default_channel_id"):
        headers = {"Authorization": f"Bearer {inst['bot_token']}"}
        d = _httpx().post(
            f"{SLACK_API}/chat.postMessage",
            headers=headers,
            json={"channel": inst["default_channel_id"], "text": message, **({"blocks": blocks} if blocks else {})}
        ).json()
        if d.get("ok"):
            return True

    # 3) Último recurso: webhook (puede apuntar a canal personal)
    if inst and inst.get("incoming_webhook_url"):
        r = _httpx().post(
            inst["incoming_webhook_url"],
            json={"text": message, **({"blocks": blocks} if blocks else {})}
        )
        return r.status_code < 300

    return False

# --------- Diagnóstico extra ---------
def get_channel_info(channel_id: str) -> Dict[str, Any]:
    if not SLACK_HQ_BOT_TOKEN:
        return {"ok": False, "error": "no_token"}
    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    return _httpx().get(f"{SLACK_API}/conversations.info", params={"channel": channel_id}, headers=headers).json()

def find_channels_by_name(name: str) -> Dict[str, Any]:
    if not SLACK_HQ_BOT_TOKEN:
        return {"ok": False, "error": "no_token"}
    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    return _httpx().get(
        f"{SLACK_API}/conversations.list",
        params={"exclude_archived":"true","limit":"1000","types":"public_channel,private_channel"},
        headers=headers
    ).json()

def invite_emails_to_org_channel(conn, org_id: str, emails: List[str]) -> Dict[str, Any]:
    ch = get_hq_channel(conn, org_id)
    if not ch or not ch.get("channel_id"):
        return {"ok": False, "error": "no_channel"}
    if not SLACK_HQ_BOT_TOKEN:
        return {"ok": False, "error": "no_token"}

    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    client = _httpx()

    invited = []
    failed  = []
    for email in emails:
        u = client.get(f"{SLACK_API}/users.lookupByEmail", params={"email": email}, headers=headers).json()
        if not u.get("ok"):
            failed.append({"email": email, "error": u.get("error")}); continue
        uid = u["user"]["id"]
        j = client.post(f"{SLACK_API}/conversations.invite", data={"channel": ch["channel_id"], "users": uid}, headers=headers).json()
        if j.get("ok"):
            invited.append(email)
        else:
            failed.append({"email": email, "error": j.get("error")})
    return {"ok": len(failed) == 0, "invited": invited, "failed": failed}
