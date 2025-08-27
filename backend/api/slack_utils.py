# backend/api/slack_utils.py
import os, re
from typing import Optional, Dict, Any, Tuple
import httpx
from sqlalchemy import text

SLACK_API = "https://slack.com/api"
SLACK_HQ_BOT_TOKEN = os.getenv("SLACK_HQ_BOT_TOKEN", "").strip()

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
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    );"""))

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
        SELECT org_id, channel_id, channel_name, created_by_bot
        FROM slack_channels
        WHERE org_id = :o
    """), {"o": org_id})
    row = res.mappings().first()
    return dict(row) if row else None

def _find_channel_by_list(client: httpx.Client, headers: Dict[str,str], name: str) -> Tuple[Optional[str], Dict[str,Any]]:
    r2 = client.get(f"{SLACK_API}/conversations.list",
                    params={"exclude_archived":"true","limit":"1000"},
                    headers=headers)
    d2 = r2.json()
    if d2.get("ok"):
        for c in d2.get("channels", []):
            if c.get("name") == name:
                return c.get("id"), d2
    return None, d2

def ensure_hq_channel_verbose(conn, org_id: str) -> Dict[str, Any]:
    """
    Devuelve {ok, channel_id, channel_name, step, slack_error, slack_response}
    Intenta crear público; si falla por permisos, prueba privado como fallback.
    """
    ensure_slack_tables(conn)
    if not SLACK_HQ_BOT_TOKEN:
        return {"ok": False, "channel_id": None, "channel_name": None,
                "step": "precheck", "slack_error": "no_token", "slack_response": {}}

    # ¿ya existe?
    cur = get_hq_channel(conn, org_id)
    chan_name = f"mf-{_slug_org(org_id)}"
    if cur and cur.get("channel_id"):
        return {"ok": True, "channel_id": cur["channel_id"], "channel_name": chan_name,
                "step": "exists", "slack_error": None, "slack_response": {}}

    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    with httpx.Client(timeout=8.0) as client:
        # 1) Intento crear canal público
        r = client.post(f"{SLACK_API}/conversations.create",
                        data={"name": chan_name, "is_private": "false"},
                        headers=headers)
        data_pub = r.json()
        if data_pub.get("ok"):
            chan_id = data_pub["channel"]["id"]
            client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)
            conn.execute(text("""
                INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot)
                VALUES (:o, :c, :n, true)
                ON CONFLICT (org_id) DO UPDATE
                   SET channel_id = EXCLUDED.channel_id,
                       channel_name = EXCLUDED.channel_name
            """), {"o": org_id, "c": chan_id, "n": chan_name})
            return {"ok": True, "channel_id": chan_id, "channel_name": chan_name,
                    "step": "create_public", "slack_error": None, "slack_response": data_pub}

        # si el nombre ya existe, buscarlo
        if data_pub.get("error") == "name_taken":
            chan_id, list_resp = _find_channel_by_list(client, headers, chan_name)
            if chan_id:
                client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)
                conn.execute(text("""
                    INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot)
                    VALUES (:o, :c, :n, false)
                    ON CONFLICT (org_id) DO UPDATE
                       SET channel_id = EXCLUDED.channel_id,
                           channel_name = EXCLUDED.channel_name
                """), {"o": org_id, "c": chan_id, "n": chan_name})
                return {"ok": True, "channel_id": chan_id, "channel_name": chan_name,
                        "step": "found_by_list", "slack_error": None, "slack_response": {"create": data_pub, "list": list_resp}}

        # 2) Fallback: intentar canal PRIVADO (requiere groups:write)
        r2 = client.post(f"{SLACK_API}/conversations.create",
                         data={"name": chan_name, "is_private": "true"},
                         headers=headers)
        data_priv = r2.json()
        if data_priv.get("ok"):
            chan_id = data_priv["channel"]["id"]
            client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)
            conn.execute(text("""
                INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot)
                VALUES (:o, :c, :n, true)
                ON CONFLICT (org_id) DO UPDATE
                   SET channel_id = EXCLUDED.channel_id,
                       channel_name = EXCLUDED.channel_name
            """), {"o": org_id, "c": chan_id, "n": chan_name})
            return {"ok": True, "channel_id": chan_id, "channel_name": chan_name,
                    "step": "create_private", "slack_error": None, "slack_response": {"create_public": data_pub, "create_private": data_priv}}

        # 3) Último intento: listar y encontrar si alguien ya lo creó
        chan_id, list_resp = _find_channel_by_list(client, headers, chan_name)
        return {"ok": bool(chan_id), "channel_id": chan_id, "channel_name": chan_name,
                "step": "list_fallback",
                "slack_error": (data_pub.get("error") or data_priv.get("error")),
                "slack_response": {"create_public": data_pub, "create_private": data_priv, "list": list_resp}}

def ensure_hq_channel(conn, org_id: str) -> Optional[str]:
    res = ensure_hq_channel_verbose(conn, org_id)
    return res["channel_id"] if res.get("ok") and res.get("channel_id") else None

def post_to_org(conn, org_id: str, message: str, blocks: Optional[list]=None) -> bool:
    """
    Orden de enrutado:
      1) incoming_webhook_url → POST webhook
      2) bot_token + default_channel_id → chat.postMessage
      3) Canal HQ #mf-{org} (creado con SLACK_HQ_BOT_TOKEN)
    """
    ensure_slack_tables(conn)
    inst = get_installation(conn, org_id)

    # 1) Webhook
    if inst and inst.get("incoming_webhook_url"):
        with httpx.Client(timeout=6.0) as client:
            payload = {"text": message}
            if blocks: payload["blocks"] = blocks
            r = client.post(inst["incoming_webhook_url"], json=payload)
            return r.status_code < 300

    # 2) Bot + canal de la org
    if inst and inst.get("bot_token") and inst.get("default_channel_id"):
        headers = {"Authorization": f"Bearer {inst['bot_token']}"}
        with httpx.Client(timeout=6.0) as client:
            r = client.post(f"{SLACK_API}/chat.postMessage",
                            headers=headers,
                            json={"channel": inst["default_channel_id"], "text": message, **({"blocks": blocks} if blocks else {})})
            data = r.json()
            return bool(data.get("ok", False))

    # 3) Canal HQ (auto)
    cid = ensure_hq_channel(conn, org_id)
    if not cid or not SLACK_HQ_BOT_TOKEN:
        return False
    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    with httpx.Client(timeout=6.0) as client:
        r = client.post(f"{SLACK_API}/chat.postMessage",
                        headers=headers,
                        json={"channel": cid, "text": message, **({"blocks": blocks} if blocks else {})})
        data = r.json()
        return bool(data.get("ok", False))
