import os, re, httpx, asyncio
from typing import Optional, Dict, Any
from sqlalchemy import text

SLACK_API = "https://slack.com/api"

# Bot “HQ” (tu workspace) para auto-canales por-org cuando no hay OAuth por org
SLACK_HQ_BOT_TOKEN = os.getenv("SLACK_HQ_BOT_TOKEN", "").strip()

def _slug_org(org_id: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9\-_]", "-", org_id.strip())
    s = re.sub(r"-{2,}", "-", s).strip("-").lower()
    return s[:70]

def get_installation(conn, org_id: str) -> Optional[Dict[str, Any]]:
    r = conn.exec_driver_sql("""
        SELECT org_id, team_id, team_name, bot_user_id, bot_token, incoming_webhook_url, default_channel_id
        FROM slack_installations WHERE org_id = :o
    """, {"o": org_id}).fetchone()
    if not r: return None
    keys = ["org_id","team_id","team_name","bot_user_id","bot_token","incoming_webhook_url","default_channel_id"]
    return dict(zip(keys, r))

def get_hq_channel(conn, org_id: str) -> Optional[Dict[str, Any]]:
    r = conn.exec_driver_sql("""
        SELECT org_id, channel_id, channel_name, created_by_bot
        FROM slack_channels WHERE org_id = :o
    """, {"o": org_id}).fetchone()
    if not r: return None
    keys=["org_id","channel_id","channel_name","created_by_bot"]
    return dict(zip(keys, r))

async def ensure_hq_channel(conn, org_id: str) -> Optional[str]:
    """
    Crea (o encuentra) un canal #mf-{org_id} en TU workspace (usa SLACK_HQ_BOT_TOKEN).
    Guarda el channel en slack_channels. Devuelve channel_id o None.
    """
    if not SLACK_HQ_BOT_TOKEN:
        return None

    row = get_hq_channel(conn, org_id)
    if row and row["channel_id"]:
        return row["channel_id"]

    chan_name = f"mf-{_slug_org(org_id)}"
    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}

    async with httpx.AsyncClient(timeout=8.0) as client:
        # Intento de creación
        r = await client.post(f"{SLACK_API}/conversations.create", data={"name": chan_name}, headers=headers)
        data = r.json()
        if not data.get("ok"):
            chan_id = None
            if data.get("error") == "name_taken":
                # listar y buscar
                r2 = await client.get(f"{SLACK_API}/conversations.list",
                                      params={"exclude_archived":"true","limit":"1000"},
                                      headers=headers)
                d2 = r2.json()
                if d2.get("ok"):
                    for c in d2.get("channels", []):
                        if c.get("name") == chan_name:
                            chan_id = c.get("id")
                            break
        else:
            chan_id = data["channel"]["id"]

        if not chan_id:
            return None

        # Unirse por si acaso
        await client.post(f"{SLACK_API}/conversations.join", data={"channel": chan_id}, headers=headers)

    # Persistir
    conn.exec_driver_sql("""
        INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot)
        VALUES (:o, :c, :n, true)
        ON CONFLICT (org_id) DO UPDATE SET channel_id = EXCLUDED.channel_id, channel_name = EXCLUDED.channel_name
    """, {"o": org_id, "c": chan_id, "n": chan_name})

    return chan_id

async def post_to_org(conn, org_id: str, message: str, blocks: Optional[list]=None) -> bool:
    """
    Enrutado por organización:
      1) Si la org tiene incoming_webhook_url → POST a su webhook (rápido).
      2) Si tiene bot_token y default_channel_id → chat.postMessage.
      3) Si no tiene nada → auto-canal en TU workspace con SLACK_HQ_BOT_TOKEN y post ahí.
    """
    inst = get_installation(conn, org_id)

    # 1) Webhook de la org
    if inst and inst.get("incoming_webhook_url"):
        async with httpx.AsyncClient(timeout=6.0) as client:
            payload = {"text": message}
            if blocks: payload["blocks"] = blocks
            r = await client.post(inst["incoming_webhook_url"], json=payload)
            return r.status_code < 300

    # 2) Bot token de la org
    if inst and inst.get("bot_token") and inst.get("default_channel_id"):
        headers = {"Authorization": f"Bearer {inst['bot_token']}"}
        async with httpx.AsyncClient(timeout=6.0) as client:
            r = await client.post(f"{SLACK_API}/chat.postMessage",
                                  headers=headers,
                                  json={"channel": inst["default_channel_id"],
                                        "text": message, **({"blocks": blocks} if blocks else {})})
            return bool(r.json().get("ok", False))

    # 3) Auto-canal en tu workspace (HQ)
    chan_id = await ensure_hq_channel(conn, org_id)
    if not chan_id:
        return False

    if not SLACK_HQ_BOT_TOKEN:
        return False

    headers = {"Authorization": f"Bearer {SLACK_HQ_BOT_TOKEN}"}
    async with httpx.AsyncClient(timeout=6.0) as client:
        r = await client.post(f"{SLACK_API}/chat.postMessage",
                              headers=headers,
                              json={"channel": chan_id, "text": message, **({"blocks": blocks} if blocks else {})})
        return bool(r.json().get("ok", False))
