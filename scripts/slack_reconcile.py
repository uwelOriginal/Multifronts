#!/usr/bin/env python3
"""
Crea/asegura un canal #mf-{org_id} por cada organización de la tabla orgs en Neon
y guarda el resultado en slack_channels. Usa el bot de TU workspace (SLACK_HQ_BOT_TOKEN).

Uso:
  export DATABASE_URL="postgresql+psycopg://USER:PWD@HOST:5432/DB?sslmode=require"
  export SLACK_HQ_BOT_TOKEN="xoxb-..."
  python scripts/slack_reconcile.py
"""

import os, re, asyncio, time
import httpx
import psycopg
from psycopg.rows import tuple_row

SLACK_API = "https://api.slack.com/apps/A09C15UH5E2"

def _slug_org(org_id: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9\-_]", "-", str(org_id).strip())
    s = re.sub(r"-{2,}", "-", s).strip("-").lower()
    return s[:70]

async def ensure_channel(client: httpx.AsyncClient, token: str, name: str) -> str | None:
    # intenta crear público; si ya existe, lo busca
    r = await client.post(f"{SLACK_API}/conversations.create",
                          data={"name": name, "is_private": "false"},
                          headers={"Authorization": f"Bearer {token}"})
    data = r.json()
    chan_id = None
    if data.get("ok"):
        chan_id = data["channel"]["id"]
    elif data.get("error") == "name_taken":
        r2 = await client.get(f"{SLACK_API}/conversations.list",
                              params={"exclude_archived":"true","limit":"1000"},
                              headers={"Authorization": f"Bearer {token}"})
        d2 = r2.json()
        if d2.get("ok"):
            for c in d2.get("channels", []):
                if c.get("name") == name:
                    chan_id = c.get("id"); break
    if not chan_id:
        return None
    # join por si acaso
    await client.post(f"{SLACK_API}/conversations.join",
                      data={"channel": chan_id},
                      headers={"Authorization": f"Bearer {token}"})
    return chan_id

async def main():
    db_url = os.getenv("DATABASE_URL", "")
    bot_token = os.getenv("SLACK_HQ_BOT_TOKEN", "")
    if not db_url or not bot_token:
        raise SystemExit("Faltan envs: DATABASE_URL y/o SLACK_HQ_BOT_TOKEN")

    # Conecta a Neon
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            # Asegura tabla slack_channels
            cur.execute("""
            CREATE TABLE IF NOT EXISTS slack_channels (
              org_id         TEXT PRIMARY KEY,
              channel_id     TEXT NOT NULL,
              channel_name   TEXT NOT NULL,
              created_by_bot BOOLEAN NOT NULL DEFAULT FALSE,
              created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """)
            cur.execute("SELECT org_id FROM orgs ORDER BY org_id;")
            orgs = [r[0] for r in cur.fetchall()]

        async with httpx.AsyncClient(timeout=8.0) as client:
            created_or_verified = 0
            for org_id in orgs:
                chan_name = f"mf-{_slug_org(org_id)}"
                chan_id = await ensure_channel(client, bot_token, chan_name)
                if chan_id:
                    with conn.cursor() as cur:
                        cur.execute("""
                          INSERT INTO slack_channels(org_id, channel_id, channel_name, created_by_bot)
                          VALUES (%s, %s, %s, true)
                          ON CONFLICT (org_id) DO UPDATE
                          SET channel_id = EXCLUDED.channel_id, channel_name = EXCLUDED.channel_name;
                        """, (org_id, chan_id, chan_name))
                    created_or_verified += 1
                # Respeta rate limiting suave
                await asyncio.sleep(0.6)

    print(f"Listo. Canales creados o verificados: {created_or_verified}")

if __name__ == "__main__":
    asyncio.run(main())
