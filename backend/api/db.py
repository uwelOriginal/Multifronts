# api/db.py
from __future__ import annotations
import datetime, os, json as _json
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, DateTime, JSON, Numeric, UniqueConstraint,
    select, and_, insert, text, Boolean
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from .config import settings

DB_URL = settings.DATABASE_URL
if DB_URL.startswith("sqlite"):
    os.makedirs("data", exist_ok=True)

# Engine con pool
if DB_URL.startswith("sqlite"):
    engine: Engine = create_engine(DB_URL, future=True, connect_args={"check_same_thread": False})
else:
    engine: Engine = create_engine(DB_URL, future=True, pool_pre_ping=True, pool_size=5, max_overflow=10)

meta = MetaData()

orders_tbl = Table(
    "orders_confirmed", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("org_id", String(128), nullable=False, index=True),
    Column("store_id", String(128), nullable=False, index=True),
    Column("sku_id", String(128), nullable=False, index=True),
    Column("qty", Numeric, nullable=False),
    Column("approved_at", DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("approved_by", String(128)),
    Column("idem_key", String(256), nullable=False),
    UniqueConstraint("org_id", "store_id", "sku_id", "idem_key", name="uq_orders_idem"),
    sqlite_autoincrement=True,
)

transfers_tbl = Table(
    "transfers_confirmed", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("org_id", String(128), nullable=False, index=True),
    Column("from_store", String(128), nullable=False, index=True),
    Column("to_store", String(128), nullable=False, index=True),
    Column("sku_id", String(128), nullable=False, index=True),
    Column("qty", Numeric, nullable=False),
    Column("approved_at", DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("approved_by", String(128)),
    Column("idem_key", String(256), nullable=False),
    UniqueConstraint("org_id", "from_store", "to_store", "sku_id", "idem_key", name="uq_transfers_idem"),
    sqlite_autoincrement=True,
)

events_tbl = Table(
    "events", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("org_id", String(128), nullable=False, index=True),
    Column("ts", DateTime, nullable=False, default=datetime.datetime.utcnow),
    Column("type", String(64), nullable=False),
    Column("payload", JSON, nullable=False, default={}),
    sqlite_autoincrement=True,
)

def init_db() -> None:
    meta.create_all(engine)

def insert_event(org_id: str, type_: str, payload: dict) -> dict:
    with engine.begin() as conn:
        ts = datetime.datetime.utcnow()
        res = conn.execute(insert(events_tbl).values(org_id=org_id, ts=ts, type=type_, payload=payload))
        ev_id = int(res.inserted_primary_key[0])
        # NOTIFY solo si es Postgres
        if engine.dialect.name == "postgresql":
            try:
                chan = f"org_events_{org_id}"
                conn.execute(text("SELECT pg_notify(:chan, :payload)"),
                             {"chan": chan, "payload": _json.dumps({"id": ev_id, "type": type_})})
            except Exception:
                pass
        return {"id": ev_id, "org_id": org_id, "ts": ts.isoformat()+"Z", "type": type_, "payload": payload}

def poll_events(org_id: str, after: int = 0, limit: int = 200):
    with engine.begin() as conn:
        rows = conn.execute(
            select(events_tbl.c.id, events_tbl.c.ts, events_tbl.c.type, events_tbl.c.payload)
            .where(and_(events_tbl.c.org_id == org_id, events_tbl.c.id > after))
            .order_by(events_tbl.c.id.asc()).limit(limit)
        ).fetchall()
        evs = [{"id": int(r.id), "ts": r.ts.isoformat()+"Z", "type": r.type, "payload": r.payload} for r in rows]
        cursor = evs[-1]["id"] if evs else after
        return evs, cursor

# --- Mesajería ----
slack_installs = Table(
    "slack_installs", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("org_id", String(128), nullable=False, index=True, unique=True),
    Column("team_id", String(64), nullable=False, index=True),
    Column("bot_token", String(256), nullable=True),  # xoxb-...
    Column("webhook_url", String(512), nullable=True),  # devuelta por scope incoming-webhook
    Column("webhook_channel", String(128), nullable=True),
    Column("installed_by", String(128), nullable=True),  # email/usuario opcional
    Column("created_at", DateTime, nullable=False, default=datetime.datetime.utcnow),
)

def save_slack_install(org_id: str, team_id: str, bot_token: str | None,
                       webhook_url: str | None, webhook_channel: str | None,
                       installed_by: str | None = None):
    with engine.begin() as conn:
        # upsert simple por org_id
        existing = conn.execute(
            select(slack_installs.c.id).where(slack_installs.c.org_id == org_id)
        ).first()
        if existing:
            conn.execute(
                slack_installs.update()
                .where(slack_installs.c.org_id == org_id)
                .values(team_id=team_id, bot_token=bot_token, webhook_url=webhook_url,
                        webhook_channel=webhook_channel, installed_by=installed_by)
            )
        else:
            conn.execute(
                slack_installs.insert().values(
                    org_id=org_id, team_id=team_id, bot_token=bot_token,
                    webhook_url=webhook_url, webhook_channel=webhook_channel,
                    installed_by=installed_by
                )
            )

def get_slack_status(org_id: str):
    with engine.begin() as conn:
        row = conn.execute(
            select(slack_installs.c.team_id, slack_installs.c.webhook_url, slack_installs.c.webhook_channel)
            .where(slack_installs.c.org_id == org_id)
        ).first()
        if not row:
            return {"connected": False}
        return {
            "connected": True,
            "team_id": row.team_id,
            "webhook_url": row.webhook_url,
            "webhook_channel": row.webhook_channel,
        }