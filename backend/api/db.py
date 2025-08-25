# api/db.py
from __future__ import annotations
import datetime, os, json as _json
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, DateTime, JSON, Numeric, UniqueConstraint,
    select, and_, insert, text
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
