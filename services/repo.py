# services/repo.py
from __future__ import annotations
import os, datetime
from typing import Dict, Any, Iterable, Tuple, List
from urllib.parse import quote
import json as _json

# Streamlit secrets si estamos en app
try:
    import streamlit as st
except Exception:
    st = None

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, DateTime, JSON, Numeric, UniqueConstraint,
    select, and_, insert, text
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

def _get_database_url() -> str:
    # 1) Streamlit secrets directa
    if st is not None:
        try:
            v = st.secrets.get("DATABASE_URL", None)  # type: ignore[attr-defined]
            if v:
                return v
        except Exception:
            pass

    # 2) Variable de entorno ya armada
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url

    # 3) Ensamblar desde PG* sin exponer secretos
    host = os.getenv("PGHOST")
    user = os.getenv("PGUSER")
    pwd  = os.getenv("PGPASSWORD")
    db   = os.getenv("PGDATABASE")
    port = os.getenv("PGPORT")
    ssl  = os.getenv("PGSSLMODE", "require")

    if host and user and pwd and db:
        pwd_q = quote(pwd, safe="")  # URL-encode
        port_part = f":{port}" if port else ""
        return f"postgresql+psycopg://{user}:{pwd_q}@{host}{port_part}/{db}?sslmode={ssl}"

    # 4) Fallback local
    return "sqlite:///data/app.db"

DB_URL = _get_database_url()
if DB_URL.startswith("sqlite"):
    os.makedirs("data", exist_ok=True)

# Engine con pool y pre_ping (Postgres). En SQLite desactivamos check_same_thread.
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

def _maybe_fix_sqlite_pk(conn, table_name: str):
    """Si la PK no es INTEGER PRIMARY KEY, recrea la tabla correctamente (SQLite only)."""
    if engine.dialect.name != "sqlite":
        return
    info = conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
    if not info:
        return
    id_row = next((r for r in info if str(r[1]) == "id"), None)
    if not id_row:
        return
    col_type = str(id_row[2] or "").upper()
    pk_flag = int(id_row[5] or 0)
    if not (pk_flag == 1 and col_type == "INTEGER"):
        tmp = f"__tmp_{table_name}"
        conn.exec_driver_sql(f"ALTER TABLE {table_name} RENAME TO {tmp}")
        meta.create_all(bind=conn, tables=[orders_tbl if table_name=="orders_confirmed"
                                           else transfers_tbl if table_name=="transfers_confirmed"
                                           else events_tbl])
        cols = "org_id, ts, type, payload" if table_name=="events" else \
               "org_id, store_id, sku_id, qty, approved_at, approved_by, idem_key" if table_name=="orders_confirmed" else \
               "org_id, from_store, to_store, sku_id, qty, approved_at, approved_by, idem_key"
        conn.exec_driver_sql(f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM {tmp}")
        conn.exec_driver_sql(f"DROP TABLE {tmp}")

def init_db() -> None:
    meta.create_all(engine)
    if engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            _maybe_fix_sqlite_pk(conn, "events")
            _maybe_fix_sqlite_pk(conn, "orders_confirmed")
            _maybe_fix_sqlite_pk(conn, "transfers_confirmed")

def health() -> str:
    try:
        with engine.begin() as conn:
            conn.execute(select(1))
        return "ok"
    except Exception as e:
        return f"error: {e}"

def insert_event(org_id: str, type_: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    with engine.begin() as conn:
        ts = datetime.datetime.utcnow()
        res = conn.execute(
            insert(events_tbl).values(org_id=org_id, ts=ts, type=type_, payload=payload)
        )
        ev_id = res.inserted_primary_key[0]
        # NOTIFY sólo en Postgres
        if engine.dialect.name == "postgresql":
            try:
                chan = f"org_events_{org_id}"
                conn.execute(text("SELECT pg_notify(:chan, :payload)"),
                             {"chan": chan, "payload": _json.dumps({"id": int(ev_id), "type": type_})})
            except Exception:
                pass
        return {"id": int(ev_id), "org_id": org_id, "ts": ts.isoformat()+"Z", "type": type_, "payload": payload}

def save_orders(org_id: str, rows: Iterable[Dict[str, Any]], approved_by: str, idem_prefix: str) -> Tuple[int,int]:
    new, dup = 0, 0
    now = datetime.datetime.utcnow()
    with engine.begin() as conn:
        for r in rows:
            idem_key = f"{idem_prefix}:{r['store_id']}:{r['sku_id']}"
            ins = insert(orders_tbl).values(
                org_id=org_id,
                store_id=r["store_id"], sku_id=r["sku_id"],
                qty=r.get("qty", 0),
                approved_at=now, approved_by=approved_by,
                idem_key=idem_key,
            )
            try:
                conn.execute(ins)
                new += 1
            except IntegrityError:
                dup += 1
    insert_event(org_id, "order_approved", {"count": new, "dup": dup})
    return new, dup

def save_transfers(org_id: str, rows: Iterable[Dict[str, Any]], approved_by: str, idem_prefix: str) -> Tuple[int,int]:
    new, dup = 0, 0
    now = datetime.datetime.utcnow()
    with engine.begin() as conn:
        for r in rows:
            idem_key = f"{idem_prefix}:{r['from_store']}:{r['to_store']}:{r['sku_id']}"
            ins = insert(transfers_tbl).values(
                org_id=org_id,
                from_store=r["from_store"], to_store=r["to_store"], sku_id=r["sku_id"],
                qty=r.get("qty", 0),
                approved_at=now, approved_by=approved_by,
                idem_key=idem_key,
            )
            try:
                conn.execute(ins)
                new += 1
            except IntegrityError:
                dup += 1
    insert_event(org_id, "transfer_approved", {"count": new, "dup": dup})
    return new, dup

def list_events(org_id: str, after: int = 0, limit: int = 200) -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            select(events_tbl.c.id, events_tbl.c.ts, events_tbl.c.type, events_tbl.c.payload)
            .where(and_(events_tbl.c.org_id == org_id, events_tbl.c.id > after))
            .order_by(events_tbl.c.id.asc()).limit(limit)
        ).fetchall()
        return [{"id": int(r.id), "ts": r.ts.isoformat()+"Z", "type": r.type, "payload": r.payload} for r in rows]
