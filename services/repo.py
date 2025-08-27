# services/repo.py
from __future__ import annotations

import os
import datetime as _dt
from typing import Optional, Tuple, List, Dict
from urllib.parse import quote_plus, urlparse

import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, DateTime, Numeric,
    UniqueConstraint, insert, select
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

try:
    import streamlit as st
except Exception:
    st = None

# =========================
# Conexión a BD (optimizada)
# =========================

meta = MetaData()

def _read_secret(name: str) -> Optional[str]:
    if st is not None and hasattr(st, "secrets"):
        try:
            v = st.secrets.get(name, None)
            if v:
                return str(v)
        except Exception:
            pass
    return os.getenv(name)

def _compose_pg_url_from_parts() -> Optional[str]:
    host = _read_secret("PGHOST")
    db   = _read_secret("PGDATABASE")
    user = _read_secret("PGUSER")
    pwd  = _read_secret("PGPASSWORD")
    port = _read_secret("PGPORT") or "5432"
    if not host or not db or not user or not pwd:
        return None
    return (
        f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(pwd)}@"
        f"{host}:{port}/{db}?sslmode=require"
    )

def _get_database_url() -> str:
    if st is not None and hasattr(st, "secrets"):
        try:
            v = st.secrets.get("DATABASE_URL", None)
            if v and isinstance(v, str) and "://" in v:
                return v.strip()
        except Exception:
            pass
    env_url = os.getenv("DATABASE_URL")
    if env_url and "://" in env_url:
        return env_url.strip()
    composed = _compose_pg_url_from_parts()
    if composed:
        return composed
    os.makedirs("data", exist_ok=True)
    return "sqlite:///data/app.db"

DB_URL: str = _get_database_url()

def _engine_args_for(url: str) -> dict:
    base = dict(future=True)
    if url.startswith("sqlite"):
        return {**base, "connect_args": {"check_same_thread": False}}
    # Neon/pg: pool chico estable
    return {
        **base,
        "pool_pre_ping": True,
        "pool_size": int(os.getenv("DB_POOL_SIZE", "5")),
        "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "2")),
        "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", "10")),
        "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "300")),
    }

# Cachea el Engine SOLO en procesos con Streamlit (evita recrearlo en cada rerun)
if st is not None:
    @st.cache_resource(show_spinner=False)
    def _cached_engine(url: str, args: dict) -> Engine:
        return create_engine(url, **args)
    engine: Engine = _cached_engine(DB_URL, _engine_args_for(DB_URL))
else:
    engine: Engine = create_engine(DB_URL, **_engine_args_for(DB_URL))

def get_engine() -> Engine:
    return engine

def mask_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(parsed.password, "*****")
        return url
    except Exception:
        return url

def current_db_info() -> Tuple[str, Optional[str], str]:
    dialect = engine.dialect.name
    host = None
    try:
        p = urlparse(DB_URL)
        host = p.hostname
    except Exception:
        pass
    return dialect, host, mask_url(DB_URL)

def init_db() -> None:
    meta.create_all(engine)

# ======================
# Esquema de Movimientos
# ======================

orders_tbl = Table(
    "orders_confirmed", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("org_id", String(128), nullable=False, index=True),
    Column("store_id", String(128), nullable=False, index=True),
    Column("sku_id", String(128), nullable=False, index=True),
    Column("qty", Numeric, nullable=False),
    Column("approved_at", DateTime, nullable=False, default=_dt.datetime.utcnow),
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
    Column("approved_at", DateTime, nullable=False, default=_dt.datetime.utcnow),
    Column("approved_by", String(128)),
    Column("idem_key", String(256), nullable=False),
    UniqueConstraint("org_id", "from_store", "to_store", "sku_id", "idem_key", name="uq_transfers_idem"),
    sqlite_autoincrement=True,
)

inventory_tbl = Table(
    "inventory_levels", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("org_id", String(128), nullable=False, index=True),
    Column("store_id", String(128), nullable=False, index=True),
    Column("sku_id", String(128), nullable=False, index=True),
    Column("on_hand", Numeric, nullable=False, default=0),
    Column("updated_at", DateTime, nullable=False, default=_dt.datetime.utcnow),
    UniqueConstraint("org_id", "store_id", "sku_id", name="uq_inventory_key"),
    sqlite_autoincrement=True,
)

def ensure_movements_schema() -> None:
    meta.create_all(engine, tables=[orders_tbl, transfers_tbl, inventory_tbl])

def _now_utc() -> _dt.datetime:
    return _dt.datetime.utcnow()

def seed_inventory_from_snapshot(
    org_id: str,
    snapshot_df: pd.DataFrame,
    store_col: str = "store_id",
    sku_col: str = "sku_id",
    on_hand_col: str = "on_hand_units",
) -> int:
    ensure_movements_schema()
    if snapshot_df is None or snapshot_df.empty:
        return 0
    df = snapshot_df[[store_col, sku_col, on_hand_col]].copy()
    df = df.rename(columns={store_col: "store_id", sku_col: "sku_id", on_hand_col: "on_hand"})
    df["org_id"] = org_id
    df["updated_at"] = _now_utc()

    affected = 0
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            sql = """
            INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
            VALUES (:org_id, :store_id, :sku_id, :on_hand, :updated_at)
            ON CONFLICT (org_id, store_id, sku_id)
            DO UPDATE SET on_hand = EXCLUDED.on_hand, updated_at = EXCLUDED.updated_at;
            """
        else:
            sql = """
            INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
            VALUES (:org_id, :store_id, :sku_id, :on_hand, :updated_at)
            ON CONFLICT(org_id, store_id, sku_id)
            DO UPDATE SET on_hand = excluded.on_hand, updated_at = excluded.updated_at;
            """
        conn.exec_driver_sql(sql, df.to_dict(orient="records"))
        affected = len(df)
    return affected

def fetch_inventory_levels(
    org_id: str,
    store_ids: Optional[List[str]] = None,
    sku_ids: Optional[List[str]] = None,
) -> pd.DataFrame:
    ensure_movements_schema()
    stores_list = [str(s) for s in list(store_ids) ] if store_ids else None
    skus_list   = [str(s) for s in list(sku_ids)   ] if sku_ids   else None

    cols = [
        inventory_tbl.c.store_id,
        inventory_tbl.c.sku_id,
        inventory_tbl.c.on_hand.label("on_hand_units"),
        inventory_tbl.c.updated_at,
    ]
    stmt = select(*cols).where(inventory_tbl.c.org_id == org_id)
    if stores_list:
        stmt = stmt.where(inventory_tbl.c.store_id.in_(stores_list))
    if skus_list:
        stmt = stmt.where(inventory_tbl.c.sku_id.in_(skus_list))

    with engine.begin() as conn:
        df = pd.read_sql(stmt, conn)
    return df

def save_orders(*, org_id: str, rows: List[Dict], approved_by: str, idem_prefix: str) -> tuple[int, int]:
    ensure_movements_schema()
    nuevos, duplicados = 0, 0
    with engine.begin() as conn:
        for r in rows:
            qty = r.get("qty", 0)
            if qty is None:
                continue
            try:
                qty = int(qty)
            except Exception:
                continue
            if qty <= 0:
                continue
            payload = {
                "org_id": org_id,
                "store_id": str(r["store_id"]),
                "sku_id": str(r["sku_id"]),
                "qty": qty,
                "approved_at": _now_utc(),
                "approved_by": approved_by,
                "idem_key": f"{idem_prefix}:order:{r['store_id']}:{r['sku_id']}",
            }
            try:
                conn.execute(insert(orders_tbl), [payload])
                nuevos += 1
            except IntegrityError:
                duplicados += 1
    return nuevos, duplicados

def save_transfers(
    *, org_id: str, rows: List[Dict], approved_by: str, idem_prefix: str
) -> tuple[int, int, int]:
    ensure_movements_schema()
    applied, dup, insufficient = 0, 0, 0
    dialect = engine.dialect.name

    with engine.begin() as conn:
        def _ensure_key(store_id: str, sku_id: str) -> None:
            if dialect == "postgresql":
                conn.exec_driver_sql(
                    """
                    INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
                    VALUES (:org_id, :store_id, :sku_id, 0, NOW())
                    ON CONFLICT (org_id, store_id, sku_id) DO NOTHING;
                    """,
                    {"org_id": org_id, "store_id": str(store_id), "sku_id": str(sku_id)},
                )
            else:
                conn.exec_driver_sql(
                    """
                    INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
                    VALUES (:org_id, :store_id, :sku_id, 0, CURRENT_TIMESTAMP)
                    ON CONFLICT(org_id, store_id, sku_id) DO NOTHING;
                    """,
                    {"org_id": org_id, "store_id": str(store_id), "sku_id": str(sku_id)},
                )

        for r in rows:
            qty = r.get("qty", 0)
            if qty is None:
                continue
            try:
                qty = int(qty)
            except Exception:
                continue
            if qty <= 0:
                continue

            from_store = str(r["from_store"])
            to_store   = str(r["to_store"])
            sku_id     = str(r["sku_id"])

            if from_store == to_store:
                continue

            payload = {
                "org_id": org_id,
                "from_store": from_store,
                "to_store": to_store,
                "sku_id": sku_id,
                "qty": qty,
                "approved_at": _now_utc(),
                "approved_by": approved_by,
                "idem_key": f"{idem_prefix}:transfer:{from_store}:{to_store}:{sku_id}",
            }
            try:
                conn.execute(insert(transfers_tbl), [payload])
            except IntegrityError:
                dup += 1
                continue

            _ensure_key(from_store, sku_id)
            _ensure_key(to_store, sku_id)

            res = conn.exec_driver_sql(
                """
                UPDATE inventory_levels
                   SET on_hand = on_hand - :q, updated_at = NOW()
                 WHERE org_id=:o AND store_id=:s AND sku_id=:k AND on_hand >= :q
                """,
                {"o": org_id, "s": from_store, "k": sku_id, "q": qty},
            )
            if res.rowcount == 0:
                insufficient += 1
                continue

            conn.exec_driver_sql(
                """
                INSERT INTO inventory_levels(org_id, store_id, sku_id, on_hand, updated_at)
                VALUES (:o, :s, :k, :q, NOW())
                ON CONFLICT (org_id, store_id, sku_id)
                DO UPDATE SET on_hand = inventory_levels.on_hand + EXCLUDED.on_hand,
                              updated_at = NOW();
                """,
                {"o": org_id, "s": to_store, "k": sku_id, "q": qty},
            )
            applied += 1

    return applied, dup, insufficient
