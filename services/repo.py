# services/repo.py
from __future__ import annotations

import os
import datetime as _dt
from typing import Optional, Tuple, List, Dict
from urllib.parse import quote_plus, urlparse

import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, DateTime, Numeric,
    UniqueConstraint, insert, select, text
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

# ==============================
# Semilla / Lectura de Inventario
# ==============================

def seed_inventory_from_snapshot(
    org_id: str,
    snapshot_df: pd.DataFrame,
    store_col: str = "store_id",
    sku_col: str = "sku_id",
    on_hand_col: str = "on_hand_units",
) -> int:
    """
    Llena inventory_levels con el snapshot inicial de la org (idempotente por clave única).
    Usa SQLAlchemy text() para que los binds :param funcionen en psycopg3.
    """
    ensure_movements_schema()
    if snapshot_df is None or snapshot_df.empty:
        return 0

    df = snapshot_df[[store_col, sku_col, on_hand_col]].copy()
    df = df.rename(columns={store_col: "store_id", sku_col: "sku_id", on_hand_col: "on_hand"})
    df["org_id"] = org_id
    ts = _now_utc()
    df["updated_at"] = ts

    sql = text("""
        INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
        VALUES (:org_id, :store_id, :sku_id, :on_hand, :updated_at)
        ON CONFLICT (org_id, store_id, sku_id)
        DO UPDATE SET on_hand = EXCLUDED.on_hand,
                      updated_at = :updated_at;
    """)
    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))
    return len(df)

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

# ===================
# Guardado de pedidos
# ===================

def save_orders(*, org_id: str, rows: List[Dict], approved_by: str, idem_prefix: str) -> tuple[int, int]:
    """
    Inserta órdenes confirmadas y AUMENTA inventario en Neon (idempotente por idem_key).
    - Si la orden ya existe (duplicada), NO vuelve a sumar inventario.
    """
    ensure_movements_schema()
    nuevos, duplicados = 0, 0
    ts = _now_utc()

    upsert_inv = text("""
        INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
        VALUES (:org_id, :store_id, :sku_id, :delta, :ts)
        ON CONFLICT (org_id, store_id, sku_id)
        DO UPDATE SET on_hand = inventory_levels.on_hand + EXCLUDED.on_hand,
                      updated_at = :ts;
    """)

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

            store_id = str(r["store_id"])
            sku_id   = str(r["sku_id"])

            payload = {
                "org_id": org_id,
                "store_id": store_id,
                "sku_id": sku_id,
                "qty": qty,
                "approved_at": ts,
                "approved_by": approved_by,
                "idem_key": f"{idem_prefix}:order:{store_id}:{sku_id}",
            }

            # 1) Inserta la orden (idempotente)
            try:
                conn.execute(insert(orders_tbl), [payload])
            except IntegrityError:
                duplicados += 1
                continue

            # 2) Upsert de inventario: on_hand += qty
            conn.execute(
                upsert_inv,
                {"org_id": org_id, "store_id": store_id, "sku_id": sku_id, "delta": qty, "ts": ts},
            )
            nuevos += 1

    return nuevos, duplicados

# ========================
# Guardado de transferencias
# ========================

def save_transfers(
    *, org_id: str, rows: List[Dict], approved_by: str, idem_prefix: str
) -> tuple[int, int, int]:
    """
    Inserta transferencias confirmadas y MUEVE inventario A→B (idempotente por idem_key).
    - Descuenta del origen solo si hay stock suficiente.
    - Si hay duplicado, no vuelve a aplicar.
    """
    ensure_movements_schema()
    applied, dup, insufficient = 0, 0, 0
    ts = _now_utc()

    ensure_key_sql = text("""
        INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
        VALUES (:org_id, :store_id, :sku_id, 0, :ts)
        ON CONFLICT (org_id, store_id, sku_id) DO NOTHING;
    """)

    deduct_sql = text("""
        UPDATE inventory_levels
           SET on_hand = on_hand - :q, updated_at = :ts
         WHERE org_id = :org_id AND store_id = :store_id AND sku_id = :sku_id AND on_hand >= :q;
    """)

    add_sql = text("""
        INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
        VALUES (:org_id, :store_id, :sku_id, :q, :ts)
        ON CONFLICT (org_id, store_id, sku_id)
        DO UPDATE SET on_hand = inventory_levels.on_hand + EXCLUDED.on_hand,
                      updated_at = :ts;
    """)

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
                "approved_at": ts,
                "approved_by": approved_by,
                "idem_key": f"{idem_prefix}:transfer:{from_store}:{to_store}:{sku_id}",
            }

            try:
                conn.execute(insert(transfers_tbl), [payload])
            except IntegrityError:
                dup += 1
                continue

            # Garantiza claves A y B
            conn.execute(ensure_key_sql, {"org_id": org_id, "store_id": from_store, "sku_id": sku_id, "ts": ts})
            conn.execute(ensure_key_sql, {"org_id": org_id, "store_id": to_store,   "sku_id": sku_id, "ts": ts})

            # Descuenta en origen (si hay suficiente)
            res = conn.execute(
                deduct_sql,
                {"org_id": org_id, "store_id": from_store, "sku_id": sku_id, "q": qty, "ts": ts},
            )
            if getattr(res, "rowcount", 0) == 0:
                insufficient += 1
                continue

            # Suma en destino
            conn.execute(
                add_sql,
                {"org_id": org_id, "store_id": to_store, "sku_id": sku_id, "q": qty, "ts": ts},
            )
            applied += 1

    return applied, dup, insufficient
