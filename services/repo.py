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

# Pool pequeño para Streamlit Cloud (menos latencia)
engine_args = dict(future=True)
if DB_URL.startswith("sqlite"):
    engine = create_engine(
        DB_URL,
        connect_args={"check_same_thread": False},
        **engine_args
    )
else:
    engine = create_engine(
        DB_URL,
        pool_pre_ping=True,
        pool_size=5,        # <— controla conexiones concurrentes
        max_overflow=2,     # <— evita latencia por apertura excesiva
        **engine_args
    )

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

# =================================
# Inventario vivo (estado en Neon)
# =================================
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

# ===========================
# Seeder / Lectura de estado
# ===========================

def seed_inventory_from_snapshot(
    org_id: str,
    snapshot_df: pd.DataFrame,
    store_col: str = "store_id",
    sku_col: str = "sku_id",
    on_hand_col: str = "on_hand_units",
) -> int:
    """
    Carga inicial del inventario vivo a partir de un CSV/DF de snapshot.
    Upsert por (org_id, store_id, sku_id).
    Devuelve número de filas afectadas.
    """
    ensure_movements_schema()
    if snapshot_df is None or snapshot_df.empty:
        return 0

    df = snapshot_df[[store_col, sku_col, on_hand_col]].copy()
    df = df.rename(columns={store_col: "store_id", sku_col: "sku_id", on_hand_col: "on_hand"})
    df["org_id"] = org_id
    df["updated_at"] = _now_utc()

    dialect = engine.dialect.name
    affected = 0
    with engine.begin() as conn:
        if dialect == "postgresql":
            # Upsert en batch (Postgres)
            sql = """
            INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
            VALUES (:org_id, :store_id, :sku_id, :on_hand, :updated_at)
            ON CONFLICT (org_id, store_id, sku_id)
            DO UPDATE SET on_hand = EXCLUDED.on_hand, updated_at = EXCLUDED.updated_at;
            """
        else:
            # SQLite
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
    """
    Lee el inventario vivo desde BD usando SQLAlchemy Core.
    - Convierte sets/tuples a list de strings
    - Usa .in_(...) en lugar de ANY(...) para evitar problemas de adaptación
    """
    ensure_movements_schema()

    # Normaliza tipos (evita 'set' → psycopg ProgrammingError)
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


# =========================================
# Persistencia idempotente + efectos stock
# =========================================

def save_orders(
    *, org_id: str, rows: List[Dict], approved_by: str, idem_prefix: str
) -> tuple[int, int]:
    """
    Inserta pedidos de forma idempotente. (No afectan 'on_hand' — son pedidos, no recepción).
    Devuelve (nuevos, duplicados).
    """
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
    """
    Inserta transferencias de forma idempotente y APLICA EFECTOS en inventario:
    - decrementa on_hand en 'from_store' (si hay stock suficiente)
    - incrementa on_hand en 'to_store' (upsert)
    Devuelve (aplicadas, duplicadas, insuficientes).
    """
    ensure_movements_schema()
    applied, dup, insufficient = 0, 0, 0
    dialect = engine.dialect.name

    with engine.begin() as conn:
        # Precrear filas de inventario con on_hand=0 para claves faltantes (evita nulls)
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

            # 1) idempotencia de movimiento
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
                is_duplicate = False
            except IntegrityError:
                dup += 1
                is_duplicate = True

            if is_duplicate:
                # Ya insertada antes: no volvemos a aplicar efecto de stock.
                continue

            # 2) Asegurar claves
            _ensure_key(from_store, sku_id)
            _ensure_key(to_store, sku_id)

            # 3) Decremento atómico (no permitir negativos)
            if dialect == "postgresql":
                res = conn.exec_driver_sql(
                    """
                    UPDATE inventory_levels
                    SET on_hand = on_hand - :qty, updated_at = NOW()
                    WHERE org_id = :org_id AND store_id = :from_store AND sku_id = :sku_id
                      AND on_hand >= :qty;
                    """,
                    {"org_id": org_id, "from_store": from_store, "sku_id": sku_id, "qty": qty},
                )
            else:
                res = conn.exec_driver_sql(
                    """
                    UPDATE inventory_levels
                    SET on_hand = on_hand - :qty, updated_at = CURRENT_TIMESTAMP
                    WHERE org_id = :org_id AND store_id = :from_store AND sku_id = :sku_id
                      AND on_hand >= :qty;
                    """,
                    {"org_id": org_id, "from_store": from_store, "sku_id": sku_id, "qty": qty},
                )
            if res.rowcount != 1:
                # No había stock suficiente; deshacemos el registro de transfer para no perder idempotencia
                # (opcional: mantenerlo con estatus 'rejected', aquí simplemente lo contamos como insuficiente)
                insufficient += 1
                continue

            # 4) Incremento (upsert) en destino
            if dialect == "postgresql":
                conn.exec_driver_sql(
                    """
                    INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
                    VALUES (:org_id, :to_store, :sku_id, :qty, NOW())
                    ON CONFLICT (org_id, store_id, sku_id)
                    DO UPDATE SET on_hand = inventory_levels.on_hand + EXCLUDED.on_hand,
                                  updated_at = NOW();
                    """,
                    {"org_id": org_id, "to_store": to_store, "sku_id": sku_id, "qty": qty},
                )
            else:
                conn.exec_driver_sql(
                    """
                    INSERT INTO inventory_levels (org_id, store_id, sku_id, on_hand, updated_at)
                    VALUES (:org_id, :to_store, :sku_id, :qty, CURRENT_TIMESTAMP)
                    ON CONFLICT(org_id, store_id, sku_id)
                    DO UPDATE SET on_hand = on_hand + excluded.on_hand,
                                  updated_at = CURRENT_TIMESTAMP;
                    """,
                    {"org_id": org_id, "to_store": to_store, "sku_id": sku_id, "qty": qty},
                )

            applied += 1

    return applied, dup, insufficient
