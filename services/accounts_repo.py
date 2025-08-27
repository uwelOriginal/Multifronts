# services/accounts_repo.py
from __future__ import annotations
import datetime
from pathlib import Path
from typing import Optional, Set, Tuple

import pandas as pd
from sqlalchemy import (
    Table, Column, MetaData, Integer, String, DateTime,
    select, func
)

from .repo import get_engine

engine = get_engine()
meta = MetaData()

# --- Tablas de cuentas/organización ---
orgs_tbl = Table(
    "orgs", meta,
    Column("org_id", String(128), primary_key=True),
    Column("display_name", String(256)),
    Column("slack_webhook", String(512)),
    Column("created_at", DateTime, nullable=False, default=datetime.datetime.utcnow),
)

users_tbl = Table(
    "users", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email", String(320), nullable=False, unique=True, index=True),
    Column("password", String(256), nullable=False),
    Column("org_id", String(128), nullable=False),
    Column("role", String(64), nullable=False, default="member"),
    Column("display_name", String(256)),
    Column("created_at", DateTime, nullable=False, default=datetime.datetime.utcnow),
)

org_store_map_tbl = Table(
    "org_store_map", meta,
    Column("org_id", String(128), nullable=False),
    Column("store_id", String(128), nullable=False),
)

org_sku_map_tbl = Table(
    "org_sku_map", meta,
    Column("org_id", String(128), nullable=False),
    Column("sku_id", String(128), nullable=False),
)

# --------------------------------------------------------------------
# Alineación de esquema mínima en Neon (idempotente y segura)
# --------------------------------------------------------------------
def ensure_accounts_schema() -> None:
    """Alinea el esquema mínimo para que coincida con el modelo actual."""
    with engine.begin() as conn:
        # users.created_at
        conn.exec_driver_sql("""
            ALTER TABLE IF EXISTS users
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
        """)
        # org_store_map / org_sku_map: crear si no existen
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS org_store_map (
                org_id TEXT NOT NULL,
                store_id TEXT NOT NULL
            );
        """)
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS org_sku_map (
                org_id TEXT NOT NULL,
                sku_id TEXT NOT NULL
            );
        """)
        # Renombrar columna 'sku' -> 'sku_id' si aplica
        conn.exec_driver_sql("""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='org_sku_map' AND column_name='sku'
          ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='org_sku_map' AND column_name='sku_id'
          ) THEN
            EXECUTE 'ALTER TABLE org_sku_map RENAME COLUMN sku TO sku_id';
          END IF;
        END$$;
        """)
        # Índice funcional para email case-insensitive
        conn.exec_driver_sql("""
            CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_key ON users (lower(email));
        """)

def init_accounts_db() -> None:
    meta.create_all(engine, tables=[orgs_tbl, users_tbl, org_store_map_tbl, org_sku_map_tbl])
    ensure_accounts_schema()

# --------------------------------------------------------------------
# CRUD básico
# --------------------------------------------------------------------
def upsert_org(org_id: str, display_name: Optional[str] = None, slack_webhook: Optional[str] = None) -> None:
    init_accounts_db()
    with engine.begin() as conn:
        row = conn.execute(select(orgs_tbl.c.org_id).where(orgs_tbl.c.org_id == org_id)).first()
        if row:
            conn.execute(
                orgs_tbl.update().where(orgs_tbl.c.org_id == org_id)
                .values(display_name=display_name, slack_webhook=slack_webhook)
            )
        else:
            conn.execute(
                orgs_tbl.insert().values(
                    org_id=org_id,
                    display_name=display_name,
                    slack_webhook=slack_webhook,
                    created_at=datetime.datetime.utcnow(),
                )
            )

def get_user_by_email(email: str) -> Optional[dict]:
    """Búsqueda case-insensitive y sin exigir created_at (por compatibilidad)."""
    init_accounts_db()
    email = (email or "").strip().lower()
    if not email:
        return None
    with engine.connect() as conn:
        row = conn.execute(
            select(
                users_tbl.c.id,
                users_tbl.c.email,
                users_tbl.c.password,
                users_tbl.c.org_id,
                users_tbl.c.role,
                users_tbl.c.display_name,
            ).where(func.lower(users_tbl.c.email) == email)
        ).mappings().first()
        return dict(row) if row else None

def create_user(email: str, password: str, org_id: str, role: str = "member", display_name: Optional[str] = None) -> int:
    init_accounts_db()
    email = (email or "").strip().lower()
    with engine.begin() as conn:
        existing = conn.execute(
            select(users_tbl.c.id).where(func.lower(users_tbl.c.email) == email)
        ).first()
        if existing:
            return int(existing[0])
        res = conn.execute(
            users_tbl.insert().values(
                email=email,
                password=str(password),
                org_id=str(org_id),
                role=str(role or "member"),
                display_name=display_name,
                created_at=datetime.datetime.utcnow(),
            )
        )
        try:
            return int(res.inserted_primary_key[0])
        except Exception:
            row = conn.execute(
                select(users_tbl.c.id).where(func.lower(users_tbl.c.email) == email)
            ).first()
            return int(row[0]) if row else 0

# --------------------------------------------------------------------
# Lecturas tipo DataFrame (usadas por la UI)
# --------------------------------------------------------------------
def df_users() -> pd.DataFrame:
    init_accounts_db()
    try:
        return pd.read_sql(select(users_tbl), con=engine)
    except Exception:
        # Si falta created_at en alguna DB vieja, trae subset
        cols = [
            users_tbl.c.id,
            users_tbl.c.email,
            users_tbl.c.password,
            users_tbl.c.org_id,
            users_tbl.c.role,
            users_tbl.c.display_name,
        ]
        return pd.read_sql(select(*cols), con=engine)

def df_orgs() -> pd.DataFrame:
    init_accounts_db()
    try:
        return pd.read_sql(select(orgs_tbl), con=engine)
    except Exception:
        return pd.DataFrame(columns=["org_id","display_name","slack_webhook","created_at"])

def df_org_store_map() -> pd.DataFrame:
    init_accounts_db()
    try:
        return pd.read_sql(select(org_store_map_tbl), con=engine)
    except Exception:
        return pd.DataFrame(columns=["org_id","store_id"])

def df_org_sku_map() -> pd.DataFrame:
    init_accounts_db()
    try:
        return pd.read_sql(select(org_sku_map_tbl), con=engine)
    except Exception:
        return pd.DataFrame(columns=["org_id","sku_id"])

# --------------------------------------------------------------------
# Migración desde CSV (completa e idempotente)
# --------------------------------------------------------------------
def migrate_from_csv(data_dir: Path) -> None:
    """
    Migra cuentas desde ./data/accounts/*.csv a la DB actual.
    - Tolera CSVs faltantes.
    - orgs.csv puede traer 'display_name' o 'org_name'.
    - Si no hay orgs.csv, crea orgs a partir de org_id únicos en users.csv.
    - Evita duplicados con verificaciones previas.
    """
    init_accounts_db()
    acc_dir = Path(data_dir) / "accounts"

    def _read_csv(p: Path, cols: list[str]) -> pd.DataFrame:
        if p.exists() and p.stat().st_size > 0:
            try:
                df = pd.read_csv(p)
                return df if isinstance(df, pd.DataFrame) else pd.DataFrame(columns=cols)
            except Exception:
                return pd.DataFrame(columns=cols)
        return pd.DataFrame(columns=cols)

    orgs_df  = _read_csv(acc_dir / "orgs.csv", ["org_id","display_name","org_name","slack_webhook"])
    users_df = _read_csv(acc_dir / "users.csv", ["email","password","org_id","role","display_name"])
    osm_df   = _read_csv(acc_dir / "org_store_map.csv", ["org_id","store_id"])
    osk_df   = _read_csv(acc_dir / "org_sku_map.csv", ["org_id","sku_id"])

    # --- ORGS ---
    created_orgs: Set[str] = set()
    if not orgs_df.empty:
        for _, r in orgs_df.iterrows():
            oid = str(r.get("org_id") or "").strip()
            if not oid:
                continue
            display = None
            if "display_name" in r and not pd.isna(r["display_name"]):
                display = str(r["display_name"]).strip()
            elif "org_name" in r and not pd.isna(r["org_name"]):
                display = str(r["org_name"]).strip()
            slack_w = r.get("slack_webhook")
            slack_w = str(slack_w).strip() if slack_w is not None and not pd.isna(slack_w) else None
            upsert_org(oid, display_name=(display or None), slack_webhook=slack_w)
            created_orgs.add(oid)

    # Si no hay orgs.csv, infiere orgs desde users.csv
    if orgs_df.empty and not users_df.empty and "org_id" in users_df.columns:
        for oid in sorted(set(str(x).strip() for x in users_df["org_id"].tolist() if str(x).strip())):
            if oid not in created_orgs:
                upsert_org(oid, display_name=oid)

    # --- USERS ---
    if not users_df.empty:
        for _, r in users_df.iterrows():
            email = str(r.get("email") or "").strip().lower()
            if not email:
                continue
            password = str(r.get("password") or "").strip()
            org_id   = str(r.get("org_id") or "default").strip()
            role     = str(r.get("role") or "member").strip()
            display  = r.get("display_name")
            display  = str(display).strip() if display is not None and not pd.isna(display) else None
            if not get_user_by_email(email):
                create_user(
                    email=email,
                    password=password,
                    org_id=org_id,
                    role=role,
                    display_name=display
                )

    # --- MAPS: insertar sólo faltantes (idempotente simple) ---
    # org_store_map
    if not osm_df.empty:
        with engine.connect() as conn:
            existing = set(
                (str(row[0]), str(row[1]))
                for row in conn.execute(select(org_store_map_tbl.c.org_id, org_store_map_tbl.c.store_id))
            )
        to_add = []
        for _, r in osm_df.iterrows():
            oid = str(r.get("org_id") or "").strip()
            sid = r.get("store_id")
            sid = "" if (sid is None or (isinstance(sid, float) and pd.isna(sid))) else str(sid).strip()
            key = (oid, sid)
            if oid and sid and key not in existing:
                to_add.append({"org_id": oid, "store_id": sid})
        if to_add:
            with engine.begin() as conn:
                for row in to_add:
                    conn.execute(org_store_map_tbl.insert().values(**row))

    # org_sku_map
    if not osk_df.empty:
        with engine.connect() as conn:
            existing = set(
                (str(row[0]), str(row[1]))
                for row in conn.execute(select(org_sku_map_tbl.c.org_id, org_sku_map_tbl.c.sku_id))
            )
        to_add = []
        for _, r in osk_df.iterrows():
            oid = str(r.get("org_id") or "").strip()
            kid = r.get("sku_id")
            kid = "" if (kid is None or (isinstance(kid, float) and pd.isna(kid))) else str(kid).strip()
            key = (oid, kid)
            if oid and kid and kid.lower() != "nan" and key not in existing:
                to_add.append({"org_id": oid, "sku_id": kid})
        if to_add:
            with engine.begin() as conn:
                for row in to_add:
                    conn.execute(org_sku_map_tbl.insert().values(**row))

# --------------------------------------------------------------------
# Sync idempotente de mapas para UNA org (usado en registro)
# --------------------------------------------------------------------
def sync_org_maps_from_csv(org_id: str, data_dir: Path) -> Tuple[int, int]:
    """
    Inserta en Neon sólo lo que falte para esa org: (org_store_map, org_sku_map).
    Limpia valores NaN y normaliza a str.
    Devuelve (stores_agregados, skus_agregados).
    """
    init_accounts_db()
    acc_dir = Path(data_dir) / "accounts"
    stores_added = 0
    skus_added = 0

    osm_path = acc_dir / "org_store_map.csv"
    osk_path = acc_dir / "org_sku_map.csv"

    # EXISTENTES
    with engine.connect() as conn:
        existing_stores = set(
            (str(r[0]) for r in conn.execute(select(org_store_map_tbl.c.store_id).where(org_store_map_tbl.c.org_id == org_id)))
        )
        existing_skus = set(
            (str(r[0]) for r in conn.execute(select(org_sku_map_tbl.c.sku_id).where(org_sku_map_tbl.c.org_id == org_id)))
        )

    # STORES
    if osm_path.exists() and osm_path.stat().st_size > 0:
        try:
            osm_df = pd.read_csv(osm_path)
        except Exception:
            osm_df = pd.DataFrame(columns=["org_id","store_id"])
        if not osm_df.empty:
            to_insert = []
            for _, r in osm_df.iterrows():
                oid = str(r.get("org_id") or "").strip()
                sid_raw = r.get("store_id")
                sid = "" if (sid_raw is None or (isinstance(sid_raw, float) and pd.isna(sid_raw))) else str(sid_raw).strip()
                if oid == org_id and sid and sid not in existing_stores:
                    to_insert.append({"org_id": oid, "store_id": sid})
            if to_insert:
                with engine.begin() as conn:
                    for row in to_insert:
                        conn.execute(org_store_map_tbl.insert().values(**row))
                stores_added = len(to_insert)

    # SKUS
    if osk_path.exists() and osk_path.stat().st_size > 0:
        try:
            osk_df = pd.read_csv(osk_path)
        except Exception:
            osk_df = pd.DataFrame(columns=["org_id","sku_id"])
        if not osk_df.empty:
            to_insert = []
            for _, r in osk_df.iterrows():
                oid = str(r.get("org_id") or "").strip()
                kid_raw = r.get("sku_id")
                kid = "" if (kid_raw is None or (isinstance(kid_raw, float) and pd.isna(kid_raw))) else str(kid_raw).strip()
                if oid == org_id and kid and kid.lower() != "nan" and kid not in existing_skus:
                    to_insert.append({"org_id": oid, "sku_id": kid})
            if to_insert:
                with engine.begin() as conn:
                    for row in to_insert:
                        conn.execute(org_sku_map_tbl.insert().values(**row))
                skus_added = len(to_insert)

    return stores_added, skus_added
