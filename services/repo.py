# services/repo.py
from __future__ import annotations

import os
from typing import Optional, Tuple
from urllib.parse import quote_plus, urlparse

from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine

try:
    import streamlit as st
except Exception:
    st = None

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
    """
    Construye una URL de Postgres a partir de PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE.
    FUERZA sslmode=require (Neon lo exige).
    """
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
    # 1) URL completa en secrets
    if st is not None and hasattr(st, "secrets"):
        try:
            v = st.secrets.get("DATABASE_URL", None)
            if v and isinstance(v, str) and "://" in v:
                return v.strip()
        except Exception:
            pass

    # 2) URL completa en ENV
    env_url = os.getenv("DATABASE_URL")
    if env_url and "://" in env_url:
        return env_url.strip()

    # 3) Piezas PGHOST/PGUSER/...
    composed = _compose_pg_url_from_parts()
    if composed:
        return composed

    # 4) Fallback local
    os.makedirs("data", exist_ok=True)
    return "sqlite:///data/app.db"

DB_URL: str = _get_database_url()

engine_args = dict(future=True)
if DB_URL.startswith("sqlite"):
    engine = create_engine(DB_URL, connect_args={"check_same_thread": False}, **engine_args)
else:
    engine = create_engine(DB_URL, pool_pre_ping=True, **engine_args)

def mask_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(parsed.password, "*****")
        return url
    except Exception:
        return url

def current_db_info() -> Tuple[str, Optional[str], str]:
    """(dialect, host, url_masked)"""
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
