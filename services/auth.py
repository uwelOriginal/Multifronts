# services/auth.py
from __future__ import annotations

import os
import re
import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path
import hashlib
import pandas as pd
import streamlit as st
from typing import Optional
try:
    import streamlit as st
except Exception:
    st = None
import requests

from services.accounts_repo import (
    init_accounts_db, migrate_from_csv,
    df_users as db_df_users,
    df_orgs as db_df_orgs,
    df_org_store_map as db_df_org_store_map,
    df_org_sku_map as db_df_org_sku_map,
    get_user_by_email as db_get_user_by_email,
    create_user as db_create_user,
    upsert_org as db_upsert_org,
    sync_org_maps_from_csv,   # <--- se mantiene
)
from services.repo import current_db_info
from services.client_events import publish_event 

EMAIL_RX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

@dataclass
class User:
    email: str
    org_id: str
    role: str = "member"
    display_name: str = ""

def _csv_path(data_dir: Path, name: str) -> Path:
    return data_dir / "accounts" / name

def _read_csv_or_empty(p: Path, cols: list[str]) -> pd.DataFrame:
    try:
        if p.exists():
            df = pd.read_csv(p)
            if isinstance(df, pd.DataFrame) and not df.empty:
                for c in cols:
                    if c not in df.columns:
                        df[c] = pd.Series(dtype=object)
                return df
    except Exception:
        pass
    return pd.DataFrame(columns=cols)

def _load_from_csv(data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    users = _read_csv_or_empty(_csv_path(data_dir, "users.csv"),
                               ["id","email","password","org_id","role","display_name"])
    orgs  = _read_csv_or_empty(_csv_path(data_dir, "orgs.csv"),
                               ["org_id","display_name","slack_webhook"])
    osm   = _read_csv_or_empty(_csv_path(data_dir, "org_store_map.csv"),
                               ["org_id","store_id"])
    osk   = _read_csv_or_empty(_csv_path(data_dir, "org_sku_map.csv"),
                               ["org_id","sku_id"])
    return users, orgs, osm, osk

def _seed_admin_from_secrets() -> bool:
    email = st.secrets.get("SEED_ADMIN_EMAIL") if hasattr(st, "secrets") else os.getenv("SEED_ADMIN_EMAIL")
    pwd   = st.secrets.get("SEED_ADMIN_PASSWORD") if hasattr(st, "secrets") else os.getenv("SEED_ADMIN_PASSWORD")
    org   = st.secrets.get("SEED_ADMIN_ORG") if hasattr(st, "secrets") else os.getenv("SEED_ADMIN_ORG")
    if not email or not pwd:
        return False
    try:
        db_upsert_org(str(org or "default"), display_name=str(org or "default"))
        if not db_get_user_by_email(str(email).lower()):
            db_create_user(email=str(email).lower(), password=str(pwd), org_id=str(org or "default"),
                           role="admin", display_name=str(email).split("@")[0].title())
        return True
    except Exception:
        return False

def _ensure_db_seeded(data_dir: Path) -> None:
    dialect, _, _ = current_db_info()
    if str(dialect).lower() != "postgresql":
        return
    init_accounts_db()
    du = db_df_users()
    if du is None or du.empty:
        try:
            migrate_from_csv(data_dir)
        except Exception:
            pass
        du2 = db_df_users()
        if du2 is None or du2.empty:
            _seed_admin_from_secrets()

def load_account_tables(data_dir: Path):
    st.session_state.pop("auth_fallback", None)
    st.session_state.pop("auth_fallback_reason", None)

    try:
        _ensure_db_seeded(data_dir)

        users = db_df_users()
        if users is None or users.empty:
            users = pd.DataFrame(columns=["id","email","password","org_id","role","display_name"])

        orgs = db_df_orgs()
        if orgs is None or orgs.empty:
            orgs = pd.DataFrame(columns=["org_id","display_name","slack_webhook"])

        org_store_map = db_df_org_store_map()
        if org_store_map is None or org_store_map.empty:
            org_store_map = pd.DataFrame(columns=["org_id","store_id"])

        org_sku_map = db_df_org_sku_map()
        if org_sku_map is None or org_sku_map.empty:
            org_sku_map = pd.DataFrame(columns=["org_id","sku_id"])

        dialect, host, _ = current_db_info()
        if str(dialect).lower() != "postgresql":
            st.session_state["auth_fallback"] = "csv"
            st.session_state["auth_fallback_reason"] = f"DB no-Postgres detectada ({dialect}). Revisa DATABASE_URL / secrets."

        return users, orgs, org_store_map, org_sku_map

    except Exception as e:
        users, orgs, org_store_map, org_sku_map = _load_from_csv(data_dir)
        st.session_state["auth_fallback"] = "csv"
        st.session_state["auth_fallback_reason"] = str(e)
        return users, orgs, org_store_map, org_sku_map

def get_current_user():
    return st.session_state.get("_current_user")

def set_current_user(user):
    if user is None:
        st.session_state.pop("_current_user", None)
    else:
        st.session_state["_current_user"] = user

def try_login(email: str, password: str, users_df: pd.DataFrame | None):
    if not email or not EMAIL_RX.match(email.strip()):
        return None

    if st.session_state.get("auth_fallback") != "csv":
        try:
            row = db_get_user_by_email(email.strip().lower())
            if row:
                if str(row["password"]) != str(password):
                    return None
                return User(
                    email=row["email"],
                    org_id=row["org_id"],
                    role=row.get("role","member") or "member",
                    display_name=row.get("display_name") or row["email"],
                )
        except Exception as e:
            st.session_state["auth_fallback"] = "csv"
            st.session_state["auth_fallback_reason"] = f"DB error: {e}"

    if users_df is None or users_df.empty:
        return None
    dfrow = users_df[users_df["email"].astype(str).str.lower() == email.strip().lower()]
    if dfrow.empty:
        return None
    if "password" in dfrow.columns:
        if str(dfrow["password"].iloc[0]) != str(password):
            return None
    org_id = str(dfrow["org_id"].iloc[0]) if "org_id" in dfrow.columns else "default"
    role = str(dfrow["role"].iloc[0]) if "role" in dfrow.columns else "member"
    display_name = str(dfrow["display_name"].iloc[0]) if "display_name" in dfrow.columns else email.strip()
    return User(email=email.strip(), org_id=org_id, role=role, display_name=display_name)

def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _validate_reg_secret(secret: str) -> bool:
    cfg = (st.secrets.get("app", {}).get("registration_key") if hasattr(st, "secrets") else None) \
          or os.getenv("REGISTRATION_KEY", "")
    return bool(cfg) and (secret or "").strip() == cfg.strip()

def _run_generator_register(email: str, password: str, org_name: str, stores: int = 2, sku_fraction: float = 0.35):
    # Intento 1: import directo
    try:
        import importlib
        gen = importlib.import_module("generate_data")
        if hasattr(gen, "register_new_account"):
            org_id = gen.register_new_account(
                data_dir=Path("./data"),
                email=email,
                password=password,
                org_name=org_name,
                stores_count=stores,
                sku_fraction=sku_fraction,
            )
            return True, "Cuenta creada.", org_id
    except Exception:
        pass

    # Intento 2: CLI
    try:
        gen_path = Path.cwd() / "generate_data.py"
        if not gen_path.exists():
            return False, f"No se encontró {gen_path}", None
        cmd = [
            sys.executable, str(gen_path),
            "--register",
            "--email", email,
            "--password", password,
            "--org-name", org_name,
            "--stores", str(stores),
            "--skus-frac", str(sku_fraction),
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode != 0:
            return False, f"Error al registrar: {res.stderr.strip() or res.stdout.strip()}", None
        out = res.stdout + "\n" + res.stderr
        org_id = None
        for line in out.splitlines():
            if line.startswith("ORG_ID="):
                org_id = line.split("=", 1)[1].strip()
                break
        return True, "Cuenta creada.", org_id
    except Exception as e:
        return False, f"Error al ejecutar generate_data.py: {e}", None

def _safe_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass

def register_ui(data_dir: Path):
    """
    Registro en formulario (no re-ejecuta en cada tecla).
    Mantiene el flujo original: generate_data -> upsert org -> create user -> sync maps -> publish_event -> set session -> rerun
    """
    users_df, _, _, _ = load_account_tables(data_dir)

    with st.sidebar.expander("Crear cuenta", expanded=False):
        # ---- FORM: sólo actúa al pulsar el botón ----
        with st.form("register_form", clear_on_submit=False):
            org_name = st.text_input("Nombre de la organización", placeholder="Mi Retail, S.A.", key="reg_org_name")
            email    = st.text_input("Email de acceso", placeholder="usuario@miempresa.com", key="reg_email")
            pwd1     = st.text_input("Contraseña", type="password", key="reg_pwd1")
            pwd2     = st.text_input("Confirmar contraseña", type="password", key="reg_pwd2")
            reg_code = st.text_input("Clave del panel", type="password", key="reg_code")

            c1, c2 = st.columns(2)
            stores_n = c1.number_input("Tiendas a crear", min_value=1, max_value=5, value=2, step=1, key="reg_stores")
            sku_frac = c2.slider("Fracción de SKUs a asignar al catálogo", 0.1, 1.0, 0.35, 0.05, key="reg_sku_frac")

            submit = st.form_submit_button("Registrar nueva organización y cuenta", type="primary", use_container_width=True)

        if not submit:
            return  # no ejecutar nada hasta que se presione el botón

        # ---- Validaciones (idénticas al flujo previo) ----
        if not org_name.strip():
            st.error("Escribe el nombre de la organización."); return
        if not EMAIL_RX.match(email.strip()):
            st.error("Email inválido."); return
        if users_df is not None and not users_df.empty and not users_df[users_df["email"].astype(str).str.lower() == email.strip().lower()].empty:
            st.error("Este email ya existe. Intenta iniciar sesión."); return
        if not pwd1 or len(pwd1) < 6:
            st.error("La contraseña debe tener al menos 6 caracteres."); return
        if pwd1 != pwd2:
            st.error("Las contraseñas no coinciden."); return
        if not _validate_reg_secret(reg_code):
            st.error("Clave del panel inválida o no configurada.")
            return

        # ---- Generar datos base (generate_data) ----
        ok, msg, org_id = _run_generator_register(email.strip(), pwd1, org_name.strip(), int(stores_n), float(sku_frac))
        if not ok:
            st.error(msg); return

        # ---- Persistir en Neon ----
        errors = []
        try:
            db_upsert_org(org_id or "default", display_name=org_name.strip())
        except Exception as e:
            errors.append(f"upsert_org: {e}")

        user_id = None
        try:
            existing = db_get_user_by_email(email.strip().lower())
            if existing:
                user_id = existing.get("id")
            else:
                user_id = db_create_user(
                    email=email.strip().lower(), password=pwd1,
                    org_id=(org_id or "default"),
                    role="admin",
                    display_name=email.split("@")[0].title()
                )
        except Exception as e:
            errors.append(f"create_user: {e}")

        # ---- Sync mapas para la org recién creada (idempotente) ----
        added_stores = 0
        added_skus = 0
        try:
            added_stores, added_skus = sync_org_maps_from_csv(org_id or "default", Path("./data"))
        except Exception as e:
            errors.append(f"sync_maps: {e}")

        if errors:
            st.warning("Cuenta creada, pero hubo problemas al persistir en DB:\n- " + "\n- ".join(errors))
        else:
            st.success(
                f"{msg} Org: {org_id or '(desconocida)'} | Usuario ID: {user_id or '(N/D)'} | "
                f"Mapas añadidos → tiendas: {added_stores}, skus: {added_skus}"
            )
        
        # ---- Emitir evento (se mantiene) ----
        try:
            publish_event(
                org_id=(org_id or "default"),
                type_="org_created",
                payload={"created_by": email.strip().lower()},
                timeout=3.0,
            )
        except Exception:
            pass

        # ---- Abrir sesión y rerun ----
        set_current_user(User(email=email.strip(), org_id=org_id or "default", role="admin", display_name=email.split("@")[0].title()))
        st.rerun()

def login_ui(data_dir: Path):
    users_df, orgs_df, _, _ = load_account_tables(data_dir)
    user = get_current_user()

    if st.session_state.get("auth_fallback") == "csv":
        reason = st.session_state.get("auth_fallback_reason", "")
        with st.sidebar:
            st.warning("⚠️ No hay conexión a la base de datos; la información no será en vivo.")
            if reason:
                st.caption(f"Detalle: {reason}")

    if user:
        with st.sidebar.expander("Cuenta", expanded=True):
            st.write(f"**Usuario:** {user.display_name or user.email}")
            st.write(f"**Email:** {user.email}")
            st.write(f"**Organización:** {user.org_id}")
            st.write(f"**Rol:** {user.role}")
            if st.button("Cerrar sesión", use_container_width=True):
                set_current_user(None)
                st.rerun()
        return user, orgs_df

    # --- FORM de login: evita reruns por tecla ---
    with st.sidebar.form("login_form", clear_on_submit=False):
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Contraseña", type="password", key="login_password")
        submitted = st.form_submit_button("Entrar", use_container_width=True)

    if not submitted:
        return None, orgs_df

    u = try_login(email, password, users_df)
    if not u:
        st.sidebar.error("Credenciales inválidas.")
        return None, orgs_df

    set_current_user(u)
    _safe_rerun()

def _api_base() -> Optional[str]:
    """Obtiene API_BASE de secrets o env, sin la barra final."""
    try:
        if hasattr(st, "secrets"):
            b = st.secrets.get("API_BASE", None)
            if b:
                return str(b).rstrip("/")
    except Exception:
        pass
    env_b = os.getenv("API_BASE", "").strip()
    return env_b.rstrip("/") if env_b else None

def _valid_url(url: str | None) -> bool:
    return bool(url) and (str(url).startswith("http://") or str(url).startswith("https://"))

def resolve_org_webhook(orgs_df: pd.DataFrame | None, org_id: str) -> str | None:
    if orgs_df is None or orgs_df.empty:
        return None
    df = orgs_df[orgs_df["org_id"].astype(str) == str(org_id)]
    if df.empty:
        return None
    val = df["slack_webhook"].iloc[0] if "slack_webhook" in df.columns else None
    val = str(val).strip() if val is not None else None
    return val if _valid_url(val) else None

def resolve_org_webhook_oauth_first(orgs_df: pd.DataFrame | None, org_id: str) -> Optional[str]:
    """
    Orden de resolución:
      1) Backend (Render) vía OAuth: GET {API_BASE}/slack/status?org_id=...
         Acepta llaves: 'webhook', 'incoming_webhook_url', 'webhook_url', 'url'
      2) Secret global: SLACK_WEBHOOK_URL
      3) Mapeo local en orgs_df (columna 'slack_webhook')
    """
    # 1) Backend OAuth (preferido)
    base = _api_base()
    if base:
        try:
            r = requests.get(f"{base}/slack/status", params={"org_id": org_id}, timeout=3.0)
            if r.ok:
                data = r.json() or {}
                candidates = [
                    data.get("webhook"),
                    data.get("incoming_webhook_url"),
                    data.get("webhook_url"),
                    data.get("url"),
                ]
                for url in candidates:
                    if _valid_url(url):
                        return str(url)
        except Exception:
            # No rompemos el flujo si el backend está caído o sin CORS
            pass

    # 2) Secret global
    try:
        if hasattr(st, "secrets"):
            wh = st.secrets.get("SLACK_WEBHOOK_URL", None)
            if _valid_url(wh):
                return str(wh)
    except Exception:
        pass

    # 3) Mapeo en orgs_df (CSV/DB)
    return resolve_org_webhook(orgs_df, org_id)
