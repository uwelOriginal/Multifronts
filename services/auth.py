import os
import re
import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path
import hashlib
import pandas as pd
import streamlit as st

EMAIL_RX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

@dataclass
class User:
    email: str
    org_id: str
    role: str = "member"
    display_name: str = ""

def _load_df(path: Path) -> pd.DataFrame | None:
    try:
        if path.exists():
            return pd.read_csv(path)
    except Exception:
        pass
    return None

def load_account_tables(data_dir: Path):
    acc_dir = data_dir / "accounts"
    users = _load_df(acc_dir / "users.csv")
    orgs  = _load_df(acc_dir / "orgs.csv")
    org_store_map = _load_df(acc_dir / "org_store_map.csv")
    org_sku_map   = _load_df(acc_dir / "org_sku_map.csv")
    return users, orgs, org_store_map, org_sku_map

def try_login(email: str, password: str, users_df: pd.DataFrame | None):
    if users_df is None or users_df.empty:
        return None
    row = users_df[users_df["email"].str.lower() == email.strip().lower()]
    if row.empty:
        return None
    if "password" in row.columns:
        ok = str(row["password"].iloc[0]) == str(password)
        if not ok:
            return None
    org_id = str(row["org_id"].iloc[0]) if "org_id" in row.columns else "default"
    role = str(row["role"].iloc[0]) if "role" in row.columns else "member"
    display_name = str(row["display_name"].iloc[0]) if "display_name" in row.columns else email.strip()
    return User(email=email.strip(), org_id=org_id, role=role, display_name=display_name)

def get_current_user():
    return st.session_state.get("_current_user")

def set_current_user(user):
    if user is None:
        st.session_state.pop("_current_user", None)
    else:
        st.session_state["_current_user"] = user

def resolve_org_webhook(orgs_df: pd.DataFrame | None, org_id: str) -> str | None:
    if orgs_df is not None and not orgs_df.empty and "org_id" in orgs_df.columns:
        row = orgs_df[orgs_df["org_id"].astype(str) == str(org_id)]
        if not row.empty and "slack_webhook" in row.columns:
            val = str(row["slack_webhook"].iloc[0]).strip()
            if val:
                return val
    env = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    return env or None

# ---------- Registro (sin hard-codear la clave) ----------

def _get_reg_secret_hash() -> str | None:
    # Lee de st.secrets o del entorno. No hay valores por defecto.
    # Ejemplo de configuración:
    #   REGISTRATION_SECRET_HASH = sha256("4rribakonfront")
    try:
        return st.secrets.get("REGISTRATION_SECRET_HASH")  # type: ignore[attr-defined]
    except Exception:
        pass
    return os.getenv("REGISTRATION_SECRET_HASH")

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _validate_reg_secret(user_input: str) -> bool:
    cfg_hash = _get_reg_secret_hash()
    if not cfg_hash:
        # Si no hay hash configurado, no permitimos registro (evita bypass).
        return False
    return _sha256_hex(user_input.strip()) == cfg_hash.strip().lower()

def _run_generator_register(email: str, password: str, org_name: str, stores: int = 2, sku_fraction: float = 0.35) -> tuple[bool, str, str | None]:
    """
    Ejecuta generate_data.py en modo --register (o lo importa si es posible).
    Devuelve (ok, msg, org_id).
    """
    # Intento 1: importar y llamar a la función (más rápido)
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
    except Exception as e:
        # Fallback a CLI
        pass

    # Intento 2: ejecutar como subproceso CLI
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
        # El script imprime la org_id en una línea "ORG_ID=<id>"
        out = res.stdout + "\n" + res.stderr
        org_id = None
        for line in out.splitlines():
            if line.startswith("ORG_ID="):
                org_id = line.split("=", 1)[1].strip()
                break
        return True, "Cuenta creada.", org_id
    except Exception as e:
        return False, f"Error al ejecutar generate_data.py: {e}", None

def register_ui(data_dir: Path):
    users_df, _, _, _ = load_account_tables(data_dir)

    with st.sidebar.expander("Crear cuenta (panel)", expanded=False):
        org_name = st.text_input("Nombre de la organización", placeholder="Mi Retail, S.A.")
        email = st.text_input("Email de acceso", placeholder="usuario@miempresa.com")
        pwd1 = st.text_input("Contraseña", type="password")
        pwd2 = st.text_input("Confirmar contraseña", type="password")
        reg_code = st.text_input("Clave del panel", type="password", help="Solicítala a la organización. No se guarda, sólo se valida.")

        stores_n = st.number_input("Tiendas a crear", min_value=1, max_value=5, value=2, step=1, help="Se sintetizan con datos de demo.")
        sku_frac = st.slider("Fracción de SKUs a asignar al catálogo", 0.1, 1.0, 0.35, 0.05)

        if st.button("Registrar nueva organización y cuenta", type="primary", use_container_width=True):
            # Validaciones
            if not org_name.strip():
                st.error("Escribe el nombre de la organización.")
                return
            if not EMAIL_RX.match(email.strip()):
                st.error("Email inválido (formato nombre@dominio.ext).")
                return
            if users_df is not None and not users_df[users_df["email"].str.lower() == email.strip().lower()].empty:
                st.error("Este email ya existe. Intenta iniciar sesión.")
                return
            if not pwd1 or len(pwd1) < 6:
                st.error("La contraseña debe tener al menos 6 caracteres.")
                return
            if pwd1 != pwd2:
                st.error("Las contraseñas no coinciden.")
                return
            if not _validate_reg_secret(reg_code):
                st.error("Clave del panel inválida o configuración faltante.")
                st.info("Pide al admin que configure REGISTRATION_SECRET_HASH en secrets o variables de entorno.")
                return

            ok, msg, org_id = _run_generator_register(email.strip(), pwd1, org_name.strip(), int(stores_n), float(sku_frac))
            if not ok:
                st.error(msg)
                return
            st.success(f"{msg} Organización: {org_id or '(desconocida)'}")
            # Login automático
            set_current_user(User(email=email.strip(), org_id=org_id or "default", role="admin", display_name=email.split("@")[0].title()))
            st.rerun()

def login_ui(data_dir: Path):
    users_df, orgs_df, _, _ = load_account_tables(data_dir)
    user = get_current_user()

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

    # Si NO hay usuario, mostramos login + registro
    with st.sidebar.expander("Iniciar sesión", expanded=True):
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Contraseña", type="password", key="login_pwd")
        if st.button("Entrar", type="primary", use_container_width=True):
            u = try_login(email, password, users_df)
            if u:
                set_current_user(u)
                st.toast(f"Sesión iniciada: {u.display_name or u.email}")
                st.rerun()
            else:
                st.error("Credenciales inválidas.")

    # Solo cuando no hay sesión activa se ofrece registro
    register_ui(data_dir)
    return None, orgs_df
