# streamlit_app.py
import streamlit as st
import pathlib
import time, select, re, sys, os, requests

ROOT = pathlib.Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# --- Import bootstrap: asegura que 'multifronts/' quede en sys.path ---
_THIS_FILE = pathlib.Path(__file__).resolve()
_PKG_ROOT  = _THIS_FILE.parent            # /mount/src/multifronts
_REPO_ROOT = _PKG_ROOT.parent             # /mount/src
for p in (str(_PKG_ROOT), str(_REPO_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)
# ---------------------------------------------------------------------

# Instanciar data csv
try:
    from generate_data import init_all as _init_all
except Exception:
    _init_all = None

# Núcleo y utilidades propias
try:
    from core.load import load_data  # layout antiguo
except Exception:
    # fallback mínimo (no se usa si existen los módulos reales)
    def load_data():
        DATA_DIR_loaded = DATA_DIR
        def _read(name):
            import pandas as pd
            p = DATA_DIR_loaded / f"{name}.csv"
            return pd.read_csv(p) if p.exists() else __import__("pandas").DataFrame()
        stores     = _read("stores")
        skus       = _read("skus")
        sales      = _read("sales")
        inv        = _read("inventory_levels")
        lt         = _read("lead_time")
        promos     = _read("promos")
        distances  = _read("distances")
        orders_c   = _read("orders")
        transfers_c= _read("transfers")
        notifications = _read("notifications")
        return (DATA_DIR_loaded, stores, skus, sales, inv, lt, promos, distances, orders_c, transfers_c, notifications)
    
from core.headers import nice_headers
from ui.kpis import kpi_cards
from features.metrics import compute_baseline
from services.auth import login_ui, register_ui, get_current_user
from services.guardrails import get_allowed_sets
from utils.labels import make_store_labels

# OO y vistas
from core.context import AppContext
from ui.filters import FilterPanel
from views.operation import OperationView
from views.summary import SummaryView

# Repositorio (para obtener DB_URL y, si quieres, init_db)
from services import repo

# psycopg para LISTEN/NOTIFY
try:
    import psycopg
except Exception:
    psycopg = None  # si no está instalado, desactivamos live updates y avisamos

# 🔧 aliasa el módulo queue para evitar UnboundLocalError
import re, threading
import queue as pyqueue

# === NEW: helper para detectar historial en Neon ===
from sqlalchemy import text

@st.cache_data(show_spinner=False, ttl=120)
def _org_has_neon_history(org_id: str) -> bool:
    """
    Devuelve True si existen registros en orders_confirmed o transfers_confirmed para la org.
    """
    try:
        eng = repo.get_engine()
        with eng.begin() as conn:
            val = conn.execute(
                text("""
                    SELECT EXISTS (
                        SELECT 1 FROM orders_confirmed WHERE org_id = :o
                    ) OR EXISTS (
                        SELECT 1 FROM transfers_confirmed WHERE org_id = :o
                    ) AS has_hist
                """),
                {"o": str(org_id)}
            ).scalar()
        return bool(val)
    except Exception:
        # En caso de error de conexión, no bloquear el UI
        return False

# Config de página (evita re-renders raros)
st.set_page_config(page_title="Multifronts", layout="wide", initial_sidebar_state="expanded")

# === RUTAS/CONFIG BASE ===
# DATA_DIR debe existir ANTES del login/registro
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# API base (Slack, backend)
API_BASE = (
    st.secrets.get("api", {}).get("base")
    or os.environ.get("API_BASE")
)

# --- Slack helpers cacheados ---
@st.cache_data(show_spinner=False, ttl=300)
def _slack_workspace():
    try:
        r = requests.get(f"{API_BASE}/slack/workspace", timeout=10)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

@st.cache_data(show_spinner=False, ttl=500)
def _slack_status(org_id: str) -> dict:
    try:
        r = requests.get(f"{API_BASE}/slack/status", params={"org_id": org_id}, timeout=10)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

@st.cache_data(show_spinner=False, ttl=500)
def _slack_channel_info(org_id: str) -> dict:
    try:
        r = requests.get(f"{API_BASE}/debug/slack/channel_info", params={"org_id": org_id}, timeout=10)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _slack_invite_self(org_id: str, email: str) -> dict:
    try:
        r = requests.post(
            f"{API_BASE}/admin/slack/invite",
            params={"org_id": org_id, "emails": email},
            timeout=10,
        )
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _init_all_safe():
    if _init_all:
        try:
            _init_all()
        except Exception:
            # No bloquees la app si el generador falla/no existe
            pass

def navbar(available_summary: bool) -> tuple[str, str]:
    mode = st.sidebar.radio("Modo", ["Simplificado", "Técnico"], index=1, key="nav_mode")
    items = ["Operación"]
    if available_summary:
        items.append("Reporte")
    section = st.sidebar.radio("Sección", items, index=0, key="nav_section")
    return mode, section

def main():
    st.title("🧭 MULTI FRONTS")

    # ---- Login/Registro en sidebar (formularios) ----
    user, orgs_df = login_ui(DATA_DIR)
    actor = get_current_user()
    if not actor:
        try:
            register_ui(DATA_DIR)
        except Exception:
            pass
        st.info("Inicia sesión o crea tu cuenta desde el panel lateral.")
        st.stop()

    # ---- (Opcional) Generar CSV base si faltan tras login ----
    if not (DATA_DIR / "stores.csv").exists():
        _init_all_safe()

    # ---- Ahora sí, carga de datos pesada (cacheada internamente) ----
    (DATA_DIR_loaded, stores, skus, sales, inv, lt, promos, distances, orders_c, transfers_c, notifications) = load_data()

    st.caption(f"Sesión: **{actor.display_name or actor.email}** de **{actor.org_id}**")

    # Scope por organización
    allowed_stores, allowed_skus = get_allowed_sets(DATA_DIR, actor.org_id)
    if not allowed_stores or not allowed_skus:
        st.error("Tu organización aún no tiene tiendas y/o SKUs asignados. Contacta al administrador.")
        st.stop()

    # ⤵️ Si Neon aún no tiene inventario para esta org, siembra desde el snapshot CSV
    try:
        inv_db_check = repo.fetch_inventory_levels(
            org_id=actor.org_id,
            store_ids=list(allowed_stores),
            sku_ids=list(allowed_skus),
        )
        if inv_db_check is None or inv_db_check.empty:
            if inv is not None and not inv.empty:
                repo.seed_inventory_from_snapshot(actor.org_id, inv)
    except Exception as _e:
        st.info(f"(info) No se pudo inicializar inventario en Neon: {_e}")

    # Catálogos/ventas scopeados
    stores_scoped = stores[stores["store_id"].isin(sorted(allowed_stores))].copy()
    skus_scoped   = skus[skus["sku_id"].isin(sorted(allowed_skus))].copy()
    sales_scoped  = sales[sales["store_id"].isin(allowed_stores) & sales["sku_id"].isin(allowed_skus)].copy()

    id_to_label, label_to_id = make_store_labels(stores_scoped)

    # KPIs base por org
    kpis, recent = compute_baseline(sales_scoped)
    kpi_cards(kpis, st.session_state.get("nav_mode", "Técnico"))

    # ¿Hay movimientos previos o de esta sesión?
    has_past_moves = (orders_c is not None and not orders_c.empty) or (transfers_c is not None and not transfers_c.empty)
    has_session_moves = st.session_state.get("movements_this_session", False)

    # === NEW: considerar historial en Neon ===
    has_neon_history = _org_has_neon_history(actor.org_id)

    available_summary = has_past_moves or has_session_moves or has_neon_history

    mode, section = navbar(available_summary=available_summary)

    # Guardar versiones scopeadas de movimientos para Summary
    orders_scoped, transfers_scoped = None, None
    if orders_c is not None and not orders_c.empty:
        m = orders_c["store_id"].isin(allowed_stores) & orders_c["sku_id"].isin(allowed_skus)
        if "org_id" in orders_c.columns:
            m = m & (orders_c["org_id"].astype(str) == str(actor.org_id))
        orders_scoped = orders_c[m].copy()
    if transfers_c is not None and not transfers_c.empty:
        m = (transfers_c["from_store"].isin(allowed_stores) &
             transfers_c["to_store"].isin(allowed_stores) &
             transfers_c["sku_id"].isin(allowed_skus))
        if "org_id" in transfers_c.columns:
            m = m & (transfers_c["org_id"].astype(str) == str(actor.org_id))
        transfers_scoped = transfers_c[m].copy()

    # Contexto compartido (dataclass)
    ctx = AppContext(
        DATA_DIR=DATA_DIR,
        stores=stores_scoped,
        skus=skus_scoped,
        sales=sales_scoped,
        inv=inv,
        lt=lt,
        promos=promos,
        distances=distances,
        actor_email=actor.email,
        actor_display=(actor.display_name or actor.email),
        org_id=actor.org_id,
        allowed_stores=set(allowed_stores),
        allowed_skus=set(allowed_skus),
        id_to_label=id_to_label,
        label_to_id=label_to_id,
        kpis=kpis,
        recent=recent,
        orders_scoped=orders_scoped,
        transfers_scoped=transfers_scoped,
    )

    # Filtros (encapsulados; el panel ya usa st.form para evitar reruns por tecla)
    fpanel = FilterPanel(ctx)
    filters = fpanel.render(mode=mode, default_expand=(section == "Operación"))

    # ======= Render de vistas en contenedor (repinta sin recargar toda la página) =======
    dyn = st.container()

    def render_views_in():
        with dyn:
            if section == "Operación":
                OperationView(ctx, filters, mode=mode).render()
            else:
                SummaryView(ctx, filters).render()

    # Render inicial
    render_views_in()

    # ---- Bloque: Enlace de invitación al workspace Slack ----
    with st.sidebar.expander("🙌 Únete al workspace de Slack", expanded=True):
        ws = _slack_workspace()
        invite_url = os.getenv("SLACK_WORKSPACE_INVITE_URL", "").strip() or None
        team_id = (ws or {}).get("team_id")

        if invite_url:
            try:
                st.link_button("Entrar al Slack (workspace)", invite_url, use_container_width=True)
            except Exception:
                st.markdown(f"[Entrar al Slack (workspace)]({invite_url})")
            st.caption(
                "Primero únete al workspace con el botón de arriba. "
                "Luego podrás invitarte o unirte al canal de tu organización desde el bloque de Slack más abajo."
            )
        else:
            st.info(
                "Pide al admin que configure `SLACK_WORKSPACE_INVITE_URL` en el backend "
                "para mostrar aquí el enlace de acceso al Workspace."
            )

        if team_id:
            st.caption(f"Workspace ID: `{team_id}`")

    # ---- Bloque: Canal Slack por organización ----
    with st.sidebar.expander("🔔 Slack de tu organización", expanded=True):
        org_id = ctx.org_id
        user_email = ctx.actor_email
        if not org_id:
            st.info("Inicia sesión para ver tu canal de Slack.")
        else:
            st.caption("Canal asignado por organización (se crea automáticamente).")
            info = _slack_channel_info(org_id)
            status = _slack_status(org_id)

            web_url = (info or {}).get("web_url")
            if web_url:
                st.markdown(f"[Abrir canal de Slack]({web_url})")

            if not (info or {}).get("ok"):
                st.warning("Aún no hay canal registrado o falta autorización del bot.")
            else:
                ch = (info or {}).get("info", {}).get("channel", {})
                is_private = ch.get("is_private", False)
                st.write(f"Visibilidad: {'Privado' if is_private else 'Público'}")
                if user_email:
                    if st.button("Invitarme al canal"):
                        resp = _slack_invite_self(org_id, user_email)
                        if resp.get("ok"):
                            st.success("Invitación enviada. Revisa Slack 👌")
                        else:
                            st.error(f"No se pudo invitar: {resp.get('error') or resp}")
                else:
                    st.caption("No detecté tu email; inicia sesión para poder invitarte automáticamente.")

    # ======= Live updates SIN recarga: Postgres LISTEN/NOTIFY =======
    st.session_state.setdefault("live_updates", True)
    st.sidebar.toggle("⚡ Live updates", key="live_updates")

    if st.session_state["live_updates"]:
        if repo.DB_URL.startswith("sqlite"):
            st.sidebar.info("Servicio Live no disponible (error de PostgreSQL, fallback a SQLite)")
        elif psycopg is None:
            st.sidebar.warning("psycopg no está instalado. Ejecuta: pip install 'psycopg[binary]'")
        else:
            @st.cache_resource(show_spinner=False)
            def get_pg_listener(db_url: str, channel_name: str):
                url = db_url.replace("+psycopg", "")
                q: "pyqueue.Queue[str]" = pyqueue.Queue()
                stop_evt = threading.Event()

                def _run():
                    try:
                        with psycopg.connect(url) as conn:
                            conn.autocommit = True
                            with conn.cursor() as cur:
                                cur.execute(f"LISTEN {channel_name};")
                                while not stop_evt.is_set():
                                    if select.select([conn], [], [], 1.0) == ([], [], []):
                                        continue
                                    conn.poll()
                                    while conn.notifies:
                                        note = conn.notifies.pop(0)
                                        try:
                                            q.put_nowait(note.payload or "")
                                        except Exception:
                                            pass
                    except Exception:
                        pass

                th = threading.Thread(target=_run, name="pg-listener", daemon=True)
                th.start()
                return q, stop_evt

            chan = "org_events_" + re.sub(r"[^a-zA-Z0-9_]", "_", str(ctx.org_id))
            try:
                q, stop_evt = get_pg_listener(repo.DB_URL, chan)
            except Exception:
                q, stop_evt = None, None

            if q is not None:
                drained = False
                for _ in range(256):
                    try:
                        _msg = q.get_nowait()
                        drained = True
                    except pyqueue.Empty:
                        break

                if drained:
                    dyn.empty()
                    render_views_in()

            with st.sidebar.expander("Modo escucha puntual", expanded=False):
                secs = st.number_input("Segundos", min_value=1, max_value=10, value=3, step=1)
                if st.button("👂 Escuchar ahora"):
                    import time
                    deadline = time.time() + float(secs)
                    while time.time() < deadline:
                        try:
                            msg = q.get(timeout=1.0)
                            dyn.empty()
                            render_views_in()
                        except pyqueue.Empty:
                            pass

if __name__ == "__main__":
    main()
