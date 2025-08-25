import streamlit as st
from pathlib import Path, PurePath
import time, select, re

# Núcleo y utilidades propias
from core.load import load_data
from core.headers import nice_headers
from ui.kpis import kpi_cards
from features.metrics import compute_baseline
from services.auth import login_ui, get_current_user
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

import re, threading, queue

st.set_page_config(page_title="Control de Inventario — Operación & Resumen 4.5", layout="wide")

def navbar(available_summary: bool) -> tuple[str, str]:
    mode = st.sidebar.radio("Modo", ["Simplificado", "Técnico"], index=1, key="nav_mode")
    items = ["Operación"]
    if available_summary:
        items.append("Resumen 4.5")
    section = st.sidebar.radio("Sección", items, index=0, key="nav_section")
    return mode, section

def main():
    # Carga datos crudos del repositorio
    (DATA_DIR, stores, skus, sales, inv, lt, promos, distances, orders_c, transfers_c, notifications) = load_data()

    st.title("🧭 MULTI FRONTS")

    # Login obligatorio
    user, orgs_df = login_ui(DATA_DIR)
    actor = get_current_user()
    if not actor:
        st.title("Inicio de sesión requerido")
        st.info("Inicia sesión desde el panel lateral.")
        st.stop()

    st.caption(f"Sesión: **{actor.display_name or actor.email}** @ **{actor.org_id}**")

    # Scope por organización
    allowed_stores, allowed_skus = get_allowed_sets(DATA_DIR, actor.org_id)
    if not allowed_stores or not allowed_skus:
        st.error("Tu organización aún no tiene tiendas y/o SKUs asignados. Contacta al administrador.")
        st.stop()

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
    available_summary = has_past_moves or has_session_moves

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

    # Filtros (encapsulados)
    fpanel = FilterPanel(ctx)
    filters = fpanel.render(mode=mode, default_expand=(section == "Operación"))

    # ======= Render de vistas dentro de un CONTENEDOR (para repintar sin recargar toda la página) =======
    dyn = st.container()

    def render_views_in():
        with dyn:
            if section == "Operación":
                OperationView(ctx, filters, mode=mode).render()
            else:
                SummaryView(ctx, filters).render()

    # Render inicial
    render_views_in()

    # ======= Live updates SIN recarga: Postgres LISTEN/NOTIFY =======
    st.session_state.setdefault("live_updates", True)
    st.sidebar.toggle("🔔 Live updates", key="live_updates")

    if st.session_state["live_updates"]:
        if repo.DB_URL.startswith("sqlite"):
            st.sidebar.info("Servicio Live no disponible (error de PostgreSQL, fallback a SQLite)")
        elif psycopg is None:
            st.sidebar.warning("psycopg no está instalado. Ejecuta: pip install 'psycopg[binary]'")
        else:
            # 1) Listener compartido (persistente) mediante cache_resource
            @st.cache_resource(show_spinner=False)
            def get_pg_listener(db_url: str, channel_name: str):
                url = db_url.replace("+psycopg", "")  # psycopg no entiende el sufijo de SQLAlchemy
                q: "queue.Queue[str]" = queue.Queue()
                stop_evt = threading.Event()

                def _run():
                    try:
                        with psycopg.connect(url) as conn:
                            conn.autocommit = True
                            with conn.cursor() as cur:
                                cur.execute(f"LISTEN {channel_name};")
                                while not stop_evt.is_set():
                                    # Espera activa de bajo costo en el hilo (no bloquea la UI)
                                    if select.select([conn], [], [], 1.0)[0]:
                                        conn.poll()
                                        while conn.notifies:
                                            n = conn.notifies.pop(0)
                                            # Enviamos el payload (si lo hubiere) a la cola
                                            q.put(n.payload or "{}")
                    except Exception as e:
                        # Señalizamos error en la misma cola
                        q.put(f'__ERROR__:{e}')

                th = threading.Thread(target=_run, name="pg-notify-listener", daemon=True)
                th.start()
                return q, stop_evt

            # 2) Arrancar (o recuperar) el listener para el canal de la organización
            chan = "org_events_" + re.sub(r"[^a-zA-Z0-9_]", "_", str(ctx.org_id))
            q, stop_evt = get_pg_listener(repo.DB_URL, chan)

            # 3) Drenar la cola SIN bloquear y repintar sólo el contenedor si hubo eventos
            drained = False
            # Evita bucles largos: sólo vaciamos lo que haya ya encolado
            for _ in range(256):  # límite razonable por tick
                try:
                    msg = q.get_nowait()
                    # si quieres depurar: st.sidebar.write("notify:", msg[:120])
                    drained = True
                except queue.Empty:
                    break

            if drained:
                # Repinta SOLO la “isla” dinámica (sin rerun)
                dyn.empty()
                render_views_in()

            # 4) (Opcional) botón para escuchar intensivamente durante N segundos (modo puntual)
            with st.sidebar.expander("Modo escucha puntual", expanded=False):
                secs = st.number_input("Segundos", min_value=5, max_value=120, value=20, step=5)
                if st.button("👂 Escuchar ahora"):
                    import time
                    deadline = time.time() + float(secs)
                    while time.time() < deadline:
                        try:
                            # Espera hasta 1s cada vez; repinta sólo cuando llega algo
                            msg = q.get(timeout=1.0)
                            dyn.empty()
                            render_views_in()
                        except queue.Empty:
                            pass

if __name__ == "__main__":
    main()
