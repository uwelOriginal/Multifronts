from __future__ import annotations
import streamlit as st
import pandas as pd
from typing import Tuple
from core.context import AppContext, FilterState

def _default_kw_for(key: str, **kwargs):
    """Devuelve kwargs (p.ej., default=..., value=...) solo si la key aún no existe."""
    return {} if key in st.session_state else kwargs

class FilterPanel:
    def __init__(self, ctx: AppContext, key_prefix: str = "hdr_"):
        self.ctx = ctx
        self.k = key_prefix

    def _defaults(self):
        stores_all = sorted(self.ctx.id_to_label.values())
        cats_all = sorted(self.ctx.skus["category"].unique().tolist()) if not self.ctx.skus.empty else []
        return {
            "stores": stores_all,
            "cats": cats_all,
            "abc_a": True,
            "abc_b": True,
            "abc_c": True,
            "service": 0.95,
            "s_factor": 1.0,
        }

    def _ensure_defaults(self) -> None:
        d = self._defaults()
        for name, val in d.items():
            st.session_state.setdefault(f"{self.k}{name}", val)

    def _clear_query_param_reset(self):
        try:
            if "reset" in st.query_params:
                qp = dict(st.query_params)
                qp.pop("reset", None)
                try:
                    st.query_params.clear()
                    for k, v in qp.items():
                        st.query_params[k] = v
                except Exception:
                    st.experimental_set_query_params(**qp)
        except Exception:
            pass

    def _reset_now(self):
        for name in ["stores","cats","abc_a","abc_b","abc_c","service","s_factor"]:
            st.session_state.pop(f"{self.k}{name}", None)
        self._clear_query_param_reset()
        st.rerun()

    def _check_reset_param(self):
        try:
            if st.query_params.get("reset") == "1":
                self._reset_now()
        except Exception:
            pass

    def render(self, mode: str, default_expand: bool = True) -> FilterState:
        # 1) Reset por query param antes de instanciar widgets
        self._check_reset_param()
        self._ensure_defaults()
        defaults = self._defaults()

        # Opciones vigentes (etiquetas)
        stores_opts = sorted(self.ctx.id_to_label.values())
        cats_opts = sorted(self.ctx.skus["category"].unique().tolist()) if not self.ctx.skus.empty else []

        # === Estado aplicado scopeado por organización (evita heredar de otra org) ===
        APPLIED_KEY_OLD = f"{self.k}applied"
        APPLIED_KEY = f"{self.k}applied:{self.ctx.org_id}"

        # Migración suave: si existe la clave vieja y no la nueva, copia y elimina la vieja
        if APPLIED_KEY not in st.session_state and APPLIED_KEY_OLD in st.session_state:
            st.session_state[APPLIED_KEY] = st.session_state.get(APPLIED_KEY_OLD, {}).copy()
            try:
                del st.session_state[APPLIED_KEY_OLD]
            except Exception:
                pass

        # Inicializa estado aplicado si no existe
        if APPLIED_KEY not in st.session_state:
            st.session_state[APPLIED_KEY] = {
                "stores": list(defaults["stores"]),
                "cats": list(defaults["cats"]),
                "abc": ["A", "B", "C"],
                "service": float(defaults["service"]),
                "s_factor": float(defaults["s_factor"]),
            }

        # Normaliza el estado aplicado contra las opciones ACTUALES (muy importante)
        applied = st.session_state[APPLIED_KEY]
        applied["stores"] = [lbl for lbl in (applied.get("stores") or []) if lbl in stores_opts] or stores_opts
        applied["cats"]   = [c for c in (applied.get("cats") or []) if c in cats_opts] or cats_opts
        if not applied.get("abc"):
            applied["abc"] = ["A","B","C"]

        st.markdown('<span id="filters-start"></span>', unsafe_allow_html=True)
        with st.expander("Parámetros & Filtros", expanded=False):
            # -------- BORRADOR en FORM: no aplica hasta Submit --------
            with st.form("filters_form", clear_on_submit=False):
                c1, c2 = st.columns([1.6, 1.4])

                # Los defaults del formulario son el borrador actual
                draft_stores = st.session_state.get(f"{self.k}stores", applied["stores"])
                draft_cats   = st.session_state.get(f"{self.k}cats", applied["cats"])

                # Normaliza borrador con catálogo vigente
                draft_stores = [x for x in draft_stores if x in stores_opts] or stores_opts
                draft_cats   = [x for x in draft_cats   if x in cats_opts]   or cats_opts

                store_labels_sel = c1.multiselect(
                    "Sucursales", options=stores_opts, key=f"{self.k}stores",
                    **_default_kw_for(f"{self.k}stores", default=draft_stores),
                    help="Filtra tiendas visibles."
                )
                cat_sel = c2.multiselect(
                    "Categorías", options=cats_opts, key=f"{self.k}cats",
                    **_default_kw_for(f"{self.k}cats", default=draft_cats),
                    help="Familias de producto."
                )

                t1, t2, t3 = st.columns(3)
                abc_a = t1.toggle("A 🔴", key=f"{self.k}abc_a",
                                  **_default_kw_for(f"{self.k}abc_a", value=st.session_state.get(f"{self.k}abc_a", True)))
                abc_b = t2.toggle("B 🟠", key=f"{self.k}abc_b",
                                  **_default_kw_for(f"{self.k}abc_b", value=st.session_state.get(f"{self.k}abc_b", True)))
                abc_c = t3.toggle("C 🟡", key=f"{self.k}abc_c",
                                  **_default_kw_for(f"{self.k}abc_c", value=st.session_state.get(f"{self.k}abc_c", True)))
                abc_sel = [x for x, v in zip(["A","B","C"], [abc_a, abc_b, abc_c]) if v] or ["A","B","C"]

                with st.expander("⚙️ Controles avanzados", expanded=(mode == "Técnico")):
                    a1, a2 = st.columns(2)
                    service_level = a1.slider(
                        "Nivel de servicio (z implícito)", 0.80, 0.99, step=0.01,
                        key=f"{self.k}service",
                        **_default_kw_for(f"{self.k}service", value=float(st.session_state.get(f"{self.k}service", defaults["service"]))),
                    )
                    order_up_factor = a2.number_input(
                        "Factor S (× μ_LT)", min_value=0.1, max_value=3.0, step=0.1,
                        key=f"{self.k}s_factor",
                        **_default_kw_for(f"{self.k}s_factor", value=float(st.session_state.get(f"{self.k}s_factor", defaults["s_factor"]))),
                    )

                apply_btn = st.form_submit_button("Aplicar filtros", use_container_width=True)

            # -------- Al pulsar, “congelamos” en APPLIED y saneamos selección --------
            if apply_btn:
                sel_stores = [lbl for lbl in (store_labels_sel or []) if lbl in stores_opts] or stores_opts
                sel_cats   = [c for c in (cat_sel or []) if c in cats_opts] or cats_opts
                st.session_state[APPLIED_KEY] = {
                    "stores": sel_stores,
                    "cats": sel_cats,
                    "abc": abc_sel,
                    "service": float(service_level),
                    "s_factor": float(order_up_factor),
                }
                applied = st.session_state[APPLIED_KEY]  # refresca referencia

        # Construir FilterState desde estado APLICADO (mapear etiquetas → IDs con tolerancia)
        applied = st.session_state[APPLIED_KEY]
        # Vuelve a normalizar por si cambió el catálogo durante el form
        applied["stores"] = [lbl for lbl in (applied.get("stores") or []) if lbl in stores_opts] or stores_opts
        # Mapear a IDs de forma segura (evita KeyError)
        stores_ids = [self.ctx.label_to_id[lbl] for lbl in applied["stores"] if lbl in self.ctx.label_to_id]
        if not stores_ids:
            # Fallback: todas las opciones vigentes
            stores_ids = [self.ctx.label_to_id[lbl] for lbl in stores_opts if lbl in self.ctx.label_to_id]

        return FilterState(
            store_sel=stores_ids,
            cat_sel=applied["cats"],
            abc_sel=applied["abc"],
            service_level=float(applied["service"]),
            order_up_factor=float(applied["s_factor"]),
        )


    @staticmethod
    @st.cache_data(show_spinner=False, ttl=180)
    def _apply_filters_cached(
        _df_sales: pd.DataFrame,
        _df_inv: pd.DataFrame,
        _df_lt: pd.DataFrame,
        _df_skus: pd.DataFrame,   # se ignora en el filtrado, pero lo dejamos para simetría
        store_ids: tuple[str, ...],   # <-- SIN guion bajo (sí se hashea)
        sku_ids: tuple[str, ...],     # <-- SIN guion bajo (sí se hashea)
    ):
        """Filtra dataframes por tiendas/SKUs. Los parámetros con '_' no se hashean."""
        sales_f = _df_sales[
            (_df_sales["store_id"].isin(store_ids)) & (_df_sales["sku_id"].isin(sku_ids))
        ].copy()
        inv_f = _df_inv[
            (_df_inv["store_id"].isin(store_ids)) & (_df_inv["sku_id"].isin(sku_ids))
        ].copy()
        lt_f = _df_lt[
            (_df_lt["store_id"].isin(store_ids)) & (_df_lt["sku_id"].isin(sku_ids))
        ].copy()
        return sales_f, inv_f, lt_f

    def apply_to(self, df_sales, df_inv, df_lt, df_skus, f):
        # 1) SKUs permitidos por categoría/ABC (igual que antes)
        allowed = df_skus[
            (df_skus["category"].isin(f.cat_sel)) & (df_skus["abc_class"].isin(f.abc_sel))
        ]["sku_id"].tolist()

        # 2) Clave de cache: ids ordenados (tuplas inmutables)
        store_ids = tuple(sorted(f.store_sel))
        sku_ids   = tuple(sorted(allowed))

        # 3) Llamar al helper de módulo (¡sin self!)
        sales_f, inv_f, lt_f = self._apply_filters_cached(
            df_sales, df_inv, df_lt, df_skus, store_ids, sku_ids
        )
        return sales_f, inv_f, lt_f, allowed
