from __future__ import annotations
import streamlit as st
import pandas as pd
from typing import Tuple
from core.context import AppContext, FilterState

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

        # 2) Defaults sólo en session_state
        self._ensure_defaults()
        defaults = self._defaults()

        # 2.1) Normaliza estado EXISTENTE antes de crear widgets (seguro en Streamlit)
        store_key = f"{self.k}stores"
        cat_key   = f"{self.k}cats"

        # Normalizar sucursales: elimina labels inexistentes; si queda vacío, usa todas
        if isinstance(st.session_state.get(store_key, None), list):
            st.session_state[store_key] = [x for x in st.session_state[store_key] if x in defaults["stores"]]
            if not st.session_state[store_key]:
                st.session_state[store_key] = list(defaults["stores"])
        else:
            st.session_state[store_key] = list(defaults["stores"])

        # Normalizar categorías
        if isinstance(st.session_state.get(cat_key, None), list):
            st.session_state[cat_key] = [x for x in st.session_state[cat_key] if x in defaults["cats"]]
            # si no hay categorías disponibles, deja lista vacía; si hay, usa todas
            if defaults["cats"] and not st.session_state[cat_key]:
                st.session_state[cat_key] = list(defaults["cats"])
        else:
            st.session_state[cat_key] = list(defaults["cats"])

        st.markdown('<span id="filters-start"></span>', unsafe_allow_html=True)

        with st.expander("Parámetros & Filtros", expanded=False):
            # ---------- Fila 1: Sucursales y Categorías ----------
            c1, c2 = st.columns([1.6, 1.4])

            store_labels_sel = c1.multiselect(
                "Sucursales",
                options=defaults["stores"],
                help="Filtra tiendas visibles (etiquetas locales a tu organización, p.ej. S01 — CDMX).",
                key=store_key,
            )
            # Fallback local si el usuario deja vacío: aplica 'todas' SIN escribir session_state
            effective_store_labels = store_labels_sel or defaults["stores"]
            if not store_labels_sel and defaults["stores"]:
                c1.caption("Sin selección ⇒ **todas** las sucursales.")
            store_sel = [self.ctx.label_to_id[lbl] for lbl in effective_store_labels]

            cat_sel = c2.multiselect(
                "Categorías",
                options=defaults["cats"],
                help="Familias de producto (puedes combinarlas con ABC para priorizar).",
                key=cat_key,
            )
            effective_cats = cat_sel or defaults["cats"]
            if not cat_sel and defaults["cats"]:
                c2.caption("Sin selección ⇒ **todas** las categorías.")

            # ---------- Fila 2: ABC (toggles) ----------
            t1, t2, t3 = st.columns(3)
            abc_a = t1.toggle("A 🔴", key=f"{self.k}abc_a", help="Alta prioridad (alto impacto/rotación)")
            abc_b = t2.toggle("B 🟠", key=f"{self.k}abc_b", help="Prioridad media")
            abc_c = t3.toggle("C 🟡", key=f"{self.k}abc_c", help="Prioridad baja")

            abc_sel = [x for x, v in zip(["A","B","C"], [abc_a, abc_b, abc_c]) if v]
            if not abc_sel:
                abc_sel = ["A","B","C"]

            # ---------- Fila 3: controles avanzados ----------
            with st.expander("⚙️ Controles avanzados", expanded=(mode == "Técnico")):
                a1, a2 = st.columns(2)
                service_level = a1.slider(
                    "Nivel de servicio (z implícito)",
                    0.80, 0.99, step=0.01,
                    help="Objetivo de fill-rate; determina el factor z del stock de seguridad en ROP.",
                    key=f"{self.k}service",
                )
                order_up_factor = a2.number_input(
                    "Factor S (× μ_LT)",
                    min_value=0.1, max_value=3.0, step=0.1,
                    help="Multiplicador para el nivel S (hasta dónde reponer sobre la demanda del lead time).",
                    key=f"{self.k}s_factor",
                )

            st.caption(
                "ABC = prioridad por impacto/rotación: **A** alto, **B** medio, **C** bajo. "
                "Revisa primero A, luego B; C se monitorea con umbrales más amplios."
            )

        return FilterState(
            store_sel=store_sel,
            cat_sel=effective_cats,
            abc_sel=abc_sel,
            service_level=float(st.session_state[f"{self.k}service"]),
            order_up_factor=float(st.session_state[f"{self.k}s_factor"]),
        )

    def apply_to(self,
                 df_sales: pd.DataFrame,
                 df_inv: pd.DataFrame,
                 df_lt: pd.DataFrame,
                 df_skus: pd.DataFrame,
                 f: FilterState):
        allowed = df_skus[
            (df_skus["category"].isin(f.cat_sel)) &
            (df_skus["abc_class"].isin([x for x in ["A","B","C"] if x in f.abc_sel]))
        ]["sku_id"].tolist()
        sales_f = df_sales[(df_sales["store_id"].isin(f.store_sel)) & (df_sales["sku_id"].isin(allowed))]
        inv_f   = df_inv  [(df_inv["store_id"].isin(f.store_sel))   & (df_inv["sku_id"].isin(allowed))]
        lt_f    = df_lt   [(df_lt["store_id"].isin(f.store_sel))    & (df_lt["sku_id"].isin(allowed))]
        return sales_f, inv_f, lt_f, allowed
