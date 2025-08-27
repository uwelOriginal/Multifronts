# views/operation.py
from __future__ import annotations
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import streamlit as st

from core.context import AppContext, FilterState
from views.base import BaseView
from utils.labels import attach_store_label
from core.headers import nice_headers

# Dominio / servicios
from features.risk import risk_table
from inventory import enrich_with_rop, suggest_order_for_row
from features.selection import render_selectable_editor, selection_to_dataframe
from services.exec_summary import gen_exec_summary_text
from services.slack_notify import send_slack_notifications
from services.auth import load_account_tables, resolve_org_webhook_oauth_first
from services.guardrails import (
    enforce_orders_scope, enforce_transfers_scope, filter_distances_to_scope
)
from optimizer import suggest_transfers

# Persistencia BD y eventos
from services import repo
from services.client_events import publish_event

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class OperationView(BaseView):
    """Operación diaria: riesgos, top, pedidos, transferencias y detalle SKU–Sucursal."""

    def __init__(self, ctx: AppContext, filters: FilterState, mode: str = "Técnico"):
        super().__init__(ctx, filters)
        self.mode = mode

    # ----- Secciones -----
    def _risks_by_store(self, enriched: pd.DataFrame):
        st.subheader("Riesgos por Sucursal")
        agg_store = pd.DataFrame(columns=["Sucursal", "Riesgo de quiebre", "Sobrestock", "Baja demanda", "Normal"])
        if not enriched.empty:
            tmp = attach_store_label(enriched, self.ctx.stores, label_col="Sucursal")
            agg_store = tmp.groupby("Sucursal").apply(
                lambda df: pd.Series({
                    "Riesgo de quiebre": (df["risk"] == "Riesgo de quiebre").sum(),
                    "Sobrestock": (df["risk"] == "Sobrestock").sum(),
                    "Baja demanda": (df["risk"] == "Baja demanda").sum(),
                    "Normal": (df["risk"] == "Normal").sum(),
                })
            ).reset_index()
        st.dataframe(agg_store, use_container_width=True, hide_index=True, height=240)

    def _top_risks(self, enriched: pd.DataFrame):
        if self.mode != "Técnico":
            return
        st.subheader("Top riesgos (Riesgo de quiebre)")
        top_risk = enriched[enriched["risk"] == "Riesgo de quiebre"].copy()
        if top_risk.empty:
            st.info("Sin riesgos de quiebre bajo los filtros seleccionados.")
            return
        top_risk["doc"] = np.round(top_risk["days_of_cover"], 1)
        top_risk = top_risk.sort_values(["doc"]).head(50)
        top_risk_disp = attach_store_label(top_risk, self.ctx.stores, label_col="Sucursal")
        cols_to_show = [
            "Sucursal", "sku_id", "on_hand_units", "avg_daily_sales_28d",
            "lead_time_mean_days", "doc", "risk", "ROP", "S_level", "suggested_order_qty"
        ]
        st.dataframe(nice_headers(top_risk_disp[cols_to_show]), use_container_width=True, hide_index=True, height=360)

    def _orders(self, enriched: pd.DataFrame):
        orders = enriched[(enriched["suggested_order_qty"] > 0)].copy()
        st.subheader(f"Pedidos sugeridos (on-hand < RDP) — {len(orders)}")
        if orders.empty:
            st.info("No hay pedidos sugeridos bajo los filtros.")
            return
        orders_disp = attach_store_label(orders, self.ctx.stores, label_col="Sucursal")

        with st.form("orders_form"):
            selected_order_ids = render_selectable_editor(
                df=orders_disp,
                id_cols=["store_id", "sku_id"],
                display_cols=["Sucursal", "sku_id", "on_hand_units", "ROP", "S_level", "suggested_order_qty"],
                key="orders_tbl",
                approve_label="Aprobar",
                height_px=360,
                rename_func=nice_headers,
            )
            approve_orders = st.form_submit_button("✓ Aprobar pedidos seleccionados")

        if approve_orders:
            chosen = selection_to_dataframe(orders, selected_order_ids, ["store_id", "sku_id"])[
                ["store_id", "sku_id", "on_hand_units", "ROP", "S_level", "suggested_order_qty"]
            ].copy()
            if chosen.empty:
                st.info("No quedó ningún pedido seleccionado.")
                return

            out = chosen.rename(columns={"suggested_order_qty": "qty"})
            out_valid, out_block = enforce_orders_scope(out, self.ctx.allowed_stores, self.ctx.allowed_skus)
            if not out_block.empty:
                st.warning(f"Se bloquearon {len(out_block)} pedido(s) por reglas de organización.")
            if out_valid.empty:
                st.info("No quedó ningún pedido válido para aprobar.")
                return

            out_valid = out_valid.copy()
            out_valid["org_id"] = self.ctx.org_id
            out_valid["actor"] = self.ctx.actor_email
            out_valid["ts_iso"] = _now_utc_iso()

            rows_db = out_valid[["store_id", "sku_id", "qty"]].to_dict(orient="records")
            idem_prefix = f"{self.ctx.org_id}:{self.ctx.actor_email}:{int(time.time())}"
            nuevos, duplicados = repo.save_orders(
                org_id=self.ctx.org_id,
                rows=rows_db,
                approved_by=self.ctx.actor_email,
                idem_prefix=idem_prefix,
            )

            notif_now = out_valid.copy()
            notif_now.insert(0, "kind", "order")

            # Slack: primero intenta OAuth en backend, luego secrets locales
            webhook = resolve_org_webhook_oauth_first(
                load_account_tables(self.ctx.DATA_DIR)[1],  # accounts_df
                self.ctx.org_id
            )
            if webhook:
                ok, msg = send_slack_notifications(notif_now, webhook)
                st.toast(msg, icon="✅" if ok else "⚠️")
            else:
                st.toast("No hay Slack webhook configurado (org o env).", icon="⚠️")

            # Evento al backend (Render)
            try:
                publish_event(
                    org_id=self.ctx.org_id,
                    type_="orders_approved",
                    payload={
                        "approved_by": self.ctx.actor_email,
                        "count_new": int(nuevos),
                        "count_dup": int(duplicados),
                        "rows": rows_db,
                    },
                    timeout=3.0,
                )
            except Exception as e:
                st.warning(f"No se pudo publicar evento en backend: {e}")

            st.session_state["movements_this_session"] = True
            st.success(f"Órdenes guardadas en BD: {nuevos} nuevas, {duplicados} duplicadas (omitidas).")

    def _transfers(self, enriched: pd.DataFrame):
        distances_scoped = filter_distances_to_scope(self.ctx.distances, self.ctx.allowed_stores) \
                           if self.ctx.distances is not None else None
        transfers = suggest_transfers(
            enriched=enriched,
            distances=distances_scoped,
            max_per_sku=20,
            allowed_stores=self.ctx.allowed_stores,
            allowed_skus=self.ctx.allowed_skus,
        )
        st.subheader(f"Transferencias sugeridas — {0 if transfers is None else len(transfers)}")
        if transfers is None or transfers.empty:
            st.info("No hay transferencias sugeridas.")
            return
        transfers_disp = transfers.copy()
        transfers_disp["De"] = transfers_disp["from_store"].map(self.ctx.id_to_label)
        transfers_disp["A"]  = transfers_disp["to_store"].map(self.ctx.id_to_label)

        with st.form("transfers_form"):
            selected_transfer_ids = render_selectable_editor(
                df=transfers_disp,
                id_cols=["sku_id", "from_store", "to_store"],
                display_cols=["sku_id", "De", "A", "qty", "distance_km"] + (["cost_est"] if "cost_est" in transfers_disp.columns else []),
                key="transfers_tbl",
                approve_label="Aprobar",
                height_px=360,
                rename_func=nice_headers,
            )
            approve_transfers = st.form_submit_button("✓ Aprobar transferencias seleccionadas")

        if approve_transfers:
            chosen_t = selection_to_dataframe(transfers, selected_transfer_ids, ["sku_id", "from_store", "to_store"])
            if chosen_t.empty:
                st.info("No quedó ninguna transferencia seleccionada.")
                return

            valid_t, blocked_t = enforce_transfers_scope(chosen_t, self.ctx.allowed_stores, self.ctx.allowed_skus)
            if not blocked_t.empty:
                st.warning(f"Se bloquearon {len(blocked_t)} transferencia(s) por reglas de organización.")
            if valid_t.empty:
                st.info("No quedó ninguna transferencia válida para aprobar.")
                return

            out_t = valid_t.copy()
            out_t["org_id"] = self.ctx.org_id
            out_t["actor"] = self.ctx.actor_email
            out_t["ts_iso"] = _now_utc_iso()

            rows_db_t = out_t[["from_store", "to_store", "sku_id", "qty"]].to_dict(orient="records")
            idem_prefix = f"{self.ctx.org_id}:{self.ctx.actor_email}:{int(time.time())}"
            aplicadas, duplicadas, insuficientes = repo.save_transfers(
                org_id=self.ctx.org_id,
                rows=rows_db_t,
                approved_by=self.ctx.actor_email,
                idem_prefix=idem_prefix,
            )

            notif_now = out_t.copy()
            notif_now.insert(0, "kind", "transfer")

            webhook = resolve_org_webhook_oauth_first(
                load_account_tables(self.ctx.DATA_DIR)[1],
                self.ctx.org_id
            )
            if webhook:
                ok, msg = send_slack_notifications(notif_now, webhook)
                st.toast(msg, icon="✅" if ok else "⚠️")
            else:
                st.toast("No hay Slack webhook configurado (org o env).", icon="⚠️")

            try:
                publish_event(
                    org_id=self.ctx.org_id,
                    type_="transfers_approved",
                    payload={
                        "approved_by": self.ctx.actor_email,
                        "count_applied": int(aplicadas),
                        "count_dup": int(duplicadas),
                        "count_insufficient": int(insuficientes),
                        "rows": rows_db_t,
                    },
                    timeout=3.0,
                )
            except Exception as e:
                st.warning(f"No se pudo publicar evento en backend: {e}")

            st.session_state["movements_this_session"] = True
            st.success(f"Transferencias aplicadas: {aplicadas} | duplicadas: {duplicadas} | sin stock: {insuficientes}")

    def _detail(self, enriched: pd.DataFrame):
        if self.mode != "Técnico":
            return
        st.subheader("Detalle SKU–Sucursal")
        with st.form("detalle"):
            c1, c2 = st.columns(2)
            sku_opts = sorted(enriched["sku_id"].unique().tolist()) if not enriched.empty else []
            store_opts_labels = sorted(self.ctx.id_to_label[s] for s in enriched["store_id"].unique()) if not enriched.empty else []
            sku_pick = c1.selectbox("SKU", sku_opts)
            store_label_pick = c2.selectbox("Sucursal", store_opts_labels)
            submitted = st.form_submit_button("Ver detalle", disabled=enriched.empty)
        if not submitted:
            return
        store_pick = self.ctx.label_to_id[store_label_pick]
        sel = enriched[(enriched["sku_id"] == sku_pick) & (enriched["store_id"] == store_pick)]
        if sel.empty:
            st.warning("⚠️ No hay datos disponibles para esta combinación de SKU y sucursal.")
            return
        row = sel.iloc[0].to_dict()
        st.write(f"**Sucursal:** {self.ctx.id_to_label.get(row['store_id'], row['store_id'])}")
        st.write(f"**Inventario**: {int(row['on_hand_units'])} uds")
        st.write(f"**Venta diaria (28d)**: {row['avg_daily_sales_28d']:.2f} uds")
        st.write(f"**Lead time**: {row['lead_time_mean_days']:.1f} ± {row['lead_time_std_days']:.1f} días")
        st.write(f"**Cobertura**: {row['days_of_cover']:.1f} d — **Riesgo**: {row['risk']}")
        st.write(f"**RDP/ROP**: {row['ROP']:.1f}  |  **S**: {row['S_level']:.1f}")
        order = suggest_order_for_row(
            row,
            service_level=self.f.service_level,
            order_up_factor=self.f.order_up_factor,
        )
        if order and order.get("qty", 0) > 0:
            st.success(f"📦 Pedido sugerido: {order['qty']} uds (hasta S {order['S']:.1f})")
            st.caption(order.get("explanation", ""))
        else:
            st.info(order.get("explanation", "No se requiere pedido.") if order else "No se requiere pedido.")
        with st.expander("Fórmulas con valores para este SKU–tienda"):
            st.latex(r"\mu_{LT} = \bar{d}\cdot LT_{\mathrm{mean}}")
            st.latex(r"\sigma_{LT} = \bar{d}\cdot LT_{\mathrm{std}}")
            st.latex(r"\mathrm{ROP} = \mu_{LT} + z\cdot \sigma_{LT}")
            st.latex(r"S = \mathrm{ROP} + k\cdot \mu_{LT}")
            if order and "latex" in order and "values" in order["latex"]:
                st.latex(order["latex"]["values"])

    def render(self):
        # 1) Filtros
        from ui.filters import FilterPanel
        fp = FilterPanel(self.ctx)
        sales_f, inv_f, lt_f, allowed_skus_after_filter = fp.apply_to(
            df_sales=self.ctx.recent,
            df_inv=self.ctx.inv,
            df_lt=self.ctx.lt,
            df_skus=self.ctx.skus,
            f=self.f
        )
        if not allowed_skus_after_filter:
            st.warning("⚠️ No hay SKUs después de aplicar filtros de categoría/ABC.")
            return

        # 2) Sustituir inventario por estado vivo en BD si existe (menor latencia + consistencia)
        try:
            db_inv = repo.fetch_inventory_levels(
                org_id=self.ctx.org_id,
                store_ids=list(self.ctx.allowed_stores),
                sku_ids=list(allowed_skus_after_filter)
            )
            if not db_inv.empty:
                inv_f = inv_f.drop(columns=["on_hand_units"], errors="ignore").merge(
                    db_inv, on=["store_id", "sku_id"], how="left"
                )
                inv_f["on_hand_units"] = inv_f["on_hand_units"].fillna(0)
        except Exception as e:
            st.info(f"(info) Inventario vivo no disponible: {e}")

        # 3) Métrica base y ROP/S
        base = risk_table(sales_f, inv_f, lt_f)
        enriched = enrich_with_rop(base, service_level=self.f.service_level, order_up_factor=self.f.order_up_factor)

        # 4) Secciones
        self._risks_by_store(enriched)
        self._top_risks(enriched)

        col1, col2 = st.columns(2)
        with col1:
            self._orders(enriched)
        with col2:
            self._transfers(enriched)

        self._detail(enriched)
