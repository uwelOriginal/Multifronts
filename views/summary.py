from __future__ import annotations
import streamlit as st
import pandas as pd
from core.context import AppContext, FilterState
from views.base import BaseView
from utils.labels import attach_store_label
from core.headers import nice_headers

from features.risk import risk_table
from inventory import enrich_with_rop
from features.future import compute_future_state, enrich_with_future_metrics, summarize_impact
from ui.charts import category_impact_chart, category_dashboard_chart

class SummaryView(BaseView):
    """Resumen 4.5: proyección, comparativa, dashboards y export/write-back."""

    def render(self):
        st.title("Resumen 4.5 — Post-aprobación, Comparativa y Reportes")

        orders_c = self.ctx.orders_scoped
        transfers_c = self.ctx.transfers_scoped

        include_orders = st.checkbox("Incluir ÓRDENES en la proyección del estado futuro", value=True)

        future_df = compute_future_state(
            inv_snapshot=self.ctx.inv,
            orders_c=orders_c,
            transfers_c=transfers_c,
            include_orders_in_future=include_orders
        )
        fut_scope = future_df[
            future_df["store_id"].isin(self.ctx.allowed_stores) &
            future_df["sku_id"].isin(self.ctx.allowed_skus)
        ]

        # Enriquecidos “antes” y “después” con filtros actuales
        recent_scope = self.ctx.recent[
            (self.ctx.recent["store_id"].isin(self.f.store_sel))
        ]
        lt_scope = self.ctx.lt[
            (self.ctx.lt["store_id"].isin(self.f.store_sel))
        ]
        base_before = risk_table(
            recent_scope,
            self.ctx.inv[(self.ctx.inv["store_id"].isin(self.f.store_sel))],
            lt_scope,
        )
        before_enriched = enrich_with_rop(base_before, service_level=self.f.service_level, order_up_factor=self.f.order_up_factor)
        future_enriched = enrich_with_future_metrics(fut_scope, recent=recent_scope, lt=lt_scope)

        impact = summarize_impact(before_enriched=before_enriched, after_enriched=future_enriched)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Δ Riesgo de quiebre", impact.get("Δ_Riesgo de quiebre", 0))
        c2.metric("Δ Sobrestock",        impact.get("Δ_Sobrestock", 0))
        c3.metric("Δ Baja demanda",      impact.get("Δ_Baja demanda", 0))
        c4.metric("Δ Normal",            impact.get("Δ_Normal", 0))

        st.subheader("Comparativa Antes vs. Futuro (por SKU–Sucursal)")
        comp = future_enriched.merge(
            before_enriched[["store_id", "sku_id", "on_hand_units", "days_of_cover", "risk"]],
            on=["store_id", "sku_id"],
            how="left",
            suffixes=("_future_calc", "_before"),
        )
        comp_disp = attach_store_label(comp, self.ctx.stores, label_col="Sucursal")

        show_cols = ["Sucursal", "sku_id", "on_hand_units", "on_hand_after_transfers"]
        if include_orders and "on_hand_after_orders" in comp_disp.columns:
            show_cols.append("on_hand_after_orders")
        show_cols += ["days_of_cover", "risk", "days_of_cover_future", "risk_future", "delta_on_hand"]

        comp_view = nice_headers(
            comp_disp[show_cols].rename(
                columns={
                    "on_hand_units": "on_hand_before",
                    "risk_future": "Riesgo futuro",
                    "days_of_cover_future": "Cobertura futura (días)",
                    "days_of_cover": "Cobertura (días)",
                    "risk": "Riesgo",
                }
            )
        ).rename(
            columns={
                "on_hand_after_transfers": "Inventario (post-transfers)",
                "on_hand_after_orders":    "Inventario (post-órdenes)",
            }
        )
        st.dataframe(comp_view, use_container_width=True, hide_index=True, height=360)

        st.subheader("Impacto por Categoría (visual)")
        comp_cat = comp_disp.merge(self.ctx.skus[["sku_id", "category"]], on="sku_id", how="left")
        agg_cat = comp_cat.groupby("category").agg(
            inv_antes=("on_hand_units", "sum"),
            inv_post=("on_hand_after_transfers", "sum"),
        ).reset_index()
        if include_orders and "on_hand_after_orders" in comp_cat.columns:
            agg_cat["inv_post_ordenes"] = comp_cat["on_hand_after_orders"].groupby(comp_cat["category"]).sum().values
        category_impact_chart(agg_cat)

        st.subheader("Exportar estado futuro & Write-back")
        cbt1, cbt2 = st.columns(2)
        if cbt1.button("💾 Exportar estado futuro (CSV)"):
            out_path = (self.ctx.DATA_DIR / "future_state_inventory.csv")
            fut_scope.to_csv(out_path, index=False)
            st.success(f"Exportado a {out_path}")

        wb_include_orders = st.checkbox("Write-back incluyendo ÓRDENES (además de transferencias)", value=False)
        if cbt2.button("✍️ Aplicar write-back a inventory_snapshot.csv"):
            inv_new = self.ctx.inv.copy()
            use_col = "on_hand_after_orders" if wb_include_orders and "on_hand_after_orders" in fut_scope.columns else "on_hand_after_transfers"
            merged = inv_new.merge(fut_scope[["store_id", "sku_id", use_col]], on=["store_id", "sku_id"], how="left")
            merged["on_hand_units"] = merged[use_col].fillna(merged["on_hand_units"])
            if use_col in merged.columns:
                merged.drop(columns=[use_col], inplace=True)
            merged.to_csv(self.ctx.DATA_DIR / "inventory_snapshot.csv", index=False)
            st.success("Write-back aplicado (scope por organización).")
