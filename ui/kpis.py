import streamlit as st

def kpi_cards(kpis: dict, mode: str):
    if mode == "Simplificado":
        col1, col2, col3 = st.columns(3)
        col1.metric("Última fecha", kpis["last_date"])
        col2.metric("Unidades (28 días)", f"{kpis['total_units_28d']:,}")
        col3.metric("Promedio diario", f"{kpis['avg_daily_units']:,}")
    else:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Última fecha", kpis["last_date"])
        col2.metric("Unidades (28 días)", f"{kpis['total_units_28d']:,}")
        col3.metric("Promedio diario", f"{kpis['avg_daily_units']:,}")
        col4.metric("SKU–Tienda activos", f"{kpis['sku_store_pairs']:,}")
