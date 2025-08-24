import pandas as pd
import altair as alt
import streamlit as st

def _ensure_cols(df: pd.DataFrame, cols: list[str], fill=0):
    """Asegura que las columnas existan para graficar."""
    for c in cols:
        if c not in df.columns:
            df[c] = fill
    return df

def category_impact_chart(agg_cat: pd.DataFrame):
    """
    Espera columnas:
      - category
      - inv_antes
      - inv_post
      - (opcional) inv_post_ordenes
    Convierte a formato largo (Estado, Unidades) para evitar transform_fold.
    """
    if agg_cat is None or agg_cat.empty:
        st.info("Sin datos para el gráfico por categoría.")
        return

    value_cols = ["inv_antes", "inv_post"]
    if "inv_post_ordenes" in agg_cat.columns:
        value_cols.append("inv_post_ordenes")

    agg_cat = _ensure_cols(agg_cat.copy(), ["category"] + value_cols)
    long_df = pd.melt(
        agg_cat,
        id_vars=["category"],
        value_vars=value_cols,
        var_name="Estado",
        value_name="Unidades",
    )
    # Asegurar tipos
    long_df["Estado"] = long_df["Estado"].astype(str)
    long_df["Unidades"] = pd.to_numeric(long_df["Unidades"], errors="coerce").fillna(0)

    chart = (
        alt.Chart(long_df)
        .mark_bar()
        .encode(
            x=alt.X("category:N", title="Categoría"),
            y=alt.Y("Unidades:Q", title="Unidades"),
            color=alt.Color("Estado:N", title="Estado"),
            tooltip=["category:N", "Estado:N", "Unidades:Q"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)

def category_dashboard_chart(by_cat: pd.DataFrame):
    """
    Espera columnas:
      - category
      - riesgo_quiebre
      - sobrestock
      - normal
    Convierte a formato largo (Tipo, Casos) para evitar transform_fold.
    """
    if by_cat is None or by_cat.empty:
        st.info("Sin datos para el dashboard por categoría.")
        return

    value_cols = ["riesgo_quiebre", "sobrestock", "normal"]
    by_cat = _ensure_cols(by_cat.copy(), ["category"] + value_cols)
    long_df = pd.melt(
        by_cat,
        id_vars=["category"],
        value_vars=value_cols,
        var_name="Tipo",
        value_name="Casos",
    )
    long_df["Tipo"] = long_df["Tipo"].astype(str)
    long_df["Casos"] = pd.to_numeric(long_df["Casos"], errors="coerce").fillna(0)

    chart = (
        alt.Chart(long_df)
        .mark_bar()
        .encode(
            x=alt.X("category:N", title="Categoría"),
            y=alt.Y("Casos:Q"),
            color=alt.Color("Tipo:N", title="Estado"),
            tooltip=["category:N", "Tipo:N", "Casos:Q"],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)
