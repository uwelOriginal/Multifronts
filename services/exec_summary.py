import os
from datetime import datetime
import pandas as pd

def _deterministic_summary(df: pd.DataFrame, skus: pd.DataFrame) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    merged = df.merge(skus[["sku_id", "category"]], on="sku_id", how="left")
    by_cat = merged.groupby("category").agg(
        skus=("sku_id", "nunique"),
        riesgo_quiebre=("risk", lambda s: (s == "Riesgo de quiebre").sum()),
        sobrestock=("risk", lambda s: (s == "Sobrestock").sum()),
        baja=("risk", lambda s: (s == "Baja demanda").sum()),
        normal=("risk", lambda s: (s == "Normal").sum()),
        inv_total=("on_hand_units", "sum"),
        ventas_d=("avg_daily_sales_28d", "sum"),
    ).reset_index().sort_values("riesgo_quiebre", ascending=False)

    top_sku = (
        merged[merged["risk"] == "Riesgo de quiebre"]
        .groupby(["sku_id", "category"])
        .size()
        .reset_index(name="sucursales_en_riesgo")
        .sort_values("sucursales_en_riesgo", ascending=False)
        .head(10)
    )

    lines = ["# Resumen Ejecutivo (determinístico)"]
    total_skus = int(merged["sku_id"].nunique())
    total_pairs = int(merged[["sku_id", "store_id"]].drop_duplicates().shape[0])
    risk_counts = merged["risk"].value_counts().to_dict()

    lines.append(f"- Fecha generación: {now}")
    lines.append(f"- Cobertura: {total_skus} SKUs | {total_pairs} combinaciones SKU–Sucursal")
    lines.append(f"- Riesgo de quiebre: {risk_counts.get('Riesgo de quiebre', 0)} | "
                 f"Sobrestock: {risk_counts.get('Sobrestock', 0)} | "
                 f"Baja demanda: {risk_counts.get('Baja demanda', 0)} | "
                 f"Normal: {risk_counts.get('Normal', 0)}")
    lines.append("\n## Categorías con más riesgo de quiebre")
    for r in by_cat.head(5).itertuples():
        lines.append(f"- {r.category}: {int(r.riesgo_quiebre)} casos, inv total {int(r.inv_total)} uds")

    if not top_sku.empty:
        lines.append("\n## Top SKUs en riesgo (por # de sucursales)")
        for r in top_sku.itertuples():
            lines.append(f"- {r.sku_id} ({r.category}): {int(r.sucursales_en_riesgo)} sucursales")

    lines.append("\n## Recomendaciones")
    lines.append("- Acelerar reposiciones en categorías líderes de riesgo; revisar lead time.")
    lines.append("- Rebalancear sobrestock hacia tiendas con riesgo de quiebre (transferencias).")
    lines.append("- Ajustar S (order-up-to) para SKUs con alta varianza de lead time.")
    lines.append("\n*Generado automáticamente.*")
    return "\n".join(lines)

def gen_exec_summary_text(enriched: pd.DataFrame, skus: pd.DataFrame, use_llm: bool = True) -> str:
    """
    Si OPENAI_API_KEY está presente y use_llm=True, intenta GPT-4o-mini.
    Si no, genera un resumen determinista.
    """
    if not (use_llm and os.getenv("OPENAI_API_KEY")):
        return _deterministic_summary(enriched, skus)

    try:
        import openai  # pip install openai
        client = openai.OpenAI()
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        merged = enriched.merge(skus[["sku_id", "category"]], on="sku_id", how="left")
        by_cat = merged.groupby("category").agg(
            skus=("sku_id", "nunique"),
            riesgo_quiebre=("risk", lambda s: (s == "Riesgo de quiebre").sum()),
            sobrestock=("risk", lambda s: (s == "Sobrestock").sum()),
            baja=("risk", lambda s: (s == "Baja demanda").sum()),
            normal=("risk", lambda s: (s == "Normal").sum()),
            inv_total=("on_hand_units", "sum"),
            ventas_d=("avg_daily_sales_28d", "sum"),
        ).reset_index().sort_values("riesgo_quiebre", ascending=False)
        top_sku = (
            merged[merged["risk"] == "Riesgo de quiebre"]
            .groupby(["sku_id", "category"])
            .size()
            .reset_index(name="sucursales_en_riesgo")
            .sort_values("sucursales_en_riesgo", ascending=False)
            .head(10)
        )

        prompt = f"""
Genera un resumen ejecutivo, conciso y accionable, del estado de inventario multi-sucursal.
Fecha generación: {now}.
Incluye: 1) Hallazgos clave, 2) Categorías críticas, 3) Top SKUs en riesgo, 4) Recomendaciones, 5) Próximos pasos.
Usa viñetas. Evita jerga técnica. Máx ~250 palabras.

[Categorías]
{by_cat.to_csv(index=False)}

[Top SKU riesgo]
{top_sku.to_csv(index=False)}
"""
        chat = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Eres un analista de retail muy conciso y accionable."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        text = chat.choices[0].message.content.strip()
        return "# Resumen Ejecutivo (LLM)\n" + text
    except Exception:
        return _deterministic_summary(enriched, skus)
