import pandas as pd

RENAME_MAP = {
    "store_id": "Sucursal",
    "sku_id": "SKU",
    "on_hand_units": "Inventario",
    "avg_daily_sales_28d": "Venta diaria (28d)",
    "lead_time_mean_days": "Lead time (media)",
    "lead_time_std_days": "Lead time (σ)",
    "days_of_cover": "Cobertura (días)",
    "risk": "Riesgo",
    "ROP": "Punto de Reorden (RDP)",
    "S_level": "Nivel S (order-up-to)",
    "suggested_order_qty": "Pedido sugerido",
    "order_explanation": "Explicación",
    "doc": "Cobertura (días)",
    "distance_km": "Distancia (km)",
    "cost_est": "Costo estimado",
    "from_store": "De",
    "to_store": "A",
    "reason": "Razón",
    "delta_on_hand": "Δ Inventario",
}

def nice_headers(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={k: v for k, v in RENAME_MAP.items() if k in df.columns})
