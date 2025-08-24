import numpy as np
import pandas as pd

def compute_future_state(inv_snapshot: pd.DataFrame,
                         orders_c: pd.DataFrame,
                         transfers_c: pd.DataFrame,
                         include_orders_in_future: bool = True) -> pd.DataFrame:
    """
    Proyecta inventario aplicando transferencias (siempre) y órdenes (opcional).
    Devuelve columnas:
      - on_hand_before
      - on_hand_after_transfers
      - on_hand_after_orders (si aplica)
      - delta_on_hand (contra on_hand_before)
    """
    df = inv_snapshot.copy()
    df = df[["date", "store_id", "sku_id", "on_hand_units"]].rename(columns={"on_hand_units": "on_hand_before"})

    # transferencias
    if transfers_c is not None and not transfers_c.empty:
        for row in transfers_c.itertuples(index=False):
            try:
                sku = row.sku_id
                frm = row.from_store
                to = row.to_store
                qty = int(row.qty)
            except Exception:
                continue
            mask_from = (df["store_id"] == frm) & (df["sku_id"] == sku)
            df.loc[mask_from, "on_hand_before"] = df.loc[mask_from, "on_hand_before"].sub(qty, fill_value=0).clip(lower=0)
            mask_to = (df["store_id"] == to) & (df["sku_id"] == sku)
            df.loc[mask_to, "on_hand_before"] = df.loc[mask_to, "on_hand_before"].add(qty, fill_value=0)

    df["on_hand_after_transfers"] = df["on_hand_before"]

    # órdenes
    if include_orders_in_future and orders_c is not None and not orders_c.empty:
        for row in orders_c.itertuples(index=False):
            try:
                store = row.store_id
                sku = row.sku_id
                qty = int(getattr(row, "qty", getattr(row, "suggested_order_qty", 0)))
            except Exception:
                continue
            mask = (df["store_id"] == store) & (df["sku_id"] == sku)
            df.loc[mask, "on_hand_after_transfers"] = df.loc[mask, "on_hand_after_transfers"].add(qty, fill_value=0)
        df["on_hand_after_orders"] = df["on_hand_after_transfers"]

    # delta vs. estado original
    df["delta_on_hand"] = df["on_hand_after_transfers"] - df["on_hand_before"]
    return df

def enrich_with_future_metrics(future_df: pd.DataFrame, recent: pd.DataFrame, lt: pd.DataFrame) -> pd.DataFrame:
    avg = (
        recent.groupby(["store_id", "sku_id"])["units_sold"]
        .mean()
        .reset_index()
        .rename(columns={"units_sold": "avg_daily_sales_28d"})
    )
    out = future_df.merge(avg, on=["store_id", "sku_id"], how="left").merge(lt, on=["store_id", "sku_id"], how="left")
    inv_col = "on_hand_after_orders" if "on_hand_after_orders" in out.columns else "on_hand_after_transfers"
    out["days_of_cover_future"] = np.where(
        out["avg_daily_sales_28d"] > 0,
        out[inv_col] / out["avg_daily_sales_28d"],
        np.inf,
    )
    out["risk_future"] = np.select(
        [
            out["avg_daily_sales_28d"] == 0,
            out["days_of_cover_future"] < out["lead_time_mean_days"],
            out["days_of_cover_future"] > 45,
        ],
        ["Baja demanda", "Riesgo de quiebre", "Sobrestock"],
        default="Normal",
    )
    return out

def summarize_impact(before_enriched: pd.DataFrame, after_enriched: pd.DataFrame) -> dict:
    def risk_counts(df, col):
        c = df[col].value_counts()
        return {f"{col}_{k}": int(v) for k, v in c.items()}

    before = before_enriched.rename(columns={"risk": "risk_before"})
    after  = after_enriched.rename(columns={"risk_future": "risk_after"})

    kpis = {}
    kpis.update(risk_counts(before, "risk_before"))
    kpis.update(risk_counts(after,  "risk_after"))

    for cat in ["Riesgo de quiebre", "Sobrestock", "Baja demanda", "Normal"]:
        b = kpis.get(f"risk_before_{cat}", 0)
        a = kpis.get(f"risk_after_{cat}", 0)
        kpis[f"Δ_{cat}"] = a - b

    return kpis
