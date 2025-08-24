import numpy as np
import pandas as pd

def risk_table(recent: pd.DataFrame, inv: pd.DataFrame, lt: pd.DataFrame):
    avg = (
        recent.groupby(["store_id", "sku_id"])["units_sold"]
        .mean()
        .reset_index()
        .rename(columns={"units_sold": "avg_daily_sales_28d"})
    )
    base = inv.merge(avg, on=["store_id", "sku_id"], how="left").fillna({"avg_daily_sales_28d": 0.0})
    base = base.merge(lt, on=["store_id", "sku_id"], how="left")
    base["days_of_cover"] = np.where(
        base["avg_daily_sales_28d"] > 0,
        base["on_hand_units"] / base["avg_daily_sales_28d"],
        np.inf,
    )
    base["risk"] = np.select(
        [
            base["avg_daily_sales_28d"] == 0,
            base["days_of_cover"] < base["lead_time_mean_days"],
            base["days_of_cover"] > 45,
        ],
        ["Baja demanda", "Riesgo de quiebre", "Sobrestock"],
        default="Normal",
    )
    return base

def validate_day2_rules(enriched_df: pd.DataFrame) -> dict:
    required_cols = ["avg_daily_sales_28d", "days_of_cover", "lead_time_mean_days", "risk"]
    status = {"ok": True, "checks": []}

    for c in required_cols:
        ok = c in enriched_df.columns
        status["checks"].append((f"Columna '{c}' presente", ok))
        status["ok"] &= ok

    if "risk" in enriched_df.columns:
        counts = enriched_df["risk"].value_counts(dropna=False)
        nonzero_categories = counts[counts > 0].index.tolist()
        ok = len(nonzero_categories) >= 2
        status["checks"].append(("Categorías de riesgo no vacías (≥2)", ok))
        status["ok"] &= ok

    return status
