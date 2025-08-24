import numpy as np
import pandas as pd

def compute_baseline(sales: pd.DataFrame):
    last_date = sales["date"].max()
    start_28 = last_date - pd.Timedelta(days=28)
    recent = sales[sales["date"] >= start_28]
    kpis = {
        "last_date": last_date.date().isoformat(),
        "total_units_28d": int(recent["units_sold"].sum()),
        "avg_daily_units": float(np.round(recent.groupby("date")["units_sold"].sum().mean(), 2)),
        "sku_store_pairs": int(recent[["store_id", "sku_id"]].drop_duplicates().shape[0]),
    }
    return kpis, recent
