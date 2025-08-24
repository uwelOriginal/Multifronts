from __future__ import annotations
import pandas as pd
from typing import Dict, Tuple

def make_store_labels(stores_df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Mapea store_id -> "Snn — Nombre" y su inverso.
    Espera columnas: store_id, store_code, store_name.
    """
    if "store_code" in stores_df.columns and "store_name" in stores_df.columns:
        labels = stores_df.apply(lambda r: f"{r['store_code']} — {r['store_name']}", axis=1)
    else:
        labels = stores_df.apply(lambda r: str(r.get("store_name", r["store_id"])), axis=1)
    id_to_label = dict(zip(stores_df["store_id"], labels))
    label_to_id = {v: k for k, v in id_to_label.items()}
    return id_to_label, label_to_id

def attach_store_label(df: pd.DataFrame, stores_df: pd.DataFrame, label_col: str = "Sucursal") -> pd.DataFrame:
    """Agrega columna humana 'Sucursal' a un DF que tiene 'store_id'."""
    if "store_id" not in df.columns or df.empty:
        return df
    id_to_label, _ = make_store_labels(stores_df)
    out = df.copy()
    out[label_col] = out["store_id"].map(id_to_label)
    return out
