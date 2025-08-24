from __future__ import annotations
import os
from pathlib import Path
import pandas as pd

def _append_csv(df: pd.DataFrame, path: Path) -> Path:
    """
    Append seguro que escribe encabezado si el archivo no existe o está vacío.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = True
    mode = "w"
    if path.exists():
        try:
            if os.path.getsize(path) > 0:
                write_header = False
                mode = "a"
        except Exception:
            pass
    df.to_csv(path, index=False, header=write_header, mode=mode)
    return path

def write_orders_csv(df: pd.DataFrame, path: Path) -> Path:
    # Normalizar columnas mínimas
    for col in ["org_id", "store_id", "sku_id", "qty"]:
        if col not in df.columns:
            df[col] = None
    # Reorden sugerido
    cols = ["org_id", "store_id", "sku_id", "qty", "actor", "ts_iso"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    return _append_csv(df, path)

def write_transfers_csv(df: pd.DataFrame, path: Path) -> Path:
    for col in ["org_id", "from_store", "to_store", "sku_id", "qty"]:
        if col not in df.columns:
            df[col] = None
    cols = ["org_id", "from_store", "to_store", "sku_id", "qty", "actor", "ts_iso"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    return _append_csv(df, path)

def log_notifications(records: list[dict], path: Path) -> Path:
    df = pd.DataFrame(records)
    # columnas canónicas (rellenar ausentes)
    cols = [
        "kind", "org_id", "actor", "ts_iso",
        "store_id", "from_store", "to_store", "sku_id", "qty", "message"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    return _append_csv(df, path)
