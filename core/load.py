from __future__ import annotations
from pathlib import Path
import pandas as pd

# Directorio de datos (ajusta si fuera necesario)
DATA_DIR = Path("./data")

# Schemas esperados para archivos “vacíos”
SCHEMAS = {
    "orders_confirmed.csv": [
        "org_id", "store_id", "sku_id", "qty", "actor", "ts_iso"
    ],
    "transfers_confirmed.csv": [
        "org_id", "from_store", "to_store", "sku_id", "qty", "actor", "ts_iso"
    ],
    "notifications.csv": [
        "kind", "org_id", "actor", "ts_iso",
        "store_id", "from_store", "to_store", "sku_id", "qty", "message"
    ],
}

def _safe_read_csv(path: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    """
    Lee un CSV devolviendo DataFrame vacío con schema si el archivo está vacío
    o no tiene encabezados (EmptyDataError).
    """
    if not path.exists():
        cols = SCHEMAS.get(path.name, [])
        return pd.DataFrame(columns=cols)
    try:
        return pd.read_csv(path, parse_dates=parse_dates)
    except pd.errors.EmptyDataError:
        cols = SCHEMAS.get(path.name, [])
        return pd.DataFrame(columns=cols)

def load_data():
    """
    Carga todos los datasets del MVP y devuelve la tupla:
    (DATA_DIR, stores, skus, sales, inv, lt, promos, distances, orders_c, transfers_c, notifications)
    """
    data_dir = DATA_DIR

    stores = _safe_read_csv(data_dir / "stores.csv")
    skus   = _safe_read_csv(data_dir / "skus.csv")

    sales  = _safe_read_csv(data_dir / "sales.csv", parse_dates=["date"])
    inv    = _safe_read_csv(data_dir / "inventory_snapshot.csv", parse_dates=["date"])
    lt     = _safe_read_csv(data_dir / "lead_times.csv")

    promos = _safe_read_csv(data_dir / "promotions.csv", parse_dates=["start_date", "end_date"])
    distances = _safe_read_csv(data_dir / "store_distances.csv")

    orders_c     = _safe_read_csv(data_dir / "orders_confirmed.csv")
    transfers_c  = _safe_read_csv(data_dir / "transfers_confirmed.csv")
    notifications = _safe_read_csv(data_dir / "notifications.csv")

    return (data_dir, stores, skus, sales, inv, lt, promos, distances, orders_c, transfers_c, notifications)
