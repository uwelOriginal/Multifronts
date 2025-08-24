from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Set, Optional
import pandas as pd

@dataclass
class FilterState:
    store_sel: list[str]
    cat_sel: list[str]
    abc_sel: list[str]
    service_level: float
    order_up_factor: float

@dataclass
class AppContext:
    DATA_DIR: Path
    stores: pd.DataFrame
    skus: pd.DataFrame
    sales: pd.DataFrame
    inv: pd.DataFrame
    lt: pd.DataFrame
    promos: pd.DataFrame
    distances: Optional[pd.DataFrame]
    actor_email: str
    actor_display: str
    org_id: str
    allowed_stores: Set[str]
    allowed_skus: Set[str]
    id_to_label: Dict[str, str]
    label_to_id: Dict[str, str]
    kpis: dict
    recent: pd.DataFrame
    orders_scoped: Optional[pd.DataFrame] = None
    transfers_scoped: Optional[pd.DataFrame] = None
