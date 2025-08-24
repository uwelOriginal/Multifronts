from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import Tuple, Set
from services.auth import load_account_tables

def get_allowed_sets(data_dir: Path, org_id: str) -> Tuple[Set[str], Set[str]]:
    """
    Devuelve (allowed_stores, allowed_skus) para la organización dada.
    Lee accounts/org_store_map.csv y accounts/org_sku_map.csv.
    """
    _, _, org_store_map, org_sku_map = load_account_tables(data_dir)
    stores: Set[str] = set()
    skus: Set[str] = set()
    if org_store_map is not None and not org_store_map.empty:
        stores = set(org_store_map.loc[org_store_map["org_id"].astype(str) == str(org_id), "store_id"].astype(str))
    if org_sku_map is not None and not org_sku_map.empty:
        skus = set(org_sku_map.loc[org_sku_map["org_id"].astype(str) == str(org_id), "sku_id"].astype(str))
    return stores, skus

def filter_distances_to_scope(distances_df: pd.DataFrame, allowed_stores: Set[str]) -> pd.DataFrame:
    """
    Mantiene sólo aristas (from_store, to_store) entre tiendas permitidas.
    """
    if distances_df is None or distances_df.empty:
        return distances_df
    df = distances_df.copy()
    return df[df["from_store"].isin(allowed_stores) & df["to_store"].isin(allowed_stores)].reset_index(drop=True)

def enforce_orders_scope(orders_df: pd.DataFrame, allowed_stores: Set[str], allowed_skus: Set[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Valida pedidos: store_id ∈ allowed_stores y sku_id ∈ allowed_skus.
    Devuelve (válidos, bloqueados).
    """
    if orders_df is None or orders_df.empty:
        return orders_df, orders_df
    mask = orders_df["store_id"].isin(allowed_stores) & orders_df["sku_id"].isin(allowed_skus)
    return orders_df[mask].reset_index(drop=True), orders_df[~mask].reset_index(drop=True)

def enforce_transfers_scope(transfers_df: pd.DataFrame, allowed_stores: Set[str], allowed_skus: Set[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Valida transferencias: from_store, to_store ∈ allowed_stores y sku_id ∈ allowed_skus.
    Devuelve (válidas, bloqueadas).
    """
    if transfers_df is None or transfers_df.empty:
        return transfers_df, transfers_df
    mask = (
        transfers_df["from_store"].isin(allowed_stores)
        & transfers_df["to_store"].isin(allowed_stores)
        & transfers_df["sku_id"].isin(allowed_skus)
    )
    return transfers_df[mask].reset_index(drop=True), transfers_df[~mask].reset_index(drop=True)
