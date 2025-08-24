from __future__ import annotations
import numpy as np
import pandas as pd

def _nearest_donors_for_receiver(donors: pd.DataFrame, receiver_store: str, sku: str, distances: pd.DataFrame | None, k: int = 3) -> pd.DataFrame:
    """
    Devuelve hasta k donantes más cercanos al receiver_store para un sku dado.
    """
    df = donors[donors["sku_id"] == sku]
    if df.empty:
        return df

    if distances is None or distances.empty:
        # Sin distancias: devuelve cualquiera (orden aleatorio estable por índice)
        return df.head(k)

    d = distances[(distances["to_store"] == receiver_store) & (distances["from_store"].isin(df["store_id"]))]
    if d.empty:
        return df.head(k)

    # join para traer surplus e info de donantes
    merged = (
        d.merge(df, left_on="from_store", right_on="store_id", how="inner", suffixes=("_dist", "_donor"))
        .sort_values("distance_km")
        .head(k)
        .rename(columns={"from_store": "donor_store"})
    )
    return merged

def suggest_transfers(
    enriched: pd.DataFrame,
    distances: pd.DataFrame | None = None,
    max_per_sku: int = 20,
    allowed_stores: set[str] | None = None,
    allowed_skus: set[str] | None = None,
    min_batch: int = 1,
) -> pd.DataFrame:
    """
    Sugeridor heurístico (MVP) con refuerzo multi-tenant:
    - Sólo propone transferencias dentro de allowed_stores y allowed_skus (si se pasan).
    - Receptores: riesgo == 'Riesgo de quiebre' (DOC < LT mean) o on_hand < ROP.
    - Donantes: riesgo == 'Sobrestock' o on_hand > S_level (surplus).
    - Cuantifica necesidad (need_qty) y excedente (surplus_qty); asigna por cercanía.
    """
    if enriched is None or enriched.empty:
        return pd.DataFrame()

    df = enriched.copy()

    if allowed_stores:
        df = df[df["store_id"].isin(allowed_stores)]
    if allowed_skus:
        df = df[df["sku_id"].isin(allowed_skus)]

    if df.empty:
        return pd.DataFrame(columns=["sku_id", "from_store", "to_store", "qty", "distance_km"])

    # Definir necesidad y excedente
    df["need_qty"] = (df["ROP"] - df["on_hand_units"]).clip(lower=0).astype(int)
    df["surplus_qty"] = (df["on_hand_units"] - df["S_level"]).clip(lower=0).astype(int)

    receivers = df[(df["need_qty"] > 0) | (df["risk"] == "Riesgo de quiebre")][
        ["store_id", "sku_id", "need_qty", "ROP", "S_level", "on_hand_units"]
    ].copy()
    donors = df[(df["surplus_qty"] > 0) | (df["risk"] == "Sobrestock")][
        ["store_id", "sku_id", "surplus_qty", "ROP", "S_level", "on_hand_units"]
    ].copy()

    if receivers.empty or donors.empty:
        return pd.DataFrame(columns=["sku_id", "from_store", "to_store", "qty", "distance_km"])

    # Reducir a SKUs comunes
    common_skus = sorted(set(receivers["sku_id"]).intersection(set(donors["sku_id"])))
    if not common_skus:
        return pd.DataFrame(columns=["sku_id", "from_store", "to_store", "qty", "distance_km"])

    out_rows = []
    # Distancias: si hay scope, ya deberá estar filtrada en app; pero reforzamos aquí también
    if distances is not None and not distances.empty and allowed_stores:
        distances = distances[distances["from_store"].isin(allowed_stores) & distances["to_store"].isin(allowed_stores)]

    for sku in common_skus:
        rec_sku = receivers[receivers["sku_id"] == sku].copy()
        don_sku = donors[donors["sku_id"] == sku].copy()

        # Ordenar receptores por mayor necesidad
        rec_sku = rec_sku.sort_values("need_qty", ascending=False)

        # Para cada receptor, buscar donantes más cercanos
        for _, r in rec_sku.iterrows():
            need = int(r["need_qty"])
            if need <= 0:
                continue
            # obtener donantes más cercanos
            near = _nearest_donors_for_receiver(don_sku, r["store_id"], sku, distances, k=5)
            if near is None or near.empty:
                continue

            # Asignar lotes respetando min_batch
            for _, d in near.iterrows():
                if need <= 0:
                    break
                # donante puede venir de near (merge) o de don_sku directo
                donor_id = d.get("donor_store", d.get("store_id", None))
                if donor_id is None:
                    continue

                # buscar surplus actual del donante
                # Puede estar en 'surplus_qty' si viene del merge; si no, lo buscamos
                if "surplus_qty" in d:
                    surplus = int(max(d["surplus_qty"], 0))
                else:
                    surplus = int(max(don_sku.loc[don_sku["store_id"] == donor_id, "surplus_qty"].sum(), 0))

                if surplus <= 0:
                    continue

                qty = min(need, surplus)
                if qty < min_batch:
                    continue

                dist_km = float(d.get("distance_km", np.nan))

                out_rows.append({
                    "sku_id": sku,
                    "from_store": donor_id,
                    "to_store": r["store_id"],
                    "qty": int(qty),
                    "distance_km": dist_km,
                    "cost_est": round(dist_km * qty * 0.08, 2) if not np.isnan(dist_km) else np.nan
                })

                # actualizar necesidad y excedente "en memoria"
                need -= qty
                # restar en don_sku
                mask = (don_sku["store_id"] == donor_id)
                don_sku.loc[mask, "surplus_qty"] = (don_sku.loc[mask, "surplus_qty"] - qty).clip(lower=0)

        # limitar número de propuestas por SKU
        if max_per_sku is not None and max_per_sku > 0:
            sku_rows = [r for r in out_rows if r["sku_id"] == sku]
            if len(sku_rows) > max_per_sku:
                # ordenar por distancia asc (o qty desc) y recortar
                sku_rows_sorted = sorted(sku_rows, key=lambda x: (np.nan_to_num(x["distance_km"], nan=1e9), -x["qty"]))
                keep = set(id(x) for x in sku_rows_sorted[:max_per_sku])
                out_rows = [r for r in out_rows if (r["sku_id"] != sku) or (id(r) in keep)]

    if not out_rows:
        return pd.DataFrame(columns=["sku_id", "from_store", "to_store", "qty", "distance_km", "cost_est"])

    res = pd.DataFrame(out_rows)
    # Sanidad: evitar from==to y valores no permitidos
    if allowed_stores:
        res = res[res["from_store"].isin(allowed_stores) & res["to_store"].isin(allowed_stores)]
    if allowed_skus:
        res = res[res["sku_id"].isin(allowed_skus)]
    res = res[res["from_store"] != res["to_store"]]
    return res.reset_index(drop=True)
