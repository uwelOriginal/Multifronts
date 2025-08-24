# %%
"""
Generador de datos del MVP + Registro de nuevas organizaciones/usuarios.

Cambios clave:
- store_id global único por org: "{org_id}-S{nn}"
- store_code local por org: "S{nn}" (útil para UI)
- Nombres de sucursales basados en estados de México (p.ej., "CDMX", "Querétaro")
- Categorías aleatorias: entre 3 y 10 (adjetivo + sustantivo)

Modos:
- Inicialización base (init_all()).
- Registro incremental (--register) que añade una nueva org y sus tiendas/datos.
"""
from __future__ import annotations
import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import random
import re

rng = np.random.default_rng(42)
EMAIL_RX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

DATA_DIR = Path("./data")
ACC_DIR  = DATA_DIR / "accounts"
DATA_DIR.mkdir(parents=True, exist_ok=True)
ACC_DIR.mkdir(parents=True, exist_ok=True)

# ---------- utilidades ----------

def _safe_read(path: Path, cols: list[str] | None = None) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame(columns=cols or [])
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=cols or [])

def _append(df_new: pd.DataFrame, path: Path):
    if not path.exists() or path.stat().st_size == 0:
        df_new.to_csv(path, index=False)
    else:
        df_new.to_csv(path, mode="a", header=False, index=False)

def _slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "org"

def _haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    import math as m
    dlat = m.radians(lat2-lat1)
    dlon = m.radians(lon2-lon1)
    a = (m.sin(dlat/2)**2) + m.cos(m.radians(lat1))*m.cos(m.radians(lat2))*(m.sin(dlon/2)**2)
    c = 2*m.atan2(m.sqrt(a), m.sqrt(1-a))
    return R*c

def _ensure_headers():
    # garantiza archivos de movimientos/notifs con encabezado
    orders_cols = ["org_id","store_id","sku_id","qty","actor","ts_iso"]
    transfers_cols = ["org_id","from_store","to_store","sku_id","qty","actor","ts_iso"]
    notif_cols = ["kind","org_id","actor","ts_iso","store_id","from_store","to_store","sku_id","qty","message"]
    for name, cols in {
        "orders_confirmed.csv": orders_cols,
        "transfers_confirmed.csv": transfers_cols,
        "notifications.csv": notif_cols,
    }.items():
        fp = DATA_DIR / name
        if not fp.exists() or fp.stat().st_size == 0:
            pd.DataFrame(columns=cols).to_csv(fp, index=False)

# ---------- catálogos aleatorios ----------

_ADJS = ["Eco", "Premium", "Fresco", "Urbano", "Clásico", "Vital", "Local",
         "Smart", "Orgánico", "Express", "Plus", "Norte", "Sur", "Centro", "Select"]
_NOUNS = ["Bebidas", "Snacks", "Despensa", "Lácteos", "Panadería", "Mascotas",
          "Hogar", "Limpieza", "Belleza", "Cuidado", "Electro", "Infantil", "Salud"]

def _random_categories(min_n: int = 3, max_n: int = 10) -> list[str]:
    n = int(rng.integers(min_n, max_n + 1))
    combos = list({f"{a} {b}" for a in _ADJS for b in _NOUNS})
    rng.shuffle(combos)
    if n > len(combos): n = len(combos)
    return combos[:n]

# Estados / ciudades de MX para nombres de sucursal
_STATES_MX = [
    "CDMX","Estado de México","Nuevo León","Jalisco","Querétaro","Puebla","Guanajuato","Yucatán",
    "Chihuahua","Coahuila","Tamaulipas","Veracruz","Hidalgo","Baja California","Baja California Sur",
    "Sonora","Sinaloa","Tabasco","Michoacán","Morelos","Zacatecas","San Luis Potosí","Aguascalientes",
    "Quintana Roo","Campeche","Tlaxcala","Durango","Colima","Nayarit","Oaxaca","Chiapas","Guerrero"
]

def _pick_states(k: int) -> list[str]:
    if k <= len(_STATES_MX):
        return random.sample(_STATES_MX, k)
    # si piden más que la lista, se repite con sufijos
    base = random.sample(_STATES_MX, len(_STATES_MX))
    extra = [f"{x} {i}" for i, x in enumerate(random.choices(_STATES_MX, k=k-len(_STATES_MX)), start=2)]
    return base + extra

# ---------- Inicialización base (opcional) ----------

def init_all(n_stores_total: int = 8, n_skus: int = 60, days: int = 180):
    base_lat, base_lon = 25.6866, -100.3161

    # Orgs demo
    orgs = pd.DataFrame([
        {"org_id": "alpha", "org_name": "Alpha Retail", "slack_webhook": ""},
        {"org_id": "beta",  "org_name": "Beta Retail",  "slack_webhook": ""},
    ])
    orgs.to_csv(ACC_DIR / "orgs.csv", index=False)

    users = pd.DataFrame([
        {"email":"ana@alpha.com","password":"alpha123","org_id":"alpha","role":"admin","display_name":"Ana A."},
        {"email":"carlos@alpha.com","password":"alpha123","org_id":"alpha","role":"member","display_name":"Carlos A."},
        {"email":"beatriz@beta.com","password":"beta123","org_id":"beta","role":"admin","display_name":"Beatriz B."},
        {"email":"diego@beta.com","password":"beta123","org_id":"beta","role":"member","display_name":"Diego B."},
    ])
    users.to_csv(ACC_DIR / "users.csv", index=False)

    # Categorías aleatorias
    categories = _random_categories(3, 10)

    # SKUs
    skus = []
    for i in range(1, n_skus + 1):
        cat = rng.choice(categories)
        abc = rng.choice(list("ABC"), p=[0.2, 0.5, 0.3])
        cost = float(np.round(rng.uniform(10, 120), 2))
        margin = rng.uniform(0.25, 0.6)
        price = float(np.round(cost * (1 + margin), 2))
        shelf = int(rng.integers(30, 365))
        skus.append({
            "sku_id": f"SKU{i:03d}",
            "sku_name": f"Producto {i}",
            "category": cat,
            "abc_class": abc,
            "unit_cost": cost,
            "unit_price": price,
            "shelf_life_days": shelf
        })
    skus_df = pd.DataFrame(skus).sort_values("sku_id")
    skus_df.to_csv(DATA_DIR / "skus.csv", index=False)

    # Stores por org (IDs por org, nombres por estado)
    half = math.ceil(n_stores_total / 2)
    alpha_states = _pick_states(half)
    beta_states  = _pick_states(n_stores_total - half)

    def _mk_stores(org_id: str, states: list[str]) -> list[dict]:
        out = []
        for i, state in enumerate(states, start=1):
            store_code = f"S{i:02d}"
            store_id = f"{org_id}-{store_code}"
            out.append({
                "store_id": store_id,
                "store_code": store_code,
                "store_name": state,  # estado/ciudad visible
                "region": rng.choice(["Norte","Centro","Sur"]),
                "lat": float(base_lat + rng.normal(0, 0.15)),
                "lon": float(base_lon + rng.normal(0, 0.15)),
            })
        return out

    stores = _mk_stores("alpha", alpha_states) + _mk_stores("beta", beta_states)
    stores_df = pd.DataFrame(stores).sort_values("store_id")
    stores_df.to_csv(DATA_DIR / "stores.csv", index=False)

    # Map org->stores
    org_store = [{"org_id":"alpha","store_id":row["store_id"]} for _, row in stores_df[stores_df["store_id"].str.startswith("alpha-")].iterrows()] + \
                [{"org_id":"beta","store_id":row["store_id"]}  for _, row in stores_df[stores_df["store_id"].str.startswith("beta-")].iterrows()]
    pd.DataFrame(org_store).to_csv(ACC_DIR / "org_store_map.csv", index=False)

    # org_sku_map: mitad y mitad
    half_sku = math.ceil(len(skus_df) / 2)
    org_sku = [{"org_id":"alpha","sku_id":sid} for sid in skus_df["sku_id"][:half_sku]] + \
              [{"org_id":"beta","sku_id":sid}  for sid in skus_df["sku_id"][half_sku:]]
    pd.DataFrame(org_sku).to_csv(ACC_DIR / "org_sku_map.csv", index=False)

    # Promos
    start_date = (datetime.today().date() - timedelta(days=days))
    date_range = pd.date_range(start_date, periods=days, freq="D")

    promos = []
    num_promos = 40
    for _ in range(num_promos):
        sku_id = rng.choice(skus_df["sku_id"])
        store_id = rng.choice(stores_df["store_id"])
        start_idx = int(rng.integers(0, days - 14))
        duration = int(rng.integers(5, 12))
        uplift = float(np.round(rng.uniform(1.2, 1.8), 2))
        promos.append({
            "store_id": store_id,
            "sku_id": sku_id,
            "start_date": str((start_date + timedelta(days=start_idx))),
            "end_date": str((start_date + timedelta(days=start_idx + duration))),
            "uplift_factor": uplift,
            "name": f"Promo_{store_id}_{sku_id}"
        })
    pd.DataFrame(promos).to_csv(DATA_DIR / "promotions.csv", index=False)

    # Lead times
    lt_rows = []
    for s in stores_df["store_id"]:
        for sku in skus_df["sku_id"]:
            lt_mean = float(np.round(rng.uniform(5, 15), 1))
            lt_std  = float(np.round(rng.uniform(0.5, 3.0), 1))
            lt_rows.append({"store_id": s, "sku_id": sku, "lead_time_mean_days": lt_mean, "lead_time_std_days": lt_std})
    pd.DataFrame(lt_rows).to_csv(DATA_DIR / "lead_times.csv", index=False)

    # Ventas
    def weekly_seasonality(dow): return 1.15 if dow >= 5 else 1.0
    def yearly_seasonality(day_index): return 1 + 0.15 * np.sin(2 * np.pi * (day_index / 365.0))

    promos_df = pd.read_csv(DATA_DIR / "promotions.csv")
    sales_rows = []
    sku_base = {row.sku_id: rng.uniform(0.5, 12.0) * (1.8 if row.abc_class == "A" else 1.0) for _, row in skus_df.iterrows()}
    store_mult = {row.store_id: rng.uniform(0.8, 1.2) for _, row in stores_df.iterrows()}
    intermittent_skus = set(rng.choice(skus_df["sku_id"], size=int(0.25 * len(skus_df)), replace=False))

    for d_idx, date in enumerate(date_range):
        dow = date.weekday()
        y_season = yearly_seasonality(d_idx)
        for s in stores_df["store_id"]:
            for sku in skus_df["sku_id"]:
                lam = sku_base[sku] * store_mult[s] * weekly_seasonality(dow) * y_season
                active = promos_df[
                    (promos_df["store_id"] == s) &
                    (promos_df["sku_id"] == sku) &
                    (promos_df["start_date"] <= str(date.date())) &
                    (promos_df["end_date"] >= str(date.date()))
                ]
                if not active.empty:
                    lam *= float(active["uplift_factor"].iloc[0])
                if sku in intermittent_skus and rng.uniform() < 0.35:
                    lam *= 0.1
                units = int(rng.poisson(max(lam, 0.05)))
                sales_rows.append({"date": str(date.date()), "store_id": s, "sku_id": sku, "units_sold": units})
    pd.DataFrame(sales_rows).to_csv(DATA_DIR / "sales.csv", index=False)

    # Inventario snapshot
    current_date = date_range[-1].date()
    recent_sales = pd.read_csv(DATA_DIR / "sales.csv")
    recent_sales = recent_sales[recent_sales["date"] >= str(current_date - timedelta(days=28))]
    avg_recent = recent_sales.groupby(["store_id","sku_id"])["units_sold"].mean().reset_index().rename(columns={"units_sold":"avg_daily_sales_28d"})
    doc = rng.uniform(2, 60, size=len(avg_recent))
    low_idx = rng.choice(len(doc), size=int(0.10 * len(doc)), replace=False)
    high_idx = rng.choice(len(doc), size=int(0.10 * len(doc)), replace=False)
    doc[low_idx] = rng.uniform(0.5, 3.0, size=len(low_idx))
    doc[high_idx] = rng.uniform(60, 120, size=len(high_idx))
    on_hand = np.maximum((avg_recent["avg_daily_sales_28d"].values * doc).round().astype(int), 0)
    inventory_df = avg_recent.copy()
    inventory_df["date"] = str(current_date)
    inventory_df["on_hand_units"] = on_hand
    inventory_df = inventory_df[["date","store_id","sku_id","on_hand_units"]]
    inventory_df.to_csv(DATA_DIR / "inventory_snapshot.csv", index=False)

    # Distancias
    dist_rows = []
    for _, a in stores_df.iterrows():
        for _, b in stores_df.iterrows():
            if a.store_id == b.store_id:
                continue
            dist_rows.append({
                "from_store": a.store_id,
                "to_store": b.store_id,
                "distance_km": round(_haversine(a.lat, a.lon, b.lat, b.lon), 2)
            })
    pd.DataFrame(dist_rows).to_csv(DATA_DIR / "store_distances.csv", index=False)

    _ensure_headers()
    print(f"Datos generados en: {DATA_DIR.resolve()}")
    print(f"Cuentas en: {(ACC_DIR).resolve()}")

# ---------- Registro incremental ----------

def register_new_account(
    data_dir: Path = DATA_DIR,
    email: str = "",
    password: str = "",
    org_name: str = "",
    stores_count: int = 2,
    sku_fraction: float = 0.35,
) -> str:
    assert EMAIL_RX.match(email), "Email inválido"
    assert password and len(password) >= 6, "Contraseña inválida"
    org_name = org_name.strip() or email.split("@")[1].split(".")[0].title()
    org_id = _slugify(org_name)

    # Cargar tablas actuales
    users = _safe_read(ACC_DIR / "users.csv", ["email","password","org_id","role","display_name"])
    orgs  = _safe_read(ACC_DIR / "orgs.csv",  ["org_id","org_name","slack_webhook"])
    org_store_map = _safe_read(ACC_DIR / "org_store_map.csv", ["org_id","store_id"])
    org_sku_map   = _safe_read(ACC_DIR / "org_sku_map.csv",   ["org_id","sku_id"])

    stores_df = _safe_read(DATA_DIR / "stores.csv", ["store_id","store_code","store_name","region","lat","lon"])
    skus_df   = _safe_read(DATA_DIR / "skus.csv",   ["sku_id","sku_name","category","abc_class","unit_cost","unit_price","shelf_life_days"])
    promos_df = _safe_read(DATA_DIR / "promotions.csv", ["store_id","sku_id","start_date","end_date","uplift_factor","name"])
    sales_df  = _safe_read(DATA_DIR / "sales.csv",  ["date","store_id","sku_id","units_sold"])
    inv_df    = _safe_read(DATA_DIR / "inventory_snapshot.csv", ["date","store_id","sku_id","on_hand_units"])
    lt_df     = _safe_read(DATA_DIR / "lead_times.csv", ["store_id","sku_id","lead_time_mean_days","lead_time_std_days"])
    dist_df   = _safe_read(DATA_DIR / "store_distances.csv", ["from_store","to_store","distance_km"])

    # org_id único (sufijo si existe)
    base_id = org_id
    suffix = 1
    if not orgs.empty:
        while (orgs["org_id"] == org_id).any():
            suffix += 1
            org_id = f"{base_id}-{suffix}"

    # Crear org + usuario
    org_row = pd.DataFrame([{"org_id": org_id, "org_name": org_name, "slack_webhook": ""}])
    _append(org_row, ACC_DIR / "orgs.csv")

    display_name = email.split("@")[0].title()
    user_row = pd.DataFrame([{"email": email, "password": password, "org_id": org_id, "role": "admin", "display_name": display_name}])
    _append(user_row, ACC_DIR / "users.csv")

    # Asignar SKUs del catálogo
    total_skus = skus_df["sku_id"].tolist()
    k = max(1, int(len(total_skus) * float(sku_fraction)))
    chosen_skus = sorted(random.sample(total_skus, k))
    sku_map_rows = pd.DataFrame([{"org_id": org_id, "sku_id": sid} for sid in chosen_skus])
    _append(sku_map_rows, ACC_DIR / "org_sku_map.csv")

    # Crear tiendas NUEVAS con ID por org + nombres de estados MX
    base_lat, base_lon = 25.6866, -100.3161
    state_names = _pick_states(int(stores_count))
    new_stores = []
    for i, state in enumerate(state_names, start=1):
        store_code = f"S{i:02d}"  # empieza en S01 por org
        store_id = f"{org_id}-{store_code}"
        new_stores.append({
            "store_id": store_id,
            "store_code": store_code,
            "store_name": state,
            "region": rng.choice(["Norte","Centro","Sur"]),
            "lat": float(base_lat + rng.normal(0, 0.15)),
            "lon": float(base_lon + rng.normal(0, 0.15)),
        })
    stores_app = pd.DataFrame(new_stores)
    _append(stores_app, DATA_DIR / "stores.csv")

    store_map_rows = pd.DataFrame([{"org_id": org_id, "store_id": row["store_id"]} for _, row in stores_app.iterrows()])
    _append(store_map_rows, ACC_DIR / "org_store_map.csv")

    # Ventana temporal coherente con dataset actual
    if not sales_df.empty:
        min_d = pd.to_datetime(sales_df["date"]).min().date()
        max_d = pd.to_datetime(sales_df["date"]).max().date()
    else:
        max_d = datetime.today().date()
        min_d = max_d - timedelta(days=180)
    date_range = pd.date_range(min_d, max_d, freq="D")
    days = len(date_range)

    # Demand drivers
    def weekly_seasonality(dow): return 1.15 if dow >= 5 else 1.0
    def yearly_seasonality(day_index): return 1 + 0.15 * np.sin(2 * np.pi * (day_index / 365.0))
    sku_base = {row.sku_id: rng.uniform(0.5, 12.0) * (1.8 if row.abc_class == "A" else 1.0) for _, row in skus_df.iterrows()}
    store_mult = {row.store_id: rng.uniform(0.8, 1.2) for _, row in stores_app.iterrows()}
    intermittent_skus = set(rng.choice(skus_df["sku_id"], size=int(0.25 * len(skus_df)), replace=False))

    # Promos opcionales para las nuevas tiendas
    promo_rows = []
    for sid in stores_app["store_id"]:
        for _ in range(2):
            sku = rng.choice(chosen_skus)
            start_idx = int(rng.integers(0, max(1, days - 10)))
            duration = int(rng.integers(6, 10))
            uplift = float(np.round(rng.uniform(1.2, 1.8), 2))
            promo_rows.append({
                "store_id": sid,
                "sku_id": sku,
                "start_date": str((min_d + timedelta(days=start_idx))),
                "end_date": str((min_d + timedelta(days=min(days-1, start_idx + duration)))),
                "uplift_factor": uplift,
                "name": f"Promo_{sid}_{sku}"
            })
    if promo_rows:
        _append(pd.DataFrame(promo_rows), DATA_DIR / "promotions.csv")

    promos_all = _safe_read(DATA_DIR / "promotions.csv", ["store_id","sku_id","start_date","end_date","uplift_factor","name"])

    # Ventas sintetizadas
    sales_rows = []
    for d_idx, date in enumerate(date_range):
        dow = date.weekday()
        y_season = yearly_seasonality(d_idx)
        for s in stores_app["store_id"]:
            for sku in chosen_skus:
                lam = sku_base[sku] * store_mult[s] * weekly_seasonality(dow) * y_season
                active = promos_all[
                    (promos_all["store_id"] == s) &
                    (promos_all["sku_id"] == sku) &
                    (promos_all["start_date"] <= str(date.date())) &
                    (promos_all["end_date"] >= str(date.date()))
                ]
                if not active.empty:
                    lam *= float(active["uplift_factor"].iloc[0])
                if sku in intermittent_skus and rng.uniform() < 0.35:
                    lam *= 0.1
                units = int(rng.poisson(max(lam, 0.05)))
                sales_rows.append({"date": str(date.date()), "store_id": s, "sku_id": sku, "units_sold": units})
    if sales_rows:
        _append(pd.DataFrame(sales_rows), DATA_DIR / "sales.csv")

    # Lead times para nuevas tiendas
    lt_rows = []
    for s in stores_app["store_id"]:
        for sku in chosen_skus:
            lt_mean = float(np.round(rng.uniform(5, 15), 1))
            lt_std  = float(np.round(rng.uniform(0.5, 3.0), 1))
            lt_rows.append({"store_id": s, "sku_id": sku, "lead_time_mean_days": lt_mean, "lead_time_std_days": lt_std})
    _append(pd.DataFrame(lt_rows), DATA_DIR / "lead_times.csv")

    # Inventario snapshot para nuevas combinaciones
    sales_all = _safe_read(DATA_DIR / "sales.csv", ["date","store_id","sku_id","units_sold"])
    current_date = pd.to_datetime(sales_all["date"]).max().date() if not sales_all.empty else datetime.today().date()
    recent_sales = sales_all[sales_all["date"] >= str(current_date - timedelta(days=28))]
    avg_recent = recent_sales.groupby(["store_id","sku_id"])["units_sold"].mean().reset_index().rename(columns={"units_sold":"avg_daily_sales_28d"})
    avg_recent = avg_recent[avg_recent["store_id"].isin(stores_app["store_id"]) & avg_recent["sku_id"].isin(chosen_skus)]
    if not avg_recent.empty:
        doc = rng.uniform(2, 60, size=len(avg_recent))
        low_idx = rng.choice(len(doc), size=max(1, int(0.10 * len(doc))), replace=False)
        high_idx = rng.choice(len(doc), size=max(1, int(0.10 * len(doc))), replace=False)
        doc[low_idx] = rng.uniform(0.5, 3.0, size=len(low_idx))
        doc[high_idx] = rng.uniform(60, 120, size=len(high_idx))
        on_hand = np.maximum((avg_recent["avg_daily_sales_28d"].values * doc).round().astype(int), 0)
        inv_new = avg_recent.copy()
        inv_new["date"] = str(current_date)
        inv_new["on_hand_units"] = on_hand
        inv_new = inv_new[["date","store_id","sku_id","on_hand_units"]]
        _append(inv_new, DATA_DIR / "inventory_snapshot.csv")

    # Distancias (nuevas aristas entre TODAS las tiendas)
    stores_all = _safe_read(DATA_DIR / "stores.csv", ["store_id","store_code","store_name","region","lat","lon"])
    dist_rows = []
    # (a) de nuevas hacia todas
    for _, a in stores_all[stores_all["store_id"].isin(stores_app["store_id"])].iterrows():
        for _, b in stores_all.iterrows():
            if a.store_id == b.store_id:
                continue
            dist_rows.append({
                "from_store": a.store_id, "to_store": b.store_id,
                "distance_km": round(_haversine(a.lat, a.lon, b.lat, b.lon), 2)
            })
    # (b) de todas hacia nuevas
    for _, b in stores_all[stores_all["store_id"].isin(stores_app["store_id"])].iterrows():
        for _, a in stores_all.iterrows():
            if a.store_id == b.store_id:
                continue
            dist_rows.append({
                "from_store": a.store_id, "to_store": b.store_id,
                "distance_km": round(_haversine(a.lat, a.lon, b.lat, b.lon), 2)
            })
    if dist_rows:
        _append(pd.DataFrame(dist_rows), DATA_DIR / "store_distances.csv")

    _ensure_headers()
    print(f"[register] Nueva organización creada: {org_id} (usuario {email})")
    return org_id

# ---------- CLI ----------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--register", action="store_true", help="Registrar nueva organización/usuario sin regenerar todo.")
    parser.add_argument("--email", type=str, default="")
    parser.add_argument("--password", type=str, default="")
    parser.add_argument("--org-name", type=str, default="")
    parser.add_argument("--stores", type=int, default=2)
    parser.add_argument("--skus-frac", type=float, default=0.35)
    args = parser.parse_args()

    if args.register:
        if not EMAIL_RX.match(args.email):
            print("Email inválido", flush=True)
            raise SystemExit(2)
        if not args.password or len(args.password) < 6:
            print("Contraseña inválida", flush=True)
            raise SystemExit(2)
        oid = register_new_account(
            data_dir=DATA_DIR,
            email=args.email,
            password=args.password,
            org_name=args.org_name,
            stores_count=int(args.stores),
            sku_fraction=float(args.skus_frac),
        )
        print(f"ORG_ID={oid}", flush=True)
        raise SystemExit(0)

    # Si no se pasa --register, ejecuta init base:
    init_all()
