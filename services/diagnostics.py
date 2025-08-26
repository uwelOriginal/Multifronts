# services/diagnostics.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional

import pandas as pd
from sqlalchemy import text, select, func

from .repo import engine, current_db_info
from .accounts_repo import (
    users_tbl, orgs_tbl, org_store_map_tbl, org_sku_map_tbl,
    get_user_by_email, create_user,
)

DATA_DIR = Path("./data")
ACC_DIR = DATA_DIR / "accounts"

def neon_info() -> Dict[str, Any]:
    """Meta-información real de la conexión a Neon."""
    d, host, url_mask = current_db_info()
    out = {"sqlalchemy_dialect": d, "host": host, "url_masked": url_mask}
    try:
        with engine.connect() as conn:
            q = conn.exec_driver_sql("select current_user as u, current_database() as db, current_schema() as sch;")
            row = q.mappings().first() or {}
            out.update({"db_user": row.get("u"), "db": row.get("db"), "schema": row.get("sch")})
            q = conn.exec_driver_sql("show search_path;")
            sp = q.fetchone()
            out["search_path"] = sp[0] if sp else None
            q = conn.exec_driver_sql("select version() as v, now() as ts;")
            row2 = q.mappings().first() or {}
            out.update({"version": row2.get("v"), "server_time": str(row2.get("ts"))})
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out

def counts_for_org(org_id: str) -> Dict[str, int]:
    """Conteos por tabla para una organización."""
    res = {"orgs": 0, "users": 0, "org_store_map": 0, "org_sku_map": 0}
    with engine.connect() as conn:
        res["orgs"] = int(conn.execute(select(func.count()).select_from(orgs_tbl).where(orgs_tbl.c.org_id == org_id)).scalar() or 0)
        res["users"] = int(conn.execute(select(func.count()).select_from(users_tbl).where(users_tbl.c.org_id == org_id)).scalar() or 0)
        res["org_store_map"] = int(conn.execute(select(func.count()).select_from(org_store_map_tbl).where(org_store_map_tbl.c.org_id == org_id)).scalar() or 0)
        res["org_sku_map"] = int(conn.execute(select(func.count()).select_from(org_sku_map_tbl).where(org_sku_map_tbl.c.org_id == org_id)).scalar() or 0)
    return res

def csv_snapshot(org_id: str) -> Dict[str, Any]:
    """Qué hay en CSV para esa org (y ruta absoluta, para descartar rutas equivocadas)."""
    def safe_read(p: Path, cols: list[str]) -> pd.DataFrame:
        if not p.exists() or p.stat().st_size == 0:
            return pd.DataFrame(columns=cols)
        try:
            return pd.read_csv(p)
        except Exception:
            return pd.DataFrame(columns=cols)

    orgs = safe_read(ACC_DIR / "orgs.csv", ["org_id","org_name","display_name","slack_webhook"])
    users = safe_read(ACC_DIR / "users.csv", ["email","password","org_id","role","display_name"])
    osm = safe_read(ACC_DIR / "org_store_map.csv", ["org_id","store_id"])
    osk = safe_read(ACC_DIR / "org_sku_map.csv", ["org_id","sku_id"])

    out = {
        "accounts_dir": str(ACC_DIR.resolve()),
        "orgs_rows": int(orgs.shape[0]),
        "users_rows": int(users.shape[0]),
        "osm_rows": int(osm.shape[0]),
        "osk_rows": int(osk.shape[0]),
        "osk_rows_for_org": int(osk[osk["org_id"].astype(str) == org_id].shape[0]) if not osk.empty and "org_id" in osk.columns else 0,
        "osm_rows_for_org": int(osm[osm["org_id"].astype(str) == org_id].shape[0]) if not osm.empty and "org_id" in osm.columns else 0,
        "has_org_in_csv": bool((not orgs.empty) and (orgs["org_id"].astype(str) == org_id).any()),
        "has_user_in_csv": bool((not users.empty) and (users["org_id"].astype(str) == org_id).any()),
        "sample_osk": osk[osk["org_id"].astype(str) == org_id].head(5).to_dict(orient="records") if "org_id" in osk.columns and not osk.empty else [],
        "sample_osm": osm[osm["org_id"].astype(str) == org_id].head(5).to_dict(orient="records") if "org_id" in osm.columns and not osm.empty else [],
    }
    return out

def dryrun_org_id_resolved(org_name: str, email_used: str) -> Dict[str, Any]:
    """
    Si el generador sufija la org, aquí puedes comparar lo que crees vs lo que hay en CSV.
    """
    from re import sub
    base = sub(r"[^a-z0-9]+","-", org_name.lower()).strip("-") or "org"
    # Busca en CSV la última org que empiece por ese slug
    import pandas as pd
    orgs = pd.read_csv(ACC_DIR / "orgs.csv") if (ACC_DIR / "orgs.csv").exists() else pd.DataFrame(columns=["org_id","org_name"])
    candidates = sorted([o for o in orgs.get("org_id", pd.Series([], dtype=str)).astype(str).tolist() if o == base or o.startswith(base+"-")])
    resolved = candidates[-1] if candidates else base
    return {"base_slug": base, "csv_last_org_id_for_slug": resolved}

def test_insert_user(email: str, password: str, org_id: str) -> Dict[str, Any]:
    """Intenta crear un usuario y reporta el resultado (sin silenciar excepciones)."""
    out: Dict[str, Any] = {"email": email, "org_id": org_id}
    try:
        existing = get_user_by_email(email.strip().lower())
        if existing:
            out["existing_id"] = existing.get("id")
            out["result"] = "exists"
            return out
        uid = create_user(email=email.strip().lower(), password=password, org_id=org_id, role="admin", display_name=email.split("@")[0].title())
        out["created_id"] = uid
        out["result"] = "created"
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out

def try_sync_sku_map_for_org(org_id: str, limit: int = 50) -> Dict[str, Any]:
    """Inserta SKUs faltantes para una org leyendo CSV; devuelve números y errores."""
    report: Dict[str, Any] = {"org_id": org_id, "inserted": 0, "errors": []}
    p = ACC_DIR / "org_sku_map.csv"
    if not p.exists() or p.stat().st_size == 0:
        report["errors"].append("org_sku_map.csv no existe o está vacío")
        return report

    osk = pd.read_csv(p)
    if "org_id" not in osk.columns or "sku_id" not in osk.columns:
        report["errors"].append("org_sku_map.csv no tiene columnas esperadas (org_id, sku_id)")
        return report

    osk = osk[osk["org_id"].astype(str) == org_id]
    osk["sku_id"] = osk["sku_id"].astype(str).fillna("").str.strip()
    osk = osk[osk["sku_id"] != ""]
    if osk.empty:
        report["errors"].append(f"No hay filas para org_id={org_id} en org_sku_map.csv")
        return report

    # existentes
    with engine.connect() as conn:
        existing = set(str(x[0]) for x in conn.execute(select(org_sku_map_tbl.c.sku_id).where(org_sku_map_tbl.c.org_id == org_id)))
    to_add = [row for row in osk["sku_id"].tolist() if row not in existing][:limit]

    if not to_add:
        report["note"] = "Nada que insertar (ya estaba todo o >limit)"
        return report

    try:
        with engine.begin() as conn:
            for sk in to_add:
                conn.execute(org_sku_map_tbl.insert().values(org_id=org_id, sku_id=sk))
        report["inserted"] = len(to_add)
    except Exception as e:
        report["errors"].append(f"INSERT failed: {type(e).__name__}: {e}")

    return report
