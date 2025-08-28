# services/slack_notify.py
from __future__ import annotations

import os
import math
import requests
from typing import Iterable, Union

# (Opcional) pandas para detectar DataFrame en runtime
try:
    import pandas as _pd  # alias runtime
except Exception:
    _pd = None  # type: ignore

# ----------------------------
# Helpers ya existentes (deja)
# ----------------------------

def _is_valid_url(u: object) -> bool:
    return isinstance(u, str) and u.strip().startswith(("http://", "https://"))

def _is_nan_like(v: object) -> bool:
    if v is None:
        return True
    if isinstance(v, float):
        try:
            return math.isnan(v)
        except Exception:
            return False
    if isinstance(v, str):
        return v.strip().lower() in ("", "nan", "none", "null")
    return False

# Estos 2 se siguen usando para el Fallback (webhook)
def _as_int(x: object) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0

def _fmt_line(row: dict) -> str:
    kind = str(row.get("kind", "")).lower()
    actor = row.get("actor") or "usuario"
    sku = row.get("sku_id")
    qty = _as_int(row.get("qty", 0))

    if kind.startswith("order"):
        store = row.get("store_id")
        return f"- {actor} aprobó *PEDIDO* • SKU `{sku}` → Sucursal `{store}` • +{qty} uds"

    if kind.startswith("transfer"):
        from_store = row.get("from_store")
        to_store = row.get("to_store")
        return f"- {actor} aprobó *TRANSFERENCIA* • SKU `{sku}` • `{from_store}` → `{to_store}` • {qty} uds"

    target = row.get("store_id") or f"{row.get('from_store')}→{row.get('to_store')}"
    return f"- {actor} aprobó *MOVIMIENTO* • SKU `{sku}` • {target} • {qty} uds"

def _build_text(payload) -> str:
    """Texto estilo 'manual' (solo para fallback al webhook)."""
    if _pd is not None and isinstance(payload, _pd.DataFrame) and not payload.empty:
        first = payload.iloc[0]
        header_kind = str(first.get("kind", "")).lower()
        title = "📦 Pedidos aprobados" if header_kind.startswith("order") \
            else "🔁 Transferencias aprobadas" if header_kind.startswith("transfer") \
            else "✅ Movimientos aprobados"
        org = None
        if "org_id" in payload.columns:
            try:
                cand = payload["org_id"].iloc[0]
                if isinstance(cand, str) and cand.strip():
                    org = cand.strip()
            except Exception:
                pass
        lines = [title + (f" · org `{org}`" if org else "")]
        for _, r in payload.fillna("").iterrows():
            lines.append(_fmt_line(dict(r)))
        return "\n".join(lines[:30])

    if isinstance(payload, dict):
        return _fmt_line(payload)

    try:
        it = list(payload)
        if not it:
            return "Aprobados: movimientos registrados."
        header_kind = str(it[0].get("kind", "")).lower() if isinstance(it[0], dict) else ""
        title = "📦 Pedidos aprobados" if header_kind.startswith("order") \
            else "🔁 Transferencias aprobadas" if header_kind.startswith("transfer") \
            else "✅ Movimientos aprobados"
        lines = [title]
        lines += [_fmt_line(r) for r in it[:30]]
        return "\n".join(lines)
    except Exception:
        try:
            n = len(list(payload))
        except Exception:
            n = 1
        return f"Aprobados: {n} movimiento(s)."

# ----------------------------
# NUEVO: misma vía que diagnóstico
# ----------------------------

# API_BASE exactamente como la resuelve auth._api_base() (sin barra final)
try:
    from services import auth as _auth
    _API_BASE = _auth._api_base()
except Exception:
    _API_BASE = (os.getenv("API_BASE", "") or "").rstrip("/")

def _extract_org_kind_rows_actor(payload) -> tuple[str | None, str | None, list[dict], str | None]:
    """
    Normaliza datos para /events/publish como hace el diagnóstico:
    - org_id (string o None)
    - kind (puede venir como 'order.manual'/'transfer.manual' o similar; si no, None)
    - rows (list[dict])
    - actor (email del aprobador si viene)
    """
    org_id, kind, actor = None, None, None
    rows: list[dict] = []

    if _pd is not None and isinstance(payload, _pd.DataFrame) and not payload.empty:
        try:
            cand_org = payload.iloc[0].get("org_id")
            if isinstance(cand_org, str) and cand_org.strip():
                org_id = cand_org.strip()
        except Exception:
            pass
        k = payload.iloc[0].get("kind")
        if isinstance(k, str) and k.strip():
            kind = k.strip()
        a = payload.iloc[0].get("actor")
        if isinstance(a, str) and a.strip():
            actor = a.strip()
        rows = payload.fillna("").to_dict(orient="records")
        return org_id, kind, rows, actor

    if isinstance(payload, dict):
        org_id = str(payload.get("org_id") or "").strip() or None
        k = str(payload.get("kind") or "").strip()
        kind = k or None
        a = str(payload.get("actor") or "").strip()
        actor = a or None
        rows = [payload]
        return org_id, kind, rows, actor

    try:
        it = list(payload)
        if it and isinstance(it[0], dict):
            org_id = str(it[0].get("org_id") or "").strip() or None
            k = str(it[0].get("kind") or "").strip()
            kind = k or None
            a = str(it[0].get("actor") or "").strip()
            actor = a or None
            rows = it
    except Exception:
        pass

    return org_id, kind, rows, actor

def _type_like_diagnostic(kind: str | None, rows: list[dict]) -> str:
    """
    Igual que el diagnóstico: usamos 'orders_approved' o 'transfers_approved'.
    Si 'kind' sugiere transfer, usamos transfers_approved; si no, orders_approved.
    """
    k = (kind or "").lower()
    if k.startswith("transfer"):
        return "transfers_approved"
    if k.startswith("order"):
        return "orders_approved"
    # Inferir por filas
    if any(("from_store" in r and "to_store" in r) for r in rows):
        return "transfers_approved"
    return "orders_approved"

def send_slack_notifications(
    payload: Union[Iterable[dict], "object", dict],  # evitamos tipar a pd.DataFrame para no molestar a Pylance
    webhook_url: object
):
    """
    Enviar SIEMPRE por el backend (igual que el diagnóstico):
      POST {_API_BASE}/events/publish  con:
        { org_id, type: 'orders_approved'|'transfers_approved', payload: { approved_by, rows } }
    Si no hay API_BASE/org_id o falla, Fallback al webhook (texto estilo 'manual').
    """
    org_id, kind, rows, actor = _extract_org_kind_rows_actor(payload)

    # 1) Ruta preferida (organizacional, como el diagnóstico)
    if _API_BASE and org_id:
        ev_type = _type_like_diagnostic(kind, rows)
        body = {
            "org_id": org_id,
            "type": ev_type,
            "payload": {
                "approved_by": actor,
                "rows": rows,
            }
        }
        try:
            r = requests.post(f"{_API_BASE}/events/publish", json=body, timeout=6)
            if r.ok:
                return True, "Backend: movimiento publicado al canal de la organización."
            # si devuelve error explícito, cae al fallback
        except Exception:
            pass

    # 2) Fallback (webhook legacy) — mantiene tu formato “manual”
    if _is_nan_like(webhook_url) or not _is_valid_url(webhook_url):
        return False, "Slack: no se pudo usar backend y el webhook es inválido o vacío."

    text = _build_text(payload)
    try:
        resp = requests.post(str(webhook_url).strip(), json={"text": text}, timeout=5)
        if 200 <= resp.status_code < 300:
            return True, "Slack (fallback webhook): notificación enviada."
        return False, f"Slack (fallback webhook): {resp.status_code} {resp.text[:200]}"
    except requests.exceptions.RequestException as e:
        return False, f"Slack (fallback webhook): error de red. {e}"
