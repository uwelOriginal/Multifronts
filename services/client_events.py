# services/client_events.py
import os, requests
from typing import Tuple, List, Dict, Any

try:
    import streamlit as st  # en Cloud leeremos API_BASE desde secrets
except Exception:
    st = None  # ejecución fuera de Streamlit

def _api_base() -> str | None:
    url = None
    if st is not None:
        try:
            url = st.secrets.get("API_BASE", None)  # type: ignore[attr-defined]
        except Exception:
            pass
    if not url:
        url = os.getenv("API_BASE", "").strip()
    if isinstance(url, str) and url.strip().startswith(("http://", "https://")):
        return url.strip()
    return None  # sin backend configurado

def poll_events(org_id: str, cursor: int, timeout: float = 5.0) -> Tuple[List[Dict[str, Any]], int]:
    """
    Consulta eventos nuevos después de 'cursor'. Retorna (events, new_cursor).
    Si no hay API_BASE válido, devuelve ([], cursor) sin error.
    """
    base = _api_base()
    if not base:
        return [], cursor
    try:
        r = requests.get(
            f"{base}/events/poll",
            params={"org_id": org_id, "after": cursor, "limit": 200},
            timeout=timeout
        )
        r.raise_for_status()
        data = r.json()
        evs = data.get("events", []) or []
        new_cur = int(data.get("cursor", cursor))
        return evs, new_cur
    except Exception:
        return [], cursor

def publish_event(org_id: str, type_: str, payload: dict, timeout: float = 5.0):
    """
    Publica un evento (útil tras aprobar pedidos/transferencias).
    Si no hay API_BASE válido, hace no-op y responde (False, {...}).
    """
    base = _api_base()
    if not base:
        return False, {"error": "API_BASE no configurado"}
    try:
        r = requests.post(
            f"{base}/events/publish",
            json={"org_id": org_id, "type": type_, "payload": payload},
            timeout=timeout,
        )
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        return False, {"error": str(e)}
