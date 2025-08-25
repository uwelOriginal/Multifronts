# services/slack_notify.py
from __future__ import annotations
import json
from typing import Iterable, Union
import requests

try:
    import pandas as pd  # para detectar DataFrame
except Exception:
    pd = None

def _is_valid_url(u: object) -> bool:
    return isinstance(u, str) and u.strip().startswith(("http://", "https://"))

def _is_nan_like(v: object) -> bool:
    try:
        import math, numpy as np  # type: ignore
        if v is None:
            return True
        if isinstance(v, float) and math.isnan(v):
            return True
        if "numpy" in globals() and isinstance(v, (np.floating,)) and np.isnan(v):
            return True
    except Exception:
        pass
    return isinstance(v, str) and v.strip().lower() == "nan"

def _build_text(payload: Union[Iterable[dict], "pd.DataFrame", dict]) -> str:
    # Mensaje muy simple; puedes personalizarlo si ya tenías formato con blocks.
    if pd is not None and isinstance(payload, pd.DataFrame):
        return f"Aprobados: {len(payload)} movimiento(s)."
    if isinstance(payload, dict):
        return f"Aprobados: {json.dumps(payload, ensure_ascii=False)[:1800]}"
    try:
        it = list(payload)  # Iterable[dict]
        return f"Aprobados: {len(it)} movimiento(s)."
    except Exception:
        return "Aprobados: movimientos registrados."

def send_slack_notifications(payload: Union[Iterable[dict], "pd.DataFrame", dict], webhook_url: object):
    """
    Envía un mensaje simple a Slack vía webhook.
    Retorna (ok: bool, msg: str). No lanza excepciones hacia el llamador.
    """
    if _is_nan_like(webhook_url) or not _is_valid_url(webhook_url):
        return False, "Slack: webhook inválido o vacío; no se envió notificación."

    url = str(webhook_url).strip()
    data = {"text": _build_text(payload)}

    try:
        resp = requests.post(url, json=data, timeout=5)
        if 200 <= resp.status_code < 300:
            return True, "Slack: notificación enviada."
        return False, f"Slack: respuesta {resp.status_code} {resp.text[:200]}"
    except requests.exceptions.MissingSchema as e:
        return False, f"Slack: URL inválida. {e}"
    except requests.exceptions.RequestException as e:
        return False, f"Slack: error de red. {e}"
