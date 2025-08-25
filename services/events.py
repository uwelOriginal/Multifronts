# services/events.py
"""
Redis fue removido. Este módulo provee un wrapper compatible que
publica eventos usando la tabla 'events' y (si estás en Postgres)
emite NOTIFY para que la UI escuche con LISTEN.
"""
from __future__ import annotations
from typing import Dict, Any
from . import repo

def publish_redis(org_id: str, event: Dict[str, Any]) -> bool:
    """
    Compatibilidad hacia atrás: si alguien seguía llamando publish_redis(...),
    usamos Postgres como bus de eventos.

    event esperado: {"type": "...", "payload": {...}}
    """
    try:
        type_ = str(event.get("type", "generic"))
        payload = event.get("payload", {}) or {}
        repo.insert_event(org_id, type_, payload)
        return True
    except Exception:
        return False
