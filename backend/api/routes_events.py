from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from .dbconn import engine
from .db import insert_event, poll_events
from .schemas import PublishIn, PollOut
from .slack_utils import ensure_slack_tables, ensure_hq_channel, post_to_org
import os

router = APIRouter()

# (Opcional) pandas para detectar DataFrame en runtime
try:
    import pandas as _pd  # alias runtime
except Exception:
    _pd = None  # type: ignore

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

    elif kind.startswith("transfer"):
        from_store = row.get("from_store")
        to_store = row.get("to_store")
        return f"- {actor} aprobó *TRANSFERENCIA* • SKU `{sku}` • `{from_store}` → `{to_store}` • {qty} uds"
    
    else:
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

def _infer_kind_from_type(etype: str) -> str:
    et = (etype or "").lower()
    if et.startswith("transfer"):
        return "transfer"
    if et.startswith("order"):
        return "order"
    return "order"  # por defecto

@router.post("/events/publish")
def events_publish(body: PublishIn):
    try:
        ev = insert_event(org_id=body.org_id, type_=body.type, payload=body.payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    etype, org_id, payload = body.type, body.org_id, (body.payload or {})
    rows = (payload or {}).get("rows") or []
    actor = payload.get("approved_by") or payload.get("actor") or "usuario"

    movement_types = {
        "orders_approved", "transfers_approved",
        "order.manual", "transfer.manual",
        "order_manual_approved", "transfer_manual_approved",
        "movement.approved"
    }

    if etype in movement_types or etype == "org_created":
        with engine.begin() as conn:
            ensure_slack_tables(conn)
            ensure_hq_channel(conn, org_id)

            if etype == "org_created":
                invite_url = os.getenv("SLACK_WORKSPACE_INVITE_URL", "").strip()
                chan_name  = f"mf-{org_id}"  # nombre estándar de canal organizacional
                created_by = (payload or {}).get("created_by") or "usuario"

                # Construimos un mensaje claro con ambos enlaces (workspace y canal)
                # Nota: el canal se muestra por nombre; Slack lo autoenlaza para miembros del workspace.
                lines = [
                    f"🆕 *Organización creada* — org `{org_id}`",
                    f"Creado por: `{created_by}`",
                ]
                if invite_url:
                    lines.append(f"Invitación al *workspace*: {invite_url}")
                #else:
                #    lines.append("_(Falta configurar SLACK_WORKSPACE_INVITE_URL en el backend)_")
                lines.append(f"Canal de la organización: `#{chan_name}`")
                text_msg = "\n".join(lines)

                post_to_org(conn, org_id, text_msg, None)
            else:
                # ---------- NUEVO: usar SIEMPRE build_text de slack_notify ----------
                # Annotate filas con org/kind/actor para que build_text genere el título y líneas correctas
                klabel = _infer_kind_from_type(etype)
                annotated = []
                for r in rows[:1000]:  # cap prudente
                    rr = dict(r)
                    rr.setdefault("org_id", org_id)
                    rr.setdefault("actor", actor)
                    rr.setdefault("kind", klabel)
                    annotated.append(rr)

                text_msg = None
                if annotated:
                    try:
                        # Preferimos DataFrame para que build_text incluya " · org ..."
                        import pandas as pd  # type: ignore
                        df = pd.DataFrame(annotated)
                        text_msg = _build_text(df)
                    except Exception:
                        # Si no hay pandas, igual funciona con lista (sin sufijo org en título)
                        text_msg = _build_text(annotated)
                else:
                    # Fallback neutral (sin conteos)
                    title = "🔁 Transferencias aprobadas" if klabel == "transfer" else "📦 Pedidos aprobados"
                    text_msg = f"{title} · org {org_id}"

                post_to_org(conn, org_id, text_msg, None)
                # -------------------------------------------------------------------

    return {"ok": True, "event": ev}

@router.get("/events/poll", response_model=PollOut)
def events_poll(org_id: str, after: int = 0, limit: int = 200):
    try:
        evs, cursor = poll_events(org_id=org_id, after=after, limit=limit)
        return {"events": evs, "cursor": cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
