from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from .dbconn import engine
from .db import insert_event, poll_events
from .schemas import PublishIn, PollOut
from .slack_utils import ensure_slack_tables, ensure_hq_channel, post_to_org
from services.slack_notify import _build_text as build_text 

router = APIRouter()

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
                # Mensaje de bienvenida (no es un movimiento)
                post_to_org(
                    conn, org_id,
                    f"🆕 org_created — org `{org_id}`",
                    [
                        {"type":"section","text":{"type":"mrkdwn","text": f"*org_created* — org `{org_id}`"}},
                        {"type":"section","text":{"type":"mrkdwn","text": "Canal HQ creado o verificado."}}
                    ]
                )
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
                        text_msg = build_text(df)
                    except Exception:
                        # Si no hay pandas, igual funciona con lista (sin sufijo org en título)
                        text_msg = build_text(annotated)
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
