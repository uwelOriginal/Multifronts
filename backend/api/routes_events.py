from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from .dbconn import engine
from .db import insert_event, poll_events
from .schemas import PublishIn, PollOut
from .slack_utils import ensure_slack_tables, ensure_hq_channel, post_to_org

router = APIRouter()

@router.post("/events/publish")
def events_publish(body: PublishIn):
    try:
        ev = insert_event(org_id=body.org_id, type_=body.type, payload=body.payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    etype, org_id, payload = body.type, body.org_id, (body.payload or {})
    if etype in ("org_created","orders_approved","transfers_approved"):
        with engine.begin() as conn:
            ensure_slack_tables(conn)
            if etype == "org_created":
                ensure_hq_channel(conn, org_id)
            else:
                if etype == "orders_approved":
                    msg = f"✅ {etype} por {payload.get('approved_by','?')}: nuevas={payload.get('count_new',0)} dup={payload.get('count_dup',0)}"
                else:
                    msg = f"✅ {etype} por {payload.get('approved_by','?')}: aplicadas={payload.get('count_applied',0)} dup={payload.get('count_dup',0)} sin_stock={payload.get('count_insufficient',0)}"
                blocks = [
                    {"type":"section","text":{"type":"mrkdwn","text": f"*{etype}* — org `{org_id}`"}},
                    {"type":"section","text":{"type":"mrkdwn","text": msg}},
                ]
                post_to_org(conn, org_id, msg, blocks)
    return {"ok": True, "event": ev}

@router.get("/events/poll", response_model=PollOut)
def events_poll(org_id: str, after: int = 0, limit: int = 200):
    try:
        evs, cursor = poll_events(org_id=org_id, after=after, limit=limit)
        return {"events": evs, "cursor": cursor}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
