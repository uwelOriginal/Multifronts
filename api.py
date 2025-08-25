# api.py
import os, json, time
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from services import repo, events

app = FastAPI(title="Multifronts Events API", version="0.1.0")

class PublishIn(BaseModel):
    org_id: str
    type: str          # e.g., "order_approved" | "transfer_approved" | "note"
    payload: Dict[str, Any] = {}

@app.on_event("startup")
def on_start():
    repo.init_db()   # crea tablas si no existen

@app.get("/health")
def health():
    return {"ok": True, "db": repo.health()}

@app.post("/events/publish")
def publish(ev: PublishIn):
    if not ev.org_id or not ev.type:
        raise HTTPException(400, "org_id y type son obligatorios")
    persisted = repo.insert_event(ev.org_id, ev.type, ev.payload)
    events.publish_redis(ev.org_id, persisted)   # best-effort (no falla si no hay Redis)
    return {"event": persisted}

@app.get("/events/poll")
def poll(org_id: str = Query(...), after: int = 0, limit: int = 100):
    evs = repo.list_events(org_id=org_id, after=after, limit=min(500, max(1, limit)))
    cursor = evs[-1]["id"] if evs else after
    return {"events": evs, "cursor": cursor}
