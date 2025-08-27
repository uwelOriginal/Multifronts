# backend/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from .config import settings
from .db import init_db
from .routes_events import router as events_router
from .routes_slack  import router as slack_router

app = FastAPI(title="Multifronts API")

# Compresión y CORS
app.add_middleware(GZipMiddleware, minimum_size=512)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOW_ORIGINS or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()
app.include_router(events_router)
app.include_router(slack_router)
