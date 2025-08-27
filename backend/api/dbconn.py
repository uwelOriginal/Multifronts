# backend/api/dbconn.py
import os
from sqlalchemy import create_engine

DB_URL = os.getenv("DATABASE_URL", "")
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "8")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "4")),
    pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "10")),
    pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "300")),
    future=True,
)
