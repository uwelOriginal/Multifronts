# api/config.py
from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    # DB y CORS
    DATABASE_URL: str
    CORS_ALLOW_ORIGINS: List[str] = ["https://multifronts.streamlit.app"]  # ej: ["https://tu-app-streamlit.streamlit.app"]
    # Slack (opcional: para endpoint de eventos entrantes)
    SLACK_SIGNING_SECRET: str | None = None
    # Otros
    LOG_LEVEL: str = "info"

    class Config:
        env_file = ".env"  # opcional; en Render se usan Environment vars
        env_file_encoding = "utf-8"

settings = Settings()
