#!/usr/bin/env bash
set -euo pipefail

# 1) .env
if [ ! -f ".env" ]; then
  echo "[setup] Creando .env desde .env.example"
  cp .env.example .env
fi

# 2) carpetas útiles
mkdir -p data scripts

# 3) verificación de archivos clave
echo "[setup] Variables de entorno:"
grep -E '^(DATABASE_URL|API_BASE|SLACK_WEBHOOK_URL)=' .env || true

echo "[setup] Listo. Puedes ejecutar:  docker compose up -d --build"
