-- Schema base para Multifronts (Neon / PostgreSQL)
-- Idempotente: puedes ejecutarlo varias veces sin romper nada.

-- =========================
-- Organizaciones
-- =========================
CREATE TABLE IF NOT EXISTS orgs (
  org_id        TEXT PRIMARY KEY,
  display_name  TEXT,
  slack_webhook TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================
-- Usuarios
-- =========================
CREATE TABLE IF NOT EXISTS users (
  id           SERIAL PRIMARY KEY,
  email        TEXT NOT NULL,
  password     TEXT NOT NULL,
  org_id       TEXT NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  role         TEXT NOT NULL DEFAULT 'member',
  display_name TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unicidad case-insensitive por email
CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_key ON users (lower(email));

-- Índice útil por organización
CREATE INDEX IF NOT EXISTS users_org_id_idx ON users (org_id);

-- =========================
-- Mapeo Org -> Tiendas
-- =========================
CREATE TABLE IF NOT EXISTS org_store_map (
  org_id  TEXT NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  store_id TEXT NOT NULL,
  PRIMARY KEY (org_id, store_id)
);

CREATE INDEX IF NOT EXISTS org_store_map_org_idx ON org_store_map (org_id);
CREATE INDEX IF NOT EXISTS org_store_map_store_idx ON org_store_map (store_id);

-- =========================
-- Mapeo Org -> SKUs
-- =========================
CREATE TABLE IF NOT EXISTS org_sku_map (
  org_id TEXT NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  sku_id TEXT NOT NULL,
  PRIMARY KEY (org_id, sku_id)
);

CREATE INDEX IF NOT EXISTS org_sku_map_org_idx ON org_sku_map (org_id);
CREATE INDEX IF NOT EXISTS org_sku_map_sku_idx ON org_sku_map (sku_id);

-- =========================
-- Instalaciones de Slack (OAuth / Webhooks por org)
-- =========================
CREATE TABLE IF NOT EXISTS slack_installs (
  id               SERIAL PRIMARY KEY,
  org_id           TEXT NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  team_id          TEXT NOT NULL,
  bot_token        TEXT,
  webhook_url      TEXT,
  webhook_channel  TEXT,
  installed_by     TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS slack_installs_org_idx  ON slack_installs (org_id);
CREATE INDEX IF NOT EXISTS slack_installs_team_idx ON slack_installs (team_id);

-- =========================
-- Eventos (para auditoría / notificaciones)
-- =========================
CREATE TABLE IF NOT EXISTS events (
  id      BIGSERIAL PRIMARY KEY,
  org_id  TEXT NOT NULL REFERENCES orgs(org_id) ON DELETE CASCADE,
  ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
  type    TEXT NOT NULL,
  payload JSONB
);

CREATE INDEX IF NOT EXISTS events_org_ts_idx ON events (org_id, ts DESC);

-- =========================
-- (Opcional) Limpieza de esquemas antiguos
-- Si la columna antigua 'sku' existía en org_sku_map, renómbrala a 'sku_id'.
-- Ejecuta este bloque una sola vez si lo necesitas.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='org_sku_map' AND column_name='sku'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='org_sku_map' AND column_name='sku_id'
  ) THEN
    EXECUTE 'ALTER TABLE org_sku_map RENAME COLUMN sku TO sku_id';
  END IF;
END$$;

-- Pedidos confirmados (idempotentes por idem_key)
CREATE TABLE IF NOT EXISTS orders_confirmed (
  id           SERIAL PRIMARY KEY,
  org_id       TEXT NOT NULL,
  store_id     TEXT NOT NULL,
  sku_id       TEXT NOT NULL,
  qty          NUMERIC NOT NULL,
  approved_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  approved_by  TEXT,
  idem_key     TEXT NOT NULL,
  CONSTRAINT uq_orders_idem UNIQUE (org_id, store_id, sku_id, idem_key)
);

-- Transferencias confirmadas (idempotentes por idem_key)
CREATE TABLE IF NOT EXISTS transfers_confirmed (
  id           SERIAL PRIMARY KEY,
  org_id       TEXT NOT NULL,
  from_store   TEXT NOT NULL,
  to_store     TEXT NOT NULL,
  sku_id       TEXT NOT NULL,
  qty          NUMERIC NOT NULL,
  approved_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  approved_by  TEXT,
  idem_key     TEXT NOT NULL,
  CONSTRAINT uq_transfers_idem UNIQUE (org_id, from_store, to_store, sku_id, idem_key)
);

-- Inventario vivo por Org–Sucursal–SKU
CREATE TABLE IF NOT EXISTS inventory_levels (
  id         SERIAL PRIMARY KEY,
  org_id     TEXT NOT NULL,
  store_id   TEXT NOT NULL,
  sku_id     TEXT NOT NULL,
  on_hand    NUMERIC NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_inventory_key UNIQUE (org_id, store_id, sku_id)
);

CREATE INDEX IF NOT EXISTS ix_orders_org   ON orders_confirmed(org_id);
CREATE INDEX IF NOT EXISTS ix_transfers_org ON transfers_confirmed(org_id);
CREATE INDEX IF NOT EXISTS ix_inv_org      ON inventory_levels(org_id);

SELECT current_database(), current_user, now();
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM org_store_map;
SELECT COUNT(*) FROM org_sku_map;
SELECT COUNT(*) FROM events;
