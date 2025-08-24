import json
import requests
import pandas as pd
from datetime import datetime, timezone

def _row_ts_iso(row) -> str:
    if hasattr(row, "ts_iso") and str(row.ts_iso):
        return str(row.ts_iso)
    return datetime.now(timezone.utc).isoformat()

def send_slack_notifications(notif_df: pd.DataFrame, webhook: str) -> tuple[bool, str]:
    """
    Espera columnas:
      - kind: "order" | "transfer"
      - org_id (opcional)
      - actor  (email/usuario)
      - ts_iso (ISO8601 UTC) opcional; se rellena si falta
      - Para order: store_id, sku_id, qty
      - Para transfer: from_store, to_store, sku_id, qty
    """
    if not webhook:
        return False, "Webhook vacío."
    if notif_df is None or notif_df.empty:
        return False, "No hay notificaciones pendientes."

    blocks = [{"type": "header", "text": {"type": "plain_text", "text": "Inventario — Movimientos aprobados"}}]

    for r in notif_df.itertuples(index=False):
        try:
            kind = getattr(r, "kind", "unknown")
            actor = getattr(r, "actor", "desconocido")
            org_id = getattr(r, "org_id", "")
            ts = _row_ts_iso(r)

            if kind == "order":
                store = r.store_id
                sku = r.sku_id
                qty = int(r.qty)
                affected = f"Pedido — Sucursal *{store}*, SKU *{sku}*, Cant. *{qty}*"
                icon = "🧾"
            elif kind == "transfer":
                frm = r.from_store
                to  = r.to_store
                sku = r.sku_id
                qty = int(r.qty)
                affected = f"Transferencia — *{frm}* → *{to}*, SKU *{sku}*, Cant. *{qty}*"
                icon = "🔁"
            else:
                affected = json.dumps(r._asdict(), ensure_ascii=False)
                icon = "ℹ️"

            lines = [
                f"{icon} *{kind.upper()}* {'('+org_id+')' if org_id else ''}",
                f"{affected}",
                f"_Aprobado por_: *{actor}*    ·    _Hora (UTC)_: `{ts}`",
            ]
            txt = "\n".join(lines)
        except Exception:
            txt = str(r)

        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": txt}})

    try:
        resp = requests.post(webhook, json={"blocks": blocks}, timeout=10)
        if resp.status_code != 200:
            return False, f"Slack respondió {resp.status_code}: {resp.text}"
        return True, "Notificaciones enviadas a Slack."
    except Exception as e:
        return False, f"Error de red: {e}"
