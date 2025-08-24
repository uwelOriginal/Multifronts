import numpy as np
import pandas as pd

# -----------------------------
# Z table + helpers
# -----------------------------
_Z_TABLE = [
    (0.80, 0.8416),
    (0.85, 1.036),
    (0.90, 1.2816),
    (0.95, 1.6449),
    (0.975, 1.96),
    (0.98, 2.054),
    (0.99, 2.3263),
]

def z_from_service_level(p: float) -> float:
    p = float(np.clip(p, 0.8, 0.99))
    ps, zs = zip(*_Z_TABLE)
    if p <= ps[0]:
        return zs[0]
    if p >= ps[-1]:
        return zs[-1]
    for (p0, z0), (p1, z1) in zip(_Z_TABLE[:-1], _Z_TABLE[1:]):
        if p0 <= p <= p1:
            t = (p - p0) / (p1 - p0)
            return z0 + t * (z1 - z0)
    return 1.6449

# -----------------------------
# ROP / Order-up-to (S)
# -----------------------------
def compute_rop_s(avg_daily_sales: float, lt_mean: float, lt_std: float, service_level: float = 0.95, order_up_factor: float = 1.0):
    avg_daily_sales = max(0.0, float(avg_daily_sales))
    lt_mean = max(0.0, float(lt_mean))
    lt_std = max(0.0, float(lt_std))
    z = z_from_service_level(service_level)

    mu_lt = avg_daily_sales * lt_mean
    sigma_lt = avg_daily_sales * lt_std
    rop = mu_lt + z * sigma_lt     # Reorder Point (Punto de Reorden dinámico = PRD/RDP)
    S = rop + order_up_factor * mu_lt
    return max(0.0, rop), max(0.0, S), mu_lt, sigma_lt, z

def latex_explanations(mu_lt, sigma_lt, z, rop, S, order_up_factor):
    # Returns dict of LaTeX strings to render nicely in Streamlit
    latex = {
        "mu": r"\mu_{LT} = \bar{d}\cdot LT_{\mathrm{mean}}",
        "sigma": r"\sigma_{LT} = \bar{d}\cdot LT_{\mathrm{std}}",
        "rop": r"\mathrm{ROP} = \mu_{LT} + z\cdot \sigma_{LT}",
        "S": r"S = \mathrm{ROP} + k\cdot \mu_{LT}",
        "values": rf"\mu_{{LT}}={mu_lt:.2f},\ \sigma_{{LT}}={sigma_lt:.2f},\ z={z:.2f},\ \mathrm{{ROP}}={rop:.2f},\ S={S:.2f},\ k={order_up_factor:.2f}"
    }
    return latex

def enrich_with_rop(df: pd.DataFrame, service_level: float = 0.95, order_up_factor: float = 1.0) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        for c in ["ROP","S_level","suggested_order_qty","order_explanation","RDP"]:
            out[c] = []
        return out

    def _row_calc(r):
        rop, S, mu_lt, sigma_lt, z = compute_rop_s(
            r.get("avg_daily_sales_28d", 0.0),
            r.get("lead_time_mean_days", 0.0),
            r.get("lead_time_std_days", 0.0),
            service_level, order_up_factor
        )
        on_hand = float(r.get("on_hand_units", 0.0))
        qty = max(0, int(np.ceil(S - on_hand)))
        if qty > 0:
            expl = (f"Inventario {on_hand:.0f} < ROP {rop:.1f} ⇒ sugerir pedido hasta S {S:.1f}. "
                    f"(μ_LT={mu_lt:.2f}, σ_LT={sigma_lt:.2f}, z={z:.2f}, k={order_up_factor:.2f})")
        else:
            expl = (f"Inventario suficiente (on hand {on_hand:.0f} ≥ ROP {rop:.1f}).")
        return pd.Series({"ROP": rop, "S_level": S, "RDP": rop, "suggested_order_qty": qty, "order_explanation": expl})

    extra = df.apply(_row_calc, axis=1)
    enriched = pd.concat([df.reset_index(drop=True), extra], axis=1)
    return enriched

def suggest_order_for_row(row: dict, service_level: float = 0.95, order_up_factor: float = 1.0):
    rop, S, mu_lt, sigma_lt, z = compute_rop_s(
        row.get("avg_daily_sales_28d", 0.0),
        row.get("lead_time_mean_days", 0.0),
        row.get("lead_time_std_days", 0.0),
        service_level, order_up_factor
    )
    on_hand = float(row.get("on_hand_units", 0.0))
    qty = max(0, int(np.ceil(S - on_hand)))
    latex = latex_explanations(mu_lt, sigma_lt, z, rop, S, order_up_factor)
    if qty > 0:
        expl = (f"Inventario {on_hand:.0f} < ROP {rop:.1f} ⇒ pedir {qty} para llegar a S {S:.1f}.")
        return {"qty": qty, "ROP": rop, "S": S, "latex": latex, "explanation": expl}
    else:
        return {"qty": 0, "ROP": rop, "S": S, "latex": latex, "explanation": f"Inventario suficiente (on hand {on_hand:.0f} ≥ ROP {rop:.1f})."}