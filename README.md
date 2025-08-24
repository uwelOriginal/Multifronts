# 🧭 MULTI FRONTS
## Inventory MVP (Streamlit + Data)

### Estructura
- `data/` con CSVs sintéticos:
  - `stores.csv`: catálogo de sucursales con lat/lon.
  - `skus.csv`: catálogo de productos (categoría, ABC, costo, precio, vida útil).
  - `promotions.csv`: ventanas promocionales por SKU-tienda (factor de uplift).
  - `lead_times.csv`: tiempo de entrega promedio y desviación por SKU-tienda.
  - `sales.csv`: ventas diarias por SKU-tienda (últimos {days} días).
  - `inventory_snapshot.csv`: inventario disponible por SKU-tienda en la fecha más reciente.

### Correr la app
```bash
pip install -r requirements.txt
python generate_data.py
streamlit run app.py
```

## Change History (V2)
- `app.py` (Streamlit) now computes ROP/S, suggests orders with explanations, and calls `optimizer.suggest_transfers` as a hook.
- `inventory.py` holds the ROP logic and enrichment helpers.
- `optimizer.py` includes a simple heuristic to propose transfers (to be replaced by an OR-Tools solver in Day 3).
- Put your CSVs under `data/` next to these files; the app auto-detects the folder.

## Change History (V3–4)
- **app.py**: Streamlit con ROP/S, pedidos con fórmulas LaTeX, solver de transferencias, write-back a CSV y registro de notificaciones.
- **inventory.py**: cálculo ROP/S, alias **RDP** (= ROP dinámico), y helpers de explicación.
- **optimizer.py**: usa **OR-Tools** si está disponible (fallback heurístico si no). Requiere `store_distances.csv` para costos por km (opcional).
- **notifier.py**: escribe `orders_confirmed.csv`, `transfers_confirmed.csv` y `notifications.csv` en `./data`.