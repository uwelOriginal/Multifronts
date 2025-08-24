from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI(title="Inventory MVP API", version="0.1.0")

class ForecastRequest(BaseModel):
    store_id: str
    sku_id: str
    horizon_days: int = 14

class ForecastResponse(BaseModel):
    store_id: str
    sku_id: str
    horizon_days: int
    method: str = "avg_28d"
    daily_forecast: List[float]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/forecast", response_model=ForecastResponse)
def forecast(req: ForecastRequest):
    # Placeholder: devolver forecast plano para MVP Day 1
    daily = [5.0 for _ in range(req.horizon_days)]
    return ForecastResponse(store_id=req.store_id, sku_id=req.sku_id, horizon_days=req.horizon_days, daily_forecast=daily)