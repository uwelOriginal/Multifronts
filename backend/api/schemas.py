# api/schemas.py
from pydantic import BaseModel, Field
from typing import Any, List

class PublishIn(BaseModel):
    org_id: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)

class PollOut(BaseModel):
    events: List[dict]
    cursor: int

class HealthOut(BaseModel):
    ok: bool
