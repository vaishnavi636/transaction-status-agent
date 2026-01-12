from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from datetime import datetime

class Decision(BaseModel):
    transaction_id: str
    status: str
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str
    evidence: Dict[str, Any] = Field(default_factory=dict)
    rules_fired: List[str] = Field(default_factory=list)
    updated_at: Optional[datetime] = None
