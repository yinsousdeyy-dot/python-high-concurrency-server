from pydantic import BaseModel, Field

class PayloadRequest(BaseModel):
    client_id: str = Field(..., min_length=1)
    event_type: str = Field(..., min_length=1)
    timestamp: int
