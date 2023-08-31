"""Import Pydantic models here to make them available via a single import on routes."""
from pydantic import BaseModel
from typing import Optional

class AcquireLockRequest(BaseModel):
    lock_key: str
    expire_time: int = 40
    
class ExampleRequest(BaseModel):
    param1: int
    param2: int