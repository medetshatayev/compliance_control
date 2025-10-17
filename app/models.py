from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class ComplianceRequest(BaseModel):
    data: Dict[str, Any]  
    request_id: Optional[str] = None
    callback_url: Optional[str] = None

class ComplianceResponse(BaseModel):
    verdict: str
    checks: Dict[str, Any]  
