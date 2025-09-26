from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class ComplianceRequest(BaseModel):
    data: Dict[str, Any]  
    request_id: Optional[str] = None
    callback_url: Optional[str] = None

class Hit(BaseModel):
    matched: bool
    notes: Optional[str] = None

class EntityHit(Hit):
    name: str
    match_type: Optional[str] = None
    list: Optional[str] = None
    source: Optional[str] = None

class BankHit(Hit):
    swift: str
    list: Optional[str] = None
    source: Optional[str] = None

class GoodsHit(Hit):
    hs_code: Optional[str] = None
    control_basis: Optional[str] = None
    source: Optional[str] = None

class JurisdictionHit(Hit):
    route: str
    control_basis: Optional[str] = None
    source: Optional[str] = None

class ComplianceResponse(BaseModel):
    verdict: str
    risk_level: str
    checks: Dict[str, List[Dict[str, Any]]]
    lightrag_response: str
    parsed_json: Optional[Dict[str, Any]] = None
