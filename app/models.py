from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class ComplianceRequest(BaseModel):
    data: Dict[str, Any]  
    request_id: Optional[str] = None
    callback_url: Optional[str] = None

class CountryVerdict(BaseModel):
    verdict: bool
    explanation: str

class GoodsCountryVerdict(BaseModel):
    verdict: bool
    explanation: str
    hs_code: str

class ProverkaStoron(BaseModel):
    us: CountryVerdict
    uk: CountryVerdict
    eu: CountryVerdict

class Goods(BaseModel):
    us: GoodsCountryVerdict
    uk: GoodsCountryVerdict
    eu: GoodsCountryVerdict

class NewFormatChecks(BaseModel):
    proverka_storon: ProverkaStoron
    route: Optional[str] = None
    contract_type: Optional[str] = None
    goods: Optional[Goods] = None

class ComplianceResponse(BaseModel):
    verdict: str
    risk_level: str
    checks: Dict[str, Any]  
    lightrag_response: str