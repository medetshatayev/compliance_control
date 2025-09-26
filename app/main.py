from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks
import httpx
from app.models import ComplianceRequest, ComplianceResponse
from app.query_builder import QueryBuilder
from app.lightrag_client import query_lightrag
import json, re

app = FastAPI(title="Compliance Screening")

load_dotenv()

def extract_json(s: str):
    # extract the first JSON object in a text blob
    try:
        return json.loads(s)
    except Exception:
        pass
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None

# ...existing code...

@app.post("/compliance/check", response_model=ComplianceResponse)
async def compliance_check(req: ComplianceRequest, background_tasks: BackgroundTasks):
    try:
        query_builder = QueryBuilder(req.data)
        query_text = query_builder.build_query()
        lr = await query_lightrag(query_text)
        # LightRAG may return either a dict with 'response' or a string
        raw = lr if isinstance(lr, str) else lr.get("response", "")
        parsed = extract_json(raw) if isinstance(raw, str) else None

        # Decide verdict conservatively
        verdict = "flag"
        risk = "medium"
        checks = {}

        if parsed:
            # Только новый формат
            has_sanctions = False
            
            # Проверяем стороны
            if parsed.get("proverka_storon"):
                for country_data in parsed["proverka_storon"].values():
                    if country_data.get("verdict") == True:
                        has_sanctions = True
                        break
            
            # Проверяем товары
            if parsed.get("goods"):
                for country_data in parsed["goods"].values():
                    if country_data.get("verdict") == True:
                        has_sanctions = True
                        break
            
            verdict = "flag" if has_sanctions else "clear"
            risk = "medium" if has_sanctions else "none"
            checks = parsed  # Весь parsed JSON идёт в checks
        else:
            verdict = "flag"
            risk = "medium"
            checks = {}
        response = ComplianceResponse(
            verdict=verdict,
            risk_level=risk,
            checks=checks if isinstance(checks, dict) else {},
            lightrag_response=raw if isinstance(raw, str) else str(lr)
        )
        
        # Optional background callback delivery
        if req.callback_url:
            payload = {
                "request_id": req.request_id,
                "verdict": response.verdict,
                "risk_level": response.risk_level,
                "checks": response.checks,
                "lightrag_response": response.lightrag_response,
            }

            async def _post_callback(url: str, body: dict):
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        await client.post(url, json=body)
                except Exception:
                    # Swallow callback errors to not affect main response
                    pass

            background_tasks.add_task(_post_callback, req.callback_url, payload)

        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ...existing code...


@app.get("/health")
async def health():
    return {"status": "ok"}
