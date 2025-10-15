from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, BackgroundTasks
import httpx
from app.models import ComplianceRequest, ComplianceResponse
from app.query_builder import QueryBuilder
from app.lightrag_client import query_lightrag
import json, re
import logging


# Настройка логгера для явного вывода в stdout
import sys
logger = logging.getLogger("compliance_control")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)

app = FastAPI(title="Compliance Screening")

load_dotenv()

def extract_json(s: str):
    # extract the first JSON object in a text blob
    try:
        return json.loads(s)
    except Exception:
        pass
    # Try bracket-balanced scanning to handle extra text around JSON
    try:
        start_idx = s.find('{')
        while start_idx != -1:
            depth = 0
            for i in range(start_idx, len(s)):
                if s[i] == '{':
                    depth += 1
                elif s[i] == '}':
                    depth -= 1
                    if depth == 0:
                        candidate = s[start_idx:i+1]
                        try:
                            return json.loads(candidate)
                        except Exception:
                            break
            start_idx = s.find('{', start_idx + 1)
    except Exception:
        return None
    return None


@app.post("/compliance/check", response_model=ComplianceResponse)
async def compliance_check(req: ComplianceRequest, background_tasks: BackgroundTasks):
    try:
        data_obj = req.data or {}
        if isinstance(data_obj, dict) and "fields" in data_obj:
            query_builder = QueryBuilder.from_fields_data(data_obj)
        else:
            query_builder = QueryBuilder.from_flat_payload(data_obj)
        query_text = query_builder.build_query()
        logger.info("Prompt to LightRAG:\n%s", query_text)

        lr = await query_lightrag(query_text)
        logger.info("LightRAG raw response (lr): %s", repr(lr))

        raw = lr if isinstance(lr, str) else lr.get("response", "")
        parsed = extract_json(raw) if isinstance(raw, str) else None

        verdict = parsed.get("verdict", "flag") if parsed else "flag"
        checks = parsed.get("checks", {}) if parsed and "checks" in parsed else {}

        response = ComplianceResponse(
            verdict=verdict,
            checks=checks
        )

        if req.callback_url:
            payload = {
                "request_id": req.request_id,
                "verdict": response.verdict,
                "checks": response.checks,
            }
            async def _post_callback(url: str, body: dict):
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        await client.post(url, json=body)
                except Exception:
                    pass
            background_tasks.add_task(_post_callback, req.callback_url, payload)

        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/health")
async def health():
    return {"status": "ok"}
