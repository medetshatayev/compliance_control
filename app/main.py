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


@app.post("/compliance/check", response_model=ComplianceResponse)
async def compliance_check(req: ComplianceRequest, background_tasks: BackgroundTasks):
    try:
        data_obj = req.data or {}
        if isinstance(data_obj, dict) and "fields" in data_obj:
            query_builder = QueryBuilder.from_fields_data(data_obj)
        else:
            query_builder = QueryBuilder.from_flat_payload(data_obj)
        query_text = query_builder.build_query()
        lr = await query_lightrag(query_text)
        # LightRAG may return either a string, or a dict (optionally with 'response')
        raw_response = lr if not isinstance(lr, dict) else lr.get("response", lr)
        if isinstance(raw_response, (dict, list)):
            parsed = raw_response
        elif isinstance(raw_response, str):
            parsed = extract_json(raw_response)
        else:
            parsed = None


        # Decide verdict conservatively
        verdict = "flag"
        checks = {
            "check_parties": {
                "us": {"verdict": False},
                "uk": {"verdict": False},
                "eu": {"verdict": False},
            },
            "route": None,
            "contract_type": None,
            "goods": {
                "us": {"verdict": False, "hs code": "N/A"},
                "uk": {"verdict": False, "hs code": "N/A"},
                "eu": {"verdict": False, "hs code": "N/A"},
            },
            "explanation": "",
        }

        if isinstance(parsed, dict):
            src_checks = parsed.get("checks", parsed)

            check_parties_src = {}
            if isinstance(src_checks, dict):
                check_parties_src = src_checks.get("check_parties") or src_checks.get("check parties") or {}

            goods_src = src_checks.get("goods") if isinstance(src_checks, dict) else {}

            checks["check_parties"] = {
                "us": (check_parties_src.get("us") if isinstance(check_parties_src, dict) else None) or {"verdict": False},
                "uk": (check_parties_src.get("uk") if isinstance(check_parties_src, dict) else None) or {"verdict": False},
                "eu": (check_parties_src.get("eu") if isinstance(check_parties_src, dict) else None) or {"verdict": False},
            }

            checks["goods"] = {
                "us": (goods_src.get("us") if isinstance(goods_src, dict) else None) or {"verdict": False, "hs code": "N/A"},
                "uk": (goods_src.get("uk") if isinstance(goods_src, dict) else None) or {"verdict": False, "hs code": "N/A"},
                "eu": (goods_src.get("eu") if isinstance(goods_src, dict) else None) or {"verdict": False, "hs code": "N/A"},
            }

            if isinstance(src_checks, dict):
                checks["route"] = src_checks.get("route")
                checks["contract_type"] = src_checks.get("contract_type")

                explanations = []
                for part in (src_checks, check_parties_src, goods_src):
                    if isinstance(part, dict):
                        exp = part.get("explanation")
                        if isinstance(exp, str) and exp.strip():
                            explanations.append(exp.strip())
                if explanations:
                    checks["explanation"] = " ".join(explanations).strip()

            def any_true(verdicts_dict):
                if not isinstance(verdicts_dict, dict):
                    return False
                for v in verdicts_dict.values():
                    if isinstance(v, dict) and v.get("verdict") is True:
                        return True
                return False

            has_sanctions = any_true(checks["check_parties"]) or any_true(checks["goods"])
            verdict = "flag" if has_sanctions else "clear"

        response = ComplianceResponse(
            verdict=verdict,
            checks=checks if isinstance(checks, dict) else {},
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
