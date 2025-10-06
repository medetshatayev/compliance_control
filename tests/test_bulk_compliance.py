import os
import json
import uuid
import csv
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import httpx

BASE_URL: str = os.getenv("BASE_URL", "http://localhost:8000/").rstrip("/")
ARCHIVE_PATH: Path = Path(os.getenv("ARCHIVE_PATH", "../Archive")).resolve()
RESULTS_PATH: Path = Path(os.getenv("RESULTS_PATH", "../results")).resolve()

CONCURRENCY: int = int(os.getenv("CONCURRENCY", "5"))
RETRIES: int = int(os.getenv("RETRIES", "18"))
REQUEST_TIMEOUT: float = float(os.getenv("REQUEST_TIMEOUT", "30.0"))
BACKOFF_BASE_SECONDS: float = float(os.getenv("BACKOFF_BASE_SECONDS", "0.5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("bulk-compliance")

# --- EXACT columns requested by user (no test_id) ---
CSV_COLUMNS = [
    "file_path",
    "folder_name",
    "timestamp",
    "INPUT_BIK_SWIFT",
    "INPUT_CONTRACT_CURRENCY",
    "INPUT_PAYMENT_CURRENCY",
    "INPUT_CURRENCY_CONTRACT_NUMBER",
    "INPUT_CONTRACT_AMOUNT_TYPE",
    "INPUT_CONSIGNOR",
    "INPUT_CONSIGNEE",
    "INPUT_CONTRACT_DATE",
    "INPUT_CONTRACT_END_DATE",
    "INPUT_PRODUCT_CATEGORY",
    "INPUT_CLIENT",
    "INPUT_CURRENCY_CONTRACT_TYPE_CODE",
    "INPUT_COUNTERPARTY_NAME",
    "INPUT_PRODUCT_NAME",
    "INPUT_CONTRACT_DESCRIPTION",
    "INPUT_CROSS_BORDER",
    "INPUT_MANUFACTURER",
    "INPUT_PAYMENT_METHOD",
    "INPUT_REPATRIATION_TERM",
    "INPUT_DOCUMENT_REFERENCES",
    "INPUT_COUNTERPARTY_COUNTRY",
    "INPUT_AMOUNT",
    "INPUT_HS_CODE",
    "INPUT_CONTRACT_TYPE",
    "INPUT_THIRD_PARTIES",
    "INPUT_UN_CODE",
    "INPUT_CONTRACT_TYPE_SYSTEM",
    "INPUT_BANK",
    "INPUT_ROUTE",
    "OUTPUT_verdict",
    "OUTPUT_explanation",
    "OUTPUT_risk_level",
    "OUTPUT_details",
]

GLOBAL_CSV_LOCK = asyncio.Lock()
GLOBAL_CSV_PATH: Optional[Path] = None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts_for_filename() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]


def find_fb_flat_files(archive_path: Path) -> List[Path]:
    return list(archive_path.rglob("fb_flat.json"))


async def send_compliance_request(
        session: httpx.AsyncClient,
        data: Dict[str, Any],
        file_path: Path,
) -> Dict[str, Any]:
    url = f"{BASE_URL}/compliance/check"
    payload = {
        "data": data,
        "request_id": f"test_{now_utc_iso()}_{uuid.uuid4().hex[:8]}",
    }

    last_error: Optional[Exception] = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = await session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            status = resp.status_code

            parsed_json = None
            raw_text = None
            try:
                parsed_json = resp.json()
            except Exception:
                raw_bytes = await resp.aread()
                raw_text = raw_bytes.decode(errors="replace")

            if 200 <= status < 300:
                return {"ok": True, "status": status, "json": parsed_json, "text": raw_text, "error": None}
            return {"ok": False, "status": status, "json": parsed_json, "text": raw_text, "error": f"HTTP {status}"}

        except (httpx.HTTPError, httpx.TimeoutException) as e:
            last_error = e
            if attempt < RETRIES:
                await asyncio.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))

    return {"ok": False, "status": None, "json": None, "text": None, "error": f"Network error: {last_error}"}


def build_result_record(
        file_path: Path,
        input_data: Optional[Dict[str, Any]],
        response: Optional[Dict[str, Any]],
        error: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "file_path": str(file_path),
        "folder_name": file_path.parent.name,
        "timestamp": now_utc_iso(),
        "input_data": input_data,
        "response": response,
        "error": error,
    }


def is_success(result: Dict[str, Any]) -> bool:
    if result.get("error"):
        return False
    resp = result.get("response") or {}
    return bool(resp.get("ok") is True)


def _get_from_input(input_data: Optional[Dict[str, Any]], field: str) -> Optional[Any]:
    """
    Tries several fallbacks to extract the value from input JSON.
    Field is like 'INPUT_BIK_SWIFT' — we try:
      - input_data['INPUT_BIK_SWIFT']
      - input_data['BIK_SWIFT'] (without prefix)
      - input_data['bik_swift'] (lower)
      - input_data.get('input', {}).get('bik_swift') (nested)
      - stringified fallback if value exists under similar key
    """
    if not input_data:
        return None
    if field in input_data:
        return input_data[field]
    key = field
    if key.startswith("INPUT_"):
        key_no = key[len("INPUT_"):]
    else:
        key_no = key
    if key_no in input_data:
        return input_data[key_no]
    lower = key_no.lower()
    if lower in input_data:
        return input_data[lower]
    for parent in ("input", "INPUT", "payload", "data"):
        pv = input_data.get(parent)
        if isinstance(pv, dict):
            if key in pv:
                return pv[key]
            if key_no in pv:
                return pv[key_no]
            if lower in pv:
                return pv[lower]
    for k, v in input_data.items():
        if isinstance(k, str) and key_no.lower() in k.lower():
            return v
    return None


def _extract_output_fields(response: Optional[Dict[str, Any]]) -> Dict[str, Optional[Any]]:
    """
    Extract common output fields from the response structure.
    We expect response to be the dict returned by send_compliance_request:
      {"ok": True/False, "status": int, "json": {...}, "text": "...", "error": ...}
    The real API payload (resp_json) is likely under response["json"].
    We'll try multiple fallbacks to get verdict/explanation/risk_level/details.
    """
    out = {"OUTPUT_verdict": None, "OUTPUT_explanation": None, "OUTPUT_risk_level": None, "OUTPUT_details": None}
    if not response:
        return out

    resp_json = response.get("json") or {}
    candidates = []
    if isinstance(resp_json, dict):
        candidates.append(resp_json)
    if isinstance(resp_json.get("output"), dict):
        candidates.append(resp_json["output"])
    if isinstance(resp_json.get("result"), dict):
        candidates.append(resp_json["result"])
    candidates.append(response)

    for c in candidates:
        if not isinstance(c, dict):
            continue
        if out["OUTPUT_verdict"] is None:
            out["OUTPUT_verdict"] = c.get("verdict") or c.get("decision") or c.get("status")
        if out["OUTPUT_explanation"] is None:
            out["OUTPUT_explanation"] = c.get("explanation") or c.get("reason") or c.get("message")
        if out["OUTPUT_risk_level"] is None:
            out["OUTPUT_risk_level"] = c.get("risk_level") or c.get("risk") or c.get("level")
        if out["OUTPUT_details"] is None:
            details = c.get("details") or c.get("data") or c.get("debug")
            if details is not None:
                try:
                    out["OUTPUT_details"] = json.dumps(details, ensure_ascii=False)
                except Exception:
                    out["OUTPUT_details"] = str(details)

    if not any([out["OUTPUT_verdict"], out["OUTPUT_explanation"], out["OUTPUT_risk_level"], out["OUTPUT_details"]]):
        try:
            out["OUTPUT_details"] = json.dumps(resp_json, ensure_ascii=False)
        except Exception:
            out["OUTPUT_details"] = str(resp_json)

    return out


async def write_row_to_global_csv(csv_path: Path, lock: asyncio.Lock, row: Dict[str, Any], write_header: bool = False) -> None:
    async with lock:
        mode = "a"
        with open(csv_path, mode, newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(CSV_COLUMNS)
            ordered = []
            for col in CSV_COLUMNS:
                val = row.get(col)
                if isinstance(val, (dict, list)):
                    try:
                        val = json.dumps(val, ensure_ascii=False)
                    except Exception:
                        val = str(val)
                ordered.append("" if val is None else val)
            writer.writerow(ordered)



async def handle_one(
        session: httpx.AsyncClient,
        file_path: Path,
        semaphore: asyncio.Semaphore,
        results_dir: Path,
) -> Dict[str, Any]:
    async with semaphore:
        logger.info(f"Processing: {file_path}")

        input_data: Optional[Dict[str, Any]] = None
        try:
            text = file_path.read_text(encoding="utf-8")
            input_data = json.loads(text)
        except Exception as e:
            result = build_result_record(file_path, None, None, error=f"JSON read error: {e}")
            row = {
                "file_path": str(file_path),
                "folder_name": file_path.parent.name,
                "timestamp": now_utc_iso(),
            }
            for col in CSV_COLUMNS:
                row.setdefault(col, None)
            row["OUTPUT_details"] = f"JSON read error: {e}"
            await write_row_to_global_csv(GLOBAL_CSV_PATH, GLOBAL_CSV_LOCK, row)
            await save_individual_result(results_dir, result, file_path.parent.name)
            return result

        response = await send_compliance_request(session, input_data, file_path)
        result = build_result_record(file_path, input_data, response)
        await save_individual_result(results_dir, result, file_path.parent.name)

        row: Dict[str, Any] = {
            "file_path": str(file_path),
            "folder_name": file_path.parent.name,
            "timestamp": now_utc_iso(),
        }
        
        for col in CSV_COLUMNS:
            if col.startswith("INPUT_"):
                row[col] = _get_from_input(input_data, col)

        out_fields = _extract_output_fields(response)
        row.update(out_fields)

        # ensure all CSV columns exist
        for col in CSV_COLUMNS:
            row.setdefault(col, None)

        # append to global csv
        await write_row_to_global_csv(GLOBAL_CSV_PATH, GLOBAL_CSV_LOCK, row)

        return result


async def save_individual_result(results_dir: Path, result: Dict[str, Any], folder_name: str) -> None:
    """Saves the full JSON result file (kept for debugging)"""
    results_dir.mkdir(parents=True, exist_ok=True)
    uid = uuid.uuid4().hex[:6]
    base_name = f"result_{folder_name}_{ts_for_filename()}_{uid}"

    # JSON
    json_path = results_dir / f"{base_name}.json"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


async def process_files() -> None:
    global GLOBAL_CSV_PATH
    fb_files = find_fb_flat_files(ARCHIVE_PATH)
    logger.info(f"Found {len(fb_files)} file(s)")
    if not fb_files:
        return

    RESULTS_PATH.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    results: List[Dict[str, Any]] = []

    csv_name = f"batch_results_{ts_for_filename()}.csv"
    GLOBAL_CSV_PATH = RESULTS_PATH / csv_name

    # сразу пишем фиксированный заголовок
    with open(GLOBAL_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)

    async with httpx.AsyncClient() as session:
        tasks = [handle_one(session, fp, semaphore, RESULTS_PATH) for fp in fb_files]
        for coro in asyncio.as_completed(tasks):
            res = await coro
            results.append(res)

    total = len(results)
    successes = sum(1 for r in results if is_success(r))
    failed = total - successes

    summary = {
        "total_files": len(fb_files),
        "processed_successfully": successes,
        "failed": failed,
        "timestamp": now_utc_iso(),
        "results_count": len(results),
    }

    summary_path = RESULTS_PATH / f"summary_{ts_for_filename()}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Summary saved: {summary_path}")
    logger.info(f"Global CSV saved: {GLOBAL_CSV_PATH}")



def main() -> None:
    logger.info("🚀 Starting bulk compliance testing (single CSV output)...")
    if not ARCHIVE_PATH.exists():
        logger.error(f"Archive directory not found: {ARCHIVE_PATH}")
        return
    asyncio.run(process_files())


if __name__ == "__main__":
    main()
