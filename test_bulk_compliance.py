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
RETRIES: int = int(os.getenv("RETRIES", "3"))
REQUEST_TIMEOUT: float = float(os.getenv("REQUEST_TIMEOUT", "30.0"))
BACKOFF_BASE_SECONDS: float = float(os.getenv("BACKOFF_BASE_SECONDS", "0.5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("bulk-compliance")


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
                raw_text = await resp.aread()
                raw_text = raw_text.decode(errors="replace")

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
            await save_results(results_dir, result, file_path.parent.name)
            return result

        response = await send_compliance_request(session, input_data, file_path)
        result = build_result_record(file_path, input_data, response)
        await save_results(results_dir, result, file_path.parent.name)
        return result


async def save_results(results_dir: Path, result: Dict[str, Any], folder_name: str) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    uid = uuid.uuid4().hex[:6]
    base_name = f"result_{folder_name}_{ts_for_filename()}_{uid}"

    # JSON
    json_path = results_dir / f"{base_name}.json"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    # CSV
    csv_path = results_dir / f"{base_name}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["file_path", "folder_name", "timestamp", "status", "error", "response_json"])
        response = result.get("response") or {}
        writer.writerow([
            result.get("file_path"),
            result.get("folder_name"),
            result.get("timestamp"),
            response.get("status"),
            result.get("error") or response.get("error"),
            json.dumps(response.get("json"), ensure_ascii=False) if response.get("json") else None,
        ])


async def process_files() -> None:
    fb_files = find_fb_flat_files(ARCHIVE_PATH)
    logger.info(f"Found {len(fb_files)} file(s)")
    if not fb_files:
        return

    RESULTS_PATH.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    results: List[Dict[str, Any]] = []

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
        "results": results,
    }

    summary_path = RESULTS_PATH / f"summary_{ts_for_filename()}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Summary saved: {summary_path}")


def main() -> None:
    logger.info("🚀 Starting bulk compliance testing...")
    if not ARCHIVE_PATH.exists():
        logger.error(f"Archive directory not found: {ARCHIVE_PATH}")
        return
    asyncio.run(process_files())


if __name__ == "__main__":
    main()
