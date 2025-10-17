import os, httpx, asyncio
from typing import Optional

BASE = os.getenv("LIGHTRAG_BASE_URL", "http://localhost:9621")

# Granular timeout configuration
_connect_timeout = float(os.getenv("LIGHTRAG_CONNECT_TIMEOUT", "10"))
_read_timeout = float(os.getenv("LIGHTRAG_READ_TIMEOUT", os.getenv("TIMEOUT_SECONDS", "180"))) # fallback to old var
_write_timeout = float(os.getenv("LIGHTRAG_WRITE_TIMEOUT", "180"))
_pool_timeout = float(os.getenv("LIGHTRAG_POOL_TIMEOUT", "60"))
TIMEOUTS = httpx.Timeout(
    connect=_connect_timeout,
    read=_read_timeout,
    write=_write_timeout,
    pool=_pool_timeout
)

# Optional retries for transient network errors
_retries = int(os.getenv("LIGHTRAG_RETRIES", "0"))
_backoff = float(os.getenv("LIGHTRAG_RETRY_BACKOFF", "1.5"))

async def query_lightrag(query_text: str):
    api_key = os.getenv("LIGHTRAG_API_KEY")
    headers = {}
    if api_key:
        headers["X-API-Key"] = api_key

    async with httpx.AsyncClient(timeout=TIMEOUTS) as client:
        body = {
            "query": query_text,
            "mode": "mix",
            "response_type": "JSON",
            # Fast configuration - optimized for speed and cost
            "kg_top_k": 10,
            "chunk_top_k": 8,
            "enable_rerank": False,
            # Token limits: entity + relation + buffer = total
            "max_entity_tokens": 2000,
            "max_relation_tokens": 2500,
            "max_total_tokens": 6000,  # 2000 + 2500 + 1500 buffer
            # Misc flags
            "only_need_context": False,
            "stream": False,
            "history_turns": 0
        }
        
        last_exc = None
        for attempt in range(_retries + 1):
            try:
                r = await client.post(f"{BASE}/query", json=body, headers=headers)
                r.raise_for_status()
                return r.json()
            except httpx.RequestError as e:
                last_exc = e
                if attempt < _retries:
                    await asyncio.sleep(_backoff ** attempt)
        raise last_exc # re-raise the last exception if all retries fail
