"""API configuration — all settings come from the environment.

No secrets are hardcoded here. The service connects to ClickHouse with a
dedicated read-only user (``CH_API_USER``, default ``chain_api``) that has
SELECT-only grants on ``wallet_memory.*`` — the raw-SQL endpoint is safe
even if application-level validation is bypassed.
"""

from __future__ import annotations

import os


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


# ── ClickHouse ─────────────────────────────────────────────────────────
CH_HOST: str = os.getenv("CH_API_HOST") or os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST") or "127.0.0.1"
CH_PORT: int = _get_int("CH_API_PORT", _get_int("CH_PORT", _get_int("CLICKHOUSE_PORT", 9000)))
CH_USER: str = os.getenv("CH_API_USER") or os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER") or "chain_api"
CH_PASSWORD: str = os.getenv("CH_API_PASSWORD") or os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or ""
CH_DATABASE: str = os.getenv("CH_API_DATABASE") or os.getenv("CH_DATABASE") or "wallet_memory"

# Per-query guardrails (enforced client-side via clickhouse_driver settings).
CH_QUERY_TIMEOUT: int = _get_int("CH_API_QUERY_TIMEOUT", 30)  # seconds, max_execution_time
CH_QUERY_MAX_RESULT_ROWS: int = _get_int("CH_API_MAX_RESULT_ROWS", 10_000)

# ── Rate limiting ──────────────────────────────────────────────────────
# Free tier: 100 requests / minute per IP. Behind nginx the client IP is
# taken from X-Forwarded-For (first hop), else the socket address.
RATE_LIMIT_PER_MINUTE: int = _get_int("API_RATE_LIMIT_PER_MINUTE", 100)
RATE_LIMIT_KEY_PREFIX: str = os.getenv("API_RATE_LIMIT_KEY_PREFIX") or "chain-api"

# ── Pagination ─────────────────────────────────────────────────────────
DEFAULT_PAGE_LIMIT: int = _get_int("API_DEFAULT_PAGE_LIMIT", 100)
MAX_PAGE_LIMIT: int = _get_int("API_MAX_PAGE_LIMIT", 1000)
MAX_PAGE_OFFSET: int = _get_int("API_MAX_PAGE_OFFSET", 100_000)

# ── Logging ────────────────────────────────────────────────────────────
LOG_LEVEL: str = (os.getenv("LOG_LEVEL") or "INFO").upper()

# ── Service ────────────────────────────────────────────────────────────
API_TITLE: str = os.getenv("API_TITLE") or "Chain Query API"
API_VERSION: str = os.getenv("API_VERSION") or "1.0.0"

# ── LanceDB vector search ──────────────────────────────────────────────
# Vector collections are served from the same LANCE_DIR the indexer writes
# to. When the directory is unreachable the search endpoints return 503.
LANCE_DIR: str = os.getenv("LANCE_DIR") or "/var/data/lance"
EMBEDDING_PROVIDER: str = (os.getenv("EMBEDDING_PROVIDER") or "infinity").lower()
EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL") or "BAAI/bge-m3"
EMBEDDING_BASE_URL: str = os.getenv(
    "EMBEDDING_BASE_URL",
    "http://localhost:8001"
    if (os.getenv("EMBEDDING_PROVIDER") or "infinity").lower() in ("infinity", "ollama")
    else "https://api.openai.com/v1",
)
EMBEDDING_API_KEY: str = os.getenv("EMBEDDING_API_KEY") or ""
VECTOR_SEARCH_ENABLED: bool = (os.getenv("VECTOR_SEARCH_ENABLED") or "1") == "1"
