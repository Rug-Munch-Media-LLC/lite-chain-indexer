"""API configuration — all settings come from the environment.

No secrets are hardcoded here. The service connects to ClickHouse with a
dedicated read-only user (``CH_API_USER``, default ``chain_api``) that has
SELECT-only grants on ``wallet_memory.*`` — the raw-SQL endpoint is safe
even if application-level validation is bypassed.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        logger.debug("config value %s is not an int; using default", name)
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        logger.debug("config value %s is not a float; using default", name)
        return default


# ── ClickHouse ─────────────────────────────────────────────────────────
CH_HOST: str = os.getenv("CH_API_HOST") or os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST") or "127.0.0.1"
CH_PORT: int = _get_int("CH_API_PORT", _get_int("CH_PORT", _get_int("CLICKHOUSE_PORT", 9000)))
CH_USER: str = os.getenv("CH_API_USER") or os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER") or "chain_api"
CH_PASSWORD: str = os.getenv("CH_API_PASSWORD") or os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or ""
CH_DATABASE: str = os.getenv("CH_API_DATABASE") or os.getenv("CH_DATABASE") or "wallet_memory"

# ── ClickHouse write path (label graph) ────────────────────────────────
# Persisting labels needs INSERT privileges, which the read-only
# ``chain_api`` user deliberately does not have. These credentials are
# used only by ``api/label_store.py`` for ``POST /api/v1/labels``. They
# fall back to the generic ClickHouse vars, then to the native ``default``
# user (the indexer's own writer identity). Never hardcode these.
CH_WRITE_HOST: str = os.getenv("CH_WRITE_HOST") or os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST") or "127.0.0.1"
CH_WRITE_PORT: int = _get_int("CH_WRITE_PORT", _get_int("CH_PORT", _get_int("CLICKHOUSE_PORT", 9000)))
CH_WRITE_USER: str = os.getenv("CH_WRITE_USER") or os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER") or "default"
CH_WRITE_PASSWORD: str = (
    os.getenv("CH_WRITE_PASSWORD") or os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or ""
)
CH_WRITE_DATABASE: str = os.getenv("CH_WRITE_DATABASE") or os.getenv("CH_DATABASE") or "wallet_memory"

# ── Label write API guard (fail-closed) ─────────────────────────────────
# Shared-secret bearer token for POST /api/v1/labels. There is NO default:
# an unset token is a misconfiguration, not an invitation. The app refuses
# to start with a write route mounted (see api/write_guard.py) and the
# write itself 503s (see api/routers/labels.py) so the hole cannot be
# opened by flipping one env var in the wrong direction.
LABEL_API_TOKEN: str = os.getenv("LABEL_API_TOKEN") or ""
LABEL_API_WRITE_ENABLED: bool = (os.getenv("LABEL_API_WRITE_ENABLED") or "1") == "1"
# Explicit, greppable opt-out for the local dev flow only. Any value other
# than "1" leaves the guard armed. Never set this on a shared deployment.
API_ALLOW_UNAUTHENTICATED_WRITE: bool = (os.getenv("API_ALLOW_UNAUTHENTICATED_WRITE") or "0") == "1"

# Per-query guardrails (enforced client-side via clickhouse_driver settings).
CH_QUERY_TIMEOUT: int = _get_int("CH_API_QUERY_TIMEOUT", 30)  # seconds, max_execution_time
CH_QUERY_MAX_RESULT_ROWS: int = _get_int("CH_API_MAX_RESULT_ROWS", 10_000)

# ── Rate limiting ──────────────────────────────────────────────────────
# Free tier: 100 requests / minute per IP. The client IP is taken from the
# SOCKET peer by default. Behind nginx the peer is always 127.0.0.1, so set
# TRUSTED_PROXY_HOPS to the number of proxies in front of this app and the
# rightmost that many X-Forwarded-For hops are used. The leftmost entry is
# client-supplied and spoofable, so it is never trusted.
RATE_LIMIT_PER_MINUTE: int = _get_int("API_RATE_LIMIT_PER_MINUTE", 100)
RATE_LIMIT_KEY_PREFIX: str = os.getenv("API_RATE_LIMIT_KEY_PREFIX") or "chain-api"
# 0 = trust no proxy (use the socket peer). See api/app.py::_client_ip.
TRUSTED_PROXY_HOPS: int = _get_int("API_TRUSTED_PROXY_HOPS", 0)

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
EMBEDDING_MODE: str = (os.getenv("EMBEDDING_MODE") or "auto").lower()
EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL") or "Qwen/Qwen3-Embedding-4B"
DEEPINFRA_EMBED_URL: str = os.getenv("DEEPINFRA_EMBED_URL") or "https://api.deepinfra.com/v1/openai"
LOCAL_EMBED_URL: str = os.getenv("LOCAL_EMBED_URL") or "http://127.0.0.1:8004/v1"
DEEPINFRA_API_KEY: str = os.getenv("DEEPINFRA_API_KEY") or ""
VECTOR_SEARCH_ENABLED: bool = (os.getenv("VECTOR_SEARCH_ENABLED") or "1") == "1"
