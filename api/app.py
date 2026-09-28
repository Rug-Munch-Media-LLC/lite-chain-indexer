"""FastAPI application factory for the Chain Query API.

Wires together:

* the versioned routers (``/api/v1/...``),
* per-IP rate limiting via ``slowapi`` (default 100 req/min, stricter on
  the raw query endpoint),
* Prometheus request metrics,
* permissive CORS (public read-only data, no auth).

Run with: ``uvicorn api.main:app --host 0.0.0.0 --port 8100``
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from api import __version__
from api.config import (
    API_TITLE,
    API_VERSION,
    RATE_LIMIT_KEY_PREFIX,
    RATE_LIMIT_PER_MINUTE,
    TRUSTED_PROXY_HOPS,
)
from api.db import get_historical
from api.graphql import graphql_router
from api.routers import address, deployers, launches, search, stats, system, transfers, wallet_graph, whales
from api.routers import labels as labels_router
from api.routers import query as query_router
from api.write_guard import assert_write_auth_configured
from lite_indexer.config import WEBHOOK_MAX_PER_IP
from lite_indexer.webhooks import router as webhooks
from lite_indexer.webhooks.auth import assert_webhook_auth_configured

REQUEST_COUNT = Counter(
    "chain_api_requests_total",
    "Total Chain Query API requests",
    ["method", "path", "status"],
)


def _client_ip(request: Request) -> str:
    """Resolve the real client IP for the per-IP rate limit.

    The LEFTMOST X-Forwarded-For entry is the client-supplied value, so
    trusting it lets any caller mint a fresh bucket per request and bypass
    the limit entirely. The correct hop is the LAST one: each proxy in the
    chain appends the address it actually observed, so the rightmost entry
    is only trustworthy if every hop is ours.

    ``TRUSTED_PROXY_HOPS`` says how many trusted proxies sit in front of
    this app. We count back that many entries; if a caller supplied more
    hops than we have proxies, the extra leading entries are forged and we
    fall back to the socket peer. With no trusted proxy configured the
    default is 0 — the socket address — so a client-supplied header is
    never read unless an operator opts in.
    """
    client = request.client
    if not client:
        return "unknown"
    return client_ip_from_headers(
        client.host or "unknown", request.headers.get("x-forwarded-for", ""), TRUSTED_PROXY_HOPS
    )


def client_ip_from_headers(peer: str, forwarded: str, hops: int) -> str:
    """Pure: resolve the rate-limit key. Split from :func:`_client_ip`.

    Testable with plain strings, so the hop-counting rule needs no
    monkeypatched module global.

    Args:
        peer: The socket peer address.
        forwarded: The raw ``X-Forwarded-For`` header value, if any.
        hops: Number of trusted proxies in front of this app.

    Returns:
        The socket peer when no proxy is trusted or the chain is too long;
        otherwise the rightmost trusted hop.
    """
    if hops <= 0:
        return peer
    chain = [hop.strip() for hop in forwarded.split(",") if hop.strip()]
    if len(chain) < hops:
        return peer
    return chain[-hops]


limiter = Limiter(
    key_func=_client_ip,
    default_limits=[f"{RATE_LIMIT_PER_MINUTE}/minute"],
    storage_uri="memory://",
    headers_enabled=True,
    key_prefix=RATE_LIMIT_KEY_PREFIX,
)


def create_app() -> FastAPI:
    """Build and configure the FastAPI application."""
    # Fail closed BEFORE any route is mounted: a write route with no
    # auth secret is a misconfiguration, not a default. Refuses to start.
    assert_write_auth_configured()
    assert_webhook_auth_configured()

    app = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        description=(
            "Public read-only query API over the lite-indexer ClickHouse warehouse "
            "(295M+ transfer events, 13M+ DEX swaps across 16 chains). "
            "No authentication required; rate limited per IP."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_handler)
    app.add_middleware(SlowAPIMiddleware)

    # Transparent Historical Access (THA): the unified hot + cold layer.
    # Constructed lazily — no ClickHouse connection is forced at startup
    # (the access layer resolves its client on first use). Set eagerly so
    # app.state.historical exists even when the lifespan has not run
    # (TestClient without a context manager does not trigger lifespan).
    app.state.historical = get_historical()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "https://rugmunch.io",
            "https://rugmaps.rugmunch.io",
            "https://rugcharts.rugmunch.io",
        ],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def _metrics_middleware(request: Request, call_next: Any) -> Response:
        response = await call_next(request)
        REQUEST_COUNT.labels(
            method=request.method,
            path=request.url.path,
            status=str(response.status_code),
        ).inc()
        return response  # type: ignore[no-any-return]

    # Health/metrics are exempt from the default per-IP limit.
    app.include_router(system.router)
    system.health = limiter.exempt(system.health)  # type: ignore[no-untyped-call]
    system.metrics = limiter.exempt(system.metrics)  # type: ignore[no-untyped-call]

    app.include_router(transfers.router)
    app.include_router(address.router)
    app.include_router(deployers.router)
    app.include_router(whales.router)
    app.include_router(launches.router)
    app.include_router(stats.router)
    app.include_router(search.router)
    app.include_router(wallet_graph.router)

    # Label graph — the canonical home of wallet labels (entity
    # resolution + the label-growth write path). Reads are public;
    # writes are guarded by an optional shared-secret token.
    app.include_router(labels_router.router)

    # Raw SQL is expensive — stricter per-IP limit.
    query_router.post_query = limiter.limit("30/minute")(query_router.post_query)
    app.include_router(query_router.router)

    # Webhook management — registrations are the only mutating endpoint,
    # capped per IP (default 10/hour) to bound abuse surface.
    webhooks.register_webhook = limiter.limit(f"{WEBHOOK_MAX_PER_IP}/hour")(webhooks.register_webhook)
    app.include_router(webhooks.router)

    # GraphQL read surface (competitive parity with GraphQL-first indexers).
    # Rate limiting is inherited from the default per-IP limiter; the
    # resolvers reuse the same guarded queries as the REST routers.
    app.include_router(graphql_router)

    @app.get("/", include_in_schema=False)
    def root() -> dict[str, str]:
        """Root endpoint: point users at the docs."""
        return {
            "service": "chain-query-api",
            "version": __version__,
            "docs": "/docs",
            "health": "/health",
        }

    return app


async def _rate_limit_handler(request: Request, exc: Exception) -> Response:
    """Return a 429 with Retry-After when a client exceeds its limit."""
    from fastapi.responses import JSONResponse

    if isinstance(exc, RateLimitExceeded):
        detail = {"detail": "rate limit exceeded", "limit": str(getattr(exc, "detail", ""))}
    else:
        detail = {"detail": "rate limit exceeded"}
    return JSONResponse(status_code=429, content=detail)


app = create_app()
