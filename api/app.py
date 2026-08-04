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
from slowapi.util import get_remote_address

from api import __version__
from api.config import API_TITLE, API_VERSION, RATE_LIMIT_KEY_PREFIX, RATE_LIMIT_PER_MINUTE
from api.graphql import graphql_router
from api.routers import deployers, intel, search, stats, system, trace, transfers, whales
from api.routers import query as query_router

REQUEST_COUNT = Counter(
    "chain_api_requests_total",
    "Total Chain Query API requests",
    ["method", "path", "status"],
)


def _client_ip(request: Request) -> str:
    """Resolve the real client IP from proxy headers (nginx/Cloudflare).

    ``slowapi``'s default ``get_remote_address`` uses the socket address,
    which behind nginx is always 127.0.0.1. Use the first hop of
    X-Forwarded-For instead (nginx sets/forwards it).
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real = request.headers.get("x-real-ip")
    if real:
        return real.strip()
    return get_remote_address(request)


limiter = Limiter(
    key_func=_client_ip,
    default_limits=[f"{RATE_LIMIT_PER_MINUTE}/minute"],
    storage_uri="memory://",
    headers_enabled=True,
    key_prefix=RATE_LIMIT_KEY_PREFIX,
)


def create_app() -> FastAPI:
    """Build and configure the FastAPI application."""
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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
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
    app.include_router(deployers.router)
    app.include_router(whales.router)
    app.include_router(stats.router)
    app.include_router(search.router)

    # Raw SQL is expensive — stricter per-IP limit.
    query_router.post_query = limiter.limit("30/minute")(query_router.post_query)
    app.include_router(query_router.router)

    # Fund tracing fans out over many queries — stricter per-IP limit.
    trace.trace_tx = limiter.limit("10/minute")(trace.trace_tx)  # type: ignore[no-untyped-call]
    trace.trace_backward_address = limiter.limit("10/minute")(trace.trace_backward_address)  # type: ignore[no-untyped-call]
    trace.bridge_usage = limiter.limit("20/minute")(trace.bridge_usage)  # type: ignore[no-untyped-call]
    trace.cex_deposits = limiter.limit("20/minute")(trace.cex_deposits)  # type: ignore[no-untyped-call]
    trace.cross_chain_path = limiter.limit("10/minute")(trace.cross_chain_path)  # type: ignore[no-untyped-call]
    app.include_router(trace.router)

    # Wallet intelligence — rate-limited per-IP.
    app.include_router(intel.router)

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
