# syntax=docker/dockerfile:1
#
# Chain Query API — multi-stage Dockerfile.
#
# Stage 1 (builder) resolves + freezes the runtime dependencies as wheels
# so the final image has no build tools (pip/gcc) — smaller and safer.
# Stage 2 (runtime) installs those wheels and the api/ package only.
#
# Build context is the repo ROOT (so `COPY api ./api` works):
#   docker build -f api/Dockerfile -t lite-indexer-api:latest ..
#
# The service listens on 8101 (matches the live deployment).

# ── Builder: resolve + freeze dependencies ─────────────────────────────
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /wheels

# Runtime dependencies: the API package (api/) plus the lite_indexer
# package it imports for vector search (api/routers/search.py uses
# lite_indexer.vectors.*). --only-binary keeps the build hermetic on
# slim images.
RUN pip wheel --only-binary=:all: --wheel-dir=/wheels \
    "fastapi>=0.115.0" \
    "uvicorn[standard]>=0.30.0" \
    "slowapi>=0.1.9" \
    "pydantic>=2.0.0" \
    "clickhouse-driver>=0.2.0" \
    "prometheus-client>=0.21.0" \
    "httpx>=0.27.0" \
    "structlog>=24.4.0" \
    "PyYAML>=6.0" \
    "pyarrow>=15.0.0"

# ── Runtime ────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install the frozen wheels, then drop the wheel cache.
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

# Application code: the api package plus the lite_indexer package it
# imports (vector search). Both are pure-Python top-level packages.
COPY api ./api
COPY lite_indexer ./lite_indexer

# Non-root runtime user (production hardening). The data dir holds the
# LanceDB vector store (LANCE_DIR, created at import time by
# lite_indexer.vectors.config).
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data && chown -R appuser:appuser /app/data
USER appuser

ENV LANCE_DIR=/app/data/lance

EXPOSE 8101

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8101/health', timeout=4)"]

CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8101", "--workers", "2"]
