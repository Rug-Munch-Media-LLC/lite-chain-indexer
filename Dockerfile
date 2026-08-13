# Dockerfile for LiteChainIndexer
#
# Multi-stage build: builder installs dependencies, runtime copies only what's needed.
# Runs as unprivileged user (UID 1000) with minimal attack surface.
#
# Build: docker build -t lite-chain-indexer:latest /srv/work/repos/lite-chain-indexer
# Run:   docker compose -f /srv/work/repos/fleet-infra/rmi-infra/docker-compose.lite-indexer.yml up -d

# ── Builder Stage ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

# Install system dependencies for building wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip and install build tools
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy dependency files
WORKDIR /app
COPY pyproject.toml ./
COPY README.md ./

# Install package in development mode with all extras
RUN pip install --no-cache-dir -e ".[all]"

# ── Runtime Stage ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create unprivileged user
RUN groupadd -r -g 1000 lite-indexer && \
    useradd -r -u 1000 -g lite-indexer -d /home/lite-indexer -s /bin/bash lite-indexer && \
    mkdir -p /home/lite-indexer && \
    chown lite-indexer:lite-indexer /home/lite-indexer

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
WORKDIR /app
COPY --chown=lite-indexer:lite-indexer . .

# Install the package (editable so code changes reflect immediately in dev)
RUN pip install --no-cache-dir -e .

# Create data directories with correct ownership
RUN mkdir -p /data /tmp/indexer-demand /home/lite-indexer/.config/rclone && \
    chown -R lite-indexer:lite-indexer /data /tmp/indexer-demand /home/lite-indexer/.config

# Switch to unprivileged user
USER lite-indexer

# Health check endpoint (simple process check)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python3 -c "import sys; sys.exit(0)" && pgrep -f "lite-indexer.*daemon" > /dev/null

# Default command (overridden by docker-compose)
ENTRYPOINT ["lite-indexer"]
CMD ["--help"]

# Labels for image metadata
LABEL org.opencontainers.image.title="LiteChainIndexer" \
      org.opencontainers.image.description="Multi-chain blockchain indexer using free RPCs" \
      org.opencontainers.image.source="https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.vendor="Rug Munch Media LLC"