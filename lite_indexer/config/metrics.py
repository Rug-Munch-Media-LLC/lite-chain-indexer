"""Prometheus exporter configuration for LiteChainIndexer.

The daemon is a long-lived process, not a web app, so it starts a
``prometheus_client`` HTTP exporter on a background thread. Defaults bind
loopback only (Prometheus scrapes via the container/host network — see the
``lite-indexer`` scrape job in fleet-infra).
"""

from __future__ import annotations

import os

# Host/port for the daemon's Prometheus /metrics endpoint.
METRICS_ENABLED: bool = os.environ.get("METRICS_ENABLED", "1") != "0"
METRICS_HOST: str = os.environ.get("METRICS_HOST", "127.0.0.1")
METRICS_PORT: int = int(os.environ.get("METRICS_PORT", "9109"))

# How often the gauge-export loop refreshes process/state/quota/pool gauges.
METRICS_GAUGE_INTERVAL_SECONDS: float = float(os.environ.get("METRICS_GAUGE_INTERVAL_SECONDS", "15.0"))
