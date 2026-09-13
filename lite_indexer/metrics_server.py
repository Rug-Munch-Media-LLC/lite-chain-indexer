"""Prometheus HTTP exporter for the LiteIndexer daemon.

The daemon is a long-lived asyncio process, so it starts
``prometheus_client.start_http_server`` once on a background thread. The
call is non-blocking and never touches the event loop, so a busy port or a
missing ``prometheus_client`` degrades to a logged no-op instead of
crashing the daemon.

The scrape job (``lite-indexer`` in fleet-infra's ``prometheus.yml``)
targets the daemon host on ``METRICS_PORT`` (default 9109). The exporter
binds ``METRICS_HOST`` (default ``127.0.0.1``) — Prometheus reaches it over
the container/host network, so the port is never exposed publicly.
"""

from __future__ import annotations

import threading
from typing import Any

from lite_indexer.config import METRICS_ENABLED, METRICS_HOST, METRICS_PORT
from lite_indexer.logging import get_logger
from lite_indexer.metrics import PROMETHEUS_AVAILABLE, ensure_process_collectors

logger = get_logger(__name__)

# The running WSGI server (kept so tests / shutdown can close it).
_server: Any = None
_lock = threading.Lock()


def metrics_server_running() -> bool:
    """Whether this process has an active metrics HTTP server."""
    return _server is not None


def start_metrics_server(host: str | None = None, port: int | None = None) -> bool:
    """Start the Prometheus HTTP exporter on a background thread.

    Idempotent: a second call while a server is running is a no-op that
    returns ``True``. A bind failure (port busy, permission) is logged and
    returns ``False`` — the daemon keeps running without metrics.

    Args:
        host: Bind address; defaults to ``METRICS_HOST``.
        port: Bind port; defaults to ``METRICS_PORT`` (0 = ephemeral).

    Returns:
        ``True`` when a server is (already) running, ``False`` otherwise.
    """
    global _server
    if not METRICS_ENABLED:
        logger.info("metrics_server_disabled")
        return False
    if not PROMETHEUS_AVAILABLE:  # pragma: no cover — hard project dependency
        logger.warning("metrics_server_unavailable", reason="prometheus_client missing")
        return False
    ensure_process_collectors()
    bind_host = host or METRICS_HOST
    bind_port = METRICS_PORT if port is None else port
    with _lock:
        if _server is not None:
            logger.info("metrics_server_already_started", host=bind_host, port=bind_port)
            return True
        try:
            from prometheus_client import start_http_server

            result = start_http_server(bind_port, addr=bind_host)
            # prometheus_client >= 0.19 returns (server, thread); older None.
            _server = result[0] if isinstance(result, tuple) else result
        except (OSError, ValueError) as exc:
            logger.warning(
                "metrics_server_start_failed",
                host=bind_host,
                port=bind_port,
                error=str(exc)[:120],
            )
            return False
    logger.info("metrics_server_started", host=bind_host, port=bind_port)
    return True


def stop_metrics_server() -> None:
    """Stop the metrics HTTP server (idempotent; safe when never started)."""
    global _server
    with _lock:
        server, _server = _server, None
    if server is None:
        return
    try:
        server.shutdown()
        server.server_close()
    except (OSError, AttributeError):  # pragma: no cover — best-effort teardown
        pass
    logger.info("metrics_server_stopped")


__all__ = ["metrics_server_running", "start_metrics_server", "stop_metrics_server"]
