"""Operational endpoints: health, metrics, warehouse status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api import __version__
from api.db import ping, table_stats
from api.schemas import StatusInfo

router = APIRouter(tags=["system"])


@router.get("/health", summary="Liveness + ClickHouse connectivity")
def health() -> dict[str, Any]:
    """Return service health. ``status: ok`` only when ClickHouse responds."""
    ok, detail = ping()
    return {
        "status": "ok" if ok else "degraded",
        "service": "chain-query-api",
        "version": __version__,
        "clickhouse": detail,
    }


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    """Prometheus metrics in the text exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/api/v1/status", response_model=StatusInfo, summary="Warehouse status")
def status() -> StatusInfo:
    """Report per-table row counts for the public warehouse tables."""
    return StatusInfo(tables=table_stats(), source=ping()[1])
