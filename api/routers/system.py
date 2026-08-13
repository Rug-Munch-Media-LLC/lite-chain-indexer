"""Operational endpoints: health, metrics, warehouse status."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api import __version__
from api.db import ping, table_stats
from api.schemas import StatusInfo

router = APIRouter(tags=["system"])

# Configurable via env
DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/data/chain-indexer"))
STATE_DIR = DATA_DIR / "state"
# Chains considered critical for health (must be advancing)
CRITICAL_CHAINS = {"ethereum", "base", "bsc", "polygon", "arbitrum", "optimism", "solana"}
# Max seconds since last block advance before chain is considered stalled
STALE_THRESHOLD_SECONDS = 3600  # 1 hour


def _check_indexer_state() -> dict[str, Any]:
    """Check indexer daemon health via state files."""
    if not STATE_DIR.exists():
        return {
            "status": "no_state_dir",
            "message": f"State directory not found: {STATE_DIR}",
            "chains": {},
        }

    chains_status = {}
    overall_ok = True
    now = time.time()

    for chain in CRITICAL_CHAINS:
        state_file = STATE_DIR / f"{chain}.json"
        if not state_file.exists():
            chains_status[chain] = {
                "status": "no_state_file",
                "last_block": None,
                "age_seconds": None,
            }
            overall_ok = False
            continue

        try:
            import json

            with open(state_file) as f:
                state = json.load(f)

            last_block = state.get("last_block")
            # Try to get timestamp from state (when it was last updated)
            state_mtime = state_file.stat().st_mtime
            age_seconds = now - state_mtime

            # Check if chain is advancing (age < threshold)
            is_stalled = age_seconds > STALE_THRESHOLD_SECONDS
            if is_stalled:
                overall_ok = False

            chains_status[chain] = {
                "status": "stalled" if is_stalled else "advancing",
                "last_block": last_block,
                "age_seconds": round(age_seconds, 1),
                "state_file_mtime": state_mtime,
            }
        except Exception as exc:
            chains_status[chain] = {
                "status": "error",
                "error": str(exc)[:100],
            }
            overall_ok = False

    return {
        "status": "ok" if overall_ok else "degraded",
        "state_dir": str(STATE_DIR),
        "stale_threshold_seconds": STALE_THRESHOLD_SECONDS,
        "chains": chains_status,
    }


@router.get("/health", summary="Liveness + ClickHouse + Indexer connectivity")
def health() -> dict[str, Any]:
    """Return comprehensive service health.

    Checks:
    - ClickHouse connectivity (ping)
    - Indexer daemon state files (advancing / stalled)
    - Overall status: ok only if all checks pass
    """
    ch_ok, ch_detail = ping()
    indexer_health = _check_indexer_state()

    overall_ok = ch_ok and indexer_health["status"] == "ok"

    return {
        "status": "ok" if overall_ok else "degraded",
        "service": "chain-query-api",
        "version": __version__,
        "clickhouse": ch_detail,
        "indexer_daemon": indexer_health,
        "checks": {
            "clickhouse": "ok" if ch_ok else "failed",
            "indexer_daemon": indexer_health["status"],
        },
    }


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    """Prometheus metrics in the text exposition format."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/api/v1/status", response_model=StatusInfo, summary="Warehouse status")
def status() -> StatusInfo:
    """Report per-table row counts for the public warehouse tables."""
    return StatusInfo(tables=table_stats(), source=ping()[1])
