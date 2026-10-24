"""Polymarket read-API producer: the writer behind ``pm_trades``/``pm_markets``/``pm_holders``.

The three tables were created 2026-10-23 and had no writer at all, which is why
no history exists before that day. This package is the producer: it pulls the
keyless public Polymarket read APIs (the same ones the ``polymarket`` MCP wraps)
and writes the tables the archive spec already declares.

    pm_trades   hourly  append-only, ``ingested_at`` is the arrival/version column
    pm_markets  15 min  upsert by (condition_id, market_id), version ``asof``
    pm_holders  hourly  snapshot per hour, key includes ``asof``
"""

from __future__ import annotations

from lite_indexer.producers.polymarket.run import run

__all__ = ["run"]
