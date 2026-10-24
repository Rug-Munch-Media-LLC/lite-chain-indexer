"""One Polymarket producer cycle, and its CLI.

Cadence lives here, not in the scheduler: the systemd timer fires every 15
minutes and each dataset is due according to the last row it actually wrote
(``ingested_at`` for trades, ``asof`` for holders). A missed tick therefore
self-heals on the next one, and a restarted process cannot double-write because
every table is ReplacingMergeTree keyed on the row's natural key.

Run: ``python -m lite_indexer.producers.polymarket --json``
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys

from lite_indexer.logging import get_logger
from lite_indexer.producers.polymarket import holders, markets, trades
from lite_indexer.producers.polymarket.http import ProviderError, client
from lite_indexer.producers.polymarket.rows import HOLDER_COLUMNS, MARKET_COLUMNS, TRADE_COLUMNS
from lite_indexer.producers.polymarket.store import insert, latest
from lite_indexer.storage.clickhouse import ClickHouseWriter

logger = get_logger(__name__)

DATASETS = ("markets", "trades", "holders")
TRADES_EVERY = dt.timedelta(minutes=55)
HOLDERS_EVERY = dt.timedelta(minutes=55)


def _quiet_metric(name: str, amount: float = 1.0) -> None:
    """Metrics must never break a cycle that already wrote its rows."""
    try:
        from lite_indexer import metrics

        metrics.incr(name, amount)
    except Exception:  # noqa: BLE001 — observability is best-effort
        logger.debug("polymarket_metric_failed", metric=name)


def _due(dataset: str, last: dt.datetime | None, now: dt.datetime, force: bool) -> bool:
    if force or dataset == "markets":
        return True
    if last is None:
        return True
    interval = TRADES_EVERY if dataset == "trades" else HOLDERS_EVERY
    return now - last >= interval


def _write_trades(http, writer: ClickHouseWriter, now: dt.datetime, force: bool) -> int:
    since = None if force else latest(writer, "pm_trades", "ingested_at")
    rows = trades.collect(http, since)
    return insert(writer, "pm_trades", TRADE_COLUMNS, rows)


def _write_markets(http, writer: ClickHouseWriter, now: dt.datetime) -> int:
    asof = now.replace(second=0, microsecond=0)
    rows = markets.collect(http, asof)
    return insert(writer, "pm_markets", MARKET_COLUMNS, rows)


def _write_holders(http, writer: ClickHouseWriter, now: dt.datetime) -> int:
    asof = now.replace(minute=0, second=0, microsecond=0)
    targets = markets.top_condition_ids(writer)
    rows = holders.collect(http, targets, asof)
    return insert(writer, "pm_holders", HOLDER_COLUMNS, rows)


def run(datasets: tuple[str, ...] = DATASETS, now: dt.datetime | None = None, force: bool = False) -> dict:
    """Run the selected datasets once; never let one failure cost the others."""
    now = (now or dt.datetime.now(dt.UTC)).replace(tzinfo=None)
    writer = ClickHouseWriter()
    cursors = {
        "trades": latest(writer, "pm_trades", "ingested_at"),
        "holders": latest(writer, "pm_holders", "asof"),
    }
    results: dict[str, dict] = {}
    with client() as http:
        for dataset in datasets:
            if dataset not in DATASETS:
                results[dataset] = {"status": "skipped", "reason": "unknown dataset"}
                continue
            if not _due(dataset, cursors.get(dataset), now, force):
                results[dataset] = {"status": "skipped", "reason": "not due"}
                continue
            try:
                if dataset == "trades":
                    written = _write_trades(http, writer, now, force)
                elif dataset == "markets":
                    written = _write_markets(http, writer, now)
                else:
                    written = _write_holders(http, writer, now)
            except (ProviderError, RuntimeError) as exc:
                results[dataset] = {"status": "error", "error": str(exc)[:300]}
                _quiet_metric("polymarket_producer_errors")
                logger.warning("polymarket_dataset_failed", dataset=dataset, error=str(exc)[:300])
                continue
            results[dataset] = {"status": "ok", "rows": written}
            _quiet_metric("polymarket_rows", written)
            logger.info("polymarket_dataset_written", dataset=dataset, rows=written)
    return {"at": now.isoformat(), "force": force, "datasets": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Polymarket pm_* producer cycle")
    parser.add_argument("--datasets", default=",".join(DATASETS))
    parser.add_argument("--force", action="store_true", help="ignore the cursor and re-fetch the window")
    parser.add_argument("--json", action="store_true", help="print the cycle summary as JSON")
    args = parser.parse_args(argv)
    summary = run(tuple(d.strip() for d in args.datasets.split(",") if d.strip()), force=args.force)
    if args.json:
        sys.stdout.write(json.dumps(summary, sort_keys=True) + "\n")
    return 0 if all(r.get("status") != "error" for r in summary["datasets"].values()) else 1
