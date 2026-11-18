"""SQD → B2 day-unit backfill for one chain and one table.

WHY THIS IS NOT ``sqd_evm.evm_traces``
    ``evm_traces`` is a bounded QUERY: a block range, an optional address
    filter, and a truncation to ``limit`` (default 200). It cannot fill an
    archive because a truncated list has no row count worth verifying, and
    :func:`lite_indexer.archive.write_unit` refuses any unit whose parquet row
    count differs from ``UnitSpec.expected_rows``. A backfill needs the opposite
    contract: every row in the window, counted, then published.

WHAT A UNIT IS HERE
    One UTC day, one chain, one table. The day is ``[00:00:00, 24:00:00)``; the
    block range comes from binary search over block timestamps
    (:mod:`lite_indexer.query.sqd_day`), so the boundaries are the chain's real
    blocks. Rows are counted as they are yielded, which is what makes
    ``expected_rows`` exact rather than estimated.

TABLES THIS SERVES — VERIFIED, NOT ASSUMED (probed 2026-11-18)
    ``internal_transfers``  portal ``traces`` + sibling ``transactions``
    ``evm_transfers``       portal ``logs`` filtered on the ERC-20 Transfer topic

    Both datasets answer on all 14 chains in ``SQDSource.EVM_DATASETS``. Tables
    this adapter does NOT serve, and will not fake with traces:
    ``solana_transfers``/``tron_*`` (different decoders, different datasets),
    ``dex_events`` (needs a DEX decoder over swaps, not raw logs).

RATE LIMITS — the account is the asset
    The portal answered bursts with HTTP 529 throughout this build. So the
    reader is sequential BY CONSTRUCTION: no thread pool, no asyncio, no
    ``concurrency`` parameter, one chain at a time, sleeping between batches.
    A banned account loses all 14 chains; a slow backfill loses an afternoon.

CLI::

    python -m lite_indexer.query.sqd_backfill --chain polygon \
        --table internal_transfers --days 7 --dry-run
"""

from __future__ import annotations

import datetime as dt
import json
import time
from collections.abc import Iterator
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.sqd_day import day_block_range
from lite_indexer.query.sqd_evm import TRACE_FIELDS
from lite_indexer.query.sqd_rows import LOG_FIELDS, LOG_SELECTOR, blocks_to_rows
from lite_indexer.query.sqd_source import SQDPortalError, SQDSource
from lite_indexer.query.sqd_tables import MissingTableError, require_table

logger = get_logger(__name__)

__all__ = [
    "DEFAULT_SLEEP_S",
    "TABLES",
    "stream_window",
    "MissingTableError",
    "UnsupportedChainError",
    "UnsupportedTableError",
    "backfill_day",
]

#: Tables this adapter produces. Anything else is a report, never a fake.
TABLES: tuple[str, ...] = ("internal_transfers", "evm_transfers")

#: Portal fields for ``internal_transfers``: the trace set plus the sibling
#: ``transactions`` field, without which ``tx_hash`` is empty (see sqd_rows).
TRACE_BACKFILL_FIELDS: dict[str, Any] = {**TRACE_FIELDS, "transaction": {"hash": True}}

#: Portal selector for ``internal_transfers`` — both fields must be requested.
TRACE_SELECTOR: dict[str, Any] = {"traces": [{}], "transactions": [{}]}

#: Blocks per portal request. A pagination hint only: the portal caps a response
#: by BYTES, and a 501-block polygon trace probe returned 225 MB in 6.4 s, which
#: is already the practical ceiling for one request.
DEFAULT_BLOCK_BATCH = 200

#: Seconds between portal calls. Politeness is the rate-limit strategy.
DEFAULT_SLEEP_S = 1.0

#: Rows per yielded chunk — a memory bound, never a truncation.
DEFAULT_LIMIT_PER_BATCH = 5000


class UnsupportedTableError(ValueError):
    """Raised for a table this adapter has no verified portal field set for."""


class UnsupportedChainError(ValueError):
    """Raised for a chain with no SQD EVM traces dataset."""


def _portal_fields(table: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Portal (fields, selector) for one table."""
    if table == "internal_transfers":
        return TRACE_BACKFILL_FIELDS, TRACE_SELECTOR
    if table == "evm_transfers":
        return LOG_FIELDS, LOG_SELECTOR
    raise UnsupportedTableError(f"no verified portal field set for {table!r}; served: {list(TABLES)}")


def stream_window(
    src: SQDSource,
    chain: str,
    dataset: str,
    table: str,
    first: int,
    last: int,
    block_batch: int,
    sleep_s: float,
) -> Iterator[dict[str, Any]]:
    """Yield mapped rows for ``[first, last]``, one block batch per request.

    ``chain`` and ``dataset`` are separate arguments on purpose: the archive
    spells the chain (``polygon``) while the portal spells the dataset
    (``polygon-mainnet``), and a row carrying the dataset slug would be filed
    under a chain no reader asks for.
    """
    fields, selector = _portal_fields(table)
    cursor = first
    while cursor <= last:
        window_end = min(cursor + block_batch - 1, last)
        body = {"type": "evm", "fromBlock": cursor, "toBlock": window_end, "fields": fields, **selector}
        status, text = src._request("POST", f"{src.PORTAL}/datasets/{dataset}/stream", body, timeout=300.0)
        if status == 204:
            return
        if status != 200:
            raise SQDPortalError(f"backfill stream HTTP {status}: {text[:160]}")
        blocks = [json.loads(line) for line in text.splitlines() if line.strip()]
        yield from blocks_to_rows(chain, table, blocks)
        cursor = window_end + 1
        if sleep_s > 0 and cursor <= last:
            time.sleep(sleep_s)


def backfill_day(
    chain: str,
    table: str,
    day: dt.date,
    *,
    limit_per_batch: int = DEFAULT_LIMIT_PER_BATCH,
    block_batch: int = DEFAULT_BLOCK_BATCH,
    src: SQDSource | None = None,
    sleep_s: float = DEFAULT_SLEEP_S,
) -> Iterator[dict[str, Any]]:
    """Yield every row the portal holds for ``(chain, table, day)``.

    Sequential by design — see the module docstring. There is deliberately no
    concurrency parameter: the portal answers a burst with HTTP 529 and a banned
    account loses every chain.

    Args:
        chain: Local chain name, a key of ``SQDSource.EVM_DATASETS``.
        table: One of :data:`TABLES`.
        day: The UTC day to read.
        limit_per_batch: Rows per yielded chunk (memory bound only).
        block_batch: Blocks per portal request (pagination hint only).
        src: Injected client (tests); a default sequential client otherwise.
        sleep_s: Seconds to sleep between portal requests.

    Yields:
        Row dicts in the archive's column shape for ``table``.

    Raises:
        UnsupportedChainError: ``chain`` has no EVM traces dataset.
        UnsupportedTableError: ``table`` has no verified field set here.
        MissingTableError: the portal does not serve ``table`` for ``chain``.
        SQDPortalError: the portal refused a request.
    """
    source = src or SQDSource()
    if chain not in source.EVM_DATASETS:
        raise UnsupportedChainError(f"{chain!r} has no SQD EVM dataset; known: {sorted(source.EVM_DATASETS)}")
    _portal_fields(table)  # fail before any request when the table is unsupported
    # Measured-absent table: the request is well-formed and can never succeed, so
    # refuse before spending a day-boundary search on it.
    require_table(chain, table)
    dataset = source.EVM_DATASETS[chain]
    head = source.head(dataset)
    if head is None:
        logger.warning("sqd_backfill_no_head", chain=chain, dataset=dataset)
        return
    bounds = day_block_range(source, dataset, day, head)
    if bounds is None:
        logger.info("sqd_backfill_empty_day", chain=chain, table=table, day=day.isoformat())
        return
    first, last = bounds
    logger.info("sqd_backfill_start", chain=chain, table=table, day=day.isoformat(), first=first, last=last)
    pending: list[dict[str, Any]] = []
    for row in stream_window(source, chain, dataset, table, first, last, block_batch, sleep_s):
        pending.append(row)
        if len(pending) >= max(1, limit_per_batch):
            yield from pending
            pending = []
    if pending:
        yield from pending


if __name__ == "__main__":
    from lite_indexer.query.sqd_backfill_cli import cli

    raise SystemExit(cli())
