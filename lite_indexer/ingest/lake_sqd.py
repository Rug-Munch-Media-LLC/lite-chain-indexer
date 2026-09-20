"""SqdIngestSource — SQD portal block-range ingest for intra-day hole fills.

The sonarx lake cannot see an intra-day hole (the day still has rows) and AWS
sonarx lags ~1 week; portal.sqd.dev is near-live, full-history EVM. Reuses the
portal client and ClickHouse writer (both injected, never modified). The
``transactions`` selector supplies real tx hashes, joined per block by
transactionIndex (2026-09-20).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from lite_indexer.ingest.lake_sonarx import _iso_from_unix
from lite_indexer.logging import get_logger
from lite_indexer.query.sqd_source import ERC20_TRANSFER_TOPIC, SQDSource

logger = get_logger(__name__)

WINDOW = 10_000  #: portal block window per ``stream`` call
MAX_BATCHES = 32  #: portal-side pagination budget per window call
LOG_FIELDS: dict[str, Any] = {  # only Transfer logs leave the portal (topic0 filter)
    "block": {"number": True, "timestamp": True},
    "transaction": {"hash": True, "transactionIndex": True},
    "log": {"address": True, "topics": True, "data": True, "transactionIndex": True, "logIndex": True},
}
LOG_SELECTORS: dict[str, Any] = {"logs": [{"topic0": [ERC20_TRANSFER_TOPIC]}], "transactions": [{}]}


def _topic_address(topic: Any) -> str:
    """32-byte padded topic → lowercase ``0x``-address (last 20 bytes)."""
    return ("0x" + str(topic or "")[-40:]).lower()


def _value_str(data: Any) -> str:
    """Hex ``data`` → exact decimal string (empty/garbage → "0")."""
    raw = str(data or "").strip().lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    if not raw or any(c not in "0123456789abcdef" for c in raw):
        return "0"
    return str(int(raw, 16))


def map_sqd_transfer(
    chain: str,
    block: dict[str, Any],
    log: dict[str, Any],
    tx_hashes: dict[int, str] | None = None,
) -> dict[str, Any] | None:
    """Map one portal log to an ``evm_transfers`` dict (None if not an ERC-20 Transfer).

    ``tx_hashes`` maps transactionIndex → hash for the block (from the
    ``transactions`` selector); missing entries fall back to "".
    """
    topics = log.get("topics") or []
    if len(topics) != 3 or str(topics[0]).lower() != ERC20_TRANSFER_TOPIC:
        return None
    header = block.get("header") or {}
    tx_index = int(log.get("transactionIndex") or 0)
    return {
        "blockNumber": int(header.get("number") or 0),
        "txHash": str((tx_hashes or {}).get(tx_index) or ""),
        "from": _topic_address(topics[1]),
        "to": _topic_address(topics[2]),
        "tokenAddress": str(log.get("address") or "").lower(),
        "value": _value_str(log.get("data")),
        "blockTimestamp": _iso_from_unix(header.get("timestamp")),
        "logIndex": int(log.get("logIndex") or 0),
    }


class SqdIngestSource:
    """Portal reader streaming ``evm_transfers`` rows for a block range.

    Args:
        portal: DI seam — object with ``available()`` and
            ``stream(dataset, from, to, *, fields, selectors, max_batches)``.
        window: Blocks per portal call.
    """

    def __init__(self, *, portal: Any | None = None, window: int = WINDOW) -> None:
        self._portal = portal if portal is not None else SQDSource()
        self._window = max(1, int(window))

    def available(self) -> bool:
        return bool(self._portal.available())

    def read_transfers(self, chain: str, from_block: int, to_block: int) -> Iterator[dict[str, Any]]:
        """Yield mapped transfer rows for ``chain`` over the inclusive range."""
        dataset = SQDSource.EVM_DATASETS.get(chain)
        if dataset is None or not self.available():
            logger.warning("sqd_ingest_unavailable", chain=chain)
            return
        cursor, end = int(from_block), int(to_block)
        while cursor <= end:
            chunk_end = min(cursor + self._window - 1, end)
            blocks = self._portal.stream(
                dataset, cursor, chunk_end, fields=LOG_FIELDS, selectors=LOG_SELECTORS, max_batches=MAX_BATCHES
            )
            if not blocks:
                break
            reached = cursor - 1
            for block in blocks:
                reached = max(reached, int((block.get("header") or {}).get("number") or 0))
                tx_hashes = {
                    int(tx.get("transactionIndex") or 0): str(tx.get("hash") or "")
                    for tx in block.get("transactions") or []
                    if isinstance(tx, dict)
                }
                for log in block.get("logs") or []:
                    row = map_sqd_transfer(chain, block, log, tx_hashes)
                    if row is not None:
                        yield row
            if reached < cursor:  # defensive: no forward progress
                break
            cursor = reached + 1 if reached < chunk_end else chunk_end + 1


@dataclass
class SqdFillRequest:
    """One SQD hole fill: chain, inclusive block range, batching."""

    chain: str
    from_block: int
    to_block: int
    dry_run: bool = False
    batch_rows: int = 5000


def fill_chain_from_sqd(req: SqdFillRequest, *, source: Any | None = None, writer: Any | None = None) -> dict[str, int]:
    """Fill ``req``'s range from the portal through the writer (dedup-aware).

    ``source`` / ``writer`` are DI seams; defaults are live objects.
    """
    if req.chain not in SQDSource.EVM_DATASETS:
        raise ValueError(f"unsupported sqd chain: {req.chain!r}")
    if req.to_block < req.from_block:
        raise ValueError("to_block is before from_block")
    src = source if source is not None else SqdIngestSource()
    if not src.available():
        logger.warning("sqd_fill_unavailable", chain=req.chain)
        return {"rows": 0}
    if req.dry_run:
        rows = sum(1 for _ in src.read_transfers(req.chain, req.from_block, req.to_block))
        logger.info("sqd_fill_dry_run", chain=req.chain, rows=rows)
        return {"rows": rows}
    wh = writer if writer is not None else _default_writer()
    total, batch = 0, []
    for row in src.read_transfers(req.chain, req.from_block, req.to_block):
        batch.append(row)
        if len(batch) >= max(1, int(req.batch_rows)):
            total += int(wh.write_transfers(req.chain, batch))
            batch = []
    if batch:
        total += int(wh.write_transfers(req.chain, batch))
    logger.info("sqd_fill_written", chain=req.chain, rows=total)
    return {"rows": total}


def _default_writer() -> Any:
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    return ClickHouseWriter()
