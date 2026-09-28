"""ClickHouse row helpers — deployer EOA resolution, dedup keys, payloads.

Split out of :mod:`lite_indexer.storage.clickhouse` (2026-09-22) so the
writer stays under the per-file size ratchet. Pure helpers with no
patch seams; the writer facade re-exports them.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Max concurrent eth_getTransactionByHash calls while resolving deployer
# EOAs (keeps RPC overhead low while batching unique tx hashes).
_DEPLOYER_RPC_CONCURRENCY = 16

# How many endpoints to try per tx hash before giving up on it. The
# previous implementation queried only the single best endpoint — when that
# node was down or rate-limited, every deployer event in the batch landed
# with an empty deployer_eoa. Trying the whole pool (bounded) recovers the
# rows that a transient best-endpoint failure would otherwise lose.
_DEPLOYER_RPC_MAX_ENDPOINTS = 3

# Idempotent-rewind dedup: a re-extracted range must not append duplicates.
# Before inserting a transfer batch we look up the (block_number, tx_hash,
# log_index) keys already present for the batch's block window and skip
# them. This is the WIDTH OF ONE WINDOW, not a gate: a wider batch is walked
# in ceil(span / width) windows (one query each, results unioned) by
# ch_dedup_windows.iter_block_windows. It must never become a bail-out —
# returning "nothing exists" for a wide range re-inserts the batch verbatim,
# which is the large multi-day rewind this guard exists to contain.
_DEDUP_MAX_BLOCK_SPAN = 50_000


async def _resolve_deployer_eoas(chain: str, events: list[dict[str, Any]]) -> dict[str, str]:
    """Resolve the deployer EOA (tx `from`) for each unique tx hash.

    Groups deployer events by unique ``transactionHash`` so each tx is
    queried via ``eth_getTransactionByHash``, then maps tx_hash → from
    address. Returns {} if resolution is unavailable.

    Endpoint resilience: instead of a single ``get_best_endpoint`` call, the
    pool is rotated so each tx hash is retried against up to
    ``_DEPLOYER_RPC_MAX_ENDPOINTS`` endpoints. A transient failure on the
    healthiest node no longer blanks the whole batch — the fallback nodes
    recover the rows. Failure feedback (``record_endpoint_failure``) flows
    back into the shared pool so future batches prefer healthier endpoints.

    Args:
        chain: Chain name (used to pick RPC endpoints).
        events: Deployer event dicts (each with a ``transactionHash``).

    Returns:
        Dict mapping tx_hash → lowercased `from` address (EOA).
    """
    tx_hashes = {e.get("transactionHash", "") for e in events if isinstance(e, dict) and e.get("transactionHash")}
    if not tx_hashes:
        return {}

    # Deferred imports avoid any import-cycle risk with the storage package.
    from lite_indexer.chains import GET_LOGS_ENDPOINTS
    from lite_indexer.rpc import RpcClient, get_best_endpoint, record_endpoint_failure

    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        logger.warning("deployer_eoa_no_endpoints", chain=chain)
        return {}

    rpc = RpcClient()
    semaphore = asyncio.Semaphore(_DEPLOYER_RPC_CONCURRENCY)

    async def _try_endpoint(endpoint: str, tx_hash: str) -> str | None:
        try:
            result = await rpc.call(endpoint, "eth_getTransactionByHash", [tx_hash])
            if result and isinstance(result.get("result"), dict):
                from_addr = result["result"].get("from")
                if from_addr:
                    return str(from_addr).lower()
        except Exception as exc:  # noqa: BLE001 — resolution is best-effort
            logger.debug("deployer_eoa_fetch_failed", tx=tx_hash[:16], error=str(exc)[:60])
        record_endpoint_failure(endpoint)
        return None

    async def _get_from(tx_hash: str) -> tuple[str, str]:
        # Rotate the pool starting from the best endpoint so healthy nodes
        # are preferred, but the batch survives a dead best node.
        start_idx = endpoints.index(get_best_endpoint(endpoints))
        rotated = endpoints[start_idx:] + endpoints[:start_idx]
        for endpoint in rotated[:_DEPLOYER_RPC_MAX_ENDPOINTS]:
            async with semaphore:
                from_addr = await _try_endpoint(endpoint, tx_hash)
            if from_addr:
                return tx_hash, from_addr
        return tx_hash, ""

    results = await asyncio.gather(*(_get_from(h) for h in tx_hashes), return_exceptions=True)

    mapping: dict[str, str] = {}
    for res in results:
        if isinstance(res, tuple) and res[1]:
            mapping[res[0]] = res[1]
    return mapping


def _coerce_int(value: Any) -> int:
    """Coerce a block/log index (decimal, hex string, int) to int (0 on failure)."""
    if isinstance(value, str):
        text = value.strip()
        if text.startswith(("0x", "0X")):
            try:
                return int(text, 16)
            except ValueError:
                return 0
        value = text
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _transfer_dedup_key(block_number: Any, tx_hash: Any, log_index: Any) -> str:
    """Storage dedup key for ``wallet_memory.evm_transfers`` rows.

    Mirrors the application dedup key ``(block_number, tx_hash, log_index)``
    and populates the appended ReplacingMergeTree sorting column
    ``tx_log_key`` (migrations/018_evm_transfers_dedup_key_v2.sql), so a
    re-extracted range produces a byte-identical key and the storage layer
    collapses it at merge time.

    The component ORDER MUST stay ``(block_number, tx_hash, log_index)`` to
    match :meth:`ClickHouseWriter._existing_transfer_keys` — the unit test
    ``test_tx_log_key_matches_app_dedup_key`` pins the two together so they
    cannot drift.
    """
    return f"{_coerce_int(block_number)}:{tx_hash or ''}:{_coerce_int(log_index)}"


def _serialize_raw_payload(event: dict[str, Any]) -> str:
    """Serialize the raw RPC dict carried on an event (best-effort).

    Events may carry the raw RPC payload under ``raw_payload`` (or ``raw``)
    as a dict, so it can be re-decoded later without a re-fetch (see
    migrations/010_raw_payload_parent_hash.sql). Absent or unserializable
    payloads degrade to ``''`` — the column default — so parsers that do
    not emit the raw payload never break the writer.

    Args:
        event: Parsed event dict.

    Returns:
        Compact JSON string, or ``''`` when no payload is in hand.
    """
    raw = event.get("raw_payload") or event.get("raw")
    if not raw:
        return ""
    if isinstance(raw, str):
        return raw
    try:
        return json.dumps(raw, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
        logger.debug("raw_payload_serialize_skip")
        return ""


def _parse_ts(ts_str: Any, now: Any) -> Any:
    """Parse an ISO-8601 ``blockTimestamp`` (falls back to ``now``)."""
    if not ts_str:
        return now
    try:
        return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return now
