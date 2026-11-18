"""Per-chain SQD table availability — measured, and re-measurable.

WHY THIS EXISTS
    ``SQDSource.EVM_DATASETS`` lists 14 chains, but a chain being listed does not
    mean the portal serves every table for it. Measured 2026-11-18: a
    ``traces`` request against ``optimism-mainnet`` answers

        HTTP 400 {"code":"malformed_request",
                  "message":"couldn't parse request: table 'traces' does not exist"}

    while the same request against the other 13 answers 200. A backfill that
    only discovers this mid-run has already spent a day boundary and a request
    budget learning it, and — worse — a caller reading only
    ``EVM_DATASETS`` would assume optimism traces exist and file an empty unit.

WHAT THIS IS NOT
    It is not a hardcoded guess copied from a docstring. ``probe`` re-measures
    it against the live portal, and ``scripts/probe-sqd-tables.py`` prints the
    result. The static map below is the LAST MEASUREMENT, kept so the common
    path costs zero requests; anything that matters should call ``probe``.

WHY LOGS ARE NOT IN THE SAME BOAT
    ``evm_transfers`` reads the ``logs`` table, which every probed chain answered
    (optimism included: 1 ERC-20 Transfer in the probe block). The gap is
    trace-specific.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.sqd_source import SQDPortalError, SQDSource

logger = get_logger(__name__)

#: Header-only probe body — the cheapest way to ask "does this block exist".
_PROBE_FIELDS: dict[str, Any] = {"block": {"number": True, "timestamp": True}}

#: Chains whose portal dataset does NOT serve the ``traces`` table.
#: Measured 2026-11-18: all three answer the trace request with
#:   HTTP 400 {"code":"malformed_request",
#:             "message":"... table 'traces' does not exist"}
#: while the other 11 answer 200.
NO_TRACES: frozenset[str] = frozenset({"celo", "mantle", "optimism"})

#: Every chain in ``SQDSource.EVM_DATASETS`` answered a ``logs`` request with
#: HTTP 200. A block that happened to contain no logs (linea, metis, scroll at
#: their mid-history probe block) is NOT evidence of a missing table — only the
#: 400 "does not exist" body is. Conflating the two would drop three working
#: chains from every evm_transfers backfill.
NO_LOGS: frozenset[str] = frozenset()


class MissingTableError(RuntimeError):
    """The portal does not serve this table for this chain.

    Distinct from :class:`SQDPortalError` (transport/overload, retry later) and
    from ``UnsupportedTableError`` (we have no field set at all): this one means
    the request is well-formed and the DATASET lacks the table, so retrying can
    never succeed.
    """


def tables_for(chain: str, table: str) -> bool:
    """True when the last measurement says ``chain`` serves ``table``."""
    if table == "internal_transfers":
        return chain not in NO_TRACES
    if table == "evm_transfers":
        return chain not in NO_LOGS
    return False


def _probe_block(src: SQDSource, dataset: str) -> int | None:
    """A block to probe at — mid-history, so it is certainly produced."""
    head = src.head(dataset)
    if head is None:
        return None
    return max(1, int(head * 0.55))


def _status_for(src: SQDSource, dataset: str, block: int, extra: dict[str, Any]) -> tuple[int, str]:
    body = {"type": "evm", "fromBlock": block, "toBlock": block, "fields": _PROBE_FIELDS, **extra}
    return src._request("POST", f"{src.PORTAL}/datasets/{dataset}/stream", body, timeout=60.0)


def probe_traces(src: SQDSource, chain: str) -> bool:
    """Live-ask whether ``chain``'s dataset serves the ``traces`` table.

    Returns:
        True when the portal accepts a traces request for this chain.

    Raises:
        SQDPortalError: when the portal cannot be reached (overload/transport),
            which is a different fact from "the table is missing".
    """
    if chain not in src.EVM_DATASETS:
        return False
    dataset = src.EVM_DATASETS[chain]
    block = _probe_block(src, dataset)
    if block is None:
        return False
    status, text = _status_for(src, dataset, block, {"traces": [{}]})
    if status == 200:
        return True
    if status == 400 and "does not exist" in text:
        logger.info("sqd_traces_absent", chain=chain, dataset=dataset)
        return False
    raise SQDPortalError(f"traces probe HTTP {status} for {dataset}: {text[:120]}")


def probe_logs(src: SQDSource, chain: str) -> bool:
    """Live-ask whether ``chain``'s dataset serves the ``logs`` table."""
    if chain not in src.EVM_DATASETS:
        return False
    dataset = src.EVM_DATASETS[chain]
    block = _probe_block(src, dataset)
    if block is None:
        return False
    status, text = _status_for(src, dataset, block, {"logs": [{}]})
    if status == 200:
        return True
    if status == 400 and "does not exist" in text:
        logger.info("sqd_logs_absent", chain=chain, dataset=dataset)
        return False
    raise SQDPortalError(f"logs probe HTTP {status} for {dataset}: {text[:120]}")


def require_table(chain: str, table: str) -> None:
    """Raise :class:`MissingTableError` for a measured-absent (chain, table).

    Called before a backfill spends its day-boundary budget, so a table the
    portal does not serve costs one static lookup instead of a failing run.
    """
    if tables_for(chain, table):
        return
    raise MissingTableError(
        f"the SQD portal does not serve {table!r} for {chain!r} "
        f"(measured 2026-11-18; re-check with scripts/probe-sqd-tables.py)"
    )
