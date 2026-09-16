"""trace_wallet — one wallet, every federation tier, one answer.

Assembles a wallet's on-chain history across the whole federated stack:

* **Indexer federation** (ClickHouse hot → B2 cold → BigLake → Envio →
  live RPC) via :class:`~lite_indexer.query.router.QueryRouter` for EVM
  chains — every tier the router already federates, in one call.
* **Envio traces** — ``trace_block`` internal transactions on ethereum/base
  (bounded windows only; block-wide scans are expensive).
* **SQD Portal** — Solana (``mentionsAccount`` instruction activity), Tron
  (TRC-20 + native internals), Hyperliquid fills.

Every row carries a ``tier`` + ``kind`` provenance tag so callers can show
(and trust) where data came from.

CLI:
    python -m lite_indexer.query.trace_wallet solana <owner> --from-block <slot> --to-block <slot>
    python -m lite_indexer.query.trace_wallet ethereum 0xADDR --from-block N --to-block M [--no-traces]
    python -m lite_indexer.query.trace_wallet tron T... --from-block N --to-block M
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.envio_source import EnvioSource
from lite_indexer.query.sqd_source import SQDSource

logger = get_logger(__name__)

#: Chains whose wallet history comes from SQD directly (non-EVM shapes).
_SQD_CHAINS = {"solana", "tron", "hyperliquid"}

#: Cap on blocks scanned for trace_block windows (each block ≈ thousands of traces).
_DEFAULT_TRACE_WINDOW = 10


def _envelope(tier: str, kind: str, row: dict[str, Any]) -> dict[str, Any]:
    """Wrap a source row with provenance; normalize ordering keys."""
    block = row.get("block_number")
    if block is None:
        block = row.get("slot")
    if block is None:
        block = row.get("block")
    out = {
        "tier": tier,
        "kind": kind,
        "block": block,
        "timestamp": row.get("block_timestamp"),
    }
    for key, value in row.items():
        if key in ("block_number", "slot", "block", "block_timestamp", "chain", "ingested_at"):
            continue
        out[key] = value
    return out


def _default_router() -> Any | None:
    """Build the full federated router (hot + cold + biglake + envio + rpc)."""
    try:
        from lite_indexer.config import HISTORICAL_PARQUET_REMOTE, ON_DEMAND_RPC_ENABLED
        from lite_indexer.query import QueryRouter
        from lite_indexer.storage.clickhouse import ClickHouseWriter
        from lite_indexer.storage.cold_store import ColdStore

        return QueryRouter(
            ch_client=ClickHouseWriter().client(),
            cold_store=ColdStore(remote=HISTORICAL_PARQUET_REMOTE),
            on_demand_enabled=ON_DEMAND_RPC_ENABLED,
        )
    except Exception as exc:  # noqa: BLE001 — router is best-effort for the CLI
        logger.warning("trace_wallet_router_unavailable", error=str(exc)[:120])
        return None


async def trace_wallet(
    chain: str,
    address: str,
    *,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int = 500,
    include_traces: bool = True,
    trace_window: int = _DEFAULT_TRACE_WINDOW,
    use_router: bool = True,
    router: Any | None = None,
    envio: EnvioSource | None = None,
    sqd: SQDSource | None = None,
) -> dict[str, Any]:
    """Assemble a wallet's history from every tier that can serve it.

    Args:
        chain: Chain name (``ethereum``, ``base``, ``solana``, ``tron``,
            ``hyperliquid``, ...).
        address: Wallet address (EVM hex; base58/hex for Solana/Tron).
        from_block/to_block: Native range (blocks or slots).
        limit: Per-source row cap.
        include_traces: Include Envio ``trace_block`` internals
            (ethereum/base only, bounded window).
        trace_window: Max blocks scanned for traces.
        use_router: Try the indexer federation (hot→cold→biglake→envio→rpc).
        router/envio/sqd: Injectable sources (tests).

    Returns:
        ``{chain, address, counts, rows, notes}`` — rows sorted newest
        first, each tagged ``tier`` + ``kind``.
    """
    chain = str(chain or "").strip().lower()
    address = str(address or "").strip()
    envio = envio if envio is not None else EnvioSource()
    sqd = sqd if sqd is not None else SQDSource()
    notes: list[str] = []
    rows: list[dict[str, Any]] = []
    limit = max(1, min(int(limit), 1000))

    is_sqd_chain = chain in _SQD_CHAINS
    evm_supported = chain in EnvioSource.CHAIN_IDS and not is_sqd_chain

    # ── EVM: router federation (falls back to direct Envio) ──────────
    if evm_supported:
        served = False
        if use_router and from_block is not None and to_block is not None:
            active_router = router
            if active_router is None:
                active_router = await asyncio.to_thread(_default_router)
            if active_router is not None:
                try:
                    fed = await active_router.get_transfers(
                        chain, address, from_block=from_block, to_block=to_block, limit=limit
                    )
                    rows.extend(_envelope("indexer", "transfer", r) for r in fed)
                    served = True
                except Exception as exc:  # noqa: BLE001 — tier failures are noted, not fatal
                    notes.append(f"indexer federation failed: {str(exc)[:100]}")
        if not served:
            if from_block is None or to_block is None:
                notes.append("evm transfers: no bounded range provided (from/to block required)")
            elif envio.available():
                direct = await asyncio.to_thread(
                    envio.address_transfers, chain, address, from_block, to_block, limit=limit
                )
                rows.extend(_envelope("envio", "transfer", r) for r in direct)
            else:
                notes.append("evm transfers: Envio token missing (ENVIO_RPC_TOKEN)")

        # ── Envio traces (bounded window) ────────────────────────────
        if (
            include_traces
            and chain in EnvioSource.TRACES_CHAINS
            and from_block is not None
            and to_block is not None
        ):
            window = min(int(trace_window), to_block - from_block + 1)
            if window <= 0:
                notes.append("traces: empty range")
            else:
                start = max(from_block, to_block - window + 1)
                addr_lower = address.lower()
                traced = 0
                for block_number in range(start, start + window):
                    try:
                        block_rows = await asyncio.to_thread(
                            envio.block_traces_rows, chain, block_number
                        )
                    except Exception as exc:  # noqa: BLE001
                        notes.append(f"traces block {block_number} failed: {str(exc)[:80]}")
                        continue
                    for r in block_rows:
                        if r.get("from_address") == addr_lower or r.get("to_address") == addr_lower:
                            rows.append(_envelope("envio", "internal_transfer", r))
                            traced += 1
                if not traced:
                    notes.append(f"traces: no internal transfers in last {window} blocks")

    # ── Solana (SQD instructions mentioning the owner) ───────────────
    elif chain == "solana":
        if sqd.available() and from_block is not None and to_block is not None:
            sol_rows = await asyncio.to_thread(
                sqd.solana_owner_activity, address, from_block, to_block, limit=limit
            )
            rows.extend(_envelope("sqd", "instruction", r) for r in sol_rows)
        else:
            notes.append("solana: from/to slot range required (and SQD available)")

    # ── Tron (SQD TRC-20 + native internals) ─────────────────────────
    elif chain == "tron":
        if sqd.available() and from_block is not None and to_block is not None:
            trc20 = await asyncio.to_thread(
                sqd.tron_trc20_transfers, address, from_block, to_block, limit=limit
            )
            rows.extend(_envelope("sqd", "transfer", r) for r in trc20)
            internal = await asyncio.to_thread(
                sqd.tron_internal_transfers, address, from_block, to_block, limit=limit
            )
            rows.extend(_envelope("sqd", "internal_transfer", r) for r in internal)
        else:
            notes.append("tron: from/to block range required (and SQD available)")

    # ── Hyperliquid (SQD fills) ──────────────────────────────────────
    elif chain == "hyperliquid":
        if sqd.available() and from_block is not None and to_block is not None:
            fills = await asyncio.to_thread(
                sqd.hyperliquid_owner_fills, address, from_block, to_block, limit=limit
            )
            rows.extend(_envelope("sqd", "fill", r) for r in fills)
        else:
            notes.append("hyperliquid: from/to fill-block range required")

    else:
        notes.append(f"chain not supported by trace_wallet: {chain}")

    rows.sort(key=lambda r: r.get("block") or 0, reverse=True)
    rows = rows[: max(limit, 1) * 3]  # cap overall envelope

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["tier"]] = counts.get(row["tier"], 0) + 1

    return {
        "chain": chain,
        "address": address,
        "from_block": from_block,
        "to_block": to_block,
        "counts": counts,
        "rows": rows,
        "notes": notes,
        "generated_at": datetime.now(UTC).isoformat(),
    }


# ── CLI ──────────────────────────────────────────────────────────────


def _silence_console_logs() -> None:
    """CLI stdout must stay machine-parseable (JSON only); logs go to stderr."""
    try:
        from loguru import logger as _loguru

        _loguru.remove()
    except ImportError:
        pass


def _cli() -> int:
    _silence_console_logs()
    parser = argparse.ArgumentParser(prog="trace_wallet", description=__doc__)
    parser.add_argument("chain")
    parser.add_argument("address")
    parser.add_argument("--from-block", default=None, help="int or 0x-hex")
    parser.add_argument("--to-block", default=None)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--no-traces", action="store_true", help="skip trace_block scanning")
    parser.add_argument("--no-router", action="store_true", help="skip ClickHouse/B2 federation")
    parser.add_argument("--trace-window", type=int, default=_DEFAULT_TRACE_WINDOW)
    args = parser.parse_args()

    result = asyncio.run(
        trace_wallet(
            args.chain,
            args.address,
            from_block=int(args.from_block, 0) if args.from_block else None,
            to_block=int(args.to_block, 0) if args.to_block else None,
            limit=args.limit,
            include_traces=not args.no_traces,
            trace_window=args.trace_window,
            use_router=not args.no_router,
        )
    )
    print(json.dumps(result, default=str, indent=2))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
