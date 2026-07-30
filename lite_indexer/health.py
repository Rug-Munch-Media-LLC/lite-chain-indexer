"""
Chain health monitoring — check all configured chains for RPC availability.

Provides a unified health check function that tests each chain's ability
to fetch a block number, giving a quick "is everything working?" status
across the entire multi-chain indexer.

Usage:
    from lite_indexer.health import check_chain_health

    results = await check_chain_health()
    for chain, status in results.items():
        print(f"{chain}: {status['status']}")
"""

from __future__ import annotations

import asyncio
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.strategies import SUPPORTED_CHAINS, create_strategy

logger = get_logger(__name__)


async def check_chain_health(
    chain_names: list[str] | None = None,
    allow_paid: bool | None = None,
    timeout: float = 15.0,
) -> dict[str, dict[str, Any]]:
    """Health check for all configured chains.

    Creates a strategy for each chain and attempts to fetch the latest
    block number (or slot for Solana). Reports status as 'ok' or 'error'
    with the block number or error message.

    Args:
        chain_names: Optional list of chains to check. If None, checks
            all chains in SUPPORTED_CHAINS.
        allow_paid: Whether to allow paid RPC endpoints. Passed to
            strategy factory and get_chain_endpoints.
        timeout: Per-chain timeout in seconds (default: 15).

    Returns:
        Dict mapping chain name to status dict:
        {
            "ethereum": {
                "status": "ok",
                "block": 12345678,
                "chain_type": "evm",
                "label": "Ethereum",
            },
            "solana": {
                "status": "error",
                "error": "Connection timeout",
                "chain_type": "solana",
                "label": "Solana",
            },
        }
    """
    if chain_names is None:
        chain_names = sorted(SUPPORTED_CHAINS.keys())

    results: dict[str, dict[str, Any]] = {}

    async def _check_one(chain: str) -> tuple[str, dict[str, Any]]:
        chain_info = SUPPORTED_CHAINS.get(chain, {"type": "unknown", "label": chain.capitalize()})
        result: dict[str, Any] = {
            "chain_type": chain_info.get("type", "unknown"),
            "label": chain_info.get("label", chain.capitalize()),
        }

        try:
            strategy = create_strategy(chain=chain)
            block = await asyncio.wait_for(
                strategy.fetch_block_number(),
                timeout=timeout,
            )
            if block is not None:
                result["status"] = "ok"
                result["block"] = block
            else:
                result["status"] = "error"
                result["error"] = "fetch_block_number returned None"
        except asyncio.TimeoutError:
            result["status"] = "error"
            result["error"] = f"timeout after {timeout}s"
        except Exception as exc:
            result["status"] = "error"
            result["error"] = str(exc)[:200]
            logger.debug("health_check_failed", chain=chain, error=str(exc)[:100])

        return chain, result

    tasks = [_check_one(c) for c in chain_names]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    for outcome in outcomes:
        if isinstance(outcome, tuple):
            chain, result = outcome
            results[chain] = result
        elif isinstance(outcome, Exception):
            # Should not happen with return_exceptions=True, but handle anyway
            logger.warning("health_check_unexpected", error=str(outcome)[:100])

    return results


async def print_health_report(
    chain_names: list[str] | None = None,
    allow_paid: bool | None = None,
) -> str:
    """Run health checks and return a human-readable report string.

    Useful for CLI / monitoring / systemd health checks.

    Args:
        chain_names: Optional list of chains to check.
        allow_paid: Whether to allow paid RPC endpoints.

    Returns:
        Multi-line string with a formatted health report.
    """
    results = await check_chain_health(chain_names, allow_paid)

    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("LiteIndexer — Chain Health Report")
    lines.append("=" * 60)

    ok_count = 0
    error_count = 0

    for chain in sorted(results.keys()):
        info = results[chain]
        label = info.get("label", chain)
        status = info.get("status", "unknown")

        if status == "ok":
            block = info.get("block", "?")
            lines.append(f"  ✅ {label:20s}  block={block:>12,}")
            ok_count += 1
        else:
            err = info.get("error", "unknown error")
            lines.append(f"  ❌ {label:20s}  error={err}")
            error_count += 1

    lines.append("-" * 60)
    total = ok_count + error_count
    lines.append(f"  Total: {total:2d}  |  OK: {ok_count:2d}  |  Errors: {error_count:2d}")
    lines.append("=" * 60)

    return "\n".join(lines)


# ── CLI entry point for quick health check ─────────────────────────


async def main() -> None:
    """Run health check from command line."""
    import argparse

    parser = argparse.ArgumentParser(description="LiteIndexer chain health check")
    parser.add_argument(
        "--chains",
        type=str,
        default=None,
        help="Comma-separated chain names (default: all)",
    )
    parser.add_argument(
        "--use-paid-rpc",
        action="store_true",
        help="Allow paid RPC endpoints",
    )
    args = parser.parse_args()

    chain_names = [c.strip() for c in args.chains.split(",")] if args.chains else None
    report = await print_health_report(chain_names, allow_paid=args.use_paid_rpc)
    print(report)  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
