"""
Chain health monitoring — check all configured chains for RPC availability.

Provides a unified health check function that tests each chain's ability
to fetch a block number, giving a quick "is everything working?" status
across the entire multi-chain indexer.

Also provides a chain-lag check (``check_chain_lag``) that compares each
chain's state file (last indexed block) against the chain tip fetched
via RPC, so operators can see which chains are falling behind.

Usage:
    from lite_indexer.health import check_chain_health, check_chain_lag

    results = await check_chain_health()
    for chain, status in results.items():
        print(f"{chain}: {status['status']}")

    lag = await check_chain_lag()
    for chain, info in lag.items():
        print(f"{chain}: {info['lag_blocks']} blocks behind")
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx

from lite_indexer.config import STATE_DIR
from lite_indexer.logging import get_logger
from lite_indexer.strategies import SUPPORTED_CHAINS, create_strategy

logger = get_logger(__name__)

# Chains whose tip advances very fast (blocks/min). Used to translate a
# block lag into a human "how long behind" estimate.
_CHAIN_BLOCK_TIMES = {
    "ethereum": 12.0,
    "base": 2.0,
    "bsc": 3.0,
    "polygon": 2.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "avalanche": 2.0,
    "fantom": 1.0,
    "linea": 2.0,
    "scroll": 3.0,
    "zksync": 2.0,
    "mantle": 2.0,
    "gnosis": 5.0,
    "celo": 5.0,
    "solana": 0.4,
    "tron": 3.0,
}


async def check_chain_lag(
    chain_names: list[str] | None = None,
    timeout: float = 15.0,
) -> dict[str, dict[str, Any]]:
    """Report how far behind each chain's state file is vs the RPC tip.

    Reads each chain's state file (``{STATE_DIR}/lite_{chain}.json``),
    fetches the current tip via RPC, and reports the block gap plus an
    estimated seconds-behind figure using the chain's average block time.

    A chain whose state file is missing or stale (not updated recently)
    is reported with ``status: "no_state"`` or ``status: "stale"``.

    Args:
        chain_names: Optional list of chains to check. If None, checks
            all chains in SUPPORTED_CHAINS.
        timeout: Per-chain RPC timeout in seconds (default: 15).

    Returns:
        Dict mapping chain name to lag info:
        {
            "ethereum": {
                "status": "ok" | "behind" | "no_state" | "stale" | "error",
                "state_block": 25676000,
                "tip_block": 25676548,
                "lag_blocks": 548,
                "lag_seconds": 6576,
                "state_age_seconds": 42,
                "chain_type": "evm",
                "label": "Ethereum",
            },
        }
    """
    if chain_names is None:
        chain_names = sorted(SUPPORTED_CHAINS.keys())

    def _load_state_block(chain: str) -> tuple[int | None, float | None]:
        """Read last_block + state file age. Returns (block, age_seconds)."""
        path = Path(STATE_DIR) / f"lite_{chain}.json"
        if not path.exists():
            return None, None
        try:
            data = json.loads(path.read_text())
            block = data.get("last_block")
            if block is None:
                return None, None
            import time

            age = time.time() - path.stat().st_mtime
            return int(block), age
        except (json.JSONDecodeError, OSError, TypeError, ValueError):
            return None, None

    async def _check_one(chain: str) -> tuple[str, dict[str, Any]]:
        chain_info = SUPPORTED_CHAINS.get(chain, {"type": "unknown", "label": chain.capitalize()})
        result: dict[str, Any] = {
            "chain_type": chain_info.get("type", "unknown"),
            "label": chain_info.get("label", chain.capitalize()),
        }

        state_block, state_age = _load_state_block(chain)
        if state_block is None:
            result["status"] = "no_state"
            return chain, result
        result["state_block"] = state_block
        if state_age is not None:
            result["state_age_seconds"] = round(state_age, 1)

        try:
            strategy = create_strategy(chain=chain)
            tip = await asyncio.wait_for(strategy.fetch_block_number(), timeout=timeout)
        except TimeoutError:
            result["status"] = "error"
            result["error"] = f"timeout after {timeout}s"
            return chain, result
        except (httpx.HTTPError, ConnectionError, OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            result["status"] = "error"
            result["error"] = str(exc)[:200]
            logger.warning("lag_check_failed", chain=chain, error=str(exc)[:100])
            return chain, result

        if tip is None:
            result["status"] = "error"
            result["error"] = "fetch_block_number returned None"
            return chain, result

        result["tip_block"] = tip
        lag = max(0, tip - state_block)
        result["lag_blocks"] = lag

        block_time = _CHAIN_BLOCK_TIMES.get(chain, 12.0)
        result["lag_seconds"] = int(lag * block_time)

        # A state file that hasn't been updated in 3 poll intervals is stale
        # (the daemon may be wedged, e.g. stuck on a blocking CH call).
        poll_interval = _CHAIN_BLOCK_TIMES.get(chain, 12.0) * 3 + 30
        if state_age is not None and state_age > max(300, poll_interval):
            result["status"] = "stale"
        elif lag == 0:
            result["status"] = "ok"
        else:
            result["status"] = "behind"

        return chain, result

    tasks = [_check_one(c) for c in chain_names]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    results: dict[str, dict[str, Any]] = {}
    for outcome in outcomes:
        if isinstance(outcome, tuple):
            chain, result = outcome
            results[chain] = result
        elif isinstance(outcome, Exception):
            logger.warning("lag_check_unexpected", error=str(outcome)[:100])

    return results


async def print_chain_lag_report(
    chain_names: list[str] | None = None,
) -> str:
    """Run lag checks and return a human-readable report string.

    Args:
        chain_names: Optional list of chains to check.

    Returns:
        Multi-line string with per-chain lag status.
    """
    results = await check_chain_lag(chain_names)

    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("LiteIndexer — Chain Lag Report")
    lines.append("=" * 60)

    ok_count = 0
    behind_count = 0
    error_count = 0

    for chain in sorted(results.keys()):
        info = results[chain]
        label = info.get("label", chain)
        status = info.get("status", "unknown")

        if status == "ok":
            lines.append(f"  ✅ {label:20s}  caught up (block={info.get('state_block'):,})")
            ok_count += 1
        elif status == "behind":
            lag_s = info.get("lag_seconds", 0)
            lines.append(
                f"  ⏳ {label:20s}  behind {info.get('lag_blocks', 0):,} blocks "
                f"(~{lag_s // 60} min, state={info.get('state_block'):,} tip={info.get('tip_block'):,})"
            )
            behind_count += 1
        elif status == "stale":
            lines.append(
                f"  ⚠️  {label:20s}  STALE state file "
                f"({info.get('state_age_seconds', 0):.0f}s old, block={info.get('state_block'):,})"
            )
            behind_count += 1
        elif status == "no_state":
            lines.append(f"  ❓ {label:20s}  no state file")
            error_count += 1
        else:
            err = info.get("error", "unknown error")
            lines.append(f"  ❌ {label:20s}  error={err}")
            error_count += 1

    lines.append("-" * 60)
    total = ok_count + behind_count + error_count
    lines.append(
        f"  Total: {total:2d}  |  OK: {ok_count:2d}  |  Behind: {behind_count:2d}  |  Errors: {error_count:2d}"
    )
    lines.append("=" * 60)

    return "\n".join(lines)


async def check_chain_health(
    chain_names: list[str] | None = None,
    timeout: float = 15.0,
) -> dict[str, dict[str, Any]]:
    """Health check for all configured chains.

    Creates a strategy for each chain and attempts to fetch the latest
    block number (or slot for Solana). Reports status as 'ok' or 'error'
    with the block number or error message.

    Args:
        chain_names: Optional list of chains to check. If None, checks
            all chains in SUPPORTED_CHAINS.
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
        except TimeoutError:
            result["status"] = "error"
            result["error"] = f"timeout after {timeout}s"
        except (httpx.HTTPError, ConnectionError, OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            result["status"] = "error"
            result["error"] = str(exc)[:200]
            logger.warning("health_check_failed", chain=chain, error=str(exc)[:100])

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
) -> str:
    """Run health checks and return a human-readable report string.

    Useful for CLI / monitoring / systemd health checks.

    Args:
        chain_names: Optional list of chains to check.

    Returns:
        Multi-line string with a formatted health report.
    """
    results = await check_chain_health(chain_names)

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
    """Run health check from command line.

    Modes:
      default  — RPC availability per chain (check_chain_health)
      --lag    — state-vs-tip lag report (check_chain_lag)

    The lag mode is intended for a systemd timer: if any chain is
    ``stale`` or far behind, the timer restarts the indexer service.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="LiteIndexer chain health check")
    parser.add_argument(
        "--chains",
        type=str,
        default=None,
        help="Comma-separated chain names (default: all)",
    )
    parser.add_argument(
        "--lag",
        action="store_true",
        help="Report state-vs-tip lag instead of RPC availability",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON (one line per chain status)",
    )
    args = parser.parse_args()

    chain_names = [c.strip() for c in args.chains.split(",")] if args.chains else None

    if args.lag:
        results = await check_chain_lag(chain_names)
        if args.json:
            print(json.dumps(results, default=str))  # noqa: T201
            # Exit non-zero when any chain is stale or behind.
            sys.exit(1 if any(r.get("status") in ("stale", "behind", "error") for r in results.values()) else 0)
        report = await print_chain_lag_report(chain_names)
        print(report)  # noqa: T201
        return

    report = await print_health_report(chain_names)
    print(report)  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
