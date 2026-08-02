"""
Multi-chain parallelism — CPU affinity, memory budgets, priorities.

Improvement 6: when running multiple chains, give each chain explicit
resource guidance:

- **CPU affinity** — pin a chain to specific cores (Linux best-effort).
  Useful when running one chain per process; the multi-chain asyncio
  daemon shares one process, so affinity there is documented as a
  no-op and priorities drive scheduling instead.
- **Memory budgets** — Solana's large ``getBlock`` responses need the
  most headroom; EVM chains need far less. Exposed for logging and
  optional resource capping.
- **Priorities** — high-value chains (Ethereum, Solana) poll more
  frequently than low-value ones. :func:`effective_poll_interval`
  scales the base per-chain poll interval by priority.

All helpers are safe no-ops on platforms without the underlying OS
support (macOS, restricted containers).
"""

from __future__ import annotations

import os
import platform
from typing import Any

from lite_indexer.config import (
    CHAIN_AFFINITY_ENABLED,
    CHAIN_CPU_AFFINITY,
    CHAIN_MEMORY_BUDGET_MB,
    CHAIN_PRIORITY,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Default affinity for EVM chains not explicitly listed (cores 2-7).
_DEFAULT_EVM_AFFINITY = [2, 3, 4, 5, 6, 7]

# Baseline priority when a chain is not listed (low-value default).
_DEFAULT_PRIORITY = 5

# Top priority — chains at this level poll at their base interval.
_MAX_PRIORITY = 10


def chain_affinity(chain: str) -> list[int]:
    """Return the configured CPU affinity (core list) for a chain.

    Args:
        chain: Chain name.

    Returns:
        List of CPU core indices.
    """
    affinity = CHAIN_CPU_AFFINITY.get(chain)
    if affinity is not None:
        return list(affinity)
    # EVM chains default to the shared EVM core range.
    if chain not in ("solana", "tron"):
        return list(_DEFAULT_EVM_AFFINITY)
    return []


def memory_budget_mb(chain: str) -> int:
    """Return the configured memory budget (MB) for a chain.

    Args:
        chain: Chain name.

    Returns:
        Budget in MB (512 for unlisted EVM chains).
    """
    budget = CHAIN_MEMORY_BUDGET_MB.get(chain)
    if budget is not None:
        return budget
    return 512 if chain not in ("solana", "tron") else 1024


def chain_priority(chain: str) -> int:
    """Return the polling priority (1-10) for a chain.

    Args:
        chain: Chain name.

    Returns:
        Priority value.
    """
    return CHAIN_PRIORITY.get(chain, _DEFAULT_PRIORITY)


def effective_poll_interval(chain: str, base_interval: float) -> float:
    """Scale a chain's base poll interval by its priority.

    High-value chains (priority close to 10) poll at the base interval;
    lower-value chains poll less often (interval scaled up), reducing
    wasted RPC calls on chains nobody queries.

    Args:
        chain: Chain name.
        base_interval: Base poll interval from
            :data:`lite_indexer.chains.polling.CHAIN_POLL_INTERVALS`.

    Returns:
        Effective poll interval in seconds.
    """
    priority = chain_priority(chain)
    if priority >= _MAX_PRIORITY:
        return base_interval
    factor = (_MAX_PRIORITY + 1 - priority) / (_MAX_PRIORITY + 1 - _DEFAULT_PRIORITY)
    # Low-priority chains poll at up to ~2x the base interval.
    return max(base_interval, base_interval * factor)


def apply_chain_affinity(chain: str) -> bool:
    """Best-effort pin of the current process to a chain's CPU cores.

    Only applies when ``CHAIN_AFFINITY_ENABLED=1`` (opt-in): the default
    multi-chain daemon shares one process, so affinity only makes sense
    for single-chain deployments (one process per chain).

    Args:
        chain: Chain name.

    Returns:
        True when the affinity was applied.
    """
    if not CHAIN_AFFINITY_ENABLED:
        return False
    if platform.system() != "Linux":
        logger.debug("chain_affinity_unsupported", chain=chain, platform=platform.system())
        return False

    cores = chain_affinity(chain)
    if not cores:
        return False

    try:
        os.sched_setaffinity(0, cores)
        logger.info("chain_affinity_applied", chain=chain, cores=cores)
        return True
    except (AttributeError, OSError, ValueError) as exc:
        logger.warning("chain_affinity_failed", chain=chain, cores=cores, error=str(exc)[:80])
        return False


def log_chain_resources(chain: str, *, poll_interval: float) -> dict[str, Any]:
    """Log and return the resource profile for a chain.

    Args:
        chain: Chain name.
        poll_interval: Base poll interval being used.

    Returns:
        Dict with priority, memory budget, affinity cores, and the
        effective poll interval.
    """
    effective = effective_poll_interval(chain, poll_interval)
    profile = {
        "chain": chain,
        "priority": chain_priority(chain),
        "memory_budget_mb": memory_budget_mb(chain),
        "affinity_cores": chain_affinity(chain),
        "poll_interval_s": round(effective, 2),
    }
    logger.info("chain_resource_profile", **profile)
    return profile


__all__ = [
    "chain_affinity",
    "memory_budget_mb",
    "chain_priority",
    "effective_poll_interval",
    "apply_chain_affinity",
    "log_chain_resources",
]
