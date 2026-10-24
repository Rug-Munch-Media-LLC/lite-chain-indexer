"""
Per-chain event type demand tracking for lazy indexing.

The lite-chain-indexer indexes all 16 chains, but historically every
chain indexed DEX swap + deployer events equally, burning RPC calls on
low-traffic chains (Scroll, Mantle, Linea) whose DEX/deployer tables
nobody queries. Lazy indexing fixes this:

  - Every chain starts with transfer events only.
  - Top chains (ethereum, bsc, polygon, arbitrum, base, solana) always
    index DEX + deployer events — they are high-traffic.
  - Long-tail chains (scroll, mantle, linea, fantom, avalanche, gnosis)
    only index DEX/deployer after the scanner signals demand.

Demand signaling uses a lightweight file touch (no cross-service
imports): the rmi-backend scanner touches
``{INDEXER_DEMAND_DIR}/{chain}/dex`` (or ``deployer``) whenever it
queries the ClickHouse dex_events / deployer_events tables for that
chain. The indexer reads these files each sync cycle and enables the
event type; once enabled it stays enabled (persisted in the state file).

This module also keeps an in-memory registry of observed demand for the
current process, exposed via :func:`record_scan_demand` /
:func:`get_chain_demand`.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from lite_indexer import metrics
from lite_indexer.config import INDEXER_DEMAND_DIR, LAZY_INDEXING_ENABLED
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Track which chains have been queried by the scanner
CHAIN_EVENT_DEMAND: dict[str, set[str]] = defaultdict(set)  # chain → {"dex", "deployer"}

# Event types that can be gated by scanner demand
DEMANDABLE_EVENT_TYPES: tuple[str, ...] = ("dex", "deployer")

# High-traffic chains: DEX + deployer events always indexed (not gated by demand).
TOP_CHAINS: frozenset[str] = frozenset(
    {
        "ethereum",
        "bsc",
        "polygon",
        "arbitrum",
        "base",
        "solana",
    }
)

# Long-tail chains: transfers only until the scanner signals demand.
LAZY_CHAINS: frozenset[str] = frozenset(
    {
        "scroll",
        "mantle",
        "linea",
        "fantom",
        "avalanche",
        "gnosis",
    }
)


def _demand_root(demand_dir: Path | str | None) -> Path:
    """Resolve the demand-tree root (explicit value, else the config default)."""
    return INDEXER_DEMAND_DIR if demand_dir is None else Path(demand_dir)


def record_scan_demand(chain: str, event_type: str) -> None:
    """Record that a scanner queried this chain for this event type.

    Args:
        chain: Chain name (e.g. ``'scroll'``). Lowercased on store.
        event_type: One of :data:`DEMANDABLE_EVENT_TYPES` (``'dex'`` or
            ``'deployer'``). Unknown event types are ignored.
    """
    if event_type not in DEMANDABLE_EVENT_TYPES:
        return
    CHAIN_EVENT_DEMAND[chain.lower()].add(event_type)


def get_chain_demand(chain: str) -> set[str]:
    """Get which event types are in demand for this chain.

    Returns a copy of the in-memory demand set (never mutate the caller's
    view of the shared registry).

    Args:
        chain: Chain name.

    Returns:
        Set of event types (subset of ``{"dex", "deployer"}``).
    """
    return set(CHAIN_EVENT_DEMAND.get(chain.lower(), set()))


def demand_file_path(chain: str, event_type: str, demand_dir: Path | str | None = None) -> Path:
    """Path of the scanner's demand signal file for a chain + event type.

    Args:
        chain: Chain name.
        event_type: ``'dex'`` or ``'deployer'``.
        demand_dir: Root of the demand tree. ``None`` uses the configured
            ``INDEXER_DEMAND_DIR`` (the production path).

    Returns:
        ``{demand_dir}/{chain}/{event_type}``.
    """
    return _demand_root(demand_dir) / chain.lower() / event_type


def read_demand_files(chain: str, demand_dir: Path | str | None = None) -> set[str]:
    """Read scanner demand signal files for a chain (file-touch approach).

    Returns the set of event types currently requested via file touch,
    e.g. ``{"dex"}`` when ``{INDEXER_DEMAND_DIR}/scroll/dex`` exists.

    Args:
        chain: Chain name.
        demand_dir: Root of the demand tree. ``None`` uses the configured
            ``INDEXER_DEMAND_DIR`` (the production path).

    Returns:
        Set of event types with an existing signal file.
    """
    demanded: set[str] = set()
    chain_dir = _demand_root(demand_dir) / chain.lower()
    if not chain_dir.is_dir():
        return demanded
    try:
        for event_type in DEMANDABLE_EVENT_TYPES:
            if (chain_dir / event_type).is_file():
                demanded.add(event_type)
    except OSError as exc:
        # Errno 13 on the demand mount silently disabled lazy indexing for the
        # chain; a scan we cannot perform is a failure, not a warning.
        logger.error("demand_scan_failed", chain=chain, error=str(exc)[:80])
        metrics.incr_labeled("lite_indexer_demand_scan_failures", chain=chain)
    return demanded


def default_enabled_events(chain: str, lazy_enabled: bool | None = None) -> set[str]:
    """Default event types enabled for a chain under the lazy policy.

    - Lazy indexing disabled (``LAZY_INDEXING_ENABLED=0``): every chain
      indexes DEX + deployer (legacy behavior — CLI flags are authority).
    - Lazy indexing enabled: top chains always index DEX + deployer;
      long-tail chains start transfers-only (demand-gated).

    Args:
        chain: Chain name.
        lazy_enabled: Whether lazy indexing is on. ``None`` uses the
            configured ``LAZY_INDEXING_ENABLED`` (the production path).

    Returns:
        Set of event types enabled by default (``{"dex", "deployer"}``
        or empty set).
    """
    if not (LAZY_INDEXING_ENABLED if lazy_enabled is None else lazy_enabled) or chain.lower() in TOP_CHAINS:
        return {"dex", "deployer"}
    return set()
