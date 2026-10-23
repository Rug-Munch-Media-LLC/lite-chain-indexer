"""Daemon startup wiring: the daily-cadence contract and signal handlers."""

from __future__ import annotations

import asyncio
import signal
from functools import partial

from lite_indexer.chain_states import load_parked
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def announce_daily_cadence(chains: list[str]) -> set[str]:
    """Load parked declarations BEFORE the chain loops; return the parked set.

    A malformed declaration must fail the daemon at startup instead of
    silently quieting a dead chain's alert.
    """
    parked = load_parked(known=set(chains))
    logger.info(
        "daily_cadence_contract",
        chains=len(chains),
        parked=sorted(parked),
        cadence_s=86400,
        stall_after_s=900,
    )
    return set(parked)


def install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    """Route SIGTERM/SIGINT into ``shutdown_event`` (best effort)."""

    def _handle_signal(sig: int, _frame: object = None) -> None:
        logger.info("daemon_signal_received", signal=signal.Signals(sig).name)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, partial(_handle_signal, sig))
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, _handle_signal)
