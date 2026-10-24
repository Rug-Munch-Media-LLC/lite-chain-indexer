"""Broad-getLogs capability verdicts (exclusion, staleness, filtering).

Split out of capabilities.py (2026-10-24). Behaviour is unchanged.
"""

from __future__ import annotations

import time

from lite_indexer.rpc.capability_constants import CAPABILITY_REPROBE_INTERVAL
from lite_indexer.rpc.capability_record import CapabilityRecord
from lite_indexer.rpc.capability_registry import get_capability, set_capability


def set_logs_capability(endpoint: str, chain: str, capable: bool) -> CapabilityRecord:
    """Persist the broad-getLogs capability for one endpoint.

    Creates a record when none exists (chain + endpoint only), preserving
    any previously probed archive / range fields.
    """
    record = get_capability(endpoint)
    if record is None:
        record = CapabilityRecord(endpoint=endpoint, chain=chain)
    record.logs_capable = capable
    record.probed_at = time.time()
    set_capability(record)
    return record


def is_logs_capability_excluded(endpoint: str) -> bool:
    """Whether ``endpoint`` is known to reject broad eth_getLogs.

    A capability-excluded endpoint must never receive ``eth_getLogs`` (it is
    permanently tip/head-only). Unknown or transiently-failed endpoints
    return False so a single timeout never removes healthy capacity.
    """
    record = get_capability(endpoint)
    return record is not None and record.logs_capable is False


def is_logs_capability_reprobe_due(endpoint: str, now: float | None = None) -> bool:
    """Whether a ``logs_capable=False`` verdict is stale enough to re-test.

    A capability exclusion is a local synthesized verdict, not a provider
    ban. Once the record is older than :data:`CAPABILITY_REPROBE_INTERVAL`
    the exclusion must stop blocking the desperation probe — otherwise a
    chain whose entire log pool is capability-excluded can never probe its
    way back and stays frozen until the next hourly re-verification.

    Args:
        endpoint: RPC endpoint URL.
        now: Epoch time override (tests).

    Returns:
        True when the endpoint is capability-excluded but the verdict is
        stale and the endpoint may be probed again.
    """
    record = get_capability(endpoint)
    if record is None or record.logs_capable is not False:
        return False
    now = time.time() if now is None else now
    return (now - record.probed_at) >= CAPABILITY_REPROBE_INTERVAL


def logs_capable_endpoints(endpoints: list[str]) -> list[str]:
    """Filter ``endpoints`` to those not capability-excluded from getLogs."""
    return [ep for ep in endpoints if not is_logs_capability_excluded(ep)]
