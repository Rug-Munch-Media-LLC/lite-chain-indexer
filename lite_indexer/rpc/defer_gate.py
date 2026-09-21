"""Deferral gate + one-per-level warnings for the RPC client (extracted 2026-09-21).

Micro-pass 2 of the rpc/client.py split: the dedup state and the two helpers
move together; client.py re-exports them so import sites are unchanged.
"""

from __future__ import annotations

from lite_indexer.config import (
    RPC_CALL_CLIENTS_MAX,  # noqa: F401  (re-exported; rpc.client_pool reads it at runtime)
)
from lite_indexer.logging import get_logger
from lite_indexer.rpc.client_pool import (
    _call_clients,  # noqa: F401  (re-export for import stability)
    aclose_call_clients,  # noqa: F401  (re-export for import stability)
)
from lite_indexer.rpc.ledger import (
    LEDGER,
    account_id_for_endpoint,
    defer_reason,
    key_id_for_endpoint,
    provider_id_for_endpoint,
)
from lite_indexer.rpc.ts_cache import (  # noqa: F401  (public re-export)
    _ts_cache_get,
    _ts_cache_put,
    clear_timestamp_cache,
)

# (provider_id, key_id, level) tuples already warned about — the defer gate
# logs at warning once per provider+level per process, never per call.
_defer_warned: set[tuple[str, str, int]] = set()


logger = get_logger(__name__)


def _defer_reason_for(
    endpoint: str,
    method: str,
    *,
    range_blocks: int | None = None,
) -> str | None:
    """Guarded quota-degrade gate for the RPC fetch path.

    Derives the account-level provider/key id from the endpoint URL and asks
    the ledger whether this call should be deferred. Never raises: on any
    ledger error it degrades to ``None`` (allow the call) so enforcement can
    never break the RPC path.
    """
    try:
        return defer_reason(
            provider_id_for_endpoint(endpoint),
            method,
            key_id_for_endpoint(endpoint),
            range_blocks=range_blocks,
            account_id=account_id_for_endpoint(endpoint),
        )
    except Exception as exc:  # noqa: BLE001 — enforcement must never break RPC
        logger.debug("rpc_defer_gate_skipped", endpoint=endpoint[:50], error=str(exc)[:80])
        return None


def _warn_deferred(endpoint: str, method: str, reason: str) -> None:
    """Log a deferral at warning, once per provider+level (not spammy)."""
    provider_id = ""
    key_id = ""
    account_id = ""
    level = 0
    try:
        provider_id = provider_id_for_endpoint(endpoint)
        key_id = key_id_for_endpoint(endpoint)
        account_id = account_id_for_endpoint(endpoint)
        level = LEDGER.degrade_level_for(provider_id, key_id, account_id=account_id)
    except Exception as exc:  # noqa: BLE001 — dedup bookkeeping must never raise
        logger.debug("rpc_defer_warn_skipped", endpoint=endpoint[:50], error=str(exc)[:80])
    dedup_key = (provider_id, account_id, level)
    if dedup_key in _defer_warned:
        return
    _defer_warned.add(dedup_key)
    logger.warning(
        "rpc_call_deferred",
        provider=provider_id,
        account_id=account_id,
        key_id=key_id,
        level=level,
        method=method,
        reason=reason,
    )
