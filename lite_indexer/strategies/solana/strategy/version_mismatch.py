"""Detect a transaction-version rejection in an RPC result payload.

Split out of ``SyncMixin._rpc_call_versioned`` so the strategy module stays
under its shrink-only size ratchet, and so the rule is testable directly.
"""

from __future__ import annotations

from typing import Any


def is_version_mismatch(res: Any) -> bool:
    """True when the RPC rejected the call over the tx-version cap.

    2026-09-21 measured: endpoints answer a v0 cap with -32015 in ~0.5s,
    and the pool then burns retries and rotation on it. Treating the error
    payload as a version mismatch lets us flip the cap immediately instead
    of waiting for the pool to give up.

    Args:
        res: The raw JSON-RPC result, which may be ``None`` or an error
            envelope rather than the expected payload.

    Returns:
        True when the error names a transaction-version limit.
    """
    if not isinstance(res, dict):
        return False
    err = res.get("error") or {}
    msg = f"{err.get('code', '')} {err.get('message', '')}".lower()
    return "transaction version" in msg or "-32015" in msg


__all__ = ["is_version_mismatch"]
