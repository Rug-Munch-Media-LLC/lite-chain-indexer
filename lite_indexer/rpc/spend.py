"""Per-account count of dispatches that left the free tier.

HONEST SCOPE — READ THIS BEFORE TRUSTING THE NUMBER
    ``rpc_paid_spend_estimate`` is a **REQUEST-COUNT PROXY, NOT A USD
    MODEL.** There is no price feed in this codebase, no per-provider rate
    card, and no invoice correlation, so nothing here converts requests into
    money and nothing here should be read as a dollar figure. Inventing a
    rate table would be worse than useless: it would be a number nobody can
    audit, drift silently, and quietly drive routing decisions.

    What it *is*: a monotonic count of how many requests this process has
    pushed at billable-or-operator-blessed accounts since the last UTC
    midnight, plus a per-account gauge of the same count. That is enough to
    answer the two questions operators actually ask after a billing scare —
    "is anything spending?" and "which account?" — and enough to make the
    spillover gate's budget check observable.

    If a real USD model is ever wanted it must come from the provider's own
    published pricing, reviewed per provider, and would belong in a separate
    module so this one keeps stating plainly what it is not.

Scope: counts paid dispatches. It does not pace them (that is
``rpc/budgets.py``), does not classify them (``rpc/cost_tier.py``), and does
not decide whether one is permitted (``rpc/spillover.py``).

State is in-process and safe under asyncio (single-threaded event loop, no
locks). Counters reset at the UTC day rollover, matching ``rpc/budgets.py``.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from lite_indexer.metrics import set_labeled_gauge

#: Gauge series name; one time series per account via the ``account`` label.
PAID_SPEND_GAUGE = "rpc_paid_spend_estimate"

#: account id → (UTC day ordinal, dispatch count)
_PAID_DISPATCH: dict[str, tuple[int, int]] = {}


def record_spend(endpoint: str) -> int:
    """Count one paid dispatch for ``endpoint``'s account.

    The seam the spillover path calls: it resolves the non-secret account id
    so callers never deal with account identity themselves.

    Args:
        endpoint: The endpoint that served the call.

    Returns:
        The account's count for the current UTC day, after incrementing.
    """
    from lite_indexer.rpc.ledger import account_id_for_endpoint

    return record_paid_dispatch(account_id_for_endpoint(endpoint))


def record_paid_dispatch(account_id: str, now: float | None = None) -> int:
    """Count one dispatch to a billable account and publish its gauge.

    Called only from the spillover path, so the counter means exactly
    "requests this process sent to a paid account" — not total request
    volume.

    Args:
        account_id: Non-secret account id from
            ``lite_indexer.rpc.ledger.account_id_for_endpoint``.
        now: Optional epoch seconds (tests); defaults to ``time.time()``.

    Returns:
        The account's count for the current UTC day, after incrementing.
    """
    moment = time.time() if now is None else now
    day = datetime.fromtimestamp(moment, tz=UTC).toordinal()
    previous_day, count = _PAID_DISPATCH.get(account_id, (day, 0))
    if previous_day != day:
        count = 0
    count += 1
    _PAID_DISPATCH[account_id] = (day, count)
    set_labeled_gauge(PAID_SPEND_GAUGE, count, account=account_id)
    return count


def paid_dispatch_total(account_id: str, now: float | None = None) -> int:
    """Requests dispatched to ``account_id`` today (0 when never used).

    Args:
        account_id: Non-secret account id.
        now: Optional epoch seconds (tests); defaults to ``time.time()``.

    Returns:
        Today's count for that account, or 0 if it never dispatched.
    """
    day = datetime.fromtimestamp(time.time() if now is None else now, tz=UTC).toordinal()
    recorded_day, count = _PAID_DISPATCH.get(account_id, (day, 0))
    return count if recorded_day == day else 0


def reset_paid_dispatch() -> None:
    """Clear all per-account paid-dispatch counters (tests)."""
    _PAID_DISPATCH.clear()
