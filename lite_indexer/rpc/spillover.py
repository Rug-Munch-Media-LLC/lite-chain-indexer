"""The gate that decides whether a keyed endpoint may serve a call.

WHY THIS EXISTS
    ``select_endpoint_for_method`` used to spill over to a keyed endpoint the
    moment the free tier was unhealthy: a counter, a log warning, and the
    request went out. Nothing checked whether the account could afford it and
    nothing required an operator to have blessed the spend. A 429 on one free
    endpoint could therefore turn into billable traffic, silently and
    indefinitely, with the real budgets invisible in a root-only systemd
    ``Environment=`` line.

THE RULE
    A keyed endpoint may serve a call only when **both** hold:

    1. **budget remaining** — the account's daily request budget is not
       exhausted (``lite_indexer.rpc.budgets.budget_available``);
    2. **operator blessing** — the account appears in the
       ``RPC_SPILLOVER_ALLOWED`` set.

    If either fails the answer is ``None`` and the caller degrades
    gracefully (no endpoint this cycle, retry/backoff) rather than spending.
    This is the default-deny posture: an unconfigured deployment never spends
    money implicitly.

Scope: the permission decision only. It does not select an endpoint, does not
classify cost (``rpc/cost_tier.py``), and does not count spend
(``rpc/spend.py`` — the caller records there once a pick is actually made).

The operator set is injected via ``allowed=`` / ``budget_ok=`` keywords, the
DI seam convention of ``rpc/routing/seams.py``. Nothing is monkeypatched.
"""

from __future__ import annotations

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

#: Counter: a spillover attempt refused by this gate (with the reason as a
#: separate counter so an operator can alert on refusals, not just attempts).
METRIC_REFUSED = "rpc_spillover_refused"
METRIC_REFUSED_BUDGET = "rpc_spillover_refused_budget"
METRIC_REFUSED_UNBLESSED = "rpc_spillover_refused_unblessed"
METRIC_ALLOWED = "rpc_spillover_allowed"


def spillover_allowed(
    endpoint: str,
    *,
    allowed: set[str] | None = None,
    budget_ok: bool | None = None,
) -> bool:
    """Whether a keyed ``endpoint`` may serve a call outside the free tier.

    Both conditions must hold; the refusal reason is counted and logged so a
    denied spillover is visible rather than a silent no-op.

    Args:
        endpoint: Candidate keyed endpoint URL.
        allowed: Optional account-id allowlist. Defaults to
            :data:`RPC_SPILLOVER_ALLOWED`. Empty means nobody is blessed.
        budget_ok: Optional budget predicate. Defaults to
            ``budget_available(endpoint)``. Injectable for tests.

    Returns:
        True only when the account is both blessed and has budget remaining.
    """
    from lite_indexer.config import RPC_SPILLOVER_ALLOWED
    from lite_indexer.rpc.budgets import budget_available
    from lite_indexer.rpc.ledger import account_id_for_endpoint

    permitted = RPC_SPILLOVER_ALLOWED if allowed is None else allowed
    account = account_id_for_endpoint(endpoint)
    if account not in permitted:
        _refuse(endpoint, account, METRIC_REFUSED_UNBLESSED, "account_not_blessed")
        return False
    if not (budget_available(endpoint) if budget_ok is None else budget_ok):
        _refuse(endpoint, account, METRIC_REFUSED_BUDGET, "account_budget_exhausted")
        return False
    metrics_incr(METRIC_ALLOWED)
    return True


def _refuse(endpoint: str, account: str, reason_metric: str, reason: str) -> None:
    """Count and log one refused spillover (never raises)."""
    metrics_incr(METRIC_REFUSED)
    metrics_incr(reason_metric)
    logger.warning(
        "rpc_spillover_refused",
        endpoint=endpoint[:60],
        account=account,
        reason=reason,
    )
