"""Shared call-site for the silent-``eth_getLogs``-truncation guard.

:func:`lite_indexer.strategies.evm.log_budget.is_truncated_log_response` is the
pure predicate; this module is the one place that reacts to it, so the transfer
and dex-topic fetchers cannot drift on whether a truncated page is recorded
against the endpoint (penalising it so rotation moves on) or merely logged.

Extracted from ``strategies/evm/transfer_handler.py`` (shrink-only size
ratchet).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.strategies.evm.log_budget import is_truncated_log_response

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class GetlogsPage:
    """One ``eth_getLogs`` result plus the context needed to judge it.

    Bundled into a value so the guard takes a single argument and a new
    required field cannot be silently skipped at a call site.
    """

    logs: list[dict[str, Any]]
    from_block: int
    to_block: int
    chain: str
    endpoint: str
    record_failure: Callable[[str], None]


def reject_truncated_getlogs(page: GetlogsPage) -> bool:
    """Return True when this getLogs page must NOT be accepted as complete.

    A capped public RPC answers HTTP 200 with a SHORT result list and no error.
    Accepting it lets the watermark advance past blocks that were never
    queried, so on a hit the endpoint is penalised (making rotation move on)
    and the caller should try the next endpoint.

    Args:
        page: The raw log dicts, the requested range, and the endpoint
            context (chain, URL, failure recorder).

    Returns:
        True when the response looks truncated and must be rejected.
    """
    if not is_truncated_log_response(page.logs, page.from_block, page.to_block):
        return False
    logger.warning(
        "getlogs_truncated",
        chain=page.chain,
        endpoint=page.endpoint[:50],
        returned=len(page.logs),
        from_block=page.from_block,
        to_block=page.to_block,
    )
    page.record_failure(page.endpoint)
    return True


__all__ = ["GetlogsPage", "reject_truncated_getlogs"]
