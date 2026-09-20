"""Connection-level ClickHouse failure classification (fail-closed writes).

Split out of :mod:`lite_indexer.storage.clickhouse` so that module stays
within its shrink-only size budget.
"""

from __future__ import annotations

import importlib.util
from typing import Any


class ClickHouseUnavailable(RuntimeError):  # noqa: N818 — name is the fixed contract callers catch
    """ClickHouse is unreachable at the connection level (server down).

    Fail-closed signal: callers MUST abort the current write cycle WITHOUT
    checkpointing progress, so an outage can never be recorded as a
    successful (empty) sync. Raised only for the ``server is down`` class
    (connection refused / network / socket errors) — data-level insert
    failures keep returning 0 so a single poisoned batch stays contained.
    """


def require_client(client: Any, chain: str) -> Any:
    """Return ``client`` or raise if ClickHouse is unreachable (fail-closed)."""
    if client is None:
        raise ClickHouseUnavailable(f"ClickHouse unreachable (chain={chain})")
    return client


def is_unavailable_error(exc: BaseException) -> bool:
    """True for connection-level ClickHouse failures (server unreachable).

    Covers the stdlib socket/OS error tree (``ConnectionRefusedError``,
    ``ConnectionError``, ``TimeoutError``, bare ``OSError``) and the
    clickhouse_driver network errors. Deliberately excludes
    ``ServerException`` — a live server rejecting the query is a
    data/schema problem, not an outage, and must not abort the run.
    """
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    if importlib.util.find_spec("clickhouse_driver") is None:
        return False
    from clickhouse_driver.errors import NetworkError, SocketTimeoutError

    return isinstance(exc, (NetworkError, SocketTimeoutError))
