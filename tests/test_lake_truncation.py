"""Tests for lake row-limit policy and the truncation signal.

A caller asking for 1,000,000 rows used to receive exactly the 1000 newest
ones, as a normal result with no indication it was short. These tests pin
the two halves of the fix: the oversize limit is REJECTED rather than
clamped, and a read that does hit the ceiling reports ``truncated``.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from lite_indexer.query.biglake_rotation import set_read_with_rotation
from lite_indexer.query.biglake_source import BigLakeSource
from lite_indexer.query.lake_limits import (
    LAKE_MAX_ROWS,
    SOLANA_MAX_PAGE,
    clamp_limit,
    reject_oversize_limit,
)


@pytest.fixture(autouse=True)
def _restore_rotation_seam() -> Any:
    """The rotation seam is a module global; restore it so tests stay isolated."""
    yield
    set_read_with_rotation(None)


def test_oversize_limit_raises_rather_than_clamping() -> None:
    """A limit above the ceiling raises. It must NOT become 1000."""
    with pytest.raises(ValueError, match="exceeds ceiling"):
        reject_oversize_limit(1_000_000)
    with pytest.raises(ValueError):
        reject_oversize_limit(LAKE_MAX_ROWS + 1)
    assert reject_oversize_limit(LAKE_MAX_ROWS) == LAKE_MAX_ROWS
    assert reject_oversize_limit(1) == 1


def test_clamp_limit_is_for_cleared_limits_only() -> None:
    """clamp_limit still bounds, but only for an already-cleared limit."""
    assert clamp_limit(10) == 10
    assert clamp_limit(0) == 1
    assert clamp_limit(99_999, SOLANA_MAX_PAGE) == 50_000


def _page_reader(rows: list[dict[str, Any]], captured: dict[str, Any]) -> Any:
    """A ``read_with_rotation``-compatible callable serving a fixed page.

    Injected through the ``set_read_with_rotation`` seam, so no production
    symbol is replaced to make the test pass.
    """

    def _read(
        source: Any, read: Any, table: str, biglake_table: str, filters: Any = None, **kw: Any
    ) -> list[dict[str, Any]]:
        _sql, params = read
        captured["params"] = list(params)
        return rows

    return _read


def _offline_source(page: list[dict[str, Any]], captured: dict[str, Any], *, limit: int) -> BigLakeSource:
    """A BigLakeSource that never touches the network, for one page read.

    ``connect`` bypasses DuckDB, the rotation seam bypasses the provider loop,
    and ``scope`` pins the partition glob so the assertion does not drift with
    the calendar month. ``limit`` is the caller's requested page size; it is
    recorded so a test can assert the probe asked for one row more.
    """
    captured["limit"] = limit
    set_read_with_rotation(_page_reader(page, captured))
    return BigLakeSource(scope="2026-09-*", connect=_FakeConnection)


def _FakeConnection() -> Any:  # noqa: N802 — a stand-in, not a public name
    """A DuckDB-like connection that raises if the reader ever executes."""

    def _boom(*_a: Any, **_k: Any) -> None:
        raise AssertionError("the lake read must be served by the injected reader")

    return type("_Conn", (), {"execute": staticmethod(_boom), "close": staticmethod(lambda: None)})()


def test_biglake_query_reports_truncation() -> None:
    """A page hitting the ceiling returns (rows<=limit, truncated=True).

    The source reads ``limit + 1`` rows as a probe: the extra row's
    existence is the proof that older rows remain, and it is discarded.
    """
    captured: dict[str, Any] = {}
    src = _offline_source([{"block_number": i} for i in range(3)], captured, limit=2)

    rows, truncated = cast(tuple[list[dict[str, Any]], bool], src.query("evm_transfers", limit=2))

    assert truncated is True, "2-of-3 rows is a truncated page and must say so"
    assert len(rows) == 2, "the probe row must be discarded, never returned"
    assert captured["params"][-1] == 3, "the query must ask for limit + 1 rows"


def test_biglake_query_untruncated_page() -> None:
    """A page that fits entirely reports truncated=False."""
    captured: dict[str, Any] = {}
    src = _offline_source([{"block_number": 1}], captured, limit=10)

    rows, truncated = cast(tuple[list[dict[str, Any]], bool], src.query("evm_transfers", limit=10))
    assert truncated is False
    assert len(rows) == 1


def test_biglake_query_rejects_oversize_limit() -> None:
    """The reader itself rejects 1,000,000 rows — not just the router."""
    captured: dict[str, Any] = {}
    src = _offline_source([], captured, limit=1_000_000)

    with pytest.raises(ValueError, match="exceeds ceiling"):
        src.query("evm_transfers", limit=1_000_000)


def test_biglake_query_uses_the_pinned_scope_partition() -> None:
    """The injected scope fixes the partition glob the query reads from."""
    captured: dict[str, Any] = {}
    src = _offline_source([{"block_number": 1}], captured, limit=10)
    assert src._scope == "2026-09-*"


def test_solana_page_ceiling_is_reported_separately() -> None:
    """Solana's 50k page is distinct from the 1000 lake result ceiling."""
    assert SOLANA_MAX_PAGE == 50_000
    assert SOLANA_MAX_PAGE > LAKE_MAX_ROWS
    assert clamp_limit(10**9, SOLANA_MAX_PAGE) == 50_000
