"""Structural contract for the methods :class:`HistoricalAccess` mixes together.

``historical.py`` composes ``HistoricalAccess`` from five mixins
(``HistoricalAccessSQL``, ``HistoricalAccessSQLParse``,
``HistoricalAccessQuery``, ``HistoricalAccessMerge``,
``HistoricalAccessView``) that each live in their own module for file
size, not for cohesion. The concrete class in ``historical.py`` holds
everything the mixins need — ``AVAILABLE_TABLES``, ``__init__``'s
``_cold``/``_view_cache``/``_view_checked``, and the ``_client`` /
``_check_table`` / ``hot_cutoff`` methods — and every mixin reads them off
``self`` from a base that does not declare them, so mypy reports each read
as ``attr-defined``.

The fix is the same shape as
:mod:`lite_indexer.storage.writer_api` and
:mod:`lite_indexer.storage.parquet.mixin_api`: a ``Protocol`` the mixins
annotate on ``self`` (``self: ThaApi``), NOT a base class. The verified
MRO is ``[HistoricalAccess, HistoricalAccessSQL, HistoricalAccessSQLParse,
HistoricalAccessQuery, HistoricalAccessMerge, HistoricalAccessView,
object]`` — a Protocol base would have to appear after
``HistoricalAccess`` for its members to be satisfied, and it does not, so
``HistoricalAccess()`` (called in ``cli/`` and ``query/``) would become a
``TypeError: Can't instantiate abstract class``. Annotating ``self`` adds
nothing to any base-class list.

Signatures are copied from ``historical.py``; keep them in step.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Protocol

from lite_indexer.storage.cold_store import ColdStore
from lite_indexer.storage.historical_tables import _Split
from lite_indexer.util.bounded import BoundedDict


class ThaApi(Protocol):
    """The slice of ``HistoricalAccess`` that cross-mixin calls depend on."""

    # ── Owned by HistoricalAccess: the class attribute + __init__ state ──
    AVAILABLE_TABLES: list[str]
    _cold: ColdStore
    _view_cache: BoundedDict[str, bool]
    _view_checked: float

    # ── Owned by HistoricalAccess itself ─────────────────────────────
    def _client(self) -> Any | None: ...

    def _check_table(self, table: str) -> str: ...

    def hot_cutoff(self, table: str) -> datetime: ...

    # ── Cross-mixin methods ─────────────────────────────────────────
    # _view_available lives in HistoricalAccessView and is read by
    # HistoricalAccessSQL; _plan/_smart_merge live in
    # HistoricalAccessMerge and are read by both SQL runners. They are
    # declared here because a method annotating ``self: ThaApi`` can no
    # longer see its own class's members.
    def _view_available(self, table: str) -> bool: ...

    def _plan(
        self,
        table: str,
        from_block: int | None,
        to_block: int | None,
        *bounds: Any,
    ) -> _Split: ...

    def _smart_merge(
        self,
        table: str,
        hot_results: list[dict[str, Any]],
        cold_results: list[dict[str, Any]],
        *args: Any,
    ) -> list[dict[str, Any]]: ...

    # ── Members owned by the mixins that annotate ``self: ThaApi`` ─────
    # A method that binds its ``self`` to this protocol can no longer see
    # its own class's members, so they must be restated here. Omitting them
    # does not fix the error — it relocates it (an unannotated mixin that
    # was clean becomes a new "ThaApi has no attribute" error).
    _SELECT_RE: re.Pattern[str]
    _ORDER_RE: re.Pattern[str]
    _LIMIT_RE: re.Pattern[str]
    _OFFSET_RE: re.Pattern[str]

    def _referenced_table(self, sql: str) -> str | None: ...

    @staticmethod
    def _row_key(table: str, row: dict[str, Any]) -> tuple[Any, ...]: ...

    def _parse_sql(self, sql: str) -> tuple[str, list[str], str, str, int, int] | None: ...

    def _extract_bounds(
        self,
        where_body: str,
        params: dict[str, Any],
    ) -> tuple[str, int | None, int | None, datetime | None, datetime | None, str] | None: ...

    def _query_sql_hot(self, sql: str, params: dict[str, Any], table: str) -> list[dict[str, Any]]: ...

    @staticmethod
    def _order_rows(rows: list[dict[str, Any]], order_by: str) -> list[dict[str, Any]]: ...

    @staticmethod
    def _project(rows: list[dict[str, Any]], select_cols: list[str]) -> list[dict[str, Any]]: ...

    @staticmethod
    def _strip_limit(sql: str) -> str: ...

    async def _query_hot(
        self,
        table: str,
        chain: str,
        *args: Any,
    ) -> list[dict[str, Any]]: ...

    async def _query_cold(
        self,
        table: str,
        chain: str,
        *args: Any,
    ) -> list[dict[str, Any]]: ...


__all__ = ["ThaApi"]
