"""Schema management mixin for the ClickHouse writer.

Split from storage/clickhouse.py (de-godding 2026-08-23). All methods
operate on ``self`` (client access via ``self._connect()`` /
``self.client()``), initialized by :class:`ClickHouseWriter.__init__``.

The method bodies live in three sibling halves — ``_schema_columns`` (core
columns), ``_schema_derived_columns`` (derived columns) and ``_schema_ttl``
(table TTL / projection DDL) — so each module stays
inside the <=200-line size policy. The halves declare no base class and
never import each other, so composing them here by multiple inheritance
needs no forward references and creates no import cycle. The shared
``_ddl_memo`` memo table lives in ``_ddl_memo.py`` so it stays a single
object across all three halves.
"""

from __future__ import annotations

from lite_indexer.storage._ddl_memo import _TTL_ENSURED_AT, _TTL_REENSURE_SECONDS, _ddl_memo
from lite_indexer.storage._schema_columns import _SchemaCoreColumnMixin
from lite_indexer.storage._schema_derived_columns import _SchemaDerivedColumnMixin
from lite_indexer.storage._schema_ttl import _SchemaTtlMixin

__all__ = ["SchemaMixin", "_TTL_ENSURED_AT", "_TTL_REENSURE_SECONDS", "_ddl_memo"]


class SchemaMixin(_SchemaCoreColumnMixin, _SchemaDerivedColumnMixin, _SchemaTtlMixin):
    """Idempotent column/projection/TTL DDL for writer tables."""
