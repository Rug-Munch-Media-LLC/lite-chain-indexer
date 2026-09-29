"""Structural contract for the methods :class:`ParquetExporter` mixes together.

``writer.py`` composes ``ParquetExporter`` from three mixins
(``ExportFlowMixin``, ``ExportDayMixin``, ``ExportStateMixin``) that each
live in their own module for file size, not for cohesion. A mixin
therefore calls methods defined by a *sibling* mixin — ``ExportDayMixin``
needs ``ExportStateMixin._record_state`` and ``ParquetExporter``'s own
``_write_parquet`` — and those names are invisible to mypy from inside the
mixin, producing ``attr-defined`` on every call site.

The fix is the shape used in :mod:`lite_indexer.rpc.client_mixin_api`: a
``Protocol`` that the mixins annotate on ``self`` (``self: ParquetApi``),
NOT a base class. That distinction is the whole point:

* a base class would make the mixins abstract — every Protocol member
  becomes a required override, and the concrete provider (``ParquetExporter``
  itself, whose MRO is ``[ParquetExporter, ExportFlowMixin, ExportDayMixin,
  ExportStateMixin, object]``) has to *precede* the Protocol in that MRO for
  instantiation to work. It does not, so ``ParquetExporter()`` would fail.
* annotating ``self`` binds the type of the parameter to the protocol for
  the duration of that one method, leaving the class hierarchy untouched.

Signatures are copied from the defining mixins; keep them in step. Where a
method's real signature is wider than five parameters (``_record_state`` has
eight, ``publish_snapshot`` has nine) the extra keyword arguments are
declared ``**kwargs``: this protocol states the *capability a caller relies
on*, and restating every keyword would trip the ``too-many-params``
antipattern gate — which counts ``self`` — for no gain in type safety.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Protocol


class ParquetApi(Protocol):
    """The slice of ``ParquetExporter`` that cross-mixin calls depend on."""

    # ── Instance state owned by ParquetExporter.__init__ ────────────
    data_dir: Path
    remote: str
    state_dir: Path

    # ── Cross-mixin methods ─────────────────────────────────────────
    def _connect(self) -> Any | None: ...

    def eligible_dates(self, chain: str, cold_days: int) -> list[date]: ...

    def _write_parquet(self, rows: list[dict[str, Any]], path: Path) -> int: ...

    def _verify_parquet(self, path: Path, rows: int, columns: set[str]) -> bool: ...

    def _delete_day_rows(self, client: Any, table: str, chain: str, day: date) -> int: ...

    def _upload_dir(self, local: Path, remote: str) -> bool: ...

    def _export_one_day(self, client: Any, chain: str, day: date, *args: Any) -> None: ...

    def _ensure_state_table(self, client: Any) -> None: ...

    def _record_state(
        self,
        client: Any,
        chain: str,
        day: date,
        **kwargs: Any,
    ) -> None: ...

    def _mark_exported(
        self,
        marker: Path,
        day: date,
        transfers_rows: int,
        dex_rows: int,
    ) -> None: ...

    def publish_snapshot(
        self,
        chain: str,
        day: date,
        **kwargs: Any,
    ) -> bool: ...


__all__ = ["ParquetApi"]
