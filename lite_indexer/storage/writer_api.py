"""Structural contract for the methods :class:`ClickHouseWriter` mixes together.

``clickhouse.py`` composes ``ClickHouseWriter`` from six mixins
(``ConnectMixin``, ``TransfersMixin``, ``DexMixin``, ``DeployerMixin``,
``FeaturesMixin``, ``SchemaMixin``) that each live in their own module for
file size, not for cohesion. The DDL helpers live in the three ``_schema_*``
mixins and the connection in ``ConnectMixin``, so a write mixin calls
siblings it cannot see: ``TransfersMixin.write_transfers`` needs
``_ensure_source_column`` (a ``_SchemaCoreColumnMixin`` member) and
``_connect`` (a ``ConnectMixin`` member), and nothing in the mixin's own
MRO declares them. mypy reports each of those calls as ``attr-defined``.

The fix is the shape used in :mod:`lite_indexer.rpc.client_mixin_api` and
:mod:`lite_indexer.storage.parquet.mixin_api`: a ``Protocol`` that the
mixins annotate on ``self`` (``self: WriterApi``), NOT a base class.

Adding the Protocol as a *base* of the mixins is what the previous attempt
tried, and it is a regression: a base class with non-method members
(``_dex_numeric_amounts``, ``_wallet_projection_ready``) makes the mixins
abstract, so instantiating the concrete provider fails. The verified MRO
is ``[ClickHouseWriter, ConnectMixin, TransfersMixin, DexMixin,
DeployerMixin, FeaturesMixin, SchemaMixin, _SchemaCoreColumnMixin,
_SchemaDerivedColumnMixin, _SchemaTtlMixin, object]`` — no Protocol
appears, and none would precede ``ClickHouseWriter`` if one were added, so
``ClickHouseWriter()`` in ``cli/__init__.py`` and ``storage/__init__.py``
would raise ``TypeError: Can't instantiate abstract class``. Annotating
``self`` touches no base-class list, so the class stays concrete.

Signatures are copied from the defining mixins; keep them in step. The
per-process "ready" flags are declared because ``ch_features`` and
``ch_dex`` both read them off ``self`` while ``ConnectMixin.__init__`` is
what assigns them.
"""

from __future__ import annotations

from typing import Any, Protocol


class WriterApi(Protocol):
    """The slice of ``ClickHouseWriter`` that cross-mixin calls depend on."""

    # ── Per-process DDL/schema state, assigned by ConnectMixin.__init__ ──
    # Declared as mutable (plain annotations, not read-only properties):
    # these are genuinely reassigned after __init__ — the "*_ready" flags
    # flip once each DDL lands, and _dex_amounts_numeric caches its probe
    # result. A read-only Protocol member would conflict with
    # ConnectMixin's instance attributes and make ClickHouseWriter
    # non-instantiable ("Definition of _dex_numeric_amounts in base class
    # ConnectMixin is incompatible with definition in base class
    # DexMixin").
    _wallet_projection_ready: bool
    _token_meta_ready: bool
    _balance_cache_ready: bool
    _holder_mv_ready: bool
    _trace_table_ready: bool
    _dex_numeric_amounts: bool | None

    # ── ConnectMixin ─────────────────────────────────────────────────
    def _connect(self) -> Any | None: ...

    # ── _SchemaCoreColumnMixin ───────────────────────────────────────
    def _ensure_source_column(self, client: Any, table: str) -> None: ...

    def _ensure_raw_payload_column(self, client: Any, table: str) -> None: ...

    def _ensure_dedup_key_column(self, client: Any, table: str) -> None: ...

    # ── _SchemaDerivedColumnMixin ────────────────────────────────────
    def _ensure_dex_usd_columns(self, client: Any) -> None: ...

    def _ensure_deployer_columns(self, client: Any) -> None: ...

    def _column_types(self, client: Any, table: str) -> dict[str, str]: ...

    # ── _SchemaTtlMixin ──────────────────────────────────────────────
    def ensure_ttl_policies(self, client: Any) -> None: ...

    # ── Methods defined on the write mixins themselves ───────────────
    # These are declared here because a method that annotates
    # ``self: WriterApi`` can no longer see its own class's members — mypy
    # binds the parameter to the protocol for the rest of the body.
    def _existing_transfer_keys(
        self,
        client: Any,
        chain: str,
        keys: list[tuple[int, str, int]],
    ) -> set[tuple[int, str, int]]: ...

    def _dex_amounts_numeric(self, client: Any) -> bool: ...


__all__ = ["WriterApi"]
