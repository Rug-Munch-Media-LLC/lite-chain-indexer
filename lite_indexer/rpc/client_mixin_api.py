"""Structural contract for the methods :class:`RpcClient` mixes together.

``client.py`` composes ``RpcClient`` from seven mixins that each live in
their own module, and several mixins call methods defined by a *sibling*
mixin (``BlocksMixin`` needs ``CallMixin.call``, ``LogsMixin`` needs
``LogRangeMixin._fetch_logs_range``). Each mixin declares the shared
surface as a ``self`` annotation instead of importing the sibling, which
would close an import cycle and re-open the ``_facade`` indirection this
split was meant to remove.

Signatures are copied from the defining mixins; keep them in step. Where a
method's real signature is wider than five parameters, the extra ones are
declared ``*args``/``**kwargs`` deliberately: this protocol states the
*capability a caller relies on*, not the callee's full keyword surface,
and restating a 7-parameter signature here would trip the
``too-many-params`` antipattern gate for no gain in type safety.
"""

from __future__ import annotations

from typing import Any, Protocol


class MixinApi(Protocol):
    """The slice of ``RpcClient`` that cross-mixin calls depend on.

    Every entry is a cross-mixin edge: the mixins are split for file size,
    not for cohesion, so ``BlocksMixin`` calls ``CallMixin.call``,
    ``LogsMixin`` calls both ``LogRangeMixin._fetch_logs_range`` and its
    own ``_fetch_logs_many``, and so on. The protocol is deliberately
    complete over those edges — a partial one would only move the
    ``attr-defined`` errors onto the attributes it omits.
    """

    async def call(
        self,
        endpoint: str,
        method: str,
        params: list[Any] | None = None,
        timeout: float = ...,
    ) -> dict[str, Any] | None: ...

    async def call_batch(
        self,
        endpoint: str,
        calls: list[tuple[str, list[Any] | None]],
        *,
        timeout: float = ...,
    ) -> list[dict[str, Any] | None]: ...

    async def _call_batch_fallback(
        self,
        endpoint: str,
        calls: list[tuple[str, list[Any] | None]],
        timeout: float,
        existing: list[dict[str, Any] | None] | None = ...,
    ) -> list[dict[str, Any] | None]: ...

    async def _post_batch(
        self,
        endpoint: str,
        payload: list[dict[str, Any]],
        method: str,
        timeout: float,
    ) -> Any: ...

    async def _fetch_logs_range(
        self,
        endpoint: str,
        *args: Any,
    ) -> tuple[str, list[dict[str, Any]] | None]: ...

    async def _fetch_logs_many(
        self,
        endpoint: str,
        ranges: list[tuple[int, int]],
        topic: str,
        address: str | None,
    ) -> list[tuple[str, list[dict[str, Any]] | None]]: ...

    async def eth_get_block_by_number_with_status(
        self,
        endpoint: str,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[dict[str, Any] | None, int | None]: ...

    def _logs_filter(self, from_block: int, to_block: int, topic: str, address: str | None) -> dict[str, Any]: ...
