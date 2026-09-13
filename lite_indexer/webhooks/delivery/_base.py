"""Shared mixin contract for :class:`WebhookDelivery`.

The de-god refactor split ``WebhookDelivery`` into mixins
(:class:`~lite_indexer.webhooks.delivery.sender.SenderMixin`,
:class:`~lite_indexer.webhooks.delivery.retry.RetryMixin`,
:class:`~lite_indexer.webhooks.delivery.queue.QueueMixin`) that freely
reference attributes and methods owned by sibling mixins or the concrete
class. mypy strict checks each mixin in isolation and cannot resolve
those cross-mixin references, which produced ``attr-defined`` /
``has-type`` / ``assignment`` errors.

This module declares the shared surface once — every data attribute with
its type plus the method signatures each mixin relies on — so each mixin
can inherit the contract. Mirrors
:class:`lite_indexer.indexer._base.IndexerMixinBase`.

All members here are declarations only. The real implementations live in
the concrete ``WebhookDelivery`` or one of the mixins, all of which are
more derived in the MRO than this base. The bodies raise so that an
accidental dispatch to an undeclared implementation fails loudly instead
of silently returning ``None``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from lite_indexer.webhooks.models import DlqEntry, WebhookSubscription
    from lite_indexer.webhooks.storage import WebhookStore


class DeliveryMixinBase:
    """Declared shared surface consumed by every ``WebhookDelivery`` mixin."""

    # ── State set by ``WebhookDelivery.__init__`` ─────────────────────────
    _store: WebhookStore
    _clock: Callable[[], float]
    _schedule: tuple[float, ...]
    _jitter: bool
    _transport: httpx.AsyncBaseTransport | None
    timeout_seconds: float
    total_timeout_seconds: float
    max_attempts: int

    # ── DLQ worker state (set by ``__init__`` / the worker itself) ────────
    _stop_event: asyncio.Event
    _poll_interval: float
    _worker_task: asyncio.Task[None] | None
    _worker_running: bool
    _http_client: httpx.AsyncClient | None

    # ── Methods owned by ``SenderMixin`` ─────────────────────────────────
    async def _attempt_post(
        self,
        sub: WebhookSubscription,
        webhook_id: str,
        timestamp: int,
        subject: str,
        event_type: str,
        chain: str,
        payload: bytes,
    ) -> tuple[bool, int | None, int, str | None]:
        raise NotImplementedError

    def _log_failure(
        self,
        sub: WebhookSubscription,
        subject: str,
        *,
        attempts: int,
        status_code: int | None,
        error: str | None,
    ) -> None:
        raise NotImplementedError

    # ── Methods owned by ``RetryMixin`` ──────────────────────────────────
    def _start_worker(self) -> None:
        raise NotImplementedError

    async def _dlq_worker(self) -> None:
        raise NotImplementedError

    def _process_due_dlq(self) -> None:
        raise NotImplementedError

    def _enqueue_dlq(
        self,
        sub: WebhookSubscription,
        subject: str,
        payload: bytes,
        event_type: str,
        chain: str,
        webhook_id: str,
        timestamp: int,
        *,
        attempts: int,
        status_code: int | None,
        error: str | None,
    ) -> None:
        raise NotImplementedError

    async def _retry_later(self, entry_id: str, delay: float) -> None:
        raise NotImplementedError

    async def _retry_entry(self, entry_id: str) -> None:
        raise NotImplementedError

    def _jittered(self, delay: float) -> float:
        raise NotImplementedError

    # ── Methods owned by ``QueueMixin`` ──────────────────────────────────
    def _dlq_insert(self, entry: DlqEntry) -> None:
        raise NotImplementedError

    def _get_dlq_entry(self, entry_id: str) -> DlqEntry | None:
        raise NotImplementedError

    def _claim_dlq_entry(self, entry_id: str) -> bool:
        raise NotImplementedError

    def _reschedule_dlq(
        self,
        entry_id: str,
        attempts: int,
        next_attempt_at: float,
        status_code: int | None,
        error: str | None,
    ) -> None:
        raise NotImplementedError

    def _mark_dlq_dead(self, entry_id: str, attempts: int, reason: str) -> None:
        raise NotImplementedError

    def _delete_dlq_entry(self, entry_id: str) -> None:
        raise NotImplementedError

    def _dlq_due_rows(self, now: float) -> list[sqlite3.Row]:
        raise NotImplementedError
