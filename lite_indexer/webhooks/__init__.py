"""
Webhook delivery platform — push indexed events to registered endpoints.

Transforms the indexer from a "queryable warehouse" into a real-time
data platform: the delivery service subscribes to the same NATS
JetStream subjects the publisher writes (``chain.{chain}.transfer``,
``chain.{chain}.dex.swap``, ``chain.{chain}.deploy``,
``alerts.whale.{chain}``) and POSTs every matching event to registered
webhooks with HMAC-SHA256 signatures and bounded retries.

Subscriptions and the delivery log are persisted in SQLite
(``/var/data/chain-indexer/webhooks.db``).

Note: this package deliberately does NOT import ``router`` (FastAPI) —
the core package must import without the ``api`` extra installed. The
API surface is imported lazily by ``api.app``.
"""

from __future__ import annotations

from lite_indexer.webhooks.delivery import WebhookDelivery
from lite_indexer.webhooks.models import DeliveryAttempt, WebhookSubscription

__all__ = ["WebhookDelivery", "WebhookSubscription", "DeliveryAttempt"]
