"""Scheduled producers for external (non-chain) datasets.

One package per upstream, each a self-contained cycle: fetch -> rows -> upsert
into a ClickHouse table that is registered in ``archive_specs``. The producer
never owns a scheduler of its own; the fleet's systemd timers drive
``python -m lite_indexer.producers.<name>``.
"""

from __future__ import annotations
