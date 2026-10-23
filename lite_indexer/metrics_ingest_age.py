"""Data-freshness gauge — seconds since the newest row landed, per chain.

``lite_indexer_chain_ingest_age_seconds`` is what the critical
``LiteIndexerChainStale`` alert reads. It was declared in the rule file in
2026-09-27 and never emitted by the process, so the only data-freshness
rule in the fleet was dead code: bsc could ingest nothing for 26h and the
critical page never fired.

The gauge measures the ENABLED streams, not one hardcoded table. bsc runs
``{dex, deployer}`` only (transfers are demand-gated), so a metric pinned
to ``evm_transfers`` would be permanently stale there — a false page that
trains the operator to ignore the real one.

Semantics (match the alert rule):
    >= 0  seconds since the newest ``ingested_at`` across the chain's streams
    -1    the chain has NO row in any stream in the lookback window
"""

from __future__ import annotations

import contextlib
import os

from lite_indexer import metrics
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_SQL = """
SELECT toInt64(ifNull(min(age_s), -1)) FROM (
    SELECT dateDiff('second', max(ingested_at), now()) AS age_s FROM {db}.evm_transfers WHERE chain = %(chain)s
    UNION ALL
    SELECT dateDiff('second', max(ingested_at), now()) AS age_s FROM {db}.dex_events WHERE chain = %(chain)s
    UNION ALL
    SELECT dateDiff('second', max(ingested_at), now()) AS age_s FROM {db}.deployer_events WHERE chain = %(chain)s
)
"""


def _client():
    """Open a short-lived native-protocol ClickHouse client."""
    from clickhouse_driver import Client

    return Client(
        host=os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT") or "9000"),
        user=os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or "",
        database=os.getenv("CH_DATABASE", "wallet_memory"),
        connect_timeout=5,
        send_receive_timeout=15,
    )


def export_ingest_age(chains) -> None:
    """Publish ``lite_indexer_chain_ingest_age_seconds`` for every chain.

    Best-effort: a ClickHouse hiccup must never stop indexing or kill the
    metrics loop. A chain whose query fails keeps its previous sample.

    Args:
        chains: Iterable of chain names the daemon is running.
    """
    db = os.getenv("CH_DATABASE", "wallet_memory")
    sql = _SQL.format(db=db)
    client = None
    try:
        client = _client()
        for chain in chains:
            try:
                rows = client.execute(sql, {"chain": chain})
            except Exception as exc:  # noqa: BLE001 — per-chain isolation
                logger.warning("ingest_age_query_failed", chain=chain, error=str(exc)[:100])
                continue
            age = int(rows[0][0]) if rows and rows[0] else -1
            metrics.set_labeled_gauge("lite_indexer_chain_ingest_age_seconds", float(age), chain=chain)
            if age < 0 or age > 900:
                logger.warning("chain_ingest_stale", chain=chain, age_s=age)
    except Exception as exc:  # noqa: BLE001 — observability never breaks the daemon
        logger.warning("ingest_age_exporter_failed", error=str(exc)[:100])
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.disconnect()
