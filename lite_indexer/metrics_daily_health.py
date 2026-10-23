"""Per-chain daily health — one query, EVERY configured chain.

WHY THIS REPLACED THE PER-CHAIN EXPORTER (2026-10-31 post-mortem)
The previous exporter looped over LIVE indexers, one ClickHouse query per
chain, and only for chains that had already built an indexer. Measured on
production: 12 of 16 chains had no series at all, so the freshness alert
could not fire on exactly the chains that were stale, and
``lite_indexer_chain_stalled`` survived only as a counter for chain="none".

Semantics (stable, alert-facing):
  * ``..._ingest_age_seconds``       seconds since the newest row across the
                                     chain's enabled streams; ``-1`` = no row
                                     in the 24 h window.
  * ``..._last_ingest_timestamp_seconds`` unix ts of that newest row (0 = none).
  * ``..._rows_1h`` / ``..._rows_24h`` rows landed in that window.
  * ``..._stalled``                  1 = not parked and silent > STALL_AFTER_S.
  * ``..._parked``                   1 = declared in ``config/chain-states.yml``.
  * ``..._daily_cadence_breach``     1 = not parked and silent > 24 h.
"""

from __future__ import annotations

import contextlib
import os

from lite_indexer import metrics
from lite_indexer.chain_states import DAILY_CADENCE_SECONDS
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Silence that counts as stalled (the fleet acceptance bound: 900 s).
STALL_AFTER_S: float = 900.0

#: One statement for every chain: the three enabled streams, 24 h window.
_SQL = """
SELECT
    chain,
    toInt64(dateDiff('second', max(ingested_at), now())) AS age_s,
    toInt64(toUnixTimestamp(max(ingested_at))) AS last_ts,
    countIf(ingested_at >= now() - INTERVAL 3600 SECOND) AS rows_1h,
    count() AS rows_24h
FROM (
    SELECT chain, ingested_at FROM {db}.evm_transfers WHERE ingested_at >= now() - INTERVAL 1 DAY
    UNION ALL
    SELECT chain, ingested_at FROM {db}.dex_events WHERE ingested_at >= now() - INTERVAL 1 DAY
    UNION ALL
    SELECT chain, ingested_at FROM {db}.deployer_events WHERE ingested_at >= now() - INTERVAL 1 DAY
)
GROUP BY chain
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


def _fetch_rows(sql: str, client=None) -> dict[str, tuple[int, int, int, int]] | None:
    """Run the freshness query; ``None`` when ClickHouse cannot answer.

    ``client`` is a seam for tests; the daemon opens a short-lived client.
    """
    owned = client is None
    rows: dict[str, tuple[int, int, int, int]] = {}
    try:
        client = client or _client()
        for chain, age_s, last_ts, rows_1h, rows_24h in client.execute(sql):
            rows[str(chain)] = (int(age_s), int(last_ts), int(rows_1h), int(rows_24h))
    except Exception as exc:  # noqa: BLE001 - observability never breaks the daemon
        logger.warning("daily_health_query_failed", error=str(exc)[:120])
        return None
    finally:
        if owned and client is not None:
            with contextlib.suppress(Exception):
                client.disconnect()
    return rows


def _publish_chain(chain: str, row: tuple[int, int, int, int], parked_set: set[str]) -> None:
    """Publish every daily-health gauge for one chain (parked included)."""
    age, last_ts, rows_1h, rows_24h = row
    is_parked = chain in parked_set
    stalled = 0.0 if is_parked or 0 <= age <= STALL_AFTER_S else 1.0
    breach = 0.0 if is_parked or 0 <= age <= DAILY_CADENCE_SECONDS else 1.0
    metrics.set_labeled_gauge("lite_indexer_chain_ingest_age_seconds", float(age), chain=chain)
    metrics.set_labeled_gauge("lite_indexer_chain_last_ingest_timestamp_seconds", float(last_ts), chain=chain)
    metrics.set_labeled_gauge("lite_indexer_chain_rows_1h", float(rows_1h), chain=chain)
    metrics.set_labeled_gauge("lite_indexer_chain_rows_24h", float(rows_24h), chain=chain)
    metrics.set_labeled_gauge("lite_indexer_chain_stalled", stalled, chain=chain)
    metrics.set_labeled_gauge("lite_indexer_chain_parked", 1.0 if is_parked else 0.0, chain=chain)
    metrics.set_labeled_gauge("lite_indexer_chain_daily_cadence_breach", breach, chain=chain)
    if stalled:
        logger.warning("chain_ingest_stale", chain=chain, age_s=age, rows_24h=rows_24h)


def export_daily_health(chains, parked=(), client=None) -> None:
    """Publish the daily-health family for ``chains`` (parked included).

    Args:
        chains: Every chain the daemon is configured to run - NOT only the
            ones that already produced a cycle, so a chain that never
            ingested still gets a series (``-1`` / ``stalled=1``).
        parked: Declared parked chain names; their ``stalled`` stays 0 and
            ``parked`` becomes 1, so a declaration silences only itself.
        client: ClickHouse client seam (tests); ``None`` opens a fresh client.

    Best-effort: a ClickHouse hiccup keeps the previous samples and never
    stops indexing.
    """
    sql = _SQL.format(db=os.getenv("CH_DATABASE", "wallet_memory"))
    rows = _fetch_rows(sql, client)
    if rows is None:
        return
    parked_set = set(parked)
    for chain in dict.fromkeys(chains):
        _publish_chain(chain, rows.get(chain, (-1, 0, 0, 0)), parked_set)


__all__ = ["export_daily_health"]
