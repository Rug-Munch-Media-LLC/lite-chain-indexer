"""Writer for CEX-deposit clusters into ``wallet_memory.wallet_entities``.

Reads ``wallet_labels`` (reference set, never modified) and the funding tables,
then writes scored rows. Idempotent: ``wallet_entities`` is
``ReplacingMergeTree(last_updated_at)`` and the row's ``last_updated_at`` comes
from the evidence, so re-running over unchanged data collapses to the same row.

The connection is injected (``connect`` callable) so tests exercise the real SQL
path with a fake client instead of monkeypatching module internals.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Any, Protocol

from lite_indexer.entities import cex_deposit as cd
from lite_indexer.entities import cex_deposit_sql as sql
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


class ClickHouseLike(Protocol):
    """The slice of ``clickhouse_driver.Client`` this writer uses."""

    def execute(self, query: str, params: Any = None) -> Any: ...


ConnectFn = Callable[[], Any]

#: Chains with native-value coverage in ``internal_transfers`` today. Kept as
#: data, not a hardcoded chain list inside the query, so adding a chain is a
#: config change and not a code change.
DEFAULT_CHAINS: tuple[str, ...] = ("ethereum", "arbitrum", "bsc", "optimism", "base")


def _rows(result: Any) -> list[tuple[Any, ...]]:
    """Normalise a driver result to a list of tuples (missing -> empty)."""
    if not result:
        return []
    if isinstance(result, list):
        return [tuple(r) for r in result]
    return [tuple(result)]


def fetch_exchange_labels(client: ClickHouseLike, chain: str) -> dict[str, cd.SourceLabel]:
    """Exchange labels for one chain, keyed by lower-cased source address."""
    result = client.execute(
        sql.EXCHANGE_LABELS,
        {"chain": chain, "categories": list(cd.EXCHANGE_CATEGORIES)},
    )
    labels: dict[str, cd.SourceLabel] = {}
    for addr, name, category, tier, subtype in _rows(result):
        labels[str(addr)] = cd.SourceLabel(
            label_name=str(name or ""),
            label_category=str(category or ""),
            tier=str(tier or "C"),
            label_subtype=str(subtype or ""),
        )
    return labels


def fetch_window_start(client: ClickHouseLike, chain: str) -> int:
    """Epoch of the earliest native movement we hold for a chain (0 = unknown)."""
    for row in _rows(client.execute(sql.WINDOW_START, {"chain": chain})):
        return int(row[0] or 0)
    return 0


def collect_evidence(client: ClickHouseLike, chain: str, labels: dict[str, cd.SourceLabel]) -> list[cd.Evidence]:
    """Evidence per wallet from both tiers, native winning over token.

    Both queries already restrict to wallets whose *first* inbound came from a
    labelled source (the ``HAVING``), so no row here is an "also funded by"
    coincidence. A wallet present in both tiers keeps the native row: a native
    deposit is the real signal and a token inbound must not outrank it.
    """
    params = {"chain": chain, "categories": list(cd.EXCHANGE_CATEGORIES)}
    evidence: list[cd.Evidence] = []
    seen: set[str] = set()
    for wallet, source, epoch in _rows(client.execute(sql.NATIVE_FIRST_INBOUND, params)):
        wallet, source = str(wallet), str(source)
        evidence.append(cd.Evidence(chain, wallet, source, int(epoch or 0), cd.TIER_NATIVE))
        seen.add(wallet)
    for wallet, source, epoch in _rows(client.execute(sql.TOKEN_FIRST_INBOUND, params)):
        wallet, source = str(wallet), str(source)
        if wallet in seen:
            continue
        evidence.append(cd.Evidence(chain, wallet, source, int(epoch or 0), cd.TIER_TOKEN))
    return evidence


def table_exists(client: ClickHouseLike, name: str) -> bool:
    """True when ``wallet_memory.<name>`` exists (probe is read-only)."""
    for row in _rows(client.execute(sql.TABLE_EXISTS, {"name": name})):
        return bool(row[0])
    return False


def build_chain_rows(client: ClickHouseLike, chain: str) -> tuple[list[cd.EntityRow], dict[str, int]]:
    """Score every cluster for one chain. Returns rows and per-tier counts."""
    labels = fetch_exchange_labels(client, chain)
    if not labels:
        return [], {"native": 0, "token": 0}
    evidence = collect_evidence(client, chain, labels)
    window_start = fetch_window_start(client, chain)
    rows = cd.build_rows(evidence, labels, {chain: window_start})
    counts = {
        "native": sum(1 for e in evidence if e.tier == cd.TIER_NATIVE),
        "token": sum(1 for e in evidence if e.tier == cd.TIER_TOKEN),
    }
    return rows, counts


def insert_rows(client: ClickHouseLike, rows: Sequence[cd.EntityRow], batch: int = 5000) -> int:
    """Insert scored rows in batches; returns the number of rows written."""
    written = 0
    for start in range(0, len(rows), batch):
        chunk: Iterable[cd.EntityRow] = rows[start : start + batch]
        client.execute(sql.INSERT_ENTITIES, [r.as_tuple() for r in chunk])
        written += len(list(chunk))
    return written


def run(connect: ConnectFn, chains: Sequence[str] = DEFAULT_CHAINS, dry_run: bool = False) -> dict[str, Any]:
    """Cluster every requested chain and write the result.

    ``connect`` returns a ClickHouse client (or None when unreachable). Nothing
    is written on a dry run; the returned summary is the same either way so a
    dry run is a true rehearsal.
    """
    client = connect()
    if client is None:
        logger.warning("cex_deposit_no_clickhouse")
        return {"ok": False, "reason": "clickhouse_unavailable", "rows": 0}

    if not table_exists(client, "wallet_entities"):
        logger.warning("cex_deposit_missing_table", table="wallet_entities")
        return {"ok": False, "reason": "wallet_entities_missing", "rows": 0}

    total = 0
    per_chain: dict[str, Any] = {}
    all_rows: list[cd.EntityRow] = []
    for chain in chains:
        rows, counts = build_chain_rows(client, chain)
        all_rows.extend(rows)
        per_chain[chain] = {"clusters": len({r.entity_id for r in rows}), "wallets": len(rows), **counts}
        total += len(rows)

    if not dry_run and all_rows:
        insert_rows(client, all_rows)

    logger.info("cex_deposit_done", rows=total, chains=len(chains), dry_run=dry_run)
    return {"ok": True, "rows": total, "dry_run": dry_run, "per_chain": per_chain}
