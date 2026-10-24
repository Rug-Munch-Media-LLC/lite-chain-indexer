"""EVM edge sources for the trace: ClickHouse (recent) + BigLake (older).

ClickHouse holds a rolling window (``evm_transfers`` 30d, ``internal_transfers``
90d) and is the authority for anything inside it. BigLake (AWS public
Ethereum, read in place through ``BigLakeSource``) answers history that
predates the window for ``ethereum`` only — no other chain has a lake leg,
so nothing is invented for them.

Both sources return the same normalized edge-row shape so the graph layer
never needs to know which tier produced a row.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from lite_indexer.query.trace_sql import sql_for

#: ``run(sql, params) -> list[dict]`` — the caller owns the connection.
Run = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

#: Chains with a BigLake leg (see ``biglake_source.BigLakeSource``).
LAKE_CHAINS = frozenset({"ethereum"})


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=UTC)
        return aware.isoformat()
    return str(value or "")


def _row(
    *,
    src: str,
    dst: str,
    asset: str,
    raw: Any,
    symbol: Any,
    tx_hash: Any,
    ts: Any,
    kind: str,
    tier: str,
    tx_count: Any = 1,
) -> dict[str, Any]:
    return {
        "from": src.lower(),
        "to": dst.lower(),
        "asset": asset.lower() if asset != "native" else "native",
        "raw": int(raw or 0),
        "symbol": str(symbol or ""),
        "tx_hash": str(tx_hash or ""),
        "ts": _iso(ts),
        "kind": kind,
        "tier": tier,
        "tx_count": int(tx_count or 1),
    }


def fetch_evm(
    run: Run,
    chain: str,
    addrs: list[str],
    since: datetime,
    *,
    limit: int = 400,
    include_internal: bool = True,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch aggregated ERC-20 and native-value edges touching ``addrs``.

    One ClickHouse scan per table for the whole frontier (not per address),
    so a depth-3 trace costs a bounded number of scans.

    Args:
        run: Executor — ``(sql, params) -> rows``.
        chain: EVM chain name (``ethereum``, ``base``, ``bsc``, ...).
        addrs: Frontier addresses, already normalised (lowercase hex).
        since: Earliest ``block_timestamp`` to read.
        limit: Per-table row ceiling.
        include_internal: Also read native value from ``internal_transfers``
            (traces catch funding that never emitted an ERC-20 ``Transfer``).

    Returns:
        ``(rows, notes)`` — normalized edge rows plus degradation notes.
    """
    rows: list[dict[str, Any]] = []
    notes: list[str] = []
    if not addrs:
        return rows, ["evm: empty frontier"]

    sql, params = sql_for("evm_edges", chain=chain, since=since, addrs=addrs, limit=limit)
    for row in run(sql, params):
        rows.append(
            _row(
                src=row["from_address"],
                dst=row["to_address"],
                asset=row["token_address"] or "native",
                raw=row.get("raw_amount"),
                symbol=row.get("symbol"),
                tx_hash=row.get("first_tx") or row.get("last_tx"),
                ts=row.get("first_ts"),
                kind="erc20",
                tier="clickhouse",
                tx_count=row.get("tx_count"),
            )
        )

    if include_internal:
        sql, params = sql_for("internal_edges", chain=chain, since=since, addrs=addrs, limit=limit)
        try:
            internal = run(sql, params)
        except Exception as exc:  # noqa: BLE001 — traces are an enrichment, never fatal
            notes.append(f"internal_transfers unavailable: {str(exc)[:90]}")
            internal = []
        for row in internal:
            rows.append(
                _row(
                    src=row["from_address"],
                    dst=row["to_address"],
                    asset="native",
                    raw=row.get("raw_amount"),
                    symbol="",
                    tx_hash=row.get("first_tx") or row.get("last_tx"),
                    ts=row.get("first_ts"),
                    kind="internal",
                    tier="clickhouse",
                    tx_count=row.get("tx_count"),
                )
            )
    return rows, notes


def fetch_lake(
    chain: str,
    address: str,
    dates: list[str],
    *,
    limit: int = 400,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Read pre-window Ethereum history from the free AWS lake.

    Args:
        chain: Must be in :data:`LAKE_CHAINS`; anything else returns a note.
        address: Wallet address (any case).
        dates: Explicit ``YYYY-MM-DD`` partitions. Never a bare month glob —
            full-history listing is slow and can time out.
        limit: Row ceiling.

    Returns:
        ``(rows, notes)`` — edge rows tagged ``tier="biglake"``.
    """
    if chain not in LAKE_CHAINS:
        return [], [f"biglake: no lake leg for {chain}"]
    if not dates:
        return [], ["biglake: skipped (no explicit dates)"]
    try:
        from lite_indexer.query.biglake_source import BigLakeSource
    except Exception as exc:  # noqa: BLE001 — optional dependency
        return [], [f"biglake unavailable: {str(exc)[:90]}"]

    source = BigLakeSource()
    if not source.available():
        return [], ["biglake: disabled or duckdb missing"]
    try:
        lake_rows, truncated = source.query("evm_transfers", chain, address=address, dates=dates, limit=limit)
    except Exception as exc:  # noqa: BLE001 — the lake is a free best-effort tier
        return [], [f"biglake query failed: {str(exc)[:90]}"]

    rows = [
        _row(
            src=row.get("from_address") or "",
            dst=row.get("to_address") or "",
            asset=row.get("token_address") or "native",
            raw=row.get("raw_amount") or row.get("value") or 0,
            symbol=row.get("token_symbol"),
            tx_hash=row.get("tx_hash"),
            ts=row.get("block_timestamp"),
            kind="erc20",
            tier="biglake",
        )
        for row in lake_rows
    ]
    notes = ["biglake: truncated at limit" if truncated else "biglake: ok"]
    return rows, notes
