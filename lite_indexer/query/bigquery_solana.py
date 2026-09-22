"""Solana reader for :class:`BigQuerySource` (GCP public solana dataset).

Split out of bigquery_source.py (module size) and parameterised on
``BigQuerySpec`` so producers and consumers share one query contract.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.query.bigquery_common import ms_to_dt as _ms_to_dt
from lite_indexer.query.bigquery_spec import BigQuerySpec
from lite_indexer.query.bigquery_tables import _SOLANA_TABLES, build_query, run_bq


def query_solana(source: Any, spec: BigQuerySpec) -> list[dict[str, Any]]:
    """Read ``crypto_solana_mainnet_us`` rows in the solana row shape."""
    table = spec.table
    if table not in _SOLANA_TABLES:
        return []
    if not source.available():
        return []
    sql, params = build_query(spec)
    rows = source._runner(sql, params) if source._runner is not None else run_bq(source._bq_bin, sql, params)
    if not rows:
        return []
    return [map_solana_row(table, r) for r in rows]


def map_solana_row(table: str, raw: dict[str, Any]) -> dict[str, Any]:
    """Map a BigQuery solana row to the indexer's solana row shapes.

    ``token_transfers`` follows the ``solana_transfers`` shape used by
    :mod:`lite_indexer.query.sqd_solana` (``pre_amount`` is unknown in the
    public dataset, so it is ``"0"`` and ``delta == post_amount``);
    ``instructions`` follows the owner-activity instruction shape.
    """
    ts = _ms_to_dt(raw.get("block_timestamp"))
    ingested = datetime.now(UTC)
    if table == "instructions":
        accounts = raw.get("accounts") or []
        return {
            "chain": "solana",
            "slot": int(raw.get("slot") or 0),
            "block_timestamp": ts,
            "tx_hash": raw.get("tx_hash") or "",
            "transaction_index": raw.get("transaction_index"),
            "program_id": raw.get("program_id") or "",
            "accounts": list(accounts),
            "account_count": len(accounts),
            "data": raw.get("data") or "",
            "owner": "",
            "ingested_at": ingested,
        }
    value = str(raw.get("value") or "0")
    return {
        "chain": "solana",
        "slot": int(raw.get("slot") or 0),
        "block_timestamp": ts,
        "tx_hash": raw.get("tx_hash") or "",
        "token_account": "",
        "mint": raw.get("mint") or "",
        "owner": raw.get("owner") or "",
        "pre_amount": "0",
        "post_amount": value,
        "delta": value,
        "decimals": int(float(raw.get("decimals") or 0)),
        "token_symbol": "",
        "token_name": "",
        "source": raw.get("source") or "",
        "destination": raw.get("destination") or "",
        "transfer_type": raw.get("transfer_type") or "",
        "ingested_at": ingested,
    }

