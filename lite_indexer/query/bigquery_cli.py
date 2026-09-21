"""``bq`` CLI executor for the BigQuery provider.

Extracted 2026-09-21 from ``query/bigquery_tables.py`` (structure gate: NEW >200).
"""

from __future__ import annotations

import json
import re
import subprocess
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PROJECT = "gen-lang-client-0708984505"
_DATASET = "bigquery-public-data.crypto_ethereum"
_SOLANA_DATASET = "bigquery-public-data.crypto_solana_mainnet_us"
_SOLANA_TABLES: dict[str, str] = {
    "token_transfers": "Token Transfers",
    "instructions": "Instructions",
}
_SOLANA_SELECT: dict[str, str] = {
    "token_transfers": (
        "block_slot AS slot, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "tx_signature AS tx_hash, mint, authority AS owner, "
        "CAST(value AS STRING) AS value, CAST(decimals AS STRING) AS decimals, "
        "source, destination, transfer_type"
    ),
    "instructions": (
        "block_slot AS slot, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "tx_signature AS tx_hash, `index` AS transaction_index, "
        "program_id, accounts, data"
    ),
}
_TABLE_SELECT: dict[str, str] = {
    "transactions": (
        "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "`hash` AS tx_hash, from_address, to_address, value"
    ),
    "token_transfers": (
        "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "transaction_hash AS tx_hash, from_address, to_address, token_address, value, log_index"
    ),
    "logs": (
        "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "transaction_hash AS tx_hash, address AS token_address, log_index"
    ),
    "contracts": "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, address AS token_address",
    "blocks": "number AS block_number, UNIX_MILLIS(timestamp) AS block_timestamp, `hash` AS tx_hash, transaction_count",
}
_ADDR_COLS: dict[str, tuple[str, ...]] = {
    "transactions": ("from_address", "to_address"),
    "token_transfers": ("from_address", "to_address"),
    "logs": ("address",),
    "contracts": ("address",),
    "blocks": (),
}


def run_bq(bq_bin: str, sql: str, params: list[tuple[str, str, Any]]) -> list[dict[str, Any]]:
    """Shell out to the ``bq`` CLI with named parameters; [] on any failure."""
    cmd = [
        bq_bin,
        "--project_id",
        _PROJECT,
        "--format=json",
        "--quiet",
        "query",
        "--use_legacy_sql=false",
    ]
    for name, bigquery_type, value in params:
        rendered = json.dumps(value) if bigquery_type.startswith("ARRAY<") else str(value)
        cmd.append(f"--parameter={name}:{bigquery_type}:{rendered}")
    cmd.append(sql)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        logger.warning("bigquery_bq_failed", error=((proc.stderr or proc.stdout) or "")[:300])
        return []
    try:
        data = json.loads(proc.stdout or "[]")
    except ValueError:
        logger.warning("bigquery_bad_json", error=(proc.stdout or "")[:200])
        return []
    return data if isinstance(data, list) else []
