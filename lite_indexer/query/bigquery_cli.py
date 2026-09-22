"""``bq`` CLI executor for the BigQuery provider.

Extracted 2026-09-21 from ``query/bigquery_tables.py`` (structure gate: NEW >200).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.bigquery_common import BigQueryQuotaError, is_quota_error

logger = get_logger(__name__)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DEFAULT_PROJECT = "gen-lang-client-0708984505"
_PROJECT = _DEFAULT_PROJECT  # kept for callers/legacy imports
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


def bq_accounts() -> list[tuple[str, str | None]]:
    """Query accounts in preference order: [primary, alt].

    Two-account alternation: when the primary project's free query-bytes
    quota is exhausted, the alt account serves the rest of the run.
    primary: BIGQUERY_PROJECT_ID + GOOGLE_APPLICATION_CREDENTIALS
    alt:     BIGQUERY_ALT_PROJECT_ID + BIGQUERY_ALT_CREDENTIALS
    """
    accounts = [(
        os.environ.get("BIGQUERY_PROJECT_ID") or _DEFAULT_PROJECT,
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
    )]
    alt_project = os.environ.get("BIGQUERY_ALT_PROJECT_ID")
    if alt_project:
        accounts.append((alt_project, os.environ.get("BIGQUERY_ALT_CREDENTIALS")))
    return accounts


def bq_env(creds: str | None) -> dict[str, str]:
    """Subprocess env that FORCES ``bq`` to use ``creds``.

    Plain GOOGLE_APPLICATION_CREDENTIALS is ignored by the gcloud-backed
    ``bq`` CLI (it prefers its stored identity) — the override variable is
    what actually switches accounts, so alternation would silently no-op
    without this.
    """
    env = dict(os.environ)
    if creds:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = creds
        env["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] = creds
    return env


def run_bq(bq_bin: str, sql: str, params: list[tuple[str, str, Any]]) -> list[dict[str, Any]]:
    """Shell out to the ``bq`` CLI with named parameters; [] on any failure.

    Tries the primary account first; on a quota rejection and a configured
    alt account it swaps once (two-account alternation) and continues.
    """
    accounts = bq_accounts()
    last_quota_error: str | None = None
    for idx, (project, creds) in enumerate(accounts):
        cmd = [
            bq_bin,
            "--project_id",
            project,
            "--format=json",
            "--quiet",
            "query",
            "--use_legacy_sql=false",
            "--max_rows=200000",  # display cap (query flag); SQL LIMIT governs the page
        ]
        for name, bigquery_type, value in params:
            rendered = json.dumps(value) if bigquery_type.startswith("ARRAY<") else str(value)
            cmd.append(f"--parameter={name}:{bigquery_type}:{rendered}")
        cmd.append(sql)
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=bq_env(creds))
        if proc.returncode != 0 and "gcloud crashed" in ((proc.stderr or "") + (proc.stdout or "")):
            logger.warning("bigquery_bq_retry", project=project, error=((proc.stderr or "") or "")[:120])
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=bq_env(creds))
        if proc.returncode == 0:
            try:
                data = json.loads(proc.stdout or "[]")
            except ValueError:
                logger.warning("bigquery_bad_json", error=(proc.stdout or "")[:200])
                return []
            return data if isinstance(data, list) else []
        detail = ((proc.stderr or proc.stdout) or "")[:300]
        if is_quota_error(detail):
            last_quota_error = detail
            logger.warning("bigquery_quota_exceeded", project=project, error=detail[:200])
            if idx + 1 < len(accounts):
                logger.warning("bigquery_account_swapped", to_project=accounts[idx + 1][0])
                continue
            raise BigQueryQuotaError(detail)
        logger.warning("bigquery_bq_failed", project=project, error=detail)
        return []
    raise BigQueryQuotaError(last_quota_error or "quota exhausted on all accounts")

def _silence_console_logs() -> None:
    """CLI stdout must stay machine-parseable (JSON only)."""
    try:
        from loguru import logger as _loguru

        _loguru.remove()
    except ImportError:
        logger.debug("loguru_not_installed")


def main() -> int:
    """``python -m lite_indexer.query.bigquery_source <chain> <table> ...``.

    Low-cost validation probe: always pass ``--dates`` (DAY-partition filter)
    and a small ``--limit``. Prints mapped rows as JSON to stdout.
    """
    import argparse
    import json as _json
    import sys as _sys

    _silence_console_logs()
    parser = argparse.ArgumentParser(prog="bigquery_source", description=__doc__)
    parser.add_argument("chain", choices=["eth", "solana"])
    parser.add_argument("table")
    parser.add_argument("--dates", nargs="*", default=None, help="YYYY-MM-DD partitions")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--address", default=None)
    args = parser.parse_args()
    from lite_indexer.query.bigquery_source import BigQuerySource
    from lite_indexer.query.bigquery_spec import BigQuerySpec

    src = BigQuerySource()
    if not src.available():
        print("bigquery source unavailable (credentials or bq CLI missing)", file=_sys.stderr)  # noqa: T201
        return 1
    rows = src.query(
        BigQuerySpec(
            table=args.table, chain=args.chain, address=args.address,
            dates=args.dates, limit=args.limit,
        )
    )
    print(_json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
