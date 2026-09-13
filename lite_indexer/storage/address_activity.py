"""
Address-activity storage — ``wallet_memory.address_activity``.

The wallet-tracing keystone table: one row per (address, tx_signature)
recording that a transaction touched an address. This is what SolanaProvider
``getSignaturesForAddress`` returns and what wallet tracing queries need;
the indexer derives it from the getTransaction (jsonParsed) payloads it
already fetches (SPL transfers, DEX swaps, traces), so it costs zero
extra RPC calls.

* :func:`parse_address_activity` — pure: expand one parsed transaction
  into one row per involved account (signers, writable accounts, token
  accounts, read-only accounts).
* :func:`ensure_address_activity_table` — idempotent DDL.
* :func:`write_address_activity` — batch insert of expanded rows.

Schema matches the ClickHouse migration ``migrations/014_*.sql`` and is
also ensured at runtime (idempotent, non-fatal) so the table appears on
first address-activity write without operator DDL.

Schema (single keystone table, multi-chain): the original table was
Solana-shaped (``slot`` / ``tx_signature`` / ``program_ids`` /
``is_signer`` from the parsed Solana message). Cross-chain ingestion
(EVM + TRON) reuses the SAME table with a ``chain`` discriminator column
rather than a sibling table, so an address-first query
(``WHERE address = ...``) stays a single scan across every chain. The
``chain`` column is appended to the ReplacingMergeTree sort key —
``ORDER BY (address, tx_signature, chain)`` — which both makes the dedup
key explicit and lets migration ``015_*.sql`` upgrade a pre-existing
Solana table in place via ``ALTER ... MODIFY ORDER BY`` (the legacy
``(address, tx_signature)`` prefix is preserved).

* :func:`parse_address_activity` — Solana: expand one parsed transaction
  into one row per involved account (signers, writable accounts, token
  accounts, read-only accounts).
* :func:`parse_evm_address_activity` — EVM: expand one parsed log-derived
  event (transfer / swap / log group) into one row per address touched
  (``from``/``to``, contracts, tokens, optional log/internal-call
  addresses).
* :func:`parse_tron_address_activity` — TRON: expand one parsed TRC20
  event into one row per address touched.
* :func:`ensure_address_activity_table` — idempotent CREATE of the
  canonical multi-chain schema.
* :func:`ensure_address_activity_chain_column` — in-place upgrade of a
  pre-migration-015 table (ADD COLUMN + MODIFY ORDER BY).
* :func:`write_address_activity` — batch insert of expanded rows.

Schema matches the ClickHouse migrations ``014_*.sql`` +
``015_address_activity_chain.sql`` and is also ensured at runtime
(idempotent, non-fatal) so the table appears on first address-activity
write without operator DDL.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_ADDRESS_ACTIVITY_TABLE, RETENTION_DAYS
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

# Re-export the cross-chain parsers so ``lite_indexer.storage.address_activity``
# remains the single import surface for every address-activity consumer.
from lite_indexer.storage.address_activity_crosschain import (
    parse_evm_address_activity,
    parse_tron_address_activity,
)

logger = get_logger(__name__)

_ADDRESS_ACTIVITY_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_ADDRESS_ACTIVITY_TABLE} (
    address String CODEC(ZSTD(3)),
    tx_signature String CODEC(ZSTD(3)),
    chain String DEFAULT 'solana' CODEC(ZSTD(3)),
    slot UInt64,
    block_time DateTime,
    involved_accounts Array(String),
    program_ids Array(String),
    is_signer Bool,
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree()
ORDER BY (address, tx_signature, chain)
TTL block_time + INTERVAL {RETENTION_DAYS.get("address_activity", 365)} DAY
"""

# Upgrade path for a table provisioned by migration 014 (Solana-only, no
# ``chain`` column, sort key ``(address, tx_signature)``). ``CREATE TABLE
# IF NOT EXISTS`` is a no-op on an existing table, so
# :func:`ensure_address_activity_chain_column` runs these ALTERs to bring
# the live table up to the canonical schema. ``MODIFY ORDER BY`` appends
# ``chain`` to the legacy key — ClickHouse supports this because the old
# key remains a prefix of the new one, so no data rewrite is required.
_ADD_CHAIN_COLUMN_DDL = (
    f"ALTER TABLE {CH_ADDRESS_ACTIVITY_TABLE} ADD COLUMN IF NOT EXISTS chain String DEFAULT 'solana' CODEC(ZSTD(3))"
)
_MODIFY_ORDER_BY_DDL = f"ALTER TABLE {CH_ADDRESS_ACTIVITY_TABLE} MODIFY ORDER BY (address, tx_signature, chain)"

_INSERT_COLUMNS = (
    "address",
    "tx_signature",
    "slot",
    "block_time",
    "involved_accounts",
    "program_ids",
    "is_signer",
    "ingested_at",
    "chain",
)

# Fallback chain tag for rows produced before the ``chain`` column existed
# (e.g. Solana rows emitted by an older writer).
_DEFAULT_CHAIN = "solana"


def _collect_program_ids(message: dict[str, Any], meta: dict[str, Any]) -> list[str]:
    """Return the ordered-unique program ids invoked by a transaction.

    Walks top-level instructions plus CPI inner instructions (nested in
    ``meta.innerInstructions`` for jsonParsed payloads) and collects every
    ``programId`` in first-seen order.
    """
    program_ids: list[str] = []
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict) and ix.get("programId"):
            program_ids.append(str(ix["programId"]))
    for inner in meta.get("innerInstructions") or []:
        if not isinstance(inner, dict):
            continue
        for ix in inner.get("instructions") or []:
            if isinstance(ix, dict) and ix.get("programId"):
                program_ids.append(str(ix["programId"]))
    return list(dict.fromkeys(program_ids))


def parse_address_activity(tx_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand one parsed transaction into one row per involved address.

    Accepts a Solana ``getTransaction`` result in ``jsonParsed`` encoding
    (the shape the strategy already fetches). One row is produced for
    every account key loaded by the transaction — signers, writable
    accounts, token accounts and read-only accounts — with the account
    set and program set repeated on each row so an address-first query
    (``WHERE address = ...``) needs no join.

    Account keys may be jsonParsed objects (``{"pubkey", "signer",
    "writable", "source"}``) or plain base58 strings; for the string form
    the signer flag is derived from ``message.header.numRequiredSignatures``.

    Failed transactions are skipped (``meta.err is not None``), matching
    every other parse path in the pipeline — the table currently has no
    ``error`` column, so failed txs would be indistinguishable.

    Args:
        tx_data: Parsed getTransaction result (jsonParsed encoding).

    Returns:
        List of row dicts with keys ``address``, ``tx_signature``,
        ``slot``, ``block_time`` (aware UTC datetime), ``involved_accounts``,
        ``program_ids``, ``is_signer``. Empty when the payload is not a
        usable transaction (or the transaction failed).
    """
    meta = tx_data.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    if meta.get("err") is not None:
        return []

    transaction = tx_data.get("transaction")
    if not isinstance(transaction, dict):
        return []
    message = transaction.get("message")
    if not isinstance(message, dict):
        return []

    signatures = transaction.get("signatures") or []
    if not signatures:
        return []
    tx_signature = str(signatures[0])

    account_keys = message.get("accountKeys") or []
    if not account_keys:
        return []

    header_raw = message.get("header")
    header: dict[str, Any] = header_raw if isinstance(header_raw, dict) else {}
    try:
        num_signers = int(header.get("numRequiredSignatures") or 0)
    except (TypeError, ValueError):
        num_signers = 0

    # Expand account keys to (pubkey, is_signer) pairs. jsonParsed keys are
    # dicts carrying an explicit signer flag; plain keys need the legacy
    # header derivation (the first numRequiredSignatures keys are signers).
    expanded: list[tuple[str, bool]] = []
    for idx, key in enumerate(account_keys):
        if isinstance(key, dict):
            pubkey = str(key.get("pubkey") or "")
            signer = bool(key.get("signer", False))
        else:
            pubkey = str(key)
            signer = idx < num_signers
        if pubkey:
            expanded.append((pubkey, signer))
    if not expanded:
        return []

    try:
        slot = int(tx_data.get("slot") or 0)
    except (TypeError, ValueError):
        slot = 0

    block_time_raw = tx_data.get("blockTime")
    try:
        if block_time_raw is None:
            raise TypeError("missing blockTime")
        block_time = datetime.fromtimestamp(int(block_time_raw), tz=UTC)
    except (TypeError, ValueError, OverflowError):
        block_time = datetime.now(UTC)

    involved_accounts = list(dict.fromkeys(pubkey for pubkey, _ in expanded))
    program_ids = _collect_program_ids(message, meta)

    return [
        {
            "chain": "solana",
            "address": pubkey,
            "tx_signature": tx_signature,
            "slot": slot,
            "block_time": block_time,
            "involved_accounts": involved_accounts,
            "program_ids": program_ids,
            "is_signer": signer,
        }
        for pubkey, signer in expanded
    ]


def ensure_address_activity_chain_column(ch_client: Any) -> None:
    """Upgrade a pre-015 table with the ``chain`` discriminator (idempotent).

    ``CREATE TABLE IF NOT EXISTS`` is a no-op on an existing table, so a
    table provisioned by migration 014 (no ``chain`` column, sort key
    ``(address, tx_signature)``) would never gain it from the CREATE alone.
    These ALTERs bring such a table up to the canonical schema. They are
    best-effort: on a fresh database the table does not exist yet, both
    fail harmlessly, and the CREATE provisions the canonical schema.

    Kept separate from :func:`ensure_address_activity_table` so the Solana
    drain path remains a single CREATE + INSERT (migration 015 is the
    operator-facing upgrade); the cross-chain mixin calls this first so
    EVM/TRON ingestion self-heals an un-migrated table.
    """
    for ddl in (_ADD_CHAIN_COLUMN_DDL, _MODIFY_ORDER_BY_DDL):
        try:
            ch_client.execute(ddl)
        except Exception as exc:  # noqa: BLE001 — table may be absent/fresh
            logger.debug("address_activity_chain_ddl_skip", error=str(exc)[:80])


def ensure_address_activity_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.address_activity`` if it does not exist.

    Provisions the canonical multi-chain schema (``chain`` discriminator +
    extended sort key). For a pre-existing Solana-only table, apply
    migration ``015_address_activity_chain.sql`` or call
    :func:`ensure_address_activity_chain_column` first.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False when the
        database is missing or privileges are insufficient.
    """
    try:
        ch_client.execute(
            _ADDRESS_ACTIVITY_DDL
        )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
        logger.info("address_activity_table_ensured", table=CH_ADDRESS_ACTIVITY_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("address_activity_table_skip", error=str(exc)[:80])
        else:
            logger.warning("address_activity_table_failed", error=str(exc)[:100])
        return False


def write_address_activity(ch_client: Any, rows: list[dict[str, Any]]) -> int:
    """Insert expanded address-activity rows into ClickHouse.

    Best-effort: returns 0 on failure without raising. Rows follow the
    schema produced by :func:`parse_address_activity` (timestamps may be
    aware datetimes, ISO strings, or Unix seconds).

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        rows: Expanded address-activity row dicts.

    Returns:
        Number of rows inserted (0 on failure/unavailable).
    """
    if not rows:
        return 0

    now = datetime.now(UTC)
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        ts = row.get("block_time")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                ts = now
        if isinstance(ts, (int, float)):
            try:
                ts = datetime.fromtimestamp(int(ts), tz=UTC)
            except (ValueError, TypeError, OverflowError):
                ts = now
        if not isinstance(ts, datetime):
            ts = now
        prepared.append(
            (
                str(row.get("address") or ""),
                str(row.get("tx_signature") or ""),
                int(row.get("slot") or 0),
                ts,
                [str(a) for a in (row.get("involved_accounts") or [])],
                [str(p) for p in (row.get("program_ids") or [])],
                bool(row.get("is_signer")),
                now,
                str(row.get("chain") or _DEFAULT_CHAIN),
            )
        )

    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"INSERT INTO {CH_ADDRESS_ACTIVITY_TABLE} ({', '.join(_INSERT_COLUMNS)}) VALUES",
            prepared,
        )
        logger.info("address_activity_inserted", rows=len(prepared))
        metrics_incr("address_activity_written", len(prepared))
        return len(prepared)
    except Exception as exc:  # noqa: BLE001 — best-effort insert
        metrics_incr("clickhouse_errors")
        logger.error(
            "address_activity_insert_failed",
            rows=len(prepared),
            error=str(exc)[:150],
        )
        return 0


__all__ = [
    "CH_ADDRESS_ACTIVITY_TABLE",
    "ensure_address_activity_chain_column",
    "ensure_address_activity_table",
    "parse_address_activity",
    "parse_evm_address_activity",
    "parse_tron_address_activity",
    "write_address_activity",
]
