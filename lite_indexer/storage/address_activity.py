"""
Address-activity storage — ``wallet_memory.address_activity``.

The wallet-tracing keystone table: one row per (address, tx_signature)
recording that a transaction touched an address. This is what SolanaProvider
``getSignaturesForAddress`` returns and what wallet tracing queries need.
The indexer derives it from getTransaction (jsonParsed) payloads it already
fetches (SPL transfers, DEX swaps, traces), so it costs zero extra RPC.

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

from lite_indexer.logging import get_logger

# Re-export the cross-chain parsers so ``lite_indexer.storage.address_activity``
# remains the single import surface for every address-activity consumer.
from lite_indexer.storage.address_activity_crosschain import (
    parse_evm_address_activity,
    parse_tron_address_activity,
)

logger = get_logger(__name__)


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


def parse_address_activity(tx_data: dict[str, Any], slot: int | None = None) -> list[dict[str, Any]]:
    """Expand one parsed transaction into one row per involved address.

    Accepts a Solana ``getTransaction`` result in ``jsonParsed`` encoding
    (the shape the strategy already fetches). One row is produced for
    every account key loaded by the transaction — signers, writable
    accounts, token accounts and read-only accounts — with the account
    set and program set repeated on each row so an address-first query
    (``WHERE address = ...``) needs no join.

    ``slot`` is the transaction's slot when the caller already knows it
    (e.g. the block-based extraction pass, whose per-transaction dicts
    come from ``getBlock`` and carry no top-level ``slot``). When omitted,
    the fallback is ``tx_data["slot"]`` (present on a ``getTransaction``
    result). Passing it explicitly prevents rows from being written with
    the sentinel ``0`` when the payload shape lacks the field.

    Account keys may be jsonParsed objects (``{"pubkey", "signer",
    "writable", "source"}``) or plain base58 strings; for the string form
    the signer flag is derived from ``message.header.numRequiredSignatures``.

    Failed transactions are skipped (``meta.err is not None``), matching
    every other parse path in the pipeline — the table currently has no
    ``error`` column, so failed txs would be indistinguishable.

    Args:
        tx_data: Parsed getTransaction result (jsonParsed encoding).
        slot: Explicit slot for the transaction; overrides
            ``tx_data["slot"]`` when provided.

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
        row_slot = int(slot) if slot is not None else int(tx_data.get("slot") or 0)
    except (TypeError, ValueError):
        row_slot = 0

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
            "slot": row_slot,
            "block_time": block_time,
            "involved_accounts": involved_accounts,
            "program_ids": program_ids,
            "is_signer": signer,
        }
        for pubkey, signer in expanded
    ]


# Re-exported so this module stays the single import surface (ratchet).
from lite_indexer.storage.address_activity_ddl import (  # noqa: E402,F401
    _ADD_CHAIN_COLUMN_DDL,
    _ADDRESS_ACTIVITY_DDL,
    _DEFAULT_CHAIN,
    _INSERT_COLUMNS,
    _MODIFY_ORDER_BY_DDL,
    CH_ADDRESS_ACTIVITY_TABLE,
    ensure_address_activity_chain_column,
    ensure_address_activity_table,
)
from lite_indexer.storage.address_activity_write import write_address_activity  # noqa: E402

__all__ = [
    "_ADDRESS_ACTIVITY_DDL",
    "_INSERT_COLUMNS",
    "CH_ADDRESS_ACTIVITY_TABLE",
    "ensure_address_activity_chain_column",
    "ensure_address_activity_table",
    "parse_address_activity",
    "parse_evm_address_activity",
    "parse_tron_address_activity",
    "write_address_activity",
]
