"""
Jito Bundle (Bundled-Launch) Detection
=======================================
Detect bundled token launches on Solana from raw ``getTransaction``
(``jsonParsed``) results, without any extra RPC calls.

A *bundle* is a Jito-tipped sequence of transactions submitted together
so they land in the same slot atomically. For bundled launches the run
looks like:

    [token-create tx, buy tx from fresh wallet A, buy tx from fresh
     wallet B, ..., final tx carrying a SOL transfer >= 1000 lamports to
     one of the 8 Jito tip accounts]

The tip transfer is the hardest signal to fake: the bundler must pay
the tip to a known Jito account for the bundle to be included, and the
tip instruction rides in the LAST transaction of the run. The fresh
buyer accounts are a *lower bound* — bundler tools spread block-0 buys
across up to 12 wallets (Pump.fun) / 8 (Raydium LaunchLab, Bonk.fun) —
so ``bundle_buyer_count`` undercounts the true wallet count while the
tip-transfer + same-slot signals remain the robust detection core.

Design:

- ``TIP_ACCOUNTS`` — the 8 canonical Jito tip accounts (stable across
  releases). Re-confirmable at runtime via the ``getTipAccounts``
  JSON-RPC method; if the set ever changes, update this frozenset.
- ``find_tip_transfer`` — pure: returns the tip account when a tx
  contains a SOL transfer >= 1000 lamports to one of the tip accounts
  (checks both the outer instruction tree and CPI inner instructions).
- ``detect_bundle`` — pure: classifies one slot's transactions (in
  execution order) as a bundled launch when a token-create tx is
  followed by fresh-wallet buys AND the last tx carries the tip
  transfer.
- ``write_bundle`` — dependency-injected sink: inserts the event into
  ``wallet_memory.solana_bundles`` (table ensured idempotently) and
  publishes to NATS. Best-effort; never raises.

The parsed transaction shape follows ``parsing.py`` / ``traces/solana.py``:
``getTransaction`` with ``jsonParsed`` yields
``tx["transaction"]["message"]["instructions"]`` for outer instructions,
``tx["meta"]["innerInstructions"]`` for CPI instructions (each
``{"parsed": {"type": ..., "info": {...}}}``), and
``tx["meta"]["preTokenBalances"]`` / ``postTokenBalances`` for SPL
balance state (``owner``, ``mint``, ``uiTokenAmount.amount``). A wallet
is inferred *fresh* when its token account for the created mint has no
pre-state (account absent from ``preTokenBalances``) or a zero pre
amount — i.e. the account was created and funded within the buy tx
itself.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Jito Tip Accounts ─────────────────────────────────────────────────
# The 8 canonical tip accounts Jito bundles pay into. Stable across
# releases; re-confirm at runtime via the ``getTipAccounts`` RPC method
# (e.g. ``{"method": "getTipAccounts", "params": []}``) — if the set
# diverges, update this frozenset before trusting tip-based signals.
TIP_ACCOUNTS: frozenset[str] = frozenset(
    {
        "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
        "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
        "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
        "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
        "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
        "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
        "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
        "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
    }
)

# Minimum lamports for a SOL transfer to count as a bundle tip. Jito
# tips are at least a few hundred lamports but bundle tips are
# deliberately above dust; 1000 lamports filters out rent refunds and
# micro-donations while keeping real tips.
MIN_TIP_LAMPORTS = 1000

# SPL token program IDs (the ones that create mints in jsonParsed).
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
_TOKEN_PROGRAMS = frozenset({TOKEN_PROGRAM, TOKEN_2022_PROGRAM})

# Parsed instruction types that mark a token-create transaction.
_CREATE_TX_TYPES = frozenset({"createMint", "createMintTo", "initializeMint", "create"})


@dataclass(frozen=True)
class BundleEvent:
    """A detected Jito bundled token launch for one slot.

    Attributes:
        slot: Slot the run landed in.
        create_tx_signature: Signature of the token-create transaction.
        tip_tx_signature: Signature of the run's LAST transaction (the
            one carrying the tip transfer).
        tip_account: The Jito tip account that received the tip.
        buyer_count: Distinct fresh-wallet buyers detected after the
            create tx — a LOWER bound (bundlers spread buys across more
            wallets than we can see from token balance deltas alone).
        mint: The created token mint ("" when it cannot be extracted).
        buyer_wallets: Distinct fresh buyer wallet addresses (best-effort).
    """

    slot: int
    create_tx_signature: str
    tip_tx_signature: str
    tip_account: str
    buyer_count: int
    mint: str = ""
    buyer_wallets: list[str] = field(default_factory=list)


# ── Instruction helpers ───────────────────────────────────────────────


def _iter_instructions(tx: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten outer + CPI instructions of a parsed tx, in tree order.

    Mirrors ``traces.solana._iter_all_instructions``: outer instructions
    from ``message.instructions`` first, then every inner group from
    ``meta.innerInstructions``. Non-dict entries are skipped.
    """
    instructions: list[dict[str, Any]] = []
    message = (tx.get("transaction") or {}).get("message") or {}
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict):
            instructions.append(ix)
    meta = tx.get("meta") or {}
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        for ix in group.get("instructions") or []:
            if isinstance(ix, dict):
                instructions.append(ix)
    return instructions


def _parsed_info(ix: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Return ``(parsed_type, info_dict)`` for an instruction.

    Returns ``("", {})`` when the instruction has no parsed payload
    (raw-byte DEX router instructions etc.), matching the guard used in
    ``traces/solana.py``.
    """
    parsed = ix.get("parsed")
    if not isinstance(parsed, dict):
        return "", {}
    ptype = parsed.get("type")
    info = parsed.get("info")
    if not isinstance(info, dict):
        info = {}
    return str(ptype) if ptype is not None else "", info


def _tx_signature(tx: dict[str, Any]) -> str:
    """Return the transaction's signature (first in the signatures list)."""
    signatures = ((tx.get("transaction") or {}).get("signatures")) or []
    return str(signatures[0]) if signatures else ""


# ── Tip transfer detection ────────────────────────────────────────────


def find_tip_transfer(tx: dict[str, Any]) -> str | None:
    """Return the Jito tip account if this tx pays a bundle tip.

    A bundle tip is a SOL transfer of >= ``MIN_TIP_LAMPORTS`` lamports to
    one of the 8 canonical tip accounts (``TIP_ACCOUNTS``). Both the
    outer instruction tree and CPI inner instructions are scanned —
    bundler clients typically pay the tip through a system transfer
    nested in a CPI, so checking only top-level instructions would miss
    most tips.

    Args:
        tx: A parsed ``getTransaction`` (``jsonParsed``) result dict.

    Returns:
        The tip account address that received the tip, or None when this
        transaction does not pay a Jito tip.
    """
    for ix in _iter_instructions(tx):
        program_id = str(ix.get("programId") or "")
        ptype, info = _parsed_info(ix)
        if ptype not in ("transfer", "transferWithSeed"):
            continue
        if program_id not in ("11111111111111111111111111111111", "systemProgram"):
            continue
        destination = str(info.get("destination") or "")
        if destination not in TIP_ACCOUNTS:
            continue
        try:
            lamports = int(info.get("lamports") or 0)
        except (ValueError, TypeError):
            lamports = 0
        if lamports >= MIN_TIP_LAMPORTS:
            return destination
    return None


# ── Create / fresh-buyer helpers ──────────────────────────────────────


def _is_token_create_tx(tx: dict[str, Any]) -> bool:
    """True when the tx contains a token-program mint creation.

    Matches tokenProgram / token-2022 instructions of type
    ``createMint`` / ``createMintTo`` / ``initializeMint`` / ``create``
    in the outer tree or any CPI inner instruction — Pump.fun and
    LaunchLab both create mints via CPI, so inner instructions matter.
    """
    for ix in _iter_instructions(tx):
        if str(ix.get("programId") or "") not in _TOKEN_PROGRAMS:
            continue
        ptype, _ = _parsed_info(ix)
        if ptype in _CREATE_TX_TYPES:
            return True
    return False


def _created_mints(tx: dict[str, Any]) -> set[str]:
    """Extract mint addresses created by a token-create tx.

    Pulls ``info.mint`` from token-program create instructions (outer +
    CPI). Returns an empty set when the parsed payloads do not carry the
    mint (``jsonParsed`` usually does, but raw-byte CPI payloads are a
    known gap).
    """
    mints: set[str] = set()
    for ix in _iter_instructions(tx):
        if str(ix.get("programId") or "") not in _TOKEN_PROGRAMS:
            continue
        ptype, info = _parsed_info(ix)
        if ptype not in _CREATE_TX_TYPES:
            continue
        mint = str(info.get("mint") or "")
        if mint:
            mints.add(mint)
    return mints


def _fresh_buyers(tx: dict[str, Any], mint: str) -> set[str]:
    """Distinct fresh wallets that bought ``mint`` in this tx.

    A wallet counts as a *fresh buyer* when its token account for
    ``mint`` shows a positive balance change AND the account had no
    pre-state (absent from ``preTokenBalances``, or a pre amount of 0) —
    i.e. the account was created and funded within this very tx, the
    classic bundled block-0 buy pattern.

    Args:
        tx: Parsed ``getTransaction`` result.
        mint: Created mint to attribute buys to.

    Returns:
        Set of fresh buyer owner wallet addresses.
    """
    meta = tx.get("meta") or {}
    pre_balances = meta.get("preTokenBalances") or []
    post_balances = meta.get("postTokenBalances") or []

    pre_by_index: dict[int, int] = {}
    for bal in pre_balances:
        if not isinstance(bal, dict) or bal.get("mint") != mint:
            continue
        with contextlib.suppress(ValueError, TypeError):
            pre_by_index[int(bal.get("accountIndex") or 0)] = int((bal.get("uiTokenAmount") or {}).get("amount") or 0)

    buyers: set[str] = set()
    for bal in post_balances:
        if not isinstance(bal, dict) or bal.get("mint") != mint:
            continue
        owner = str(bal.get("owner") or "")
        if not owner:
            continue
        try:
            post_amount = int((bal.get("uiTokenAmount") or {}).get("amount") or 0)
        except (ValueError, TypeError):
            post_amount = 0
        pre_amount = pre_by_index.get(int(bal.get("accountIndex") or 0), 0)
        if post_amount > 0 and pre_amount == 0:
            buyers.add(owner)
    return buyers


# ── Detection ─────────────────────────────────────────────────────────


def detect_bundle(slot_txs: list[dict[str, Any]]) -> BundleEvent | None:
    """Detect a Jito bundled token launch among one slot's transactions.

    Classification rule (all must hold):

    1. A token-create tx exists in the slot (``_is_token_create_tx``).
    2. Transactions after the create tx contain buys from >= 1 distinct
       fresh wallet for the created mint (``_fresh_buyers``).
    3. The LAST transaction of the run pays a tip transfer to a Jito tip
       account (``find_tip_transfer``) — the tip rides in the final tx.

    ``slot_txs`` MUST be in execution order (oldest first); the last
    element is treated as the run's final transaction. This is a pure
    function — no I/O, no RPC.

    Args:
        slot_txs: All parsed transactions of one slot, in execution order.

    Returns:
        A :class:`BundleEvent` when the slot looks like a bundled launch,
        else None.
    """
    if len(slot_txs) < 2:
        return None

    create_index: int | None = None
    for idx, tx in enumerate(slot_txs):
        if _is_token_create_tx(tx):
            create_index = idx
            break
    if create_index is None:
        return None

    # Tip must be in the LAST transaction of the run.
    tip_tx = slot_txs[-1]
    tip_account = find_tip_transfer(tip_tx)
    if tip_account is None:
        return None

    # Mint(s) created by the create tx; attribute buys to them.
    created = _created_mints(slot_txs[create_index])
    if not created:
        # The create tx minted nothing extractable (raw-byte CPI, per the
        # _created_mints docstring). Guessing a mint would let anyone who lands
        # a Jito tip in the same slot as an unrelated token creation produce a
        # bundle row, so decline instead.
        return None

    buyer_wallets: set[str] = set()
    for tx in slot_txs[create_index + 1 :]:
        for mint in created:
            buyer_wallets |= _fresh_buyers(tx, mint)

    if not buyer_wallets:
        return None

    # getTransaction results carry ``slot`` at the top level of the
    # result object (``tx["slot"]``), matching the strategy's sig_info
    # slot. Prefer the create tx's own slot, fall back to any tx.
    slot = 0
    create_slot = slot_txs[create_index].get("slot")
    if create_slot is not None:
        try:
            slot = int(create_slot)
        except (ValueError, TypeError):
            slot = 0
    if slot == 0:
        for tx in slot_txs:
            raw_slot = tx.get("slot")
            if raw_slot is not None:
                try:
                    slot = int(raw_slot)
                except (ValueError, TypeError):
                    continue
                break

    mint = next(iter(created)) if created and "*" not in created else ""

    return BundleEvent(
        slot=slot,
        create_tx_signature=_tx_signature(slot_txs[create_index]),
        tip_tx_signature=_tx_signature(tip_tx),
        tip_account=tip_account,
        buyer_count=len(buyer_wallets),
        mint=mint,
        buyer_wallets=sorted(buyer_wallets),
    )


# ── ClickHouse + NATS sink ────────────────────────────────────────────

_BUNDLES_TABLE = "wallet_memory.solana_bundles"

_BUNDLES_DDL = f"""
CREATE TABLE IF NOT EXISTS {_BUNDLES_TABLE} (
    chain String DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    create_tx_signature String,
    tip_tx_signature String,
    tip_account String,
    mint String DEFAULT '',
    buyer_count UInt16,
    buyer_wallets Array(String) DEFAULT [],
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (slot, create_tx_signature)
TTL block_timestamp + INTERVAL 90 DAY
"""

_BUNDLES_INSERT_COLUMNS = (
    "chain",
    "slot",
    "block_timestamp",
    "create_tx_signature",
    "tip_tx_signature",
    "tip_account",
    "mint",
    "buyer_count",
    "buyer_wallets",
    "ingested_at",
)


def ensure_bundles_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.solana_bundles`` if it does not exist.

    Idempotent and best-effort, mirroring
    ``traces.solana.ensure_solana_transfers_table``: the table is also
    provisioned by ``migrations/009_jito_bundles.sql``; this exists so
    the table appears on first bundle write without operator DDL.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False on failure.
    """
    try:
        ch_client.execute(_BUNDLES_DDL)  # nosemgrep: formatted-sql-query — identifiers are constants
        logger.info("solana_bundles_table_ensured", table=_BUNDLES_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("solana_bundles_table_skip", error=str(exc)[:80])
        else:
            logger.warning("solana_bundles_table_failed", error=str(exc)[:100])
        return False


def _insert_bundle(ch_client: Any, bundle: BundleEvent) -> int:
    """Insert one bundle event into ClickHouse (best-effort)."""
    now = datetime.now(UTC)
    row = (
        "solana",
        bundle.slot,
        now,
        bundle.create_tx_signature,
        bundle.tip_tx_signature,
        bundle.tip_account,
        bundle.mint,
        bundle.buyer_count,
        bundle.buyer_wallets,
        now,
    )
    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"INSERT INTO {_BUNDLES_TABLE} ({', '.join(_BUNDLES_INSERT_COLUMNS)}) VALUES",
            [row],
        )
        logger.info(
            "solana_bundle_inserted",
            slot=bundle.slot,
            buyers=bundle.buyer_count,
            mint=bundle.mint[:16] or "",
        )
        return 1
    except Exception as exc:  # noqa: BLE001 — best-effort insert
        logger.error("solana_bundle_insert_failed", slot=bundle.slot, error=str(exc)[:150])
        return 0


async def write_bundle(bundle: BundleEvent, ch_writer: Any, nats: Any) -> None:
    """Persist a detected bundle to ClickHouse and publish it to NATS.

    Dependency-injected sinks: ``ch_writer`` exposes ``_connect()`` (the
    ``ClickHouseWriter``), ``nats`` an async ``publish_event(chain, type,
    data)`` (the ``NATSPublisher``). Both best-effort — a CH outage or a NATS
    circuit-open drops the event with a log line, never an exception.

    Emits to ``wallet_memory.solana_bundles`` and NATS subject
    ``chain.solana.bundle`` (type ``jito_bundle``).

    [REVIEW] ``jito_bundle`` is not yet in ``NATSPublisher.SUBJECTS``, so
    ``publish_event`` returns False (unknown type) and the bundle is only
    persisted to ClickHouse until that mapping is added.
    """
    connect = getattr(ch_writer, "_connect", None)
    if callable(connect):
        ch_client = connect()
        if ch_client is not None and ensure_bundles_table(ch_client):
            _insert_bundle(ch_client, bundle)

    publish = getattr(nats, "publish_event", None)
    if callable(publish):
        try:
            await publish(
                "solana",
                "jito_bundle",
                {
                    "slot": bundle.slot,
                    "create_tx_signature": bundle.create_tx_signature,
                    "tip_tx_signature": bundle.tip_tx_signature,
                    "tip_account": bundle.tip_account,
                    "buyer_count": bundle.buyer_count,
                    "mint": bundle.mint,
                    "buyer_wallets": bundle.buyer_wallets,
                },
            )
        except Exception as exc:  # noqa: BLE001 — publishing is best-effort
            logger.warning("solana_bundle_publish_failed", slot=bundle.slot, error=str(exc)[:120])


__all__ = [
    "BundleEvent",
    "TIP_ACCOUNTS",
    "MIN_TIP_LAMPORTS",
    "detect_bundle",
    "find_tip_transfer",
    "write_bundle",
    "ensure_bundles_table",
]
