"""
Solana trace-level data collection — full transaction trace visibility.

The base Solana strategy indexes SPL token balance changes from
``getSignaturesForAddress`` + ``getTransaction`` (jsonParsed). That view
misses most of what actually moves through a transaction: native SOL
transfers, CPI inner instructions, token account create/close events and
stake delegations. Paid products (Helius enhanced transactions, DAS APIs)
charge for this visibility — this module replicates the core capability
for free using the exact same RPC call the indexer already makes.

What ``getTransaction`` with ``maxSupportedTransactionVersion=0`` +
``encoding=jsonParsed`` gives us at zero extra RPC cost:

  * outer instruction tree, parsed per program (system / token / stake …)
  * ``meta.innerInstructions`` — the CPI call tree (with ``stackHeight``)
  * ``preTokenBalances`` / ``postTokenBalances`` — account-level deltas
  * ``preBalances`` / ``postBalances`` — lamport-level deltas
  * ``slot`` / ``blockTime`` / ``version`` — transaction context
  * ``meta.fee`` + ``accountKeys[0]`` — the fee payer

Every value movement is emitted as one row for
``wallet_memory.solana_transfers`` (schema in
``migrations/004_solana_transfers.sql``, also ensured idempotently at
runtime by :func:`ensure_solana_transfers_table`):

  * ``native_sol``     — top-level systemProgram transfer / transferWithSeed
  * ``cpi_sol``        — SOL moved through a CPI (inner system transfer)
  * ``spl_transfer``   — tokenProgram / token-2022 transfer
  * ``spl_transfer_checked`` — transferChecked (carries mint + decimals)
  * ``account_create`` — token account creation (rent SOL moved to it)
  * ``account_close``  — token account close (residual SOL returned)
  * ``stake_delegate`` — stake delegation (amount requires account data)

``instruction_path`` is ``[outer_ix]`` for top-level instructions and
``[outer_ix, inner_ix]`` for CPI instructions — free RPCs expose at most
two nesting levels; deeper paths (Helius-style ``[0, 2, 1]``) require a
paid DAS feed, so we emit the two-level path and document the limit.

Verified free endpoints for ``getTransaction`` (2026-08-04):

  * ``api.mainnet-beta.solana.com``      — OK (aggressive 429s, retry)
  * ``solana-rpc.publicnode.com``        — OK
  * ``solana-api.projectserum.com``      — DEAD (connection timeout)
  * ``rpc.ankr.com/solana``              — 403 "API key is not allowed"
  * ``solana.drpc.org``                  — 35 "chain is not available on
                                           free plan, please upgrade"

The collector reuses the verified pool from
:mod:`lite_indexer.chains.non_evm` (the two working endpoints above).

Usage::

    collector = SolanaTraceCollector()
    tx = await collector.collect_transaction_trace(signature)
    rows = await collector.extract_transfers(tx)
    graph = await collector.build_transfer_graph(signature)
    ensure_solana_transfers_table(ch_client)
    write_solana_transfers(ch_client, rows)
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from typing import Any

import httpx

from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# ClickHouse table for Solana transfer traces. Override with
# CH_SOLANA_TRANSFERS_TABLE for operators who prefer a different database.
SOLANA_TRANSFERS_TABLE: str = os.environ.get("CH_SOLANA_TRANSFERS_TABLE", "wallet_memory.solana_transfers")

# ── Program IDs ────────────────────────────────────────────────────────

SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
STAKE_PROGRAM = "Stake11111111111111111111111111111111111111"
COMPUTE_BUDGET_PROGRAM = "ComputeBudget111111111111111111111111111111"
VOTE_PROGRAM = "Vote111111111111111111111111111111111111111"

# Short display names for well-known programs; unknown programs keep their
# raw pubkey in the ``program_id`` column.
_PROGRAM_NAMES: dict[str, str] = {
    SYSTEM_PROGRAM: "systemProgram",
    TOKEN_PROGRAM: "tokenProgram",
    TOKEN_2022_PROGRAM: "token2022Program",
    ASSOCIATED_TOKEN_PROGRAM: "associatedToken",
    STAKE_PROGRAM: "stakeProgram",
    COMPUTE_BUDGET_PROGRAM: "computeBudget",
    VOTE_PROGRAM: "voteProgram",
    # Popular DEX/router programs, resolved for readability.
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "pumpfunAmm",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wF2t2j8": "raydium",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pumpfun",
}

# Transfer types stored in the ``transfer_type`` column (LowCardinality).
NATIVE_SOL = "native_sol"
CPI_SOL = "cpi_sol"
SPL_TRANSFER = "spl_transfer"
SPL_TRANSFER_CHECKED = "spl_transfer_checked"
ACCOUNT_CREATE = "account_create"
ACCOUNT_CLOSE = "account_close"
STAKE_DELEGATE = "stake_delegate"

TRANSFER_TYPES: tuple[str, ...] = (
    NATIVE_SOL,
    CPI_SOL,
    SPL_TRANSFER,
    SPL_TRANSFER_CHECKED,
    ACCOUNT_CREATE,
    ACCOUNT_CLOSE,
    STAKE_DELEGATE,
)


# ── Small helpers ──────────────────────────────────────────────────────


def _to_int(value: Any, default: int = 0) -> int:
    """Safely coerce a JSON amount (str/int) into an int.

    Args:
        value: Raw amount from a parsed instruction (usually a string).
        default: Value returned when the coercion fails.

    Returns:
        Parsed int, or ``default``.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value))
    except (ValueError, TypeError):
        return default


def _to_timestamp(block_time: Any) -> datetime | None:
    """Coerce a Solana blockTime into a UTC datetime.

    Args:
        block_time: Unix seconds (int/float) or None.

    Returns:
        Timezone-aware UTC datetime, or None.
    """
    try:
        return datetime.fromtimestamp(int(block_time), tz=UTC)
    except (ValueError, TypeError, OverflowError):
        return None


def _account_pubkey(account_keys: list[Any], account_index: int) -> str:
    """Resolve an account index to a pubkey string.

    jsonParsed encoding renders each account key as a dict with a
    ``pubkey`` field; older/compact encodings render plain strings.

    Args:
        account_keys: The transaction's ``message.accountKeys`` list.
        account_index: Index into the list.

    Returns:
        The pubkey string, or '' when out of range.
    """
    try:
        key = account_keys[account_index]
    except (IndexError, TypeError):
        return ""
    if isinstance(key, dict):
        pubkey = key.get("pubkey")
        return str(pubkey) if pubkey is not None else ""
    return str(key)


def _balance_delta(pre_balances: list[Any], post_balances: list[Any], pubkey: str, account_keys: list[Any]) -> int:
    """Compute an account's lamport change from pre/post balances.

    Used for ``account_close`` rows where the parsed instruction does not
    carry the residual lamport amount (only the account + destination).

    Args:
        pre_balances: ``meta.preBalances`` lamport list.
        post_balances: ``meta.postBalances`` lamport list.
        pubkey: Account whose balance delta is wanted.
        account_keys: The transaction's account key list (to resolve index).

    Returns:
        Absolute lamport delta (0 when the account is not in the tx).
    """
    idx = -1
    for i, _key in enumerate(account_keys):
        if _account_pubkey(account_keys, i) == pubkey:
            idx = i
            break
    if idx < 0 or idx >= len(pre_balances) or idx >= len(post_balances):
        return 0
    try:
        delta = int(post_balances[idx]) - int(pre_balances[idx])
    except (ValueError, TypeError, IndexError):
        return 0
    return abs(delta)


def _known_decimals(mint: str) -> int:
    """Return known decimals for a tracked mint, or 0.

    Looked up lazily from the strategy's tracked-token list to avoid a
    module-level import cycle (strategy imports this module).

    Args:
        mint: SPL mint address.

    Returns:
        Decimals for known mints, else 0.
    """
    try:
        from lite_indexer.strategies.solana.models import SOLANA_TRACKED_TOKENS  # noqa: PLC0415
    except ImportError:
        return 0
    for token in SOLANA_TRACKED_TOKENS:
        if token.get("mint") == mint:
            return _to_int(token.get("decimals"), 0)
    return 0


def _program_name(program_id: str) -> str:
    """Resolve a program pubkey to a friendly display name.

    Args:
        program_id: Program pubkey string.

    Returns:
        The friendly name, or the raw pubkey for unknown programs.
    """
    return _PROGRAM_NAMES.get(program_id, program_id)


def _token_amount(parsed_info: dict[str, Any]) -> tuple[str, int]:
    """Extract (raw_amount, decimals) from a parsed token instruction.

    jsonParsed token instructions carry ``tokenAmount`` (with
    ``amount`` + ``decimals``) for checked transfers and most transfers;
    plain ``transfer`` may only carry ``amount``.

    Args:
        parsed_info: The instruction's ``parsed.info`` dict.

    Returns:
        Tuple of (raw amount string, decimals int).
    """
    token_amount = parsed_info.get("tokenAmount")
    if isinstance(token_amount, dict):
        amount = str(token_amount.get("amount") or parsed_info.get("amount") or "0")
        decimals = _to_int(token_amount.get("decimals"), 0)
        return amount, decimals
    return str(parsed_info.get("amount") or "0"), 0


# ── Core extraction (pure, sync) ───────────────────────────────────────


def extract_solana_transfers(tx_data: dict[str, Any] | None, slot: int | None = None) -> list[dict[str, Any]]:
    """Extract ALL value transfers from a parsed Solana transaction.

    Scans the outer instruction tree and ``meta.innerInstructions`` (CPI
    tree) for value movements:

      * native SOL transfers (``systemProgram.transfer``)
      * SPL Token transfers (``tokenProgram.transfer`` / ``transferChecked``,
        including token-2022)
      * SOL moved through CPIs (inner systemProgram transfers)
      * token account creation / closing
      * stake delegations

    Rows follow the ``wallet_memory.solana_transfers`` schema: ``chain,
    slot, block_timestamp, signature, instruction_path, from_address,
    to_address, amount, mint, decimals, program_id, transfer_type,
    fee_payer``.

    Args:
        tx_data: Parsed ``getTransaction`` result (jsonParsed encoding).
        slot: Slot number (defaults to ``tx_data["slot"]`` when omitted).

    Returns:
        List of standardized transfer-trace row dicts (may be empty).
    """
    if not isinstance(tx_data, dict):
        return []

    meta = tx_data.get("meta")
    if not isinstance(meta, dict):
        return []
    transaction = tx_data.get("transaction")
    if not isinstance(transaction, dict):
        return []
    message = transaction.get("message")
    if not isinstance(message, dict):
        return []

    account_keys = message.get("accountKeys") or []
    signatures = transaction.get("signatures") or []
    signature = str(signatures[0]) if signatures else ""
    fee_payer = _account_pubkey(account_keys, 0)
    block_ts = _to_timestamp(tx_data.get("blockTime"))
    tx_slot = int(slot) if slot is not None else _to_int(tx_data.get("slot"), 0)

    pre_balances = meta.get("preBalances") or []
    post_balances = meta.get("postBalances") or []

    rows: list[dict[str, Any]] = []

    # First pass: find token accounts created inside this transaction
    # (systemProgram.createAccount / createAccountWithSeed, top-level or
    # CPI) and the lamports funded into them. Used to value account_close
    # rows when the account is created and closed in the same tx — the
    # net pre/post balance delta is 0 (money in, money out) but the rent
    # returned on close is a real SOL movement.
    created_lamports: dict[str, int] = {}
    for ix in _iter_all_instructions(message, meta):
        program_id = str(ix.get("programId") or "")
        parsed = ix.get("parsed")
        if program_id != SYSTEM_PROGRAM or not isinstance(parsed, dict):
            continue
        parsed_info = parsed.get("info")
        if not isinstance(parsed_info, dict):
            continue
        if parsed.get("type") in ("createAccount", "createAccountWithSeed"):
            lamports = _to_int(parsed_info.get("lamports"), 0)
            new_account = str(parsed_info.get("newAccount") or "")
            if lamports > 0 and new_account:
                created_lamports[new_account] = lamports

    # ── Outer instructions ────────────────────────────────────────────
    for outer_index, ix in enumerate(message.get("instructions") or []):
        if not isinstance(ix, dict):
            continue
        rows.extend(
            _extract_instruction_rows(
                ix,
                instruction_path=[outer_index],
                inner=False,
                account_keys=account_keys,
                pre_balances=pre_balances,
                post_balances=post_balances,
                created_lamports=created_lamports,
                chain="solana",
                slot=tx_slot,
                block_timestamp=block_ts,
                signature=signature,
                fee_payer=fee_payer,
            )
        )

    # ── CPI inner instructions ────────────────────────────────────────
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        outer_index = _to_int(group.get("index"), 0)
        for inner_index, ix in enumerate(group.get("instructions") or []):
            if not isinstance(ix, dict):
                continue
            rows.extend(
                _extract_instruction_rows(
                    ix,
                    instruction_path=[outer_index, inner_index],
                    inner=True,
                    account_keys=account_keys,
                    pre_balances=pre_balances,
                    post_balances=post_balances,
                    created_lamports=created_lamports,
                    chain="solana",
                    slot=tx_slot,
                    block_timestamp=block_ts,
                    signature=signature,
                    fee_payer=fee_payer,
                )
            )

    metrics_incr("solana_trace_rows_extracted", len(rows))
    return rows


def _iter_all_instructions(message: dict[str, Any], meta: dict[str, Any]) -> list[dict[str, Any]]:
    """Yield every instruction in a transaction (outer + CPI, flat).

    Args:
        message: The transaction's ``message`` dict.
        meta: The transaction's ``meta`` dict.

    Returns:
        Flat list of instruction dicts in tree order.
    """
    instructions: list[dict[str, Any]] = []
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict):
            instructions.append(ix)
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        for ix in group.get("instructions") or []:
            if isinstance(ix, dict):
                instructions.append(ix)
    return instructions


def _extract_instruction_rows(
    ix: dict[str, Any],
    *,
    instruction_path: list[int],
    inner: bool,
    account_keys: list[Any],
    pre_balances: list[Any],
    post_balances: list[Any],
    created_lamports: dict[str, int],
    chain: str,
    slot: int,
    block_timestamp: datetime | None,
    signature: str,
    fee_payer: str,
) -> list[dict[str, Any]]:
    """Emit transfer rows for one instruction (outer or CPI).

    Instructions without ``parsed`` data (raw byte payloads from DEX
    routers etc.) carry no structured transfer info and are skipped.

    Args:
        ix: Instruction dict from the parsed transaction.
        instruction_path: Path in the instruction tree ([outer] or
            [outer, inner]).
        inner: True for CPI instructions (affects SOL transfer typing).
        account_keys: Transaction account key list.
        pre_balances: ``meta.preBalances`` (for close-account amounts).
        post_balances: ``meta.postBalances``.
        created_lamports: Account -> lamports funded in this tx (from the
            systemProgram createAccount pass).
        chain: Chain name ('solana').
        slot: Transaction slot.
        block_timestamp: Transaction timestamp.
        signature: Transaction signature.
        fee_payer: Transaction fee payer (accountKeys[0]).

    Returns:
        Transfer rows for this instruction (0-2 usually, empty when the
        instruction is not a value movement).
    """
    program_id = str(ix.get("programId") or "")
    parsed = ix.get("parsed")
    if not isinstance(parsed, dict):
        return []
    ptype = parsed.get("type")
    parsed_info = parsed.get("info")
    if not isinstance(parsed_info, dict):
        parsed_info = {}

    def make_row(
        *,
        transfer_type: str,
        from_address: str,
        to_address: str,
        amount: int,
        mint: str = "",
        decimals: int = 9,
    ) -> dict[str, Any]:
        return {
            "chain": chain,
            "slot": slot,
            "block_timestamp": block_timestamp,
            "signature": signature,
            "instruction_path": list(instruction_path),
            "from_address": from_address,
            "to_address": to_address,
            "amount": amount,
            "mint": mint,
            "decimals": decimals,
            "program_id": _program_name(program_id),
            "transfer_type": transfer_type,
            "fee_payer": fee_payer,
        }

    # ── System program: native SOL movements ─────────────────────────
    if program_id == SYSTEM_PROGRAM:
        if ptype in ("transfer", "transferWithSeed"):
            amount = _to_int(parsed_info.get("lamports"), 0)
            if amount <= 0:
                return []
            return [
                make_row(
                    transfer_type=CPI_SOL if inner else NATIVE_SOL,
                    from_address=str(parsed_info.get("source") or ""),
                    to_address=str(parsed_info.get("destination") or ""),
                    amount=amount,
                )
            ]
        if ptype in ("createAccount", "createAccountWithSeed"):
            amount = _to_int(parsed_info.get("lamports"), 0)
            if amount <= 0:
                return []
            return [
                make_row(
                    transfer_type=ACCOUNT_CREATE,
                    from_address=str(parsed_info.get("source") or ""),
                    to_address=str(parsed_info.get("newAccount") or ""),
                    amount=amount,
                )
            ]
        return []

    # ── SPL token programs: token transfers + account state changes ──
    if program_id in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        if ptype == "transfer":
            amount_str, decimals = _token_amount(parsed_info)
            amount = _to_int(amount_str, 0)
            if amount <= 0:
                return []
            mint = str(parsed_info.get("mint") or "")
            return [
                make_row(
                    transfer_type=SPL_TRANSFER,
                    from_address=str(parsed_info.get("source") or ""),
                    to_address=str(parsed_info.get("destination") or ""),
                    amount=amount,
                    mint=mint,
                    decimals=decimals or _known_decimals(mint),
                )
            ]
        if ptype == "transferChecked":
            amount_str, decimals = _token_amount(parsed_info)
            amount = _to_int(amount_str, 0)
            if amount <= 0:
                return []
            mint = str(parsed_info.get("mint") or "")
            return [
                make_row(
                    transfer_type=SPL_TRANSFER_CHECKED,
                    from_address=str(parsed_info.get("source") or ""),
                    to_address=str(parsed_info.get("destination") or ""),
                    amount=amount,
                    mint=mint,
                    decimals=decimals or _known_decimals(mint),
                )
            ]
        if ptype == "closeAccount":
            account = str(parsed_info.get("account") or "")
            destination = str(parsed_info.get("destination") or "")
            amount = _balance_delta(pre_balances, post_balances, account, account_keys)
            if amount <= 0:
                # Account created AND closed in the same tx: the net
                # balance delta is 0, but the rent funded at creation is
                # returned on close — use the same-tx creation amount.
                amount = created_lamports.get(account, 0)
            return [
                make_row(
                    transfer_type=ACCOUNT_CLOSE,
                    from_address=account,
                    to_address=destination,
                    amount=amount,
                )
            ]
        # initializeAccount / initializeAccount3 / initializeImmutableOwner
        # etc. are account state changes — the rent SOL movement is
        # already captured by the systemProgram createAccount CPI.
        return []

    # ── Stake program: delegations ───────────────────────────────────
    if program_id == STAKE_PROGRAM and ptype == "delegate":
        return [
            make_row(
                transfer_type=STAKE_DELEGATE,
                from_address=str(parsed_info.get("stakeAccount") or parsed_info.get("stakePubkey") or ""),
                to_address=str(parsed_info.get("votePubkey") or ""),
                amount=0,
            )
        ]

    return []


# ── Collector ──────────────────────────────────────────────────────────


class SolanaTraceCollector:
    """Collect detailed Solana transaction traces from free RPCs.

    Wraps ``getTransaction`` (``jsonParsed`` +
    ``maxSupportedTransactionVersion=0``) with endpoint rotation and 429
    backoff, mirroring the Solana strategy's RPC client so the same pool
    of verified free endpoints is shared. The extraction itself is pure —
    the strategy feeds the transaction it already fetched into
    :meth:`extract_transfers` with no extra RPC call.
    """

    def __init__(
        self,
        endpoints: list[str] | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 30.0,
        concurrency: int = 10,
    ) -> None:
        """Initialize the trace collector.

        Args:
            endpoints: RPC endpoints to rotate through. Defaults to the
                verified pool from :mod:`lite_indexer.chains.non_evm`
                (api.mainnet-beta.solana.com + solana-rpc.publicnode.com).
            client: Optional shared httpx AsyncClient (created lazily).
            timeout: Per-call timeout.
            concurrency: Max parallel getTransaction calls for
                ``build_transfer_graph`` (not used by single-call methods).
        """
        self.endpoints = list(endpoints) if endpoints else list(SOLANA_ENDPOINTS)
        self._client = client
        self.timeout = timeout
        self.concurrency = max(1, concurrency)

    # ── Public API ───────────────────────────────────────────────────

    async def collect_transaction_trace(self, signature: str) -> dict[str, Any] | None:
        """Fetch a full transaction trace via getTransaction.

        Uses ``encoding=jsonParsed`` + ``maxSupportedTransactionVersion=0``
        so the response includes parsed outer instructions AND
        ``meta.innerInstructions`` (the CPI tree) for version-0 txs.

        Args:
            signature: Transaction signature (base58).

        Returns:
            The parsed transaction dict (``result`` member of the RPC
            response), or None when every endpoint failed.
        """
        result = await self._rpc_call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "finalized",
                },
            ],
        )
        if not isinstance(result, dict) or not result:
            return None
        return result

    async def extract_transfers(self, parsed_tx: dict[str, Any], slot: int | None = None) -> list[dict[str, Any]]:
        """Extract ALL value transfers from an already-fetched transaction.

        Pure parsing — no RPC calls. The strategy calls this with the
        ``getTransaction`` result it already fetched for SPL balance
        changes, so full trace visibility costs zero extra requests.

        Args:
            parsed_tx: Parsed ``getTransaction`` result (jsonParsed).
            slot: Slot number (defaults to ``parsed_tx["slot"]``).

        Returns:
            List of standardized transfer-trace row dicts.
        """
        return extract_solana_transfers(parsed_tx, slot)

    async def build_transfer_graph(self, signature: str) -> dict[str, Any]:
        """Build a directed graph of all value movement in a transaction.

        Aggregates transfer rows by (from, to, mint, transfer_type),
        summing amounts so parallel legs of a swap collapse into one edge.

        Args:
            signature: Transaction signature (base58).

        Returns:
            Graph dict: ``{signature, chain, slot, block_timestamp,
            transfers, nodes, edges}``, or ``{signature, error}`` when the
            transaction could not be fetched.
        """
        tx_data = await self.collect_transaction_trace(signature)
        if tx_data is None:
            return {"signature": signature, "error": "transaction_fetch_failed"}

        rows = extract_solana_transfers(tx_data)

        edges: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for row in rows:
            key = (row["from_address"], row["to_address"], row["mint"], row["transfer_type"])
            edge = edges.setdefault(
                key,
                {
                    "from": row["from_address"],
                    "to": row["to_address"],
                    "token": row["mint"],
                    "transfer_type": row["transfer_type"],
                    "amount": 0,
                    "decimals": row["decimals"],
                    "instruction_paths": [],
                },
            )
            edge["amount"] += int(row["amount"])
            edge["instruction_paths"].append([int(i) for i in row["instruction_path"]])

        nodes: list[str] = []
        seen: set[str] = set()
        for edge in edges.values():
            for addr in (edge["from"], edge["to"]):
                if addr and addr not in seen:
                    seen.add(addr)
                    nodes.append(addr)

        return {
            "signature": signature,
            "chain": "solana",
            "slot": _to_int(tx_data.get("slot"), 0),
            "block_timestamp": _to_timestamp(tx_data.get("blockTime")),
            "transfers": len(rows),
            "nodes": sorted(nodes),
            "edges": list(edges.values()),
        }

    # ── Internals ───────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the shared httpx client (created lazily)."""
        if self._client is None or self._client.is_closed:
            limits = httpx.Limits(
                max_connections=50,
                max_keepalive_connections=10,
                keepalive_expiry=30,
            )
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(self.timeout), limits=limits)
        return self._client

    async def _rpc_call(self, method: str, params: list[Any] | None = None) -> Any | None:
        """Call one JSON-RPC method, rotating endpoints with 429 backoff.

        Mirrors the Solana strategy's ``rpc_call``: tries each endpoint in
        the pool, backs off briefly on 429, and returns the first
        successful ``result`` (a dict for getTransaction, a list for
        getSignaturesForAddress, etc.). Returns None when all endpoints
        failed.

        Args:
            method: Solana RPC method name.
            params: Method parameters.

        Returns:
            The response ``result`` member (Any), or None on failure.
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        client = await self._get_client()
        last_error: str | None = None
        for url in self.endpoints:
            try:
                resp = await client.post(url, json=payload, timeout=self.timeout)
                if resp.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                if resp.status_code == 200:
                    data = resp.json()
                    result = data.get("result") if isinstance(data, dict) else None
                    if result is not None:
                        return result
                    if isinstance(data, dict) and "error" in data:
                        last_error = str(data["error"])
                        continue
                last_error = f"http_{resp.status_code}"
            except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPError, ValueError) as exc:
                last_error = str(exc)[:60]
                continue
        logger.debug("solana_trace_rpc_failed", method=method, error=last_error)
        return None


# ── ClickHouse storage ────────────────────────────────────────────────

_SOLANA_TRACE_DDL = f"""
CREATE TABLE IF NOT EXISTS {SOLANA_TRANSFERS_TABLE} (
    chain String DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    signature String,
    instruction_path Array(UInt16),
    from_address String,
    to_address String,
    amount UInt256,
    mint String DEFAULT '',
    decimals UInt8 DEFAULT 9,
    program_id String,
    transfer_type LowCardinality(String),
    fee_payer String,
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (signature, instruction_path)
TTL block_timestamp + INTERVAL 90 DAY
"""

_SOLANA_TRACE_INSERT_COLUMNS = (
    "chain",
    "slot",
    "block_timestamp",
    "signature",
    "instruction_path",
    "from_address",
    "to_address",
    "amount",
    "mint",
    "decimals",
    "program_id",
    "transfer_type",
    "fee_payer",
    "ingested_at",
)


def ensure_solana_transfers_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.solana_transfers`` if it does not exist.

    Idempotent and best-effort, mirroring the EVM trace store: the table
    is also provisioned by ``migrations/004_solana_transfers.sql``; this
    exists so the table appears on first trace write without operator DDL.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False on failure.
    """
    try:
        ch_client.execute(_SOLANA_TRACE_DDL)
        logger.info("solana_transfers_table_ensured", table=SOLANA_TRANSFERS_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("solana_transfers_table_skip", error=str(exc)[:80])
        else:
            logger.warning("solana_transfers_table_failed", error=str(exc)[:100])
        return False


def write_solana_transfers(ch_client: Any, rows: list[dict[str, Any]]) -> int:
    """Insert Solana transfer-trace rows into ClickHouse.

    Best-effort: returns 0 on failure without raising. Rows follow the
    schema produced by :func:`extract_solana_transfers`.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        rows: Standardized transfer-trace row dicts.

    Returns:
        Number of rows inserted (0 on failure/unavailable).
    """
    if not rows:
        return 0

    now = datetime.now(UTC)
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        ts = row.get("block_timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                ts = now
        if ts is None:
            ts = now
        prepared.append(
            (
                row.get("chain") or "solana",
                int(row.get("slot") or 0),
                ts,
                row.get("signature") or "",
                [int(i) for i in (row.get("instruction_path") or [])],
                row.get("from_address") or "",
                row.get("to_address") or "",
                int(row.get("amount") or 0),
                row.get("mint") or "",
                int(row.get("decimals") or 0),
                row.get("program_id") or "",
                row.get("transfer_type") or "",
                row.get("fee_payer") or "",
                now,
            )
        )

    try:
        ch_client.execute(
            f"INSERT INTO {SOLANA_TRANSFERS_TABLE} ({', '.join(_SOLANA_TRACE_INSERT_COLUMNS)}) VALUES",
            prepared,
        )
        logger.info("solana_transfers_inserted", rows=len(prepared))
        metrics_incr("solana_transfers_written", len(prepared))
        return len(prepared)
    except Exception as exc:  # noqa: BLE001 — best-effort insert
        metrics_incr("clickhouse_errors")
        logger.error(
            "solana_transfers_insert_failed",
            rows=len(prepared),
            error=str(exc)[:150],
        )
        return 0


__all__ = [
    "SYSTEM_PROGRAM",
    "TOKEN_PROGRAM",
    "TOKEN_2022_PROGRAM",
    "STAKE_PROGRAM",
    "TRANSFER_TYPES",
    "NATIVE_SOL",
    "CPI_SOL",
    "SPL_TRANSFER",
    "SPL_TRANSFER_CHECKED",
    "ACCOUNT_CREATE",
    "ACCOUNT_CLOSE",
    "STAKE_DELEGATE",
    "SOLANA_TRANSFERS_TABLE",
    "SolanaTraceCollector",
    "extract_solana_transfers",
    "ensure_solana_transfers_table",
    "write_solana_transfers",
]
