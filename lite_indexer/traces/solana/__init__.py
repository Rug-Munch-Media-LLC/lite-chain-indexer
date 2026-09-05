"""Solana trace-level data collection — full transaction trace visibility.

The base Solana strategy indexes SPL token balance changes from
``getSignaturesForAddress`` + ``getTransaction`` (jsonParsed). That view
misses most of what actually moves through a transaction: native SOL
transfers, CPI inner instructions, token account create/close events and
stake delegations. Paid products (Helius enhanced transactions, DAS APIs)
charge for this visibility — this package replicates the core capability
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

Sub-modules (split from the former 939-line ``solana.py``):

  * :mod:`.helpers`   — shared constants + small coercion helpers
  * :mod:`.parsers`   — trace parsing (``extract_solana_transfers``)
  * :mod:`.spl`       — SPL token transfer extraction
  * :mod:`.dex`       — DEX program names + transfer-graph aggregation
  * :mod:`.collector` — ``SolanaTraceCollector`` (RPC fetch)
  * :mod:`.storage`   — ClickHouse ensure/write
"""

from __future__ import annotations

from .collector import SolanaTraceCollector
from .dex import build_transfer_graph_from_rows
from .helpers import (
    ACCOUNT_CLOSE,
    ACCOUNT_CREATE,
    ASSOCIATED_TOKEN_PROGRAM,
    COMPUTE_BUDGET_PROGRAM,
    CPI_SOL,
    NATIVE_SOL,
    SOLANA_TRANSFERS_TABLE,
    SPL_TRANSFER,
    SPL_TRANSFER_CHECKED,
    STAKE_DELEGATE,
    STAKE_PROGRAM,
    SYSTEM_PROGRAM,
    TOKEN_2022_PROGRAM,
    TOKEN_PROGRAM,
    TRANSFER_TYPES,
    VOTE_PROGRAM,
)
from .parsers import extract_solana_transfers
from .storage import ensure_solana_transfers_table, write_solana_transfers

__all__ = [
    "ACCOUNT_CLOSE",
    "ACCOUNT_CREATE",
    "ASSOCIATED_TOKEN_PROGRAM",
    "COMPUTE_BUDGET_PROGRAM",
    "CPI_SOL",
    "NATIVE_SOL",
    "SOLANA_TRANSFERS_TABLE",
    "SPL_TRANSFER",
    "SPL_TRANSFER_CHECKED",
    "STAKE_DELEGATE",
    "STAKE_PROGRAM",
    "SYSTEM_PROGRAM",
    "TOKEN_2022_PROGRAM",
    "TOKEN_PROGRAM",
    "TRANSFER_TYPES",
    "VOTE_PROGRAM",
    "SolanaTraceCollector",
    "build_transfer_graph_from_rows",
    "ensure_solana_transfers_table",
    "extract_solana_transfers",
    "write_solana_transfers",
]
