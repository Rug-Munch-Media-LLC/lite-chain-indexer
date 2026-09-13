-- Migration 004: Solana trace-level transfer table.
--
-- The Solana strategy only tracks SPL token balance changes via
-- getSignaturesForAddress + getTransaction. This table stores the full
-- transaction trace visibility the same getTransaction (jsonParsed) call
-- already returns for free: native SOL transfers, CPI inner instructions,
-- token account create/close events and stake delegations — the core of
-- what paid products (enhanced-transaction and DAS APIs) charge for.
--
-- One row per value movement. instruction_path encodes where in the
-- instruction tree the movement happened:
--   [outer_ix]          top-level instruction
--   [outer_ix, inner_ix]  CPI instruction (free RPCs expose at most two
--                        nesting levels; deeper enhanced-style paths like
--                        [0,2,1] need a paid DAS feed)
--
-- transfer_type values:
--   native_sol            top-level systemProgram transfer/transferWithSeed
--   cpi_sol               SOL moved through a CPI (inner system transfer)
--   spl_transfer          tokenProgram / token-2022 transfer
--   spl_transfer_checked  transferChecked (carries mint + decimals)
--   account_create        token account creation (rent SOL moved to it)
--   account_close         token account close (residual SOL returned)
--   stake_delegate        stake delegation (amount requires account data)
--
-- amount: lamports for SOL rows (native_sol, cpi_sol, account_create,
-- account_close), raw token units for SPL rows; decimals distinguishes.
--
-- Also ensured idempotently at runtime by
-- lite_indexer.traces.solana.ensure_solana_transfers_table, so this file
-- is for operators provisioning the schema ahead of a deploy.

CREATE TABLE IF NOT EXISTS wallet_memory.solana_transfers (
    chain String DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    signature String,
    instruction_path Array(UInt16),  -- e.g. [0,2,1] = outer ix 0, inner ix 2, inner ix 1
    from_address String,
    to_address String,
    amount UInt256,  -- in lamports for SOL, raw token amount for SPL
    mint String DEFAULT '',  -- SPL mint address, empty for SOL
    decimals UInt8 DEFAULT 9,  -- 9 for SOL, varies for SPL
    program_id String,  -- systemProgram, tokenProgram, associatedToken, computeBudget, etc.
    transfer_type LowCardinality(String),  -- native_sol, spl_transfer, spl_transfer_checked, cpi_sol, account_create, account_close, stake_delegate
    fee_payer String,
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (signature, instruction_path)
TTL block_timestamp + INTERVAL 90 DAY;
