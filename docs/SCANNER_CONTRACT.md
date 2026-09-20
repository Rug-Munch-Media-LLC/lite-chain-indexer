# SCANNER_CONTRACT.md — canonical indexer ↔ scanner table contract

Status: canonical (cutover batch 1, 2026-09-20). All schemas verified live via
`DESCRIBE TABLE wallet_memory.<table>` on Talos (rmi-clickhouse) on 2026-09-20.
Cross-refs: `rugmunch-scanner/audit/standalone-audit-2026-09-20.md` (§4, §7),
`rmi-backend/audit/scanner-coupling-2026-09-20.md` (§2 shared stores).

The scanner reads ClickHouse tables in the `wallet_memory` database written by
lite-chain-indexer. This document is the contract: column names, types,
semantics, refresh owner, and known gaps. Scanner code MUST match these names
exactly — no aliasing, no assumed columns.

## Global semantics

- **String-encoded numerics**: `delta`, `pre_amount`, `post_amount`, `value`,
  `amount0/1_in/out`, `amount0/1` are **String** (raw on-chain values, no
  decimal scaling). Parse with `Decimal` + token `decimals` from `token_meta`
  — never `float()` directly.
- **`UInt256`** (`solana_transfers.amount`): arbitrary-precision; read as
  string via client, not int.
- **`ingested_at`**: writer-side insert time (indexer-owned). Use
  `block_timestamp` / `slot` for chain-time logic.
- **TTL**: `evm_transfers` token_symbol/name expire at 30d;
  `dex_events` detail columns expire at 90d; `deployer_events` rows expire at
  90d (TTL ingested_at). Scanner must tolerate NULL/missing enrichment.
- **Dedup**: `evm_transfers` carries `tx_log_key` (dedup key, migration 018);
  `holder_balances` is ReplacingMergeTree-style via `version`.

## Tables

### solana_spl_balance_changes
Columns (live): `slot UInt64, block_timestamp DateTime, tx_hash String,
token_account String, mint String, owner String, pre_amount String,
post_amount String, delta String, decimals UInt8, token_symbol Nullable(String),
token_name Nullable(String), ingested_at DateTime DEFAULT now()`.
- Semantics: per-token-account balance change per tx. `delta` = post − pre as
  **String** (signed, raw units). `pre_amount`/`post_amount` also String.
- **NO `amount` column** — scanner code reading `amount` is broken
  (audit §7 P0: sources/indexer_client.py:135, detectors/_indexer.py:120).
- Refresh owner: lite-chain-indexer Solana strategy (slot-driven).
- Order key: `(chain, slot)` — note table has no `chain` column live; slot is
  Solana-global.

### evm_transfers
Columns (live): `chain String, block_number UInt64, tx_hash String,
from_address String, to_address String, token_address String, value String,
block_timestamp DateTime, log_index UInt32, token_symbol Nullable(String),
token_name Nullable(String), ingested_at DateTime,
source LowCardinality(String) DEFAULT lite_indexer, raw_payload String
DEFAULT , tx_log_key String`.
- Semantics: ERC-20 Transfer logs. `value` is **String**, raw units.
- Refresh owner: lite-chain-indexer EVM strategies (block-driven).
- Gaps: symbol/name TTL 30d; `token_transfers` view is EMPTY (0 rows — audit §7).

### holder_balances
Columns (live): `chain LowCardinality(String), token_address String,
address String, balance Int128, transfer_count UInt64 DEFAULT 0,
last_updated DateTime DEFAULT now(), version UInt64 DEFAULT 0`.
- Semantics: running per-(chain, token, address) balance. `balance` Int128,
  raw units. `version` for ReplacingMergeTree dedup — use `FINAL` or argMax.
- Refresh owner: indexer balance reconciler.
- Gaps: `solana_holders` / `evm_holders` DO NOT EXIST (audit §7 P0) — scanner
  must use this table, not those names.

### deployer_events
Columns (live): `deployer String, deployer_eoa String DEFAULT ,
token_address String, token0 String DEFAULT , token1 String DEFAULT ,
pair_address String DEFAULT , chain String, block_number UInt64,
block_timestamp DateTime, tx_hash String, type String,
factory Nullable(String), ingested_at DateTime,
source LowCardinality(String) DEFAULT subsquid`.
- Semantics: token creations (contract deploys + pair/factory launches).
  `type` distinguishes event kind; `factory` nullable (EOA deploys have none).
- Order key: `(chain, block_number)` — **no `log_index` column exists**;
  bootstrap DDL fixed 2026-09-20 to match live (audit §4 P1). CH 26.7 cannot
  MODIFY ORDER BY to append existing columns (see migrations/018 header), so
  no live ALTER is required or possible without a rebuild.
- TTL: 90d from ingested_at.
- Refresh owner: indexer deployer handlers (EVM + Solana launch strategies).

### dex_events
Columns (live): `type String, chain String, block_number UInt64,
block_timestamp DateTime, tx_hash String, log_index UInt32,
contract_address String, dex String, version String, token0 String,
token1 String, sender String, recipient String, amount0_in Nullable(String),
amount1_in Nullable(String), amount0_out Nullable(String),
amount1_out Nullable(String), amount0 Nullable(String), amount1 Nullable(String),
whale UInt8, price_usd Float64 DEFAULT 0, volume_usd Float64 DEFAULT 0,
ingested_at DateTime, source LowCardinality(String) DEFAULT subsquid,
raw_payload String DEFAULT `.
- Semantics: Swap/Mint/Burn events. Amounts are **Nullable(String)**, raw
  units; direction columns (in/out) nullable — check both.
- TTL: detail columns 90d; `price_usd`/`volume_usd` survive.
- Refresh owner: indexer DEX parsers.

### token_meta
Columns (live): `chain LowCardinality(String), token_address String,
symbol String, name String, decimals Nullable(UInt8),
last_price_usd Nullable(Float64), last_price_source String,
updated_at Nullable(DateTime)`.
- Semantics: enrichment lookup. `decimals` nullable — fall back to 18 (EVM) /
  fetch on-chain before parsing String amounts.
- Refresh owner: indexer enrichment pass.

### solana_transfers
Columns (live): `chain String DEFAULT solana, slot UInt64,
block_timestamp DateTime64(3), signature String,
instruction_path Array(UInt16), from_address String, to_address String,
amount UInt256, mint String DEFAULT , decimals UInt8 DEFAULT 9,
program_id String, transfer_type LowCardinality(String), fee_payer String,
ingested_at DateTime64(3) DEFAULT now64(3)`.
- Semantics: SOL + SPL transfers per instruction path. `amount` is **UInt256**
  (raw). `transfer_type` distinguishes native/SPL.
- Refresh owner: Solana strategy transfers writer.

## Chain coverage (indexer 21 chains vs scanner registry)

Indexer `SUPPORTED_CHAINS` (strategies/__init__.py): ethereum, base, bsc,
polygon, arbitrum, optimism, avalanche, fantom, linea, scroll, zksync, mantle,
gnosis, celo, robinhood, blast, fraxtal, metis, hyperliquid, solana, tron.

Scanner rpc_router health-check registry covers 13: ethereum, bsc, polygon,
arbitrum, base, optimism, avalanche, fantom, linea, zksync, scroll, mantle,
gnosis, solana.

| Chain | Indexed? | Scanner needs it? | Note |
|---|---|---|---|
| tron | ✅ | ⚠️ needed | TronStrategy exists; scanner has no tron RPC lane — add before consuming tron rows |
| gnosis | ✅ | ✅ covered | in scanner registry |
| celo | ✅ | not needed | no scanner consumer today |
| blast | ✅ | not needed | no scanner consumer today |
| fraxtal | ✅ | not needed | no scanner consumer today |
| metis | ✅ | not needed | no scanner consumer today |
| hyperliquid | ✅ | not needed | no scanner consumer today |
| robinhood | ✅ | not needed | indexer-only (not in task gap list; noted for completeness) |

Action item: tron is the only true gap — indexer writes tron rows the scanner
cannot query. Others are indexer-ahead-of-scanner (harmless).
