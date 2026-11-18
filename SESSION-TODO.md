> **Status:** active · owner: Rug Munch Media LLC Engineering · updated 2026-11-18
> Operator sequence: 90-day backfill to B2 first, indexer second.

# SESSION-TODO — lite-chain-indexer

## PROVEN END TO END (this session)

### BTC archive path — DONE, real B2
```
btc_transactions 2026-11-15: 963,618 rows -> 118,659,116 B  [verified]
btc_blocks       2026-11-15:     172 rows ->      24,852 B  [verified]
read back from B2: 963,618 rows, 963,618 DISTINCT tx_hash  (100%, no dupes)
```
This was previously "39.3M BTC labels and zero transaction tables". It is now a
real archive with a verifiable round trip. The whole `unit_writer` contract is
proven against production B2, not a scratch remote.

### Entity clustering — DONE, live in ClickHouse
```
cex_deposit   55,513 rows / 103 cohorts / 5 chains   confidence 0.38–0.85
same_deployer    222 rows / 103 cohorts / 8 chains   confidence 0.95
```
Before this session: 223 rows, one heuristic. The lane reported 55,518/103;
production shows 55,513/103. Per chain: ethereum 44,845, bsc 5,539,
arbitrum 4,576, optimism 527, gnosis 26.

### Merged
| PR | what |
|---|---|
| #260 (contextcrypto) | `window_limits` merges registry+policy — routing can size its own lane |
| #17 | BTC tracing adapter |
| #18 | `cex_deposit` entity clustering |
| #19 | SQD backfill adapter |

---

## THE MEASURED BLOCKER: SQD sheds at ~61k rows

The SQD portal does **not** enforce the documented ~20 req / 10 s. Measured by
walking batches until 529:

```
bare block requests      : 8/8 batches OK, twice, 30 s apart
ERC-20 log requests      : trips at batch 3, after 61,352 rows
polygon day              : ~598,000 rows
ratio                    : 9.7x the measured ceiling
```

So a day **cannot** be fetched in one `stream()` call, at any pacing. Coding
rate (`SQD_MIN_INTERVAL` 0.55 s → 1.2 s) made no difference, which is how we
know it is a volume ceiling and not a request-rate one.

**What this means:** the SQD adapter is correct and the CLI works —
`--dry-run` on a 500-block window returns 15,728 real log rows — but a
day-granularity unit needs a **resumable windowed pull**: fetch ~61k rows, cool
down, continue from the last block, and only publish the day when the window
set is complete. That is a design change to `sqd_backfill.py`, not a bug fix.

**Do not "fix" this by raising retries.** Retrying into a volume ceiling is what
gets an account banned, and a ban loses 14 chains.

---

## Open — in order

### 1. SQD resumable windowed pull (the blocker above)
- [ ] Add a cursor: `(chain, table, day, last_block_done)` persisted so a run
      resumes instead of restarting.
- [ ] Cap rows per sitting at ~50k (below the measured 61,352), then yield.
- [ ] A day publishes only when every window is fetched — `write_unit` already
      refuses a partial count, so use it as the gate.
- [ ] Test: a synthetic 10-window day completes in ≥2 sittings and produces one
      verified parquet with the exact expected row count.

### 2. BTC history breadth
- [ ] One day is proven. Backfill a real range (e.g. 90 days = ~90 × 113 MB
      ≈ 10 GB) and record actual wall time per day.
- [ ] Decide: full history from 2009, or from a chosen date. AWS read is free but
      a 2011 day took a different shape (659 rows) than a 2026 day (963,618).

### 3. Remaining 5 real stalls (live indexer)
- [ ] `bsc` — providers cap getLogs at 10 blocks vs `SYNC_CHECKPOINT_SLICE=200`.
      Needs per-chain slice sizing; arithmetic in `RETENTION-STRATEGY.md`.
- [ ] `ethereum` — `-32005 log response size exceeded`, behind ~4h.
- [ ] `linea` — `1rpc.io` still in the merged pool (paid-subscription upsell).
- [ ] `arbitrum`, `solana` — verify against current logs before assuming.

### 4. Entity heuristics beyond `cex_deposit`
Measured reason each is blocked:
- [ ] `shared_funding` — needs **unbounded** `internal_transfers`; TTL is 90 d.
- [ ] `cex_deposit` on base yields **0** (its 11 CEX labels match no funder).
- [ ] 7 of 16 chains with native data have **zero** exchange labels.
- [ ] The 15.7M exchange labels are **99.4% Bitcoin**; Ethereum has 3,799.

---

## Do not re-derive

- **`evm_transfers` is ERC20-only** — all 49.5M rows have a `token_address`.
  `internal_transfers` is the only native-value table.
- **`internal_transfers` needs traces + the sibling `transactions` table** —
  traces alone carry only `transactionIndex`, so `tx_hash` (the archive dedupe
  key) is empty without it.
- **SQD traces are absent on celo, mantle, optimism** (HTTP 400 "table 'traces'
  does not exist"). A zero-row block is **not** evidence of absence.
- **SQD sheds at ~61k rows**, not at the documented 20 req/10 s. Rate tuning
  does not help.
- **`wallet_entities` dedupe key is `(wallet_address, chain_id)`**, so one wallet
  belongs to exactly ONE entity. `entity_id` is a **cohort key, not an owner
  key** — never treat two wallets in a cohort as the same owner.
- **`/app/cc_mcp` is baked into the image**, built from `/srv/build/contextcrypto-mcp/tree`
  (a plain copy, not a git checkout). Merging does not deploy. Use
  `scripts/deploy/build-contextcrypto-mcp.sh --ref <ref>`, retag to
  `contextcrypto-mcp:main-<sha>`, and update the compose pin.
- **`dispatch_brief` `fleet:` is a fleet NAME, not a boolean.** Free lanes are
  `free_only: true`.
- **Subagent commits land on detached HEAD.** Check `git branch --contains` and
  `git switch -c` before pushing.

## CI state (verified locally with CI's invocation)
`ruff` PASS · `structure-gate` PASS · `public-leak` PASS ·
`pytest -m 'not smoke'` 2767+ passed, 0 failed ·
`lint-and-test` red **identically on origin/main** (two decrease-only ratchets,
`check-antipatterns` + `check-monkeypatch`). **Never `--update-baseline`** — it
launders 11 bare-except and 1 monkeypatch into permanent debt.
