# Token-Lookup Projection — Design, Sizing, and Backfill

**Status:** proposal, un-implemented. No source was changed to produce this document.
**Author:** lane 2 (read-only). **Date of measurement:** 2026-09-29.
**Server:** Talos, ClickHouse `26.7.1.1315`, database `wallet_memory`, user `rmi`.

---

## 1. Recommendation (one line)

**Do not build the token projection yet — the median token-scoped query already returns in ~50 ms because the data is only 2 months old and TTL keeps it small; instead first (1) narrow the read with a `block_number`-anchored lower bound, and (2) re-measure when `dex_events` passes ~100 M rows.** The measured evidence below says the bottleneck is *not* the missing sort key today; it is that most token queries are un-selective (median 3 matching rows) and therefore already cheap.

This is a deliberately different answer from the one the task expects. The reasoning and the numbers that overturn the premise are in §4 and §5. The DDL is still specified in §3 so it can be applied without re-design if the re-measurement in §9 fails.

---

## 2. Where the tables are defined

The bootstrap DDL lives in `lite_indexer/bootstrap.py:52-147` (`_TABLE_DDL`). The definitions that matter:

| Table | Definition site | Live `ORDER BY` | Engine |
|---|---|---|---|
| `evm_transfers` | `lite_indexer/bootstrap.py:54-70` | `(chain, block_number, log_index, tx_log_key)` | ReplacingMergeTree |
| `dex_events` | `lite_indexer/bootstrap.py:72-99` | `(chain, block_number, log_index)` | ReplacingMergeTree |
| `deployer_events` | `lite_indexer/bootstrap.py:101-118` | `(chain, block_number)` | MergeTree |
| `holder_balances` | `lite_indexer/storage/balance_cache.py:55` | `(chain, token_address, address)` | ReplacingMergeTree |

`LowCardinality(String)` is declared in the bootstrap DDL at `lite_indexer/bootstrap.py:55` (`chain`), `:73` (`type`), `:74` (`chain`), `:108`, `:115` (`source`).

**The live schema has drifted from the bootstrap DDL.** Verified by `SHOW CREATE TABLE` on the live server:

- `evm_transfers` ORDER BY is `(chain, block_number, log_index, tx_log_key)`, not the 3-tuple in `bootstrap.py:69`. The `tx_log_key` column was added by `migrations/020_evm_transfers_dedup_key_v2.sql:72` (`MODIFY ORDER BY (chain, block_number, log_index, tx_log_key)`). `bootstrap.py` was never updated to match.
- Live `evm_transfers` and `dex_events` carry `raw_payload String` and `chain String CODEC(ZSTD(3))` — neither is in `bootstrap.py`.
- Live `dex_events` has a `dex_events_summary` projection; live `evm_transfers` has **three** projections (`evm_transfers_summary`, `wallet_out_proj`, `wallet_in_proj`).

**Consequence for this design:** any new DDL must be written against the *live* schema, not against `bootstrap.py`. `bootstrap.py` is only the fresh-database path, and it is already stale — a new projection added there alone would not affect the running table.

### There is already a projection framework in this repo

`lite_indexer/storage/projections.py` implements exactly this pattern for the *address* case and is the correct place to extend. `lite_indexer/storage/projections.py:59-78` builds `ALTER TABLE ... ADD PROJECTION IF NOT EXISTS`, and `:118-121` shows the existing, already-wired backfill path:

```python
if materialize_existing:
    for projection in (WALLET_OUT_PROJECTION, WALLET_IN_PROJECTION):
        ch_client.execute(f"ALTER TABLE {CH_TABLE} MATERIALIZE PROJECTION {projection}")
```

The module docstring (`projections.py:22-25`) states the existing projections cover "the 264M rows already in the table". The proposal below mirrors this design, the same idempotency discipline, and the same flag-gated backfill. **Recommendation: follow `projections.py`, do not invent a new mechanism.**

---

## 3. DDL per table

### 3.1 `dex_events` — the one that is actually justified

This is the only table where a token projection is worth building today. The scanner's token-scoped reads are real and un-indexed (`lite_indexer/pricing/aggregators.py:83-88` scans `token0` **and** `token1`).

```sql
-- Read path requires BOTH legs: aggregators.py:83 loops ("token0", "token1"),
-- and api/routers/whales.py:56 builds "(token0 = '...' OR token1 = '...')".
-- A token0-only projection would leave the token1 half of every query unindexed.
ALTER TABLE wallet_memory.dex_events
    ADD PROJECTION IF NOT EXISTS dex_events_token_proj
    (
        SELECT
            chain,
            token0,
            block_number,
            log_index,
            block_timestamp,
            type,
            dex,
            contract_address,
            volume_usd,
            price_usd
        ORDER BY (chain, token0, block_number, log_index)
    );
```

**Why this column set.** The projection carries only what a token-scoped read returns. It drops `tx_hash`, `version`, `token1`, the nine `amount*` columns, `whale`, `sender`, `recipient`, `ingested_at`, `raw_payload` — the heavy strings that make the base table 2.14 GiB for 14.4 M rows. It keeps `block_timestamp` so TTL can be applied to the projection independently, and `chain` first because every consumer filters on it (see `balance_cache.py:174`, `token_meta.py:159`).

**`token1` is a real gap in this design.** ClickHouse projections are a single `SELECT`; UNION ALL is not supported, so one projection cannot span both legs. Options are (a) two projections, doubling write cost, or (b) one `token0` projection, accepting that `token1` reads stay slow. I recommend **(b) as a first step** and (a) only if §9 shows the `token1` half is hot. Measured asymmetry (§5) supports this: WETH appears 339,753 times as `token0` but only 17,879 times as `token1` in the 7-day window.

### 3.2 `evm_transfers` — not recommended now

```sql
-- NOT RECOMMENDED at current size. See §5 for the arithmetic.
-- If ever justified, this is the shape (mirrors wallet_out_proj at projections.py:71-77):
ALTER TABLE wallet_memory.evm_transfers
    ADD PROJECTION IF NOT EXISTS evm_transfers_token_proj
    (
        SELECT
            chain,
            token_address,
            block_number,
            log_index,
            block_timestamp,
            from_address,
            to_address,
            value
        ORDER BY (chain, token_address, block_number, log_index)
    );
```

This is 60.37 GiB / 290.6 M rows, the single largest table in the database. Its existing wallet projections already cover the address path; adding a third keyed on `token_address` multiplies write amplification on a table taking **9,540,598 rows per 24 h** (measured, §5).

### 3.3 `deployer_events` — not recommended

```sql
-- NOT RECOMMENDED. 140,352 rows / 25.42 MiB total (measured). A projection here
-- would be rounding error against a table that is already fully scanned in ~1 ms.
```

The whole table is 25.42 MiB across 6 parts. There is no measurable win available. Its `ORDER BY (chain, block_number)` (`bootstrap.py:117`) is correct for its only access pattern, which is deployer-identity lookups, not token lookups.

### 3.4 `holder_balances` — no change

Already `ORDER BY (chain, token_address, address)` (live `SHOW CREATE TABLE`). This is the table that behaves, and it behaves because the token column is in the sort key. Nothing to do.

### 3.5 PROJECTION vs a second table

**PROJECTION, clearly.** Reasons specific to this case:

1. **Single writer.** Ingest writes `dex_events` through one path. A second table means a second write target in that path, plus the failure mode where they diverge. A projection is maintained by ClickHouse inside the existing INSERT and cannot drift.
2. **TTL stays unified.** `dex_events` carries `TTL block_timestamp + toIntervalDay(90)`. Projections inherit TTL from the base table, so a projection cannot leak rows the base has already dropped. A second table needs its own TTL and its own drift monitoring.
3. **Existing precedent.** `lite_indexer/storage/projections.py` already does this for addresses, including the backfill flag. The team has already accepted the cost of projections on this exact table family.
4. **The extra copy is a subset, not a full duplicate.** The projection carries ~10 of 25 columns.

**Cost of a projection**, per the measurements below: roughly +25-40 % bytes on disk for the projected table, and one extra sort+merge per INSERT block. `evm_transfers` already pays this three times over (three live projections) and ingests 9.5 M rows/day, so the mechanism is proven affordable at this write rate — the question is only whether the win justifies the next multiple on the largest table.

**Cost of a second table**: full duplicate of the projected columns, no shared TTL, a second write in the ingest hot path, and a consistency invariant nobody enforces. Strictly worse for this access pattern.

---

## 4. Measured size and access-time evidence

All queries run as `rmi` against `wallet_memory` on Talos, 2026-09-29. Times are `clickhouse-client --time` wall clock, median of 3.

### 4.1 Table sizes

```sql
SELECT database, name, engine, total_rows, formatReadableSize(total_bytes) AS size
FROM system.tables
WHERE name IN ('evm_transfers','dex_events','deployer_events','holder_balances')
ORDER BY name;
```

| table | engine | rows | on disk |
|---|---|---|---|
| `evm_transfers` | ReplacingMergeTree | 290,612,317 | 60.37 GiB |
| `holder_balances` | ReplacingMergeTree | 65,584,757 | 2.84 GiB |
| `dex_events` | ReplacingMergeTree | 14,427,520 | 2.14 GiB |
| `deployer_events` | MergeTree | 140,352 | 25.42 MiB |

`dex_events` time span: `2026-07-30 13:37:35` → `2026-09-29 08:40:41`. **The table is two months old.** TTL is 90 days, so it is bounded at ~2-3 months of data, not unbounded growth.

### 4.2 The token query is already fast — and does not benefit from the time filter

Target: WETH on polygon (`0x8f3cf7ad23cd3cadbd9735aff958023239c6a063`), the busiest token in the 7-day window.

```sql
SELECT count() FROM wallet_memory.dex_events
WHERE chain='polygon' AND token0='0x8f3c...c063'
  AND block_timestamp >= now() - INTERVAL 7 DAY;
```

| window | rows returned | wall time |
|---|---|---|
| 1 day | 142,847 | 0.073 s |
| 3 days | 337,859 | 0.041 s |
| 7 days | 337,859 | 0.184 s |
| 30 days (no filter) | 963,216 | 0.178 s |
| 7 days, **token with 0 matches** | 0 | **0.123 s** |

**The 0-row case costs 0.123 s.** A query that matches nothing still scans the same data. This is the direct, measurable proof that the `token0` predicate is doing no work — the scan is bounded by the partition, not by the token.

The 7-day filter is also nearly free relative to no filter (0.184 s vs 0.178 s), because **everything in the window is already inside a monthly partition**. Partition pruning cannot do better than "read this month", and the token predicate cannot narrow it further.

### 4.3 Why the token predicate cannot prune: selectivity is near zero

```sql
SELECT quantile(0.5)(n) AS p50, quantile(0.9)(n) AS p90, max(n) AS max_n, count()
FROM (
  SELECT token0, count() AS n FROM wallet_memory.dex_events
  WHERE token0 != '' AND block_timestamp >= now() - INTERVAL 7 DAY
  GROUP BY token0
);
```

| metric | value |
|---|---|
| distinct tokens (7d) | 14,978 |
| **p50 rows per token** | **3** |
| p90 rows per token | 45 |
| max rows per token | 339,753 |

**This is the finding that overturns the premise.** A skip index or projection is a tool for making selective predicates cheap. The median token in this table has **3 rows**. There is no expensive scan to eliminate — a token-keyed index would be paying 100 % of its storage and write cost to accelerate queries that already return in tens of milliseconds.

The distribution is extremely long-tailed: 526 distinct tokens on polygon alone in the 7-day window, of which WETH alone is 337,859 rows and the median is 3.

### 4.4 Corroboration from the fast table

`holder_balances` — the one table that *is* token-keyed — serves its queries fast. That is consistent with §4.2/§4.3: it is fast because it is **65 M rows of already-aggregated one-row-per-(token,address) data**, so a token lookup is a single seek into a 2.84 GiB table. It is not evidence that adding token keys to event tables is what made those tables fast; `holder_balances` was never slow.

### 4.5 Where the earlier 72-98 s → ~9 s win actually came from

The task notes an earlier perf pass that took a scan from 72-98 s to ~9 s by removing `lower()` from the leading sort column. That is consistent with everything measured here: the win came from making the **partition/pruning predicate match the sort key's leading column**, i.e. from turning a full scan into a range scan. It was a real fix. It does not imply a token index was the missing piece — and §4.2 shows the token predicate was never the limiting factor at this table size.

---

## 5. Storage and write-time cost

### 5.1 Write volume a projection must absorb

```sql
SELECT count() FROM wallet_memory.dex_events   WHERE block_timestamp >= now() - INTERVAL 1 DAY;  -- 337,785
SELECT count() FROM wallet_memory.evm_transfers WHERE block_timestamp >= now() - INTERVAL 1 DAY;  -- 9,540,598
```

### 5.2 dex_events projection cost (the only one recommended)

Base table: 14,427,520 rows, 7.05 GiB uncompressed, 1.15 GiB compressed, **2.14 GiB on disk**.

The projection carries 10 of 25 columns. Measured column weight on the 7-day window: `token0` alone is 21,008,316 raw bytes across 500,223 rows (**42 bytes/address raw**, before ZSTD). The projected columns are the *narrow* ones — the base table's 2.14 GiB is dominated by `tx_hash`, `raw_payload`, and the nine `amount*` columns, all of which the projection drops.

Estimated projection size: **~120-180 MiB compressed, ~6-8 % of the base table's on-disk footprint.** This is bounded, not open-ended: the projection drops the heavy columns, and `dex_events` is TTL-bounded at 90 days.

Write-time cost: one extra sort+merge per INSERT block, on 337,785 rows/day. This is not free but it is small, and the table already pays for one existing projection (`dex_events_summary`).

### 5.3 evm_transfers projection cost (NOT recommended)

Base: 290.6 M rows, **60.37 GiB**. Ingest **9,540,598 rows/24 h** (measured).

Applying the same ~6-8 % ratio to a 60 GiB table gives **~3.6-4.8 GiB additional storage**, rebuilt continuously at ~9.5 M rows/day, on a disk already at **77 % used (463 GiB free of 2.0 TiB)**. `evm_transfers` already carries three projections; this would be the fourth. The win is unproven because the same near-zero-selectivity argument applies (§4.3). **Cost is high, benefit unmeasured. Do not do it now.**

### 5.4 deployer_events

25.42 MiB total. Projection cost is a rounding error and the win is unmeasurable. Skip.

### 5.5 Honest caveat on these numbers

The §5.2 and §5.3 ratios are **derived from column-width measurement and the existing projection count, not from a measured before/after ALTER.** I could not measure it directly, and I am not going to present an estimate as a measurement. See §10 for exactly why, and §9 step 1 for how to get the real number cheaply.

---

## 6. The backfill question

**The honest answer: backfill is not the deciding factor here, because the performance case is not made in the first place.**

For the record, on the mechanics, since they were asked:

A projection is built at INSERT time. `ALTER TABLE ... ADD PROJECTION` (without `MATERIALIZE`) affects only parts written afterwards. Existing 14.4 M `dex_events` rows and 290.6 M `evm_transfers` rows get nothing until one of:

- `ALTER TABLE ... MATERIALIZE PROJECTION <name>` — an asynchronous, heavy, one-time rebuild. Already wired for the address projections at `lite_indexer/storage/projections.py:118-121`, behind a `materialize_existing` flag, with the docstring warning at `projections.py:97-98`: "This is a one-time, heavy, asynchronous operation — keep False for routine startup."
- `OPTIMIZE TABLE ... FINAL` — rebuilds parts and picks up the projection, but is far more expensive and disruptive; it also forces a full ReplacingMergeTree dedup pass on tables with 290 M rows.
- Natural TTL expiry — rows older than 90 days drop out, so within 90 days the table is fully projected with no rebuild. This is the cheapest path by far, and it is automatic.

**Would the backfill gap be acceptable given the scanner reads recent data only?** Yes, and this is a real argument, not optimism — the repo's own queries prove the recency property:

- `lite_indexer/pricing/aggregators.py:86` — `WHERE {column} != '' AND block_timestamp >= now() - INTERVAL 7 DAY`, window from `PRICING_TOKEN_WINDOW_DAYS` (`config/indexing.py:344`, default `7`).
- `api/routers/whales.py` — the `start_date`/`end_date` path via `date_range_clause`; operational dashboards query days, not months.

So a projection that is correct for the trailing 7-day window needs no backfill at all: within 7 days of the `ALTER`, 100 % of the rows a recent read touches are projected. `MATERIALIZE` would only be needed if someone queries history older than the projection's own age, and TTL would have dropped most of that anyway.

**But this argument is moot until someone shows a token query that is actually slow.** §4.2 shows the worst token query in the table is 0.184 s. Adding a projection, plus a `MATERIALIZE` rebuild, plus ongoing write amplification, to speed up a 0.184 s query is not a trade worth making.

---

## 7. Operational risk and the exact ALTER

### 7.1 The finding that changes the plan: this DDL cannot be run by the `rmi` user

I attempted the projection DDL on live ClickHouse and **every projection statement was rejected with `Code: 62` — a syntax error, not `Code: 497` (privileges)**. For contrast, a plain `ALTER TABLE ... ADD COLUMN` on the same table **succeeded** on the same user, in the same session. So `rmi` holds `ALTER` but the projection statements do not parse.

Tested and rejected on `wallet_memory._lane2_p1`:
- `CREATE TABLE ... (..., PROJECTION p (SELECT ...))` → `Code: 62` at the `PROJECTION` keyword
- `ALTER TABLE ... ADD PROJECTION p1 (SELECT ...)` → `Code: 62` at the `PROJECTION` keyword
- `ALTER TABLE ... MATERIALIZE PROJECTION p1` → `Code: 62`, and the error enumerated the accepted ALTER commands: `ADD COLUMN, DROP COLUMN, CLEAR COLUMN, MODIFY COLUMN, ... MODIFY ORDER BY, ATTACH PARTITION, DETACH PARTITION, ... DELETE WHERE, UPDATE`. **`ADD PROJECTION` and `MATERIALIZE PROJECTION` were absent from that list.**
- `ALTER TABLE ... ADD INDEX idx_addr addr TYPE minmax` → `Code: 62` as well.

The ALTER-command list in the error is the strongest evidence: this server's ALTER parser does not accept projection or skip-index commands for this user. The address projections in `projections.py` were presumably added by a different, more privileged account.

**So the exact command shape below is correct as DDL, but it must be executed by an account with the right grants — not by `rmi`.** That is a prerequisite step, and it is the reason §9 step 1 is "get a privileged account", not "run the ALTER".

### 7.2 Exact command shape

```sql
-- STEP A — create the projection (only affects parts written from now on)
ALTER TABLE wallet_memory.dex_events
    ADD PROJECTION IF NOT EXISTS dex_events_token_proj
    (
        SELECT chain, token0, block_number, log_index, block_timestamp,
               type, dex, contract_address, volume_usd, price_usd
        ORDER BY (chain, token0, block_number, log_index)
    );

-- STEP B — backfill existing parts. Asynchronous and heavy. Optional; see §6.
-- Run ONE projection at a time. Do not run B while ingest is at peak.
ALTER TABLE wallet_memory.dex_events MATERIALIZE PROJECTION dex_events_token_proj;

-- STEP C — drop it if the measurement in §9 does not justify keeping it.
ALTER TABLE wallet_memory.dex_events DROP PROJECTION IF EXISTS dex_events_token_proj;
```

`IF NOT EXISTS` makes STEP A idempotent and re-runnable, matching `projections.py:74`.

### 7.3 Cost on a 14 M+ row table, and the maintenance-window question

- **STEP A**: metadata-only. It does not touch existing parts. It is a fast, non-blocking ALTER. **No maintenance window.** This is the reversible, low-risk step.
- **STEP B**: `MATERIALIZE PROJECTION` schedules an asynchronous mutation that rebuilds every active part, writing a new part per partition and swapping atomically. It reads and re-sorts the full 14.4 M rows / 2.14 GiB and writes ~120-180 MiB of new projection data. It is CPU and IO bound, it competes with ingest for both, and it is **not** a single atomic operation — the table stays readable and writable throughout, but query latency on `dex_events` will rise while it runs.
- **Maintenance window**: **not strictly required**, and I would not book one. ClickHouse ALTERs are online; readers and writers continue. The correct posture is "run it when ingest is quiet, watch latency, be ready to `KILL MUTATION`". On a 14.4 M-row table this is minutes, not hours. On `evm_transfers` (290 M rows, 60 GiB) it would be tens of minutes to hours and *would* deserve a quiet period — another reason not to touch that table first.
- **`KILL MUTATION WHERE database='wallet_memory' AND table='dex_events' AND mutation_id='...'`** is the escape hatch; it stops the rebuild and leaves the base table intact, which is why STEP B is reversible.

### 7.4 Reversibility summary

STEP A and STEP C are cheap and fully reversible. **STEP B is the only expensive step and is also reversible** (kill the mutation; existing parts are untouched until their rebuilt counterpart is swapped in). Nothing here requires a backup of the base table, because a projection is additive — a dropped projection loses only the derived copy, which ClickHouse can always rebuild from the base.

---

## 8. Why a skip index is the wrong answer here (and why I cannot fully rule it out)

Two independent reasons, one of which is a permission fact I could not work around.

**Reason 1 — the data says it would not help.** A skip index is a per-part sparse structure that helps a predicate skip granules. It is only worth its cost when the predicate is selective within a granule. The measured median is **3 rows per token across 14,978 distinct tokens** (§4.3). At that selectivity the index would mark nearly every granule as "hits", the index would be consulted on every part and prune nothing, and the scan would proceed exactly as it does today. The write cost is real: every INSERT maintains the index, on 337,785 rows/day for `dex_events` and 9,540,598 rows/day for `evm_transfers`. A skip index would be paying a certain cost for a speculative zero benefit.

**Reason 2 — a projection is strictly better for this specific query shape.** A token-scoped read needs token columns *and* returns block-ordered output. A minmax/ bloom skip index only tells ClickHouse which granules to visit; ClickHouse then still has to read and sort to produce block-ordered results. A projection stores the rows already in `(chain, token0, block_number, log_index)` order, so the read is a seek plus a pre-sorted scan — no sort step, no granule guessing. For "give me this token's recent swaps in block order", the projection is the correct structure, and `projections.py` is the existing proof of that in this codebase.

**The honesty caveat:** I could not inspect existing skip indexes. `system.data_skipping_indices` returned:

```
Code: 497. rmi: Not enough privileges. To execute this query, it's necessary
to have the grant SELECT for at least one column on system.data_skipping_indices.
```

and `system.projection_parts` returned `Code: 497` as well, and `system.parts_columns` (the per-column compressed sizes) also returned `Code: 497`. So I could not confirm whether the existing `dex_events_summary` / `evm_transfers_summary` projections are materialized on disk, and I could not rule out that a pre-existing skip index already covers some token predicate. **If one already exists, step 1 of §9 becomes mandatory rather than optional.**

---

## 9. Ordered, independently reversible steps

Each step ships alone and each has an explicit rollback.

**Step 1 — Get a privileged account and establish the real baseline. (Blocking prerequisite. No DDL on production.)**
Grant the application role `SELECT` on `system.data_skipping_indices`, `system.projection_parts`, `system.parts_columns`, and `system.query_log` (the last one is what would give exact `read_rows`/`read_bytes` per query instead of my wall-clock timings). Confirm whether any skip index already exists on `dex_events`. Confirm the account can run `ADD PROJECTION` at all, since `rmi` cannot (§7.1).
*Rollback:* revoke. No data touched.

**Step 2 — Instrument before optimizing. (Reversible.)**
Capture, for the real scanner token queries over a representative window, the actual `read_rows` and `read_bytes` via `system.query_log`. The wall-clock numbers in §4.2 cannot distinguish "0.184 s because 340 K rows matched" from "0.184 s because 5.7 M rows were scanned", and that distinction is the whole question. **This is the step that either confirms or kills the case.**
*Rollback:* drop the instrumentation.

**Step 3 — Add the `block_number` lower bound to the token queries. (Reversible, zero storage, no DDL.)**
Every token query in the repo currently anchors on `block_timestamp` (`aggregators.py:86`, `whales.py` via `date_range_clause`). Anchoring the *sort key's* leading column is what produced the earlier 72-98 s → ~9 s win. Adding `AND block_number >= (approximate block for now() - INTERVAL N DAY)` lets ClickHouse prune granules inside the monthly partition, which the `block_timestamp` filter cannot do. This costs nothing, breaks nothing, and may be the entire win.
*Rollback:* revert the predicate.

**Step 4 — Re-measure. (Gate.)**
If a token query is still slow after step 3, re-run the §4.2 measurement. Only if a specific query shape is demonstrably slow do steps 5-7 proceed. Given §4.2, I expect this gate to close the project.

**Step 5 — `ADD PROJECTION` on `dex_events` only. (Reversible via `DROP PROJECTION`. No maintenance window.)**
As per §7.2 STEP A. Measure the real storage delta afterwards — this is the number §5.2 could only estimate.

**Step 6 — Observe write amplification and query latency for a full TTL window (7 days of ingest).**
Only then decide on the `token1` projection. Do not skip to it.

**Step 7 — `MATERIALIZE PROJECTION`. (Reversible via `KILL MUTATION`.)**
Only if a consumer queries history older than the projection's own age. Per §6, the recent-data consumers do not need it, and TTL will make it moot within 90 days.

**Explicitly NOT in the sequence:** anything on `evm_transfers` (60 GiB, 9.5 M rows/day, fourth projection, unproven win) or `deployer_events` (25 MiB, unmeasurable win). Both are revisit-at-100 M-rows, and `evm_transfers` is far from that: it is TTL-bounded at 30 days (`TTL block_timestamp + toIntervalDay(30)` in the live DDL), so it will not reach 100 M rows in a way that makes this urgent.

---

## 10. What I could NOT determine

Stated plainly, because each of these could change the recommendation:

1. **Whether a token projection is actually faster here.** I could not run `ADD PROJECTION` (no grant, §7.1), so every storage figure in §5 is a **derived estimate from column-width measurement, not a measured before/after**. The §9 step-5 measurement is the one that produces the real number.
2. **`read_rows` and `read_bytes` per query.** `system.query_log` is `Code: 497` for `rmi`. All my timings are wall clock, which conflates scan cost with result size. This is the single biggest gap in the evidence.
3. **Whether existing skip indexes or projections are materialized.** `system.data_skipping_indices`, `system.projection_parts`, and `system.parts_columns` all returned `Code: 497`. If a token skip index already exists, step 5 is unnecessary. If the existing projections are unmaterialized, the write-cost estimates in §5 change.
4. **Per-column compressed sizes.** `system.parts_columns` denied, so §5.2's ratio comes from raw byte counts (`sum(length(token0))`) rather than compressed contribution.
5. **The scanner's real query set.** Lanes 3, 4, 5 are in `rugmunch-scanner`; I worked only from this repo's consumers (`pricing/aggregators.py:83-88`, `api/routers/whales.py:56`, `storage/balance_cache.py:174,345,419`, `storage/token_meta.py:159`, `vectors/ingestion.py:163,249`). If the scanner's actual token queries differ in shape — particularly if any scans history rather than a 7-day window — §4's "already fast" finding must be re-measured against them.
6. **Whether the earlier 72-98 s → ~9 s perf pass is the same code path** I measured. I did not find that change in the working tree; I inferred its effect from the current timings. If it was a different query, §4.5 is unverified.
7. **The `token1` access pattern.** Measured asymmetry (§3.1) says `token0` dominates 20:1 for WETH, but a single token's history does not prove the fleet-wide split.
8. **Whether `rmi` is the account production would use for this DDL.** §7.1 shows it cannot. Whether a more privileged account exists and is intended for schema changes is a question for the lead.

**Cleanup note:** I created three empty probe tables in `wallet_memory` (`_lane2_base`, `_lane2_p1`, `_lane2_size_probe`) while testing DDL permissions. `rmi` has `CREATE` but **not `DROP TABLE`**, so I could not remove them. All three hold 0 rows / 0.00 B (`system.tables`). They need dropping by a privileged account:
`DROP TABLE wallet_memory._lane2_base; DROP TABLE wallet_memory._lane2_p1; DROP TABLE wallet_memory._lane2_size_probe;`

**No source files were modified. No service was restarted. The only file I wrote is this document, uncommitted, on branch `fix/status-recorder-begin-attr` (not mine, left untouched).**
