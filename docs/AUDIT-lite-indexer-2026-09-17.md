# FORENSIC AUDIT — lite-chain-indexer

**Date:** 2026-09-17
**Auditor:** single-agent forensic pass (`pipeline-auditor`, deepseek-flash direct, read-only)
**Lead verification:** 4 headline claims spot-checked against source by the lead — all CONFIRMED (see §5)
**Scope:** `/srv/work/repos/lite-chain-indexer` — 350 files / ~88k LOC. Fleet's canonical indexer (16 chains, Parquet export, B2 permanence); replaced Subsquid + Ponder fleet-wide (decommissioned 2026-08).
**Method:** direct source reads of the audit set + targeted grep. `context-crypto` MCP was intermittent; findings are from source, not graph.

---

## 1) CRASH ROOT CAUSES — mechanisms the watchdogs paper over

### RC-1 (P0, DATA-LOSS): quota-degrade returns `[]` for `eth_getLogs` → empty is indistinguishable from "no logs" → state advances over un-indexed blocks
- `rpc/client.py:862-865` — `_defer_reason_for(endpoint, "eth_getLogs", …)` true → `return []`. Docstring at `:854-855` states the contract is **"List of raw log dicts, or None on failure"** — the quota path violates it.
- `rpc/ledger.py:837-847` — level 2 (≥85% quota) defers ALL `eth_getLogs`; level 1 (≥70%) defers ranges >1000 blocks (`LEVEL1_GETLOGS_RANGE_BLOCKS`, `ledger.py:786`). Seeded per operator quota; on a busy catch-up this fires for hours.
- `indexer/_sync_loop.py:437-449` — no-events short-circuit executes `self.state.save(to_block, 0, …)` at `:445`, sets `status="no_events"`, returns.
- **Mechanism:** provider hits 85% quota → every `getLogs` returns `[]` (not an error) → `fetch_transfers` yields 0 events → `no_events` → `last_block = to_block` **permanently**. Blocks skipped with no error log, only an occasional `rpc_call_deferred` warning. Watchdogs then see "on live max block, wrote nothing" and bounce the daemon — which restarts from the advanced state and **never backfills the hole**.
- This is exactly the documented silent-stall gotcha (`rmi_gotchas(subsystem='indexer')`).

### RC-2 (P0, CRASH/STALL): unbounded awaits — only the transfer leg has a timeout
- `indexer/_sync_loop.py:330` — transfers wrapped in `asyncio.wait_for(..., timeout=300.0)`.
- `indexer/_sync_loop.py:386` — `dex_events = await self.fetch_swap_events(...)` **no timeout**.
- `indexer/_sync_loop.py:412` — pair-created **no timeout**.
- `indexer/_sync_loop.py:429` — `_collect_internal_transfers` **no timeout**.
- **Mechanism:** one hung `eth_getLogs`/`trace_*` on the DEX leg blocks that chain's coroutine indefinitely; other chains keep running, so the only signal is "one chain stopped writing" → data-freshness watchdog bounce (`fix(watchdog): 45m fresh window…`). `httpx.Timeout` bounds a single dial, not a retry/backoff chain (`client.py:444-565` can sleep `RPC_RETRY_ATTEMPTS` × throttle/autotune/budget waits).

### RC-3 (P0, LIVELOCK + dup-insert): 0-row write gates abort the cycle without advancing state → chain freezes and replays the same range forever
- Transfers gate: `_sync_loop.py:543-554` — `if ch_inserted == 0: … return` (no state save).
- DEX gate: `_sync_loop.py:590-601` — same.
- Best-effort writers: `_sync_loop.py:707-728` — deployer/launch/trace enabled + candidates>0 + written==0 → `status="writer_incomplete"` + `_save_current_state()` (unchanged) → next cycle re-fetches the SAME range.
- **Mechanism:** an enabled trace/deployer writer that fails deterministically (missing table, schema mismatch) pins `last_block` forever. Every cycle re-runs the full RPC fetch (budget burn) and re-inserts. `evm_transfers`/`internal_transfers`/`solana_transfers` are ReplacingMergeTree (`TODO.md:30-33`) so exact dups dedupe, but `dex_events` and `solana_spl_balance_changes` are **not unique** (`TODO.md:56-58`) → genuine duplicate rows. A legitimate 0 (batch fully deduped) is indistinguishable from failure.

### RC-4 (P1, UNVERIFIED): module-global endpoint swap under concurrent chains
- `indexer/__init__.py:623-658` — `_evm_verify_endpoints` mutates module-global `chains.GET_LOGS_ENDPOINTS` via `apply_verified_endpoints`/`restore_original_endpoints`, while up to 16 chains run as concurrent tasks. `test_concurrent_chain_sync` (`test_critical_paths.py:611`) calls `_sync_loop_inner` directly, bypassing the swap → race untested. `apply_verified_endpoints` body not in audit set → **UNVERIFIED**.

**Fix order: RC-1 → RC-2 → RC-3.**

---

## 2) LAKE PROVIDER CORRECTNESS

### A. BigLake (`query/biglake_source.py`) — PARTIALLY correct; WRONG DATA on deep queries and large values
- **Silent precision loss:** values read from DOUBLE and re-stringified (`_num_str`, `:58-71`). Wei amounts >2^53 lose precision; rows feed `evm_transfers` consumers (`:237,257`).
- **Silent incomplete history:** `self._scope` defaults to current month (`:100`); when the router can't derive dates it passes `dates=None` (`router.py:543-554`) → only current month read, returned as a valid-looking incomplete result. `_lake_dates` returns None whenever Envio fails (`router.py:498-521`). No partial signal.
- **All exceptions → `[]` (`:220-222`)** — "lake down" == "wrong schema" == "no rows".
- `int(raw.get(...) or 0)` turns `None`→0 (`:239,259`).
- Correct: date validation before path build (`:145-152`), parameterized `from_block`/`to_block`/`address` (`:196-204`), `limit` clamped 1..1000 (`:213`), anonymous S3 secret (`:136-139`), hive partitioning + `union_by_name` (`:209`).

### B. SQD (`query/sqd_source.py`) — PARTIALLY correct; WRONG DATA + silent partial + eval-only portal in prod
- **`tx_hash=""` for every EVM trace** (`:683`). Router row key for internal_transfers includes `tx_hash` (`router.py:70`) and `_merge_rows` drops later dups (`router.py:413-418`) → two txs in one block sharing a `traceAddress` collide → **one row silently dropped**. Same empty tx_hash in `solana_owner_activity` (`:314`).
- **Silent truncation:** `stream` stops after `max_batches` (default 48, `:223,233`); each accessor slices `rows[:limit]` (`:325,396,510,587,639,742`) with no truncation flag; router treats it as complete.
- **Pagination gaps:** `cursor = last + 1` where `last = max(header.number)` (`:250-253`) — a non-monotonic batch is not detected → gap skipped.
- **Eval-only portal in production:** `TODO.md:76` says the public SQD portal is "*eval-only*, not production"; yet `QueryRouter.__init__` constructs `SQDSource()` by default (`router.py:180`) in the live fallback chain.
- Correct: 429/529 + transport retry/backoff (`:177-200`), 204 = dataset height (`:242`), Cloudflare bypass (`:158-167`), bearer key never logged, tolerant ms/s timestamps (`:258-270`), Tron normalization tested (`:401-435`).

### C. DuckDB / parquet / cold store — WRONG (empty) DATA: writer layout ≠ reader layout
- **Writer** emits **day buckets**: `_local_dir = data_dir / chain / "YYYY-MM-DD"` (`storage/parquet/helpers.py:10-25`); confirmed by `test_parquet_export.py:182-198` and cron `scripts/lite-indexer-parquet-daily.sh:35`.
- **Reader** expects **hour granular**: `{chain}/{YYYY}/{MM}/{DD}/{HH}/…` (`storage/duckdb_query.py:103-117`, `_collect_mount_files:154-168`, `_collect_rclone_cat_files:171-202`); fixtures only use the hour layout (`test_duckdb_query.py:19-63`). The reader's own docstring even claims the daily script writes hourly folders — false.
- `ColdStore._collect_mount` filters `/{YYYY}/{MM}/{DD}/` (`cold_store.py:366-371`) → day buckets never match.
- **Result:** the cold query tier cannot find the production daily export → queries silently return `[]` (`cold_store.py:511-514`) → layer degrades to hot-only, under-reporting history with no error.
- `_register_views` registers empty views for no files (`duckdb_query.py:132-137`) → `count(*)` returns 0 → **a green integrity check can coexist with missing data**.
- **Raw SQL:** `query_parquet(sql, …)` executes caller-supplied SQL in DuckDB (`:250,314`) — DuckDB can read local files; HTTP reachability unverified → [REVIEW].
- `query_httpfs` interpolates the key into `SET s3_secret_access_key='{sk}'` (`:289-292`) — breaks/injects on a quote.
- Parquet JSONL fallback is **dead**: writes `.jsonl.gz` (`writer.py:196-206`) but `_verify_parquet` requires the `.parquet` path (`reader.py:95-104`) → always fails when pyarrow is absent.
- `.done` written even when `upload=False` (`writer.py:486`), including the skip-delete path (`:464-472`) → the day is permanently excluded (`eligible_dates:277-280`) and **never reaches B2**. `test_parquet_export.py:344-354` enshrines this as correct.

### D. Compaction (`storage/compaction.py`) — PARTIALLY correct; silent address corruption + output the reader can't read
- **Short values silently zero-padded** (`:423-424`) although the docstring claims non-hex/wrong-length raise (`:395-397`) → a 19-byte address becomes a zero-extended 20-byte binary, no error.
- Output hive layout `{prefix}/hive/chain={id}/{grain}={period}/part-*.parquet` (`:271-273`) is not matched by `ColdStore._collect_mount`'s `/{YYYY}/{MM}/{DD}/` filter, and files hold `fixed_size_binary` while `cold_store._normalize` (`:560-574`) compares stringified values → compacted data is unreadable/corrupt through the normal read path.
- `_list_staging_files` catches only `FileNotFoundError` (`:296-299`).
- Good: write-first/delete-second + manifests (`:708-730`), per-part footer verification (`:485-492`), idempotency short-circuits (`:572-618`), tmp cleanup (`:376-385`), size-target split (`:460-476`).

### E. Router wiring (`query/router.py`) — correct precedence, but cross-source conflicts + misrouting
- Precedence deterministic: CH → cold → BigLake → Envio → Indexed → SQD → RPC, first-wins on row key (`:795-901,413-418`); ClickHouse always wins (`:417-418`).
- **No cross-source reconciliation** — BigLake (precision-lost), Envio, Indexed, SQD and RPC can all answer; where row keys differ (SQD tx_hash="", BigLake log_index=0) both survive → duplicated/conflicting rows in one response.
- **Filter-column misinterpretation:** `_single_address(filters)` (`:703-714`) returns the single dict value for ANY column; `get_transfer` passes `filters={"tx_hash": tx}` (`:919`) → the tx hash is handed to BigLake (`:542`) and Envio/Indexed (`:582,618`) as an *address* → wasted full scans / no-op legs.
- **Sequential expensive fallback:** 5 network legs evaluated one after another (`:804-900`); SQD timeout 90 s (`sqd_source.py:224`). Slow, not wrong.
- **Injection surface:** `_append_ch_filters` interpolates from caller filter *keys* (`:349,357-359`); `_build_cold_where` interpolates `col` (`:397,400`) → [REVIEW].
- `historical.query_sql` calls the blocking DuckDB cold query synchronously (`historical.py:562`) — blocks the event loop if invoked there; the async path correctly uses `to_thread_with_timeout` (`:377-389`). [REVIEW]

### VERDICT: are the lake providers implemented correctly? — **PARTIALLY (no).**
| Provider | Verdict |
|---|---|
| BigLake | correct plumbing; WRONG/INCOMPLETE data silently (scope fallback, DOUBLE precision) |
| SQD | correct transport; WRONG/INCOMPLETE silently (tx_hash collision, silent truncation, eval-only portal in prod) |
| DuckDB/parquet/cold | **cold tier cannot read the writer's output** — highest-impact correctness defect |
| Compaction | crash-safe, but zero-pads addresses + emits output the reader can't consume |
| Router | precedence/dedupe correct; cross-source conflicts + column misrouting; slow fallback |

---

## 3) MONKEYPATCH + TEST GAPS

### Production markers
- Repo-wide grep for `HACK|WORKAROUND|TEMP|XXX|FIXME|band-aid|temporary` → **27 matches, none a production hack** (temp files, `temp = create_embedder(...)`, test temp dirs, the linter itself).
- `monkeypatch` hits inside `lite_indexer/` (33) are docstrings documenting seams, not calls. **The operator's "full of monkey patches" is NOT supported for production code.**
- Legitimate DI seams: `biglake_source.py:97` (`connect=`), `sqd_source.py:121` (`transport=`), `compaction.py:507` (`fs=`), `cold_store.py:266` (`max_files=`), `parquet/writer.py:59` (`state_dir=`), `ingest/flatfile.py:121`.
- The ledger's `monkeypatch ×11` is a false positive: 100+ `monkeypatch.setattr` in `tests/` are pytest fixtures (legitimate), heaviest in `test_rpc_resilience.py:666-712` and `test_routing.py:272-420`.

### Fake / green-washing tests
- **`test_parquet_export.py:344-354` `test_delete_without_upload_is_skipped`** — asserts `dates_exported == 1`, status `"uploaded"`, `.done` written while `upload=False, delete=True` → codifies permanent B2 loss of that day. Clearest green-wash.
- **`test_query_router.py:100-126` FakeCH** fabricates columns from the response dict; filters only on `params["chain"]` → tests the fake, not the real CH contract.
- **`test_duckdb_query.py:19-63` hour fixtures vs `test_parquet_export.py:182-198` day fixtures** — both green, integration untested = the exact §2C defect.
- **`test_biglake_dates.py:85-96`** asserts `len(dates) == 32` against a "caps at 31 days" contract (off-by-one codified).
- **`test_sqd_source.py:211-235`** uses one trace per block → cannot catch the tx_hash="" collision.
- No assertion-free tests; no stray `pytest.skip` beyond optional-dependency guards.

### Not tested (real gaps)
- RC-1 deferred-getLogs → state advance (data loss). RC-2 hung DEX/trace leg. RC-3 multi-cycle writer-gate livelock/dup.
- Concurrent EVM chains vs the global endpoint swap (`test_concurrent_chain_sync` bypasses it).
- Writer→cold-reader layout integration; compaction output read-back; SQD truncation/collision; BigLake >2^53 precision; real ClickHouse semantics; hourly vs daily rotation.
- Crash/control-flow coverage is otherwise strong but **almost entirely mock-based** — no test exercises real persistence or a real provider.

---

## 4) PATTERN DELTA + NEXT MOVES

### Violated canonical patterns
| Pattern | Evidence | Why it matters | Correction |
|---|---|---|---|
| **Hourly Parquet export** | writer daily + 5M/day cap (`reader.py:16-22,68-69`; `helpers.py:25`); reader/compaction assume hourly (`duckdb_query.py:73-77`) | >5M-row day can't export; reader can't see day buckets anyway | emit hour buckets; verify each; align the shell script; one integration test |
| **Local disk is a cache, not the record** | `.done` on `upload=False` (`writer.py:486`, `:464-472`) | local-only export treated as permanent; day never reaches B2 | mark `.done` only after verified upload; fix `test_parquet_export.py:344` |
| **06:15 integrity check must mean something** | empty views return 0 (`duckdb_query.py:132-137`); cold reads `[]` on any failure (`cold_store.py:511-514`) | a broken layout looks like "no rows" → green check, missing data | assert non-zero expected counts + file-set equality; fail on empty |
| **One chain = one config, zero forks** | chain-specific branches in `_sync_loop` (`:436,499-508,571-576`); `_guard_state_regression` duplicated (`__init__.py:503-568` vs `_sync_loop.py:146-213`) and the mixin copy is DEAD (class body wins, MRO `__init__.py:172-179`) | drift: a fix to one copy silently doesn't apply | delete the dead copy; move chain specifics into strategies |
| **Append-only / no silent dup** | writer-gate replay re-inserts; non-unique `dex_events`/`solana_spl_balance_changes` (`TODO.md:56-58`) | duplicate rows | cap gate retries; surface a counter/alert; finish RMT migration |
| **Anti-pattern budget** | bare-except ×253; god files `rpc/client.py` 1361 / `query/router.py` 1050; f-string logging ×39; cross-layer imports in `_sync_loop` | hides failures | burn down via ledger; keep `check-antipatterns.py` in CI |
| **Docs = code** | `AGENTS.md:14-27` describes flat modules + "14 EVM chains"; reality is a package/mixins/21 chains (`TODO.md:3-5`) | onboards agents wrong | regenerate AGENTS.md architecture section |

### Prioritized next moves
**P0 — crash / data-loss blockers**
1. `eth_getLogs` deferral must NOT return `[]` — raise `RpcDeferredError` (or return `None`, matching its own docstring) and make `_sync_loop` treat it as `fetch_failed` with **no state advance**. `rpc/client.py:862-865` + `indexer/_sync_loop.py:437-445`. **S**
2. Wrap every RPC phase in `asyncio.timeout` (DEX, pair-created, traces) mirroring the transfers cap. `indexer/_sync_loop.py:386,412,429`. **S**
3. Break the writer/insert-gate livelock: bound retries per range or advance-with-alert after N failures; distinguish "0 rows expected" from "write failed". `indexer/_sync_loop.py:543,590,707`. **M**

**P1 — correctness / data-integrity**
4. Reconcile parquet layouts (writer, cold reader, compaction) + one writer→reader→compaction integration test. `parquet/helpers.py:25`, `cold_store.py:360-448`, `duckdb_query.py:103-202`, `compaction.py:263-273`. **L**
5. `.done` only after verified upload + fix the green-washing test. `parquet/writer.py:486`, `tests/test_parquet_export.py:344`. **S**
6. BigLake: reject/warn on >2^53 values; never silently default to current month for bounded ranges. `biglake_source.py:58-71,100` + `router.py:498-554`. **M**
7. SQD: real `tx_hash` (or drop it from the row key), return a truncation signal, gate the eval-only portal behind an explicit env flag. `sqd_source.py:223,250-253,683` + `router.py:180,70`. **M**
8. Compaction: raise on short address/hash (no zero-pad); stop emitting output the reader can't read. `compaction.py:423-424,560-562`, `cold_store.py:360-376,560-574`. **M**
9. Security review: reachability of `query_parquet(sql)` raw SQL and filter *column-name* interpolation from HTTP. **S**

**P2 — debt + tests**
10. Delete duplicate `_guard_state_regression` / unused `_REORG_SENTINEL` (`__init__.py:161,503` vs `_sync_loop.py:57,146`). **S**
11. Add missing tests: deferred-getLogs→no-advance, hung-leg timeout, multi-cycle writer gate, concurrent endpoint swap, SQD truncation/collision, BigLake precision. **M**
12. Regenerate `AGENTS.md` architecture + chain count; continue bare-except/god-file burn-down. **M**

---

## 5) LEAD VERIFICATION (spot-check of headline claims, 2026-09-17)

| Claim | Source checked | Result |
|---|---|---|
| RC-1: quota-degrade returns `[]` for `eth_getLogs` | `rpc/client.py:862-865` — `if reason: _warn_deferred(...); return []`; docstring `:854-855` says "or None on failure" | **CONFIRMED** — and it is a **contract violation**, not merely a shape choice |
| RC-1: no-events path advances state | `indexer/_sync_loop.py:437-449` — `self.state.save(to_block, 0, …)` at `:445` | **CONFIRMED** |
| §2C: writer/reader parquet layout split | `parquet/helpers.py:25` → `{chain}/YYYY-MM-DD/`; `duckdb_query.py:103-117` → `{chain}/YYYY/MM/DD/HH/` | **CONFIRMED** — layouts genuinely differ; the reader's docstring asserts the daily script writes hourly folders |
| §2C: `.done` written even when `upload=False` | `parquet/writer.py:486` — `_mark_exported(...)` called unconditionally after the upload/delete branch | **CONFIRMED** |

## Risks / blockers
- **BLOCKER (DATA-LOSS):** RC-1 — quota-degrade silently advances state over un-indexed blocks.
- **BLOCKER (WRONG DATA):** cold tier cannot read the production parquet export → historical queries silently empty.
- **UNVERIFIED:** `apply_verified_endpoints` per-chain vs global (RC-4); HTTP reachability of `query_parquet` raw SQL and filter-column interpolation; whether the live runtime uses an hour-bucket exporter other than `lite-indexer-parquet-daily.sh`.
- BigLake precision and SQD portal licensing are operator decisions, not silent defaults.

## Summary
The watchdogs chase three real mechanisms: a quota-degrade path returns `[]` for `eth_getLogs` so state advances over lost blocks (RC-1); DEX/trace legs have no timeout while only transfers are capped (RC-2); and 0-row write gates livelock the chain while replaying the range (RC-3). The lake providers are partially implemented and silently wrong: the cold reader cannot read the writer's parquet layout, BigLake degrades to current-month on deep queries, SQD drops rows via empty `tx_hash` and truncates silently, and compaction zero-pads addresses. Monkeypatches are test seams, not production hacks — but the suite mocks away every real persistence/provider path and green-washes one B2 data-loss case.
