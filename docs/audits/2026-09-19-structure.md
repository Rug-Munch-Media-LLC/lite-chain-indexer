# Structure / Setup / Hygiene Audit — lite-chain-indexer

Date: 2026-09-19 · Scope: structure/setup/hygiene (not line-level bugs) · Read-only
Repo: /srv/work/repos/lite-chain-indexer (Talos) · 455 tracked files · 1,727 tests collected
Verdict: **B-** — layering contracts are real and green; drift is in docs, env surface, and duplicated gates.

---

## P0 — Structural findings

### P0-1. Duplicated `_guard_state_regression` — two live implementations
- `lite_indexer/indexer/__init__.py:513` (LiteIndexer method) AND
  `lite_indexer/indexer/_sync_loop.py:146` (SyncLoopMixin method).
- `LiteIndexer` composes mixins (`class LiteIndexer(...)` at `__init__.py:176`), so the
  mixin's method is shadowed by the class-body definition — one of the two is dead or,
  worse, they diverge silently. Confirmed prior-audit finding; still present.
- **Fix:** keep the mixin version (sync-loop concern), delete the `__init__.py` copy,
  re-export nothing. `__init__.py` drops ~60 lines toward the ≤200 canon (currently 702).

### P0-2. `indexer/__init__.py` (702 lines) + `_sync_loop.py` (800) — god files, canon says ≤200 new / shrink-only
- `indexer/__init__.py` holds `LiteIndexer` class body with 16+ defs plus imports from
  storage + strategies (lines 88–101). `_sync_loop.py` is 800 lines, over the 500 hard-fail
  line but allowlisted in `scripts/structure-baseline.json` (97 allowlisted files).
- **Fix (shrink-only):** move `_guard_state_regression` + state-guard helpers into
  `_resource_guards.py` or a new `_state_guard.py` (≤200 lines, one function cluster);
  move `LiteIndexer` class assembly to `indexer/core.py`, leave `__init__.py` as exports.
  Baseline update is a legal shrink.

### P0-3. Two parallel structure gates — `check-code-structure.py` is a second, weaker copy
- `scripts/check-structure.py` (fleet firm gate: >200 fail, >500 god, shrink-only baseline)
  runs in CI (`.forgejo/workflows/ci.yml:394`) but NOT in `.lefthook.yml`.
- `scripts/check-code-structure.py` (185 lines, MAX_FILE_LINES + monkeypatch ratchet)
  runs in `.lefthook.yml:127-132` but NOT in CI.
- Two baselines, two thresholds, two monkeypatch counters → drift guaranteed. The
  monkeypatch ratchet only runs locally; CI's `check-monkeypatch.py` covers it, but the
  size ratchet in the hook uses different numbers than CI's.
- **Fix:** make `check-structure.py` the single size gate (add it to lefthook pre-push),
  reduce `check-code-structure.py` to the monkeypatch ratchet only or delete it and fold
  its monkeypatch check into `check-monkeypatch.py` (already in CI). One gate, one baseline.

---

## P1 — Findings

### P1-1. Import-linter contracts: green but too thin
- `lint-imports`: 272 files, 1,713 deps, **3/3 contracts KEPT** (strategies↛indexer,
  storage↛strategies, indexer layers). Layering is coherent and enforced.
- Gap: no contract covers `query` (4,783 lines, 11 files) or `rpc` (10,222 lines, 30 files)
  — the two biggest packages are outside the boundary system. `query/router.py` imports
  all four source legs directly (biglake/envio/sqd/indexed) — fine, but nothing forbids
  `storage → query` or `rpc → indexer` regressions.
- **Fix:** add `forbidden` contracts: `lite_indexer.rpc ↛ lite_indexer.indexer`,
  `lite_indexer.query ↛ lite_indexer.indexer|strategies`. Cheap, high value.

### P1-2. Env surface: ~300 vars read in code, ~127 documented in `.env.example`
- `rg 'os.getenv|environ'` → 300 distinct uppercase var names across `lite_indexer/`;
  `.env.example` (289 lines) documents ~127 active keys. ~170 knobs are undocumented
  (ADDRESS_BACKFILL_*, ATTESTATION_*, B2_*, BACKFILL_* etc. all used, most absent).
- **Fix:** generate `.env.example` from a single source (a `KNOWN_VARS` registry in
  `config/`) or add a gate script `check-env-docs.py` that fails when a read var is
  undocumented. Also prune genuinely dead vars (vulture pass).

### P1-3. Undeclared lazy dependencies (deptry + manual confirm)
- `loguru` imported in `query/envio_source.py:346`, `query/biglake_source.py:349`,
  `query/trace_wallet.py:242` — **not declared anywhere in pyproject.toml** (works only
  if another package pulls it transitively). Same class: `pyyaml` (plugin/loader.py:26),
  `google-cloud-bigquery` (storage/archives.py:235, arguably optional-ok),
  `solders` (strategies/solana/launch.py:540), `eth-hash` (strategies/evm/bloom.py:190).
- deptry: 806 DEP003 (mostly false positives from lazy imports + `lite_indexer`
  self-references) — noisy config, but the 5 above are real undeclared deps.
- **Fix:** declare `loguru`, `pyyaml`, `solders`, `eth-hash` (core or extras per usage);
  configure deptry to ignore known-lazy optional deps so its output becomes actionable.

### P1-4. Docs drift (see table below) — README/AGENTS/ARCHITECTURE all stale
- AGENTS.md describes a repo that no longer exists: "14 EVM chains", flat module map
  (`chains.py`, `rpc.py`, `indexer.py` — reality: `chains/` package, `rpc/` 30 files,
  `indexer/` 12 files). ARCHITECTURE.md is 21 lines (data-flow boundary only).
- **Fix:** regenerate AGENTS.md module map from `tree lite_indexer -L 1`; correct chain
  count to 21; expand ARCHITECTURE.md to a real package map (~100 lines).

### P1-5. No Makefile — violates fleet standard
- AGENTS.md fleet contract says every repo has `make lint test check dev commit clean ci`.
  Repo has **no Makefile** (verified). CI + lefthook exist but local ergonomics differ
  from every other fleet repo.
- **Fix:** add minimal Makefile: `lint (ruff+lint-imports), test, check (gates), ci`.

---

## P2 — Findings

### P2-1. Strategies asymmetry (evm 12 files / solana 8 / tron 6) — acceptable, not a smell
- evm/ has real sub-concerns (bloom, receipts, reorg, range_planning); solana/tron are
  thinner because their chains are simpler. `strategies/base.py` + `evm.py` + `evm/`
  package coexist — `evm.py` is a facade over `evm/`; fine, but name-collision-prone.
- **Fix (optional):** rename `strategies/evm.py` → `strategies/evm_facade.py` or fold
  into `evm/__init__.py`.

### P2-2. Query router: legs are proper modules with contracts — good
- Each leg is a class with its own file: `BigLakeSource`, `SQDSource`, `EnvioSource`,
  `IndexedXyzSource`, plus `rpc_fetcher.py` and `cache.py`. `router.py` (1,050 lines) is
  the only offender — it's a god file doing CH+cold+merge+filter+fallback logic.
- **Fix:** split router.py into `router.py` (dispatch, ≤200) + `_ch_query.py` +
  `_cold_query.py` + `_merge.py`. Shrink-only baseline update.

### P2-3. Repo hygiene: clean tree, but two egg-info dirs and untracked clutter
- `git status --porcelain` = 0 lines; nothing bad is **committed** (no .coverage,
  egg-info, venv, pycache in `git ls-files`). `.gitignore` covers `*.egg-info/`, `.venv/`.
- On disk: BOTH `lite_chain_indexer.egg-info/` and `litechainindexer.egg-info/` exist
  (stale build artifacts from two different package-name spellings), plus `.venv`,
  `.venv-ci`, `.coverage`, `.import_linter_cache` — all ignored, all deletable.
- **Fix:** `rm -rf lite_chain_indexer.egg-info litechainindexer.egg-info` (regenerated on
  next build); confirm canonical dist name in pyproject (`litechainindexer`).

### P2-4. Testing: mock-based with a real smoke path that exists but is env-gated
- 1,727 tests collected (README says 1,100+/1,350+ — drift). `tests/test_smoke.py` is
  marked `pytest.mark.smoke` and runs REAL ClickHouse/RPC when `CH_HOST`/`RPC_URL` set —
  skipped by default. So prior audit's "mock-only" is half-right: a real-integration path
  exists but never runs in CI (CI has no CH service container).
- conftest: single `tests/conftest.py`; markers: only `smoke` declared.
- **Fix:** add a ClickHouse service to CI and run `-m smoke` in a dedicated job; or
  document that smoke is operator-run only.

### P2-5. Ops topology: systemd units duplicated across repos, source of truth ambiguous
- Repo `scripts/systemd/`: chain-query-api.service, lite-indexer-watchdog.{service,timer}.
- fleet-infra `rmi-infra/`: lite-indexer.service, lite-indexer-stall-check.{service,timer},
  docker-compose.lite-indexer.yml, deploy-lite-indexer.sh.
- Different unit sets in each place; deploy script points at fleet-infra compose only.
  Scripts duplicated too: `scripts/lite-indexer-parquet-daily.sh` DIFFERS from
  fleet-infra `scripts/parquet/lite-indexer-parquet-daily.sh`.
- **Fix:** declare fleet-infra the deploy source of truth; repo keeps only unit templates
  referenced by deploy script; delete or sync the diverged parquet script copy.

### P2-6. NATS streaming: wired, not orphaned
- `lite_indexer/streaming/nats_publisher.py` ↔ `compose.nats.yml` + `compose/nats-server.conf`
  are a matched pair (JetStream CHAIN_EVENTS stream, loopback ports 4222/8097/8222).
  Optional extra `streaming = [nats-py, websockets]` declared. OK — no action.

### P2-7. Migration doc vs applied state: consistent, still flagged REVIEW
- `docs/MIGRATION_MERGETREE_REPLACING.md` (286 lines) says mig 017 was the merge→replacing
  step; repo has `017_evm_transfers_dedup_key.sql` AND `018_evm_transfers_dedup_key_v2.sql`
  — the doc's step 5 ("add 017_merge_to_replacing.sql") doesn't match what 017/018
  actually are (dedup-key migrations). Doc's own [REVIEW] note about TTL drift from
  mig 012 remains unresolved.
- **Fix:** rewrite doc §9 step 5 to reference 017/018 as-shipped; resolve or close the
  TTL-drift REVIEW item.

### P2-8. bootstrap.py (738 lines) — one class doing snapshot download + CH import + verify
- `SnapshotBootstrap` with ~20 methods + module-level `build_import_sql`. Over canon.
- **Fix:** split `_download.py` (rclone/manifest) and `_import.py` (CH import+verify);
  keep `bootstrap.py` as the orchestrator ≤200.

---

## Docs drift table

| Doc | Claim | Reality | Severity |
|---|---|---|---|
| README.md badge | v1.3.0 | pyproject `1.5.0` | P1 |
| README.md badge | 20 chains | 21 (TODO.md: +Hyperliquid) | P1 |
| README.md badge | 1,100+ tests (body: 1,350+) | 1,727 collected | P2 |
| README.md §testing | "Full suite (1,350+ tests)" | 1,727 | P2 |
| AGENTS.md | 14 EVM chains | 21 chains (18 EVM + Solana + Tron + HL) | P1 |
| AGENTS.md | flat modules (chains.py, rpc.py, indexer.py) | packages: chains/, rpc/ (30 files), indexer/ (12) | P1 |
| ARCHITECTURE.md | 21 lines, boundary note only | 88k LOC system | P1 |
| TODO.md | v1.5.0, ~1490 tests | matches pyproject; tests now 1,727 | P2 |
| MIGRATION doc | "add 017_merge_to_replacing.sql" | 017/018 are dedup-key migs; TTL REVIEW open | P2 |
| fleet AGENTS.md | standardized Makefile targets | no Makefile in repo | P1 |

## Gate status (verified)
- `lint-imports`: 3/3 contracts KEPT ✅
- CI jobs: lint-and-test (py3.11+3.12 matrix: antipattern, monkeypatch, gate-selftest,
  ruff, import-lint, mypy strict, pytest, import-check), public-leak-gate, script-tests
  (bats), structure gate, mutation-test (informational) ✅
- lefthook: pre-commit + pre-push incl. check-code-structure (diverges from CI — P0-3)

## Top 3 actions (ranked)
1. Deduplicate `_guard_state_regression` (P0-1) — correctness risk, 1-file fix.
2. Collapse the two structure gates into one gate + one baseline (P0-3).
3. Docs truth pass: README badges, AGENTS.md module map, Makefile (P1-4/P1-5).
