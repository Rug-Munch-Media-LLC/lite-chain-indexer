# CI Gates

The ratchet gates that block every change to `lite-chain-indexer`. Each gate
is **shrink-only**: it may only get stricter over time. Lowering a baseline
is a deliberate, reviewed act — never a side effect of an unrelated change.

## The ratchet gates

Run in this exact order in CI (`.forgejo/workflows/ci.yml` and
`.github/workflows/ci.yml`), before `ruff check`:

| # | Gate | Command | Enforces |
|---|------|---------|----------|
| 1 | **Structure** | `python3 scripts/check-structure.py` | New files ≤200 lines; >500 hard-fail; allowlisted files may never grow. Baseline: `scripts/structure-baseline.json`. |
| 2 | **Antipatterns** | `python3 scripts/check-antipatterns.py` | Per-rule counts may only go down (bare-except, fstring-logging, sync-io-async, eval-exec, god-file, long-function, too-many-params, wildcard-import, mutable-default, slop-markers, ts-ignore). Baseline: `scripts/antipattern-baseline.json`. |
| 3 | **Monkeypatch** | `python3 scripts/check-monkeypatch.py` | Production code (`lite_indexer/`, `src/`) must have **zero** monkeypatch occurrences; test debt is decrease-only. Baseline: `scripts/monkeypatch-baseline.json`. |
| 4 | **Public-leak** | `python3 scripts/check-public-no-freemium.py` | The public tree must never name the curated freemium RPC stack (provider hosts, keys, tiers). That config lives in the private runtime overlay (`LITE_INDEXER_ENDPOINT_OVERLAY`). |

Two more ratchets run at pre-push, scoped to `lite_indexer/`:
`scripts/check-mypy-budget.py` (mypy error count, baseline 129) and
`scripts/check-vulture-budget.py` (dead code at 100% confidence, baseline 1).
Both refuse to raise their own baseline.

### Structure gate — module size policy

- **New files** must be ≤200 lines. A new file over 200 lines fails the build.
- **>500 lines** is a hard fail (god-file) unless explicitly allowlisted.
- **Allowlisted files** (the shrink-only baseline) may never *grow*; they may
  only shrink. The gate prints `[shrink]` hints when a file got smaller.
- Regenerate deliberately with `--update-baseline` (removes/shrinks only).

### Antipattern ratchet

Counts are compared against the committed baseline. A rule whose count
increases fails CI. Fix the code — do not raise the baseline. Lower it with
`--update-baseline` when you genuinely remove violations.

### Monkeypatch gate

Two thresholds:

- **Production hard-zero** — `monkeypatch.` in `lite_indexer/` or `src/` is a
  build failure, full stop. Build clean architecture instead.
- **Test debt decrease-only** — the test-suite count may only shrink.

### Freemium scan

Scans every tracked file for curated provider tokens (hosts, key
placeholders, tier names). Zero hits required. Checkout + `python3` only —
no dependencies.

## Coverage gate

`pytest-cov` is wired into CI with a **global floor** and a **per-diff floor**:

```bash
pytest -m "not smoke" -q --tb=short \
  --cov=lite_indexer \
  --cov-report=term \
  --cov-report=xml:coverage.xml \
  --cov-fail-under=74
```

- **Measured coverage: 75%** (2026-09-22, 1890 passed, 4 skipped).
- **Floor: 74%** — the measured number rounded **down** to the nearest whole
  percent, leaving a small margin so the suite is not flaky at the boundary.
- **RATCHET: the floor only ever RISES.** When coverage improves, raise
  `--cov-fail-under` (and the `diff-cover --fail-under`) to the new
  rounded-down value. Never lower it.
- **Per-diff gate** — `diff-cover coverage.xml --compare-branch=origin/main
  --fail-under=74`: lines *changed* vs `origin/main` must be ≥74% covered.
  On a push to `main` the diff is empty and this passes trivially.

Smoke-marked tests need live ClickHouse/RPC endpoints and are excluded
(`-m "not smoke"`).

## Running the gates locally

```bash
# All four ratchets, in order
python3 scripts/check-structure.py
python3 scripts/check-antipatterns.py
python3 scripts/check-monkeypatch.py
python3 scripts/check-public-no-freemium.py

# Coverage (same command CI runs)
pytest -m "not smoke" -q --tb=short \
  --cov=lite_indexer --cov-report=term --cov-fail-under=74

# Prove the gates actually catch violations
bash scripts/ci-gate-selftest.sh
```

## CI job names

| Job | Workflow | Purpose |
|-----|----------|---------|
| `lint-and-test` | both | Four ratchet gates → ruff → docstring → pip-audit → mypy → tests |
| `structure-gate` | `.forgejo` | Structure gate (standalone, blocking) |
| `public-leak-gate` | `.forgejo` | Freemium scan (standalone, blocking) |
| `coverage-gate` | `.forgejo` | Coverage floor + per-diff gate (blocking) |
| `script-tests` | `.forgejo` | bats regression suite for ops scripts |
| `docstring-coverage` | both | interrogate ≥80% over production code |
| `pip-audit` | `.forgejo` | pip-audit over pyproject deps (blocking) |
| `migration-sql-lint` | `.forgejo` | ClickHouse migration parse (blocking) |
| `sast` | `.forgejo` | bandit + semgrep + gitleaks |
| `security` | `.github` | gitleaks secret scan |

## Gate policy (adjudicated 2026-09-29)

A hook command ending in `|| true` or `|| echo` is a defect unless it is one of
the three documented exceptions below. Eighteen such commands existed; eight were
removed outright, four were repaired and now fail, and three remain advisory for
stated reasons.

**Removed — no subject.** `biome`, `oxlint`, `typecheck`, `knip`, `publint`,
`attw` (no `package.json`, no `tsconfig.json`; the only tracked JS file is
`commitlint.config.js` — run for real, knip exits 2, publint 1, attw 1, all
masked into green ticks), `radon` (superseded by the `long-function` antipattern
ratchet), `osv-deps` (no lockfile in tree; `osv-scanner scan source` exits 128
"No package sources found"). The `.forgejo` `supply-chain` and
`nightly-deep-scan` OSV step is retired for the same reason; its two
`|| echo "::warning::"` steps stay advisory with the reason stated inline.

**Repaired and now blocking.**

| Gate | Was | Now | Result |
|---|---|---|---|
| `pip-audit` | `pip-audit --severity high` — flag does not exist in 2.10.1, exits 2 at argparse, never scanned. Same dead `--fail-on=high` in the `.forgejo` job. | `python3 -m pip_audit . --progress-spinner off` | "No known vulnerabilities found", exit 0 |
| `deptry` | `deptry . \|\| echo "deptry not available — skipping"` — reported a PASS for a scan it had run and discarded (39 findings) | shrink-only ratchet over the 18 declaration-corrected findings | 18 findings, baseline 18 |
| `interrogate` | `.` (50.1%, counted `tests/` at 23.5%) at `--fail-under=60`, and CI ran a *different* scope at the same 60 | `lite_indexer/ api/ --fail-under=80` in hook **and** both workflows | 87.5%, exit 0 |
| `vulture` | `vulture . --min-confidence 80 \|\| true` — 10 findings, all swallowed | `scripts/check-vulture-budget.py`, confidence 100, `lite_indexer/` only, shrink-only | 1 finding, baseline 1 |
| `sqlfluff` | `--dialect postgres` on ClickHouse DDL — false-failed 20 of 23 files, hidden by `\|\| echo` | `--dialect clickhouse`, parse-only, blocking | see `migration-sql-lint` |
| `bearer-scan` | `--hide-progress` is not a bearer flag (it is `--hide-progress-bar`) — exited 1 on argparse, `2>/dev/null` ate the error | flag corrected, stays advisory | 176 checks, 31 findings |
| `trufflehog-verified` | `git rev-parse @{u}` fails on a detached worktree, so `RANGE` was empty and the scan never ran | falls back to the `origin/main` merge-base; **red on a real finding** | exit 183, verified_secrets: 1 |

**The three remaining non-blocking commands, with the reason each cannot be
enforced here.**

- `mypy-report` — triage output, not a gate. `mypy lite_indexer/` has 129 real
  errors by design (`_facade` re-exports, Protocol attributes set outside
  `__init__`). The *gate* is the shrink-only `check-mypy-budget.py`. This one
  exists to print the list a developer is about to work through.
- `bearer-scan` — advisory. Its 12 CRITICAL findings are all `subprocess.run`
  calls with an argv list and no `shell=True` (0 occurrences of `shell=True`
  under `lite_indexer/` and `api/`). bearer's SAST rule flags any subprocess
  call whose argv contains a variable, which is not command injection without a
  shell. Failing a push on 12 confirmed non-findings is exactly the noise that
  teaches people to `--no-verify`. `bandit` is the blocking SAST gate.
- `zoekt-incremental` (post-commit) — the script is absent from this repo and
  the hook only fires if a sibling `standards/` checkout exists. A reindex side
  effect is not a correctness gate.

## Known red: `tests/test_logging_redaction.py:46`

`trufflehog --only-verified --fail` exits **183** with `verified_secrets: 1` —
a 32-hex Infura URL path key used as redaction bait inside a logging-redaction
test (`Detector Type: Infura`, file `tests/test_logging_redaction.py`, line 46).

It is a realistic-format literal rather than a live credential, but it is a
VERIFIED secret to the scanner and it is in pushed history, so the gate is left
blocking and red on it deliberately. The fix is to rotate/remove the literal
(not to delete the check): a placeholder that verifies against a real service is
indistinguishable, to the scanner and to an attacker, from a live key. A known
false positive is the wrong thing to encode as an exception.

## Duplicated gates — one statement each

| Gate | Authoritative in | The others |
|---|---|---|
| structure / antipattern / monkeypatch / public-leak | `.forgejo/workflows/ci.yml` + `.github/workflows/ci.yml` (both, per `docs/CI-GATES.md`) | `.lefthook.yml` `structure-gate` mirrors it |
| mypy budget | `scripts/check-mypy-budget.py` | hook `mypy`, both workflows `check-mypy-budget.py --repo .` — identical command |
| docstring coverage | both workflows, `interrogate lite_indexer/ api/ --fail-under=80` | hook `interrogate` runs the same command |
| pip-audit | `.forgejo` job `pip-audit`, `python3 -m pip_audit . --progress-spinner off` | hook + `.github` mirror the same command |
| bandit | `.lefthook.yml` pre-push (`-s B608`, excludes restated from `[tool.bandit]`) | `.forgejo` `sast` job |
| migration SQL | `.forgejo` job `migration-sql-lint` (`--dialect clickhouse`) | hook `sqlfluff` uses the same dialect, parse-only |

The `.forgejo` `structure-gate` job passes `--scan-root lite_indexer`, which is
already the default via `[tool.fleet-gates] scan-roots` in `pyproject.toml`. It
is redundant, not wrong; both invocations exit 0.

### `migration-sql-lint` is currently red

The `.forgejo` job hardcodes one skip, `migrations/015_address_activity_chain.sql`,
**which does not exist**. The file that exists is
`migrations/017_address_activity_chain.sql`, and three others are also
unparsable under the sqlfluff 4.3.0 ClickHouse dialect:
`017`, `019`, `020`, `023` — all `ALTER TABLE … ADD COLUMN` and `ORDER BY`
clauses the dialect cannot yet parse. Re-run:

```bash
for f in migrations/*.sql; do
  sqlfluff parse "$f" --dialect clickhouse >/dev/null 2>&1 || echo "UNPARSABLE: $f"
done
# UNPARSABLE: migrations/017_address_activity_chain.sql
# UNPARSABLE: migrations/019_evm_transfers_dedup_key.sql
# UNPARSABLE: migrations/020_evm_transfers_dedup_key_v2.sql
# UNPARSABLE: migrations/023_runtime_ddl_tables.sql
```

Four files, one stale skip line. Fixing it means either upgrading sqlfluff or
extending the skip list to the four real files — deliberately, with the reason
("dialect grammar gap"), the way `015` was meant to be handled.


## Changing a baseline

Baselines are committed JSON. To lower one after genuinely removing
violations:

```bash
python3 scripts/check-structure.py --update-baseline
python3 scripts/check-antipatterns.py --update-baseline
python3 scripts/check-monkeypatch.py --update-baseline
```

`--update-baseline` only ever removes or shrinks entries. Review the diff —
a baseline that *grew* is a bug in the change, not the gate.

## Suite chunking

The full pytest suite is run as **two deterministic halves** of `tests/test_*.py`
because a monolithic run gets OOM-killed on the 10 GiB heavy-run cage. Coverage
accumulates with `--cov-append`; the `--cov-fail-under` floor applies on the
final chunk only.

**No test file is excluded.** `test_traces.py` and `test_critical_paths.py`
used to be filtered out of every run with a `grep -vE` in both workflows and
from the coverage ratchet, on the stated grounds that they were
"OOM-killed on the 10G cage". That was re-measured on 2026-09-28 and it is
**not true** — they are the two slowest files in the tree, not memory hogs:

| file | tests | wall time | cgroup `memory.peak` | verdict |
|------|-------|-----------|----------------------|---------|
| `tests/test_traces.py` | 35 | 4m28s | far below `MemoryMax=10G` | passes |
| `tests/test_critical_paths.py` | 15 | 8m26s | far below `MemoryMax=10G` | passes |

So they now run in their own step with an explicit 30-minute cap, and they
count toward the 74% coverage floor like every other file. To reproduce the
measurement:

```bash
heavy-run pytest tests/test_traces.py -q        # ~4.5 min
heavy-run pytest tests/test_critical_paths.py -q # ~8.5 min
```
