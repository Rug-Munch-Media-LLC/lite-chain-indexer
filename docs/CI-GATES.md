# CI Gates

The ratchet gates that block every change to `lite-chain-indexer`. Each gate
is **shrink-only**: it may only get stricter over time. Lowering a baseline
is a deliberate, reviewed act — never a side effect of an unrelated change.

## The four ratchet gates

Run in this exact order in CI (`.forgejo/workflows/ci.yml` and
`.github/workflows/ci.yml`), before `ruff check`:

| # | Gate | Command | Enforces |
|---|------|---------|----------|
| 1 | **Structure** | `python3 scripts/check-structure.py` | New files ≤200 lines; >500 hard-fail; allowlisted files may never grow. Baseline: `scripts/structure-baseline.json`. |
| 2 | **Antipatterns** | `python3 scripts/check-antipatterns.py` | Per-rule counts may only go down (bare-except, fstring-logging, sync-io-async, eval-exec, god-file, long-function, too-many-params, wildcard-import, mutable-default, slop-markers, ts-ignore). Baseline: `scripts/antipattern-baseline.json`. |
| 3 | **Monkeypatch** | `python3 scripts/check-monkeypatch.py` | Production code (`lite_indexer/`, `src/`) must have **zero** monkeypatch occurrences; test debt is decrease-only. Baseline: `scripts/monkeypatch-baseline.json`. |
| 4 | **Public-leak** | `python3 scripts/check-public-no-freemium.py` | The public tree must never name the curated freemium RPC stack (provider hosts, keys, tiers). That config lives in the private runtime overlay (`LITE_INDEXER_ENDPOINT_OVERLAY`). |

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
| `lint-and-test` | both | Four ratchet gates → ruff → mypy → tests |
| `structure-gate` | `.forgejo` | Structure gate (standalone, blocking) |
| `public-leak-gate` | `.forgejo` | Freemium scan (standalone, blocking) |
| `coverage-gate` | `.forgejo` | Coverage floor + per-diff gate (blocking) |
| `script-tests` | `.forgejo` | bats regression suite for ops scripts |
| `docstring-coverage` | `.forgejo` | interrogate ≥60% |
| `sast` | `.forgejo` | bandit + semgrep + gitleaks |
| `security` | `.github` | gitleaks secret scan |

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

## Suite chunking (OOM guard)

The full pytest suite is run as **two deterministic halves** of `tests/test_*.py`
(`test_traces.py` and `test_critical_paths.py` run solo) because a monolithic
run gets OOM-killed on the 10 GiB heavy-run cage. Coverage accumulates with
`--cov-append`; the `--cov-fail-under` floor applies on the final chunk only.
