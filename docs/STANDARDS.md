# House Standard — lite-chain-indexer

Canonical statement of the quality gates. CI-enforced reality: every rule below
is checked by a gate in `.forgejo/workflows/ci.yml` and proven to catch its
violation class by `scripts/ci-gate-selftest.sh`.

## Module size (scripts/check-structure.py)

- **>200 lines = HARD FAIL** for any file not in the shrink-only allowlist
  (`scripts/structure-baseline.json`). >500 = unconditional hard fail.
- **>150 lines = advisory** report.
- **NEW files with >4 top-level function/class defs = advisory** (`new file
  fn-density`) — the one-primary-function direction, made visible.
- **Allowlisted files may never grow.** Growth = hard fail.
- `--debt` prints the ledger: every allowlisted file's deficit to 200, sorted
  desc. This is the "we only go down" list — pick from it when paying debt.
- `--update-baseline` **refuses increases** and refuses to grandfather new
  >200-line files. Baselines are shrink-only, period.

## One primary function per module

Target: one primary function per module, ~50 lines each. If a module contains
more than a single function it is too big — split, don't discuss. Enforced
directionally today (fn-density advisory); less is always more.

## Zero monkeypatch in production (scripts/check-monkeypatch.py)

- **`monkeypatch.` in `lite_indexer/` = HARD FAIL.** Replace with a DI seam.
- Test-suite monkeypatching stays under the repo-wide decrease-only ratchet
  (`scripts/monkeypatch-baseline.json`, currently 512). The test count is debt:
  it may only go down.

## Shrink-only ratchets

Every baseline (structure, monkeypatch, antipatterns) only ever decreases.
Loosening a ratchet to make CI green IS the violation. No monkeypatching, no
HACK/WORKAROUND/TEMP — fix root causes.

## Gates

| Gate | Script | Rule |
|------|--------|------|
| Structure | `scripts/check-structure.py` | ≤200 hard/new, fn-density advisory, shrink-only allowlist, `--debt` ledger |
| Monkeypatch | `scripts/check-monkeypatch.py` | zero in production, decrease-only test ratchet |
| Antipatterns | `scripts/check-antipatterns.py` | per-rule baselines, decrease-only |
| Self-test | `scripts/ci-gate-selftest.sh` | proves every gate catches its violation class |
