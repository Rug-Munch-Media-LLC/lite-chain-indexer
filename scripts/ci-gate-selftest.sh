#!/usr/bin/env bash
# ci-gate-selftest.sh — prove the cost/quality gates ACTUALLY catch violations.
#
# Builds a throwaway fixture repo and injects one violation at a time. Every
# injection MUST make its gate exit 1; a clean fixture MUST exit 0. Any
# mismatch exits non-zero, which fails the CI quality job.
#
# Covered assertions:
#   (i)   501-line file                -> structure gate exits 1
#   (ii)  >300 new non-allowlisted file -> structure gate exits 1
#   (iii) allowlist-growth case        -> reported as advisory, never blocks
#   (iv)  +1 monkeypatch occurrence    -> monkeypatch gate exits 1
#   (v)   +1 antipattern occurrence    -> antipattern gate exits 1 AND names
#         the offending file:line, and refuses to raise a baseline silently
#   (0)   clean fixture                -> both gates exit 0
#
# Usage: bash scripts/ci-gate-selftest.sh
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STRUCT="$REPO_ROOT/scripts/check-structure.py"
MONKEY="$REPO_ROOT/scripts/check-monkeypatch.py"
ANTI="$REPO_ROOT/scripts/check-antipatterns.py"
MYPY_BUDGET="$REPO_ROOT/scripts/check-mypy-budget.py"

PY="${PYTHON:-python3}"
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    PY="$REPO_ROOT/.venv/bin/python"
fi

FIXTURE="$(mktemp -d)"
trap 'rm -rf "$FIXTURE"' EXIT

fails=0
pass() { printf '  ok    %s\n' "$1"; }
fail() {
    printf '  FAIL  %s (rc=%s)\n' "$1" "${2:-?}"
    fails=$((fails + 1))
}

reset_fixture() {
    rm -rf "$FIXTURE"
    mkdir -p "$FIXTURE/src/rugbrain" "$FIXTURE/scripts"
    printf 'x = 1\n' >"$FIXTURE/src/rugbrain/small.py"
}

# gen <n> <file> — write exactly n lines.
gen() {
    local n="$1" f="$2" i=0
    : >"$f"
    while [ "$i" -lt "$n" ]; do
        printf '# line\n' >>"$f"
        i=$((i + 1))
    done
}

struct_rc() {
    "$PY" "$STRUCT" --repo "$FIXTURE" >/dev/null 2>&1
    echo $?
}
monkey_rc() {
    "$PY" "$MONKEY" --repo "$FIXTURE" >/dev/null 2>&1
    echo $?
}
anti_rc() {
    "$PY" "$ANTI" --repo "$FIXTURE" >/dev/null 2>&1
    echo $?
}

expect_rc() { # expect_rc <want> <actual> <label>
    if [ "$2" -eq "$1" ]; then
        pass "$3"
    else
        fail "$3" "$2"
    fi
}

echo "gate-selftest: clean fixture must PASS (exit 0)"
reset_fixture
expect_rc 0 "$(struct_rc)" "clean fixture (structure)"
expect_rc 0 "$(monkey_rc)" "clean fixture (monkeypatch)"

echo "gate-selftest: injected violations must FAIL (exit 1)"
reset_fixture
gen 501 "$FIXTURE/src/rugbrain/god.py"
expect_rc 1 "$(struct_rc)" "501-line file -> exit 1"

reset_fixture
gen 301 "$FIXTURE/src/rugbrain/over.py"
expect_rc 1 "$(struct_rc)" "301-line non-allowlisted (new file over the 300 ceiling) -> exit 1"

reset_fixture
printf '{"files": {"src/rugbrain/growth.py": 205}}' >"$FIXTURE/scripts/structure-baseline.json"
gen 301 "$FIXTURE/src/rugbrain/growth.py"
out="$("$PY" "$STRUCT" --repo "$FIXTURE" 2>&1)"
if printf '%s' "$out" | grep -q "allowlisted file grew"; then
    pass "allowlist growth is REPORTED (advisory, not a push blocker)"
else
    fail "allowlist growth reported" "?"
fi

reset_fixture
printf '{"total": 0, "files": {}}' >"$FIXTURE/scripts/monkeypatch-baseline.json"
printf "import monkeypatch as monkeypatch\nmonkeypatch.setattr(x, 'y', z)\n" \
    >"$FIXTURE/src/rugbrain/mp.py"
expect_rc 1 "$(monkey_rc)" "+1 monkeypatch -> exit 1"

# (v) antipattern ratchet: growth fails, names the file:line, and refuses to
#     raise the baseline silently. A gate that reports only a count is ignored.
reset_fixture
printf '{"rules": {"monkeypatch": 0}, "total": 0}\n' >"$FIXTURE/scripts/antipattern-baseline.json"
printf 'import mock\n\ntry:\n    pass\nexcept ValueError:\n    pass\n' >"$FIXTURE/src/rugbrain/bad.py"
expect_rc 1 "$(anti_rc)" "antipattern growth -> exit 1"
anti_out="$("$PY" "$ANTI" --repo "$FIXTURE" 2>&1)"
if printf '%s' "$anti_out" | grep -q "src/rugbrain/bad.py:5"; then
    pass "antipattern failure names the offending file:line"
else
    fail "antipattern offender line" "?"
fi
expect_rc 1 "$("$PY" "$ANTI" --repo "$FIXTURE" --update-baseline >/dev/null 2>&1; echo $?)" \
    "REFUSES to raise the baseline without --justify"
expect_rc 0 "$("$PY" "$ANTI" --repo "$FIXTURE" --update-baseline --justify "selftest" >/dev/null 2>&1; echo $?)" \
    "--justify records the raise"

echo "gate-selftest: new-behavior assertions"
reset_fixture
gen 201 "$FIXTURE/src/rugbrain/over.py"
printf '{"files": {"src/rugbrain/over.py": 201}}' >"$FIXTURE/scripts/structure-baseline.json"
expect_rc 0 "$(struct_rc)" "allowlisted 201-line file passes (shrink-only)"
if "$PY" "$STRUCT" --repo "$FIXTURE" --debt | grep -q "src/rugbrain/over.py: 201"; then
    pass "--debt ledger lists allowlisted file"
else
    fail "--debt ledger" "?"
fi

reset_fixture
for i in 1 2 3 4 5; do printf 'def f%s():\n    pass\n\n' "$i" >>"$FIXTURE/src/rugbrain/dense.py"; done
out="$("$PY" "$STRUCT" --repo "$FIXTURE" 2>&1)"
if printf '%s' "$out" | grep -q "new file fn-density"; then
    pass "fn-density advisory reported"
else
    fail "fn-density advisory" "?"
fi
expect_rc 0 "$(struct_rc)" "fn-density is advisory (exit 0)"

reset_fixture
printf '{"total": 0, "files": {}}' >"$FIXTURE/scripts/monkeypatch-baseline.json"
printf "x = monkeypatch.getattr(m, 'n')\n" >"$FIXTURE/src/rugbrain/prod_mp.py"
expect_rc 1 "$(monkey_rc)" "monkeypatch in production -> exit 1 (hard zero)"

echo "gate-selftest: mypy-budget must refuse a DEAD type-check"
# A ratchet that cannot run must not read as "clean". Shadow `mypy` with a
# module that raises on import: the gate must exit non-zero rather than
# reporting a phantom "0 errors, PASS". (This is how a dead provider tier
# shipped looking healthy — the check ran, and checked nothing.)
reset_fixture
mkdir -p "$FIXTURE/scripts" "$FIXTURE/shadow"
printf 'raise ImportError("gate-selftest: simulated mypy is not installed")\n' \
    >"$FIXTURE/shadow/mypy.py"
printf '{"scope": "lite_indexer/", "total": 129, "files": {}}\n' \
    >"$FIXTURE/scripts/mypy-baseline.json"
out="$(PYTHONPATH="$FIXTURE/shadow" "$PY" "$MYPY_BUDGET" --repo "$FIXTURE" 2>&1)"
rc=$?
if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -q "INDETERMINATE"; then
    pass "dead mypy -> exit $rc, reported INDETERMINATE (not PASS)"
else
    fail "dead mypy must not PASS" "$rc"
fi

if [ "$fails" -ne 0 ]; then
    echo "gate-selftest: FAIL ($fails assertion(s) did not match)"
    exit 1
fi
echo "gate-selftest: PASS — every gate catches its violation class"
exit 0
