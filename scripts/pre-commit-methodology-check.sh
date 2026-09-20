#!/usr/bin/env bash
# Blocks commits that contain internal infrastructure details or methodology.
#
# ── WHY THE COMMITTED PATTERN LIST IS DELIBERATELY VAGUE ──────────────────────
# This file is installed into PUBLIC repos, so the pattern list is published with
# it. An enumerated list of our hostnames, secrets-manager paths and internal
# directory layout would therefore LEAK exactly what this gate exists to protect.
# That is not hypothetical: until 2026-09-20 the list named our hosts, our gopass
# path scheme and real deploy directories, in 14 repos, and it is in their
# permanent git history.
#
# So the committed set below is SHAPE-BASED ONLY. The specific literals load at
# runtime from an UNCOMMITTED, gitignored file:
#
#     .methodology-patterns        (override path: $METHODOLOGY_PATTERNS)
#
# One pattern per line, plain regex, '#' comments ignored. Absent that file the
# generic shapes still catch the common cases - the gate never fails open and
# never hard-errors on a missing override.
#
# Canonical source: standards/scripts/pre-commit-methodology-check.sh
set -euo pipefail

# Files allowed to contain these patterns: security scanner definitions (the
# detector, not the detected) and canonical infra documentation.
EXEMPT_FILES=(
    "scripts/pre-commit-methodology-check.sh"
    "scripts/dev/pre-commit-methodology-check.sh"
    ".gitleaks.toml"
    "SITE_OWNERSHIP.md"
    "METHODOLOGY-PROTECTION.md"
    ".methodology-patterns"
    "fleet/lead-awareness.md"
    "scripts/check-antipatterns.py"
    "scripts/check-structure.py"
    "scripts/check-monkeypatch.py"
    "scripts/ci-gate-selftest.sh"
    "scripts/check-structure.py"
    "scripts/check-baseline-freshness.py"
)

FORBIDDEN_PATTERNS=(
    # CGNAT shared address space (RFC 6598) - a public standard range, not our
    # allocation, so naming it reveals nothing.
    '100\.6[4-9]\.'
    '100\.7[0-9]\.'
    '100\.8[0-9]\.'
    '100\.9[0-9]\.'
    '100\.1[01][0-9]\.'

    # Secrets-manager INVOCATION - names the tool class, never our path scheme.
    '\b(gopass|vault|op|pass)[[:space:]]+(show|insert|get|edit)\b'

    # Internal filesystem SHAPES - the structure, not the real names.
    '/srv/[a-z0-9_-]+/[a-z0-9_-]+'
    '/root/(backend|frontend)\b'
    '/var/www/[a-z0-9_-]+'

    # Bare internal-hostname shapes. Deliberately does NOT list our hosts.
    '[a-z0-9-]+\.(internal|intranet|corp|lan)\b'

    # Methodology keywords (high-signal, generic).
    'proprietary.*algorithm'
    'internal[^.]*methodology'
    'competitor.*analysis.*method'
    'our.*scraping.*target'
    'production.*deploy.*root@'
)

# ── uncommitted, environment-specific literals ───────────────────────────────
# Our actual hostnames, path scheme and internal names live HERE, never in git.
LOCAL_PATTERNS="${METHODOLOGY_PATTERNS:-.methodology-patterns}"
if [ -f "$LOCAL_PATTERNS" ]; then
    while IFS= read -r raw || [ -n "$raw" ]; do
        line="${raw%%#*}"
        line="$(printf '%s' "$line" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
        [ -n "$line" ] && FORBIDDEN_PATTERNS+=("$line")
    done <"$LOCAL_PATTERNS"
fi

is_exempt() {
    local f="$1" exempt
    for exempt in "${EXEMPT_FILES[@]}"; do
        case "$f" in
        "$exempt") return 0 ;;
        esac
    done
    return 1
}

FOUND=0
STAGED_FILES="$(git diff --cached --name-only --diff-filter=ACM 2>/dev/null || true)"

if [ -z "$STAGED_FILES" ]; then
    echo "✅ Methodology check: clean (no staged files)"
    exit 0
fi

for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
    while IFS= read -r file; do
        [ -n "$file" ] || continue
        is_exempt "$file" && continue
        [ -f "$file" ] || continue
        if grep -lqE -- "$pattern" "$file" 2>/dev/null; then
            echo "❌ FORBIDDEN PATTERN MATCH in $file"
            echo "   (pattern withheld - see .methodology-patterns; it is an"
            echo "    internal-infrastructure guard, not reproduced here)"
            FOUND=1
        fi
    done <<<"$STAGED_FILES"
done

if [ "$FOUND" -eq 1 ]; then
    echo ""
    echo "🚫 COMMIT BLOCKED: Methodology leak detected."
    echo "   These patterns reveal internal infrastructure or methods."
    echo "   See: METHODOLOGY-PROTECTION.md"
    echo ""
    echo "   If this is a false positive, use:"
    echo "   git commit --no-verify  (with Lead approval)"
    exit 1
fi

echo "✅ Methodology check: clean"
exit 0
