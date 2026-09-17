#!/usr/bin/env bats
# Scenario tests for scripts/lite-indexer-stall-check.sh — regression suite for
# the 2026-09 restart-thrash incidents (catch-up restarts, env skip, solana
# false-positive, throttle semantics).

setup() {
  HELPERS="$(dirname "$BATS_TEST_FILENAME")/helpers"
  SAMPLES="$(dirname "$BATS_TEST_FILENAME")/../../scripts/lite-indexer-stall-check.sh"
  SHIM_LOG="$BATS_TEST_TMPDIR/calls.log"
  export SHIM_LOG
  export PATH="$HELPERS:$PATH"
  rename_shims
  export PYTHON="$HELPERS/stub_python"
  export SERVICE="lite-indexer.service"
  export RESTART_MARKER="$BATS_TEST_TMPDIR/last-restart"
  export THROTTLE_STREAK_FILE="$BATS_TEST_TMPDIR/streak"
  export CATCHUP_NOTIFY_FILE="$BATS_TEST_TMPDIR/catchup-ts"
  export NOTIFY_TS_FILE="$BATS_TEST_TMPDIR/notify-ts"
  export LITE_INDEXER_ENV_FILE="$BATS_TEST_TMPDIR/fixture.env"
  export SOL_STATE_FILE="$BATS_TEST_TMPDIR/highwater_solana.json"
  export MIGRATION_LOCK="$BATS_TEST_TMPDIR/no-migration.lock"
  export SHIM_SERVICE_ACTIVE="active"
  export SHIM_HEALTH_RC=1
  echo '{"last_block": 100}' >"$SOL_STATE_FILE"
}

# shims must be named like the real binaries
rename_shims() {
  for pair in "shim_systemctl systemctl" "shim_docker docker" "shim_curl curl"; do
    set -- $pair
    ln -sf "$HELPERS/$1" "$BATS_TEST_TMPDIR/$2"
  done
  export PATH="$BATS_TEST_TMPDIR:$HELPERS:$PATH"
}

run_check() { bash "$SAMPLES"; }

@test "healthy: all chains fresh, no state lag -> exit 0, no action" {
  export SHIM_HEALTH_JSON='{"base": {"status": "ok", "state_age_seconds": 5}}'
  export SHIM_HEALTH_RC=0
  export SHIM_FRESH_LINE="base"
  run run_check
  [ "$status" -eq 0 ]
  ! grep -q "systemctl restart" "$SHIM_LOG"
}

@test "service inactive -> starts it, exit 3" {
  export SHIM_SERVICE_ACTIVE="inactive"
  run run_check
  [ "$status" -eq 3 ]
  grep -q "systemctl start" "$SHIM_LOG"
}

@test "catch-up (stale + fresh activity elsewhere) -> NO restart, exit 1" {
  export SHIM_HEALTH_JSON='{"blast": {"status": "stale", "state_age_seconds": 6000}}'
  export SHIM_FRESH_LINE="base ethereum"
  run run_check
  [ "$status" -eq 1 ]
  ! grep -q "systemctl restart" "$SHIM_LOG"
  [[ "$output" == *"daemon active"* ]] || [[ "$output" == *"catching up"* ]]
}

@test "frozen daemon (no fresh, no-advance) -> restart, exit 2" {
  export SHIM_HEALTH_JSON='{"blast": {"status": "stale", "state_age_seconds": 6000}}'
  export SHIM_FRESH_LINE=""
  run run_check
  [ "$status" -eq 2 ]
  grep -q "systemctl restart" "$SHIM_LOG"
}

@test "restart cooldown: recent restart -> throttled, no restart, streak=1" {
  export SHIM_HEALTH_JSON='{"blast": {"status": "stale", "state_age_seconds": 6000}}'
  export SHIM_FRESH_LINE=""
  date +%s >"$RESTART_MARKER"
  run run_check
  [ "$status" -eq 1 ]
  ! grep -q "systemctl restart" "$SHIM_LOG"
  [ "$(cat "$THROTTLE_STREAK_FILE" 2>/dev/null)" = "1" ]
}

@test "throttle streak at limit -> forced restart" {
  export SHIM_HEALTH_JSON='{"blast": {"status": "stale", "state_age_seconds": 6000}}'
  export SHIM_FRESH_LINE=""
  date +%s >"$RESTART_MARKER"
  echo 3 >"$THROTTLE_STREAK_FILE"
  run run_check
  [ "$status" -eq 2 ]
  grep -q "systemctl restart" "$SHIM_LOG"
}

@test "migration in progress -> skip everything, exit 0" {
  touch "$MIGRATION_LOCK"
  export SHIM_HEALTH_JSON='{"blast": {"status": "stale", "state_age_seconds": 6000}}'
  run run_check
  [ "$status" -eq 0 ]
  ! grep -q "systemctl restart" "$SHIM_LOG"
}

@test "lag check timeout -> exit 4, no restart" {
  export SHIM_HEALTH_RC=124
  run run_check
  [ "$status" -eq 4 ]
  ! grep -q "systemctl restart" "$SHIM_LOG"
}

@test "solana silent-extraction: gap>200k, cooldown ok -> reset written + restart, exit 2" {
  export SHIM_HEALTH_JSON='{"base": {"status": "ok", "state_age_seconds": 5}}'
  export SHIM_HEALTH_RC=0
  export SHIM_FRESH_LINE="base"
  echo '{"last_block": 5000000}' >"$SOL_STATE_FILE"
  export SHIM_SOL_DATA=4000000
  run run_check
  [ "$status" -eq 2 ]
  grep -q '"last_block": 4000000' "$SOL_STATE_FILE"
  grep -q "systemctl restart" "$SHIM_LOG"
}

@test "solana guard deferred when cooldown active (no reset overwrite)" {
  export SHIM_HEALTH_JSON='{"base": {"status": "ok", "state_age_seconds": 5}}'
  export SHIM_HEALTH_RC=0
  export SHIM_FRESH_LINE="base"
  echo '{"last_block": 5000000}' >"$SOL_STATE_FILE"
  export SHIM_SOL_DATA=4000000
  date +%s >"$RESTART_MARKER"
  run run_check
  [ "$status" -eq 1 ]
  grep -q '"last_block": 5000000' "$SOL_STATE_FILE"
}
