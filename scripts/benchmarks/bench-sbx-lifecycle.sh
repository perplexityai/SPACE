#!/usr/bin/env bash
# Benchmark sandbox lifecycle operations (17 tests).
#
# All lifecycle commands use --blocking. For "to running+ready" operations this
# relies on the spaced guest/host channel delivering ReadyNotification to
# sbx-manager; for stop/delete/suspend it waits until the target lifecycle state
# is reached.
#
# Tests:                          Pre-state
#  1. create (cold)               —
#  2. create (base-snapshot)      —
#  3. create (pool)               pool exists
#  4. start (cold)                created(cold)
#  5. start (base-snapshot)       created(base-snap)
#  6. stop                        running+ready
#  7. stop (immediate-kill)       running+ready
#  8. delete                      stopped
#  9. pause                       running+ready
# 10. resume (paused)             paused (manually)
# 11. resume (pool)               paused (from pool adopt)
# 12. suspend                     running+ready
# 13. restore                     suspended
# 14. checkpoint                  running+ready
# 15. create+start (cold)         — (E2E)
# 16. create+start (base-snap)    — (E2E)
# 17. create+resume (pool)        — (E2E)
#
# Usage:
#   ./scripts/benchmarks/bench-sbx-lifecycle.sh [<host>] [--template <tpl>] [--iterations <n>] [--environment <reference>] [--only <ops>]
#
# Remote SSH user defaults to deploy; override with SSH_USER=dev if needed.
#
# --only: comma-separated list of operations to run. If omitted, all run.
#   Operations: create_cold, create_base_snap, create_pool,
#     start_cold, start_base_snap, stop, stop_immediate, delete, pause,
#     resume_paused, resume_pool, suspend, restore,
#     checkpoint, e2e_cold, e2e_base_snap, e2e_pool
set -Eeuo pipefail

SSH_USER="${SSH_USER:-deploy}"
NODE_IP="utm-space-worker"
TEMPLATE="seed"
ITERATIONS=5
ENVIRONMENT_REFERENCE=""
BIN="/space/bin"
HAS_MEMFILE=0
HAS_POOL=0
ONLY=""
TMPDIR_BENCH=$(mktemp -d /tmp/bench-sbx-XXXXXX)
CLEANUP_IDS=()
STATE_FILE="$TMPDIR_BENCH/bench_state.log"
CURRENT_PHASE=""
CURRENT_ITER=0
CURRENT_SBX=""
ABORTED=0
ABORT_REASON=""
BENCHMARK_OK=0

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o BatchMode=yes
  -o ConnectTimeout=5
  -o ControlMaster=no
  -o ControlPath=none
  -o ControlPersist=no
)
SSH_TIMED_OPTS=(
  -o StrictHostKeyChecking=no
  -o BatchMode=yes
  -o ConnectTimeout=120
  -o ControlMaster=no
  -o ControlPath=none
  -o ControlPersist=no
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --template)   TEMPLATE="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --environment) ENVIRONMENT_REFERENCE="$2"; shift 2 ;;
    --only)       ONLY="$2"; shift 2 ;;
    --*)          echo "Unknown option: $1"; exit 1 ;;
    *)            NODE_IP="$1"; shift ;;
  esac
done

# Use one selection protocol for every measured environment, including E0.
# Only the legacy path supplies a runtime selector alongside the template.
if [[ -n "$ENVIRONMENT_REFERENCE" ]]; then
  if [[ ! "$ENVIRONMENT_REFERENCE" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "ERROR: --environment requires a SHA-256 environment reference" >&2
    exit 2
  fi
  CREATE_RUNTIME_ARGS=(--environment "$ENVIRONMENT_REFERENCE")
else
  CREATE_RUNTIME_ARGS=(--runtime fc)
fi
CREATE_RUNTIME_FLAGS="${CREATE_RUNTIME_ARGS[*]}"

# Check if a benchmark should run based on --only filter.
# Usage: should_run "resume_pool" || return
should_run() {
  [[ -z "$ONLY" ]] && return 0
  echo ",$ONLY," | grep -q ",$1,"
}

# ── Helpers ─────────────────────────────────────────────────────────────────

first_line() {
  local text="${1:-}"
  printf '%s\n' "${text%%$'\n'*}"
}

last_line() {
  local text="${1:-}"
  printf '%s\n' "${text##*$'\n'}"
}

ssh_with_retries() {
  local result rc attempt
  for attempt in 1 2 3 4 5; do
    set +eE
    result=$(ssh "$@" 2>&1)
    rc=$?
    set -eE
    if [[ "$rc" -eq 0 ]]; then
      echo "$result"
      return 0
    fi
    if [[ "$rc" -ne 255 || "$attempt" -eq 5 ]]; then
      echo "$result"
      return "$rc"
    fi
    echo "WARN [phase=$CURRENT_PHASE iter=$CURRENT_ITER]: ssh transport failed on attempt $attempt/5; retrying: $(first_line "$result")" >&2
    sleep "0.$(( attempt * 2 ))"
  done
}

remote() {
  ssh_with_retries "${SSH_OPTS[@]}" "$SSH_USER@$NODE_IP" "sudo $*"
}

# Time a command on the worker. Returns: line1=ms, rest=command output.
# Exit code propagates (non-zero when the remote command or ssh itself failed).
# On non-zero rc we still return the captured output so the caller can log it,
# but we ALSO write a diagnostic line to stderr identifying the failing phase.
remote_timed() {
  local cmd="$1"
  local result rc=0
  set +eE
  result=$(ssh_with_retries "${SSH_TIMED_OPTS[@]}" "$SSH_USER@$NODE_IP" "sudo bash -c '
    t0=\$(date +%s%N)
    out=\$($cmd 2>&1)
    rc=\$?
    t1=\$(date +%s%N)
    echo \$(( (t1 - t0) / 1000000 ))
    echo \"\$out\"
    exit \$rc
  '")
  rc=$?
  set -eE
  if [[ -z "$result" ]]; then
    echo "ERROR [phase=$CURRENT_PHASE iter=$CURRENT_ITER]: remote_timed got no output (ssh rc=$rc) for: $cmd" >&2
    return 1
  fi
  if [[ $rc -ne 0 ]]; then
    echo "ERROR [phase=$CURRENT_PHASE iter=$CURRENT_ITER]: remote rc=$rc for: $cmd" >&2
    printf '%s\n' "$result" | sed -n '1,40s/^/    /p' >&2
  fi
  echo "$result"
  return $rc
}

parse_sandbox_id() {
  local line
  while IFS= read -r line; do
    if [[ "$line" =~ ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
      return 0
    fi
    if [[ "$line" =~ ^[[:space:]]*Sandbox:[[:space:]]*([0-9a-f]{8})[[:space:]]*$ ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
      return 0
    fi
  done
  return 0
}

require_sandbox_id() {
  local context="$1" output="$2" id
  id=$(printf '%s\n' "$output" | parse_sandbox_id)
  if [[ -z "$id" ]]; then
    echo "ERROR: failed to parse sandbox ID from $context:" >&2
    printf '%s\n' "$output" >&2
    return 1
  fi
  printf '%s\n' "$id"
}

track_sandbox() { CLEANUP_IDS+=("$1"); CURRENT_SBX="$1"; }

# Phase / iteration tracking so aborts identify exactly where we stopped.
begin_phase() {
  CURRENT_PHASE="$1"
  CURRENT_ITER=0
  CURRENT_SBX=""
  local ts; ts=$(date '+%Y-%m-%dT%H:%M:%S%z')
  echo ""; echo "── $1 ── ($ts)"
  echo "phase=$1 ts=$ts" >> "$STATE_FILE"
}

begin_iter() {
  CURRENT_ITER="$1"
  CURRENT_SBX=""
  local ts; ts=$(date '+%Y-%m-%dT%H:%M:%S%z')
  echo "  iter=$1 ts=$ts phase=$CURRENT_PHASE" >> "$STATE_FILE"
}

create_sandbox() {
  local flags="${1:-}"
  # Selected runs keep periodic snapshots off so every arm measures the same
  # work; memory-state operations are requested separately against the
  # resulting bound sandbox.
  if [[ -n "$ENVIRONMENT_REFERENCE" && "$flags" == "--snapshots" ]]; then
    flags=""
  fi
  local out id
  out=$(remote "$BIN/space-sbxmgr" create "${CREATE_RUNTIME_ARGS[@]}" --template "$TEMPLATE" --blocking $flags 2>&1)
  id=$(require_sandbox_id "create output" "$out")
  track_sandbox "$id"
  echo "$id"
}

# Start a sandbox and wait for guest ready (uses --blocking).
start_sandbox_blocking() {
  local id="$1"; shift
  remote "$BIN/space-sbxmgr" start "$id" --blocking "$@" >/dev/null 2>&1
}

stop_sandbox() {
  local output
  if output=$(remote "$BIN/space-sbxmgr" stop "$1" --blocking 2>&1); then
    return 0
  else
    local rc=$?
    echo "$output" >&2
    return "$rc"
  fi
}
delete_sandbox() {
  local attempt output rc=0
  # Stop and suspend can finish before the node releases its cleanup lock.
  # This untimed, idempotent cleanup must settle before the next sample.
  # Missing targets already succeed in the CLI; never guess success from errors.
  for attempt in $(seq 1 10); do
    if output=$(remote "$BIN/space-sbxmgr" delete "$1" --blocking 2>&1); then
      return 0
    else
      rc=$?
    fi
    echo "WARN: cleanup attempt $attempt/10 for $1 (rc=$rc): $output" >&2
    if [[ "$attempt" -lt 10 ]]; then
      sleep 0.2
    fi
  done
  echo "ERROR: cleanup did not settle for $1 after 10 attempts" >&2
  return "$rc"
}

wait_sandbox_state() {
  local id="$1" want="$2" timeout="${3:-120}" short
  short="${id: -8}"
  for _ in $(seq 1 "$timeout"); do
    local list row
    list=$(remote "$BIN/space-sbxmgr" list 2>&1) || true
    row=$(echo "$list" | grep "$short" || true)
    if [[ -n "$row" ]] && echo "$row" | grep -q "$want"; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: sandbox $id did not reach state '$want' within ${timeout}s" >&2
  remote "$BIN/space-sbxmgr" list >&2 || true
  return 1
}

wait_pool_ready() {
  local iter="$1"
  for _ in $(seq 1 30); do
    local tpl_list tpl_row pool_info ready
    tpl_list=$(remote "$BIN/space-tmplmgr" list 2>&1) || true
    tpl_row=$(echo "$tpl_list" | grep "$TEMPLATE" || true)
    pool_info=$(echo "$tpl_row" | grep -oE '[0-9]+/[0-9]+' || true)
    if [[ -n "$pool_info" ]]; then
      ready=$(echo "$pool_info" | cut -d/ -f1)
      [[ "$ready" -gt 0 ]] && return 0
    fi
    echo "  [$iter/$ITERATIONS] waiting for pool..."
    sleep 2
  done
  return 1
}

# ── Statistics ──────────────────────────────────────────────────────────────

compute_stats() {
  local file="$1" samples count
  samples=$(grep -E '^[0-9]+$' "$file" || true)
  count=$(printf '%s\n' "$samples" | sed '/^$/d' | wc -l | tr -d ' ')
  [[ "$count" -eq 0 ]] && { echo "- - - -"; return; }
  local sorted min max median p95
  sorted=$(printf '%s\n' "$samples" | sort -n)
  min=$(first_line "$sorted")
  max=$(last_line "$sorted")
  median=$(echo "$sorted" | sed -n "$(( (count + 1) / 2 ))p")
  local p95_idx=$(( (count * 95 + 99) / 100 ))
  [[ "$p95_idx" -gt "$count" ]] && p95_idx=$count
  p95=$(echo "$sorted" | sed -n "${p95_idx}p")
  echo "$min $median $p95 $max"
}

record() {
  local op="$1" ms="$2"
  if [[ ! "$ms" =~ ^[0-9]+$ ]]; then
    echo "WARN [phase=$CURRENT_PHASE iter=$CURRENT_ITER]: skipping non-numeric timing sample for $op: $ms" >&2
    return 0
  fi
  echo "$ms" >> "$TMPDIR_BENCH/$op.dat"
}

# ── Cleanup / abort diagnostics ─────────────────────────────────────────────

# ERR trap: fires whenever set -e would exit. Prints a block identifying the
# exact phase, iteration, line, last command, rc, and in-flight sandbox so
# we never again exit "mid-way without any comment".
fail_trap() {
  local rc=$?
  local cmd="${BASH_COMMAND:-?}"
  local line="${BASH_LINENO[0]:-?}"
  ABORTED=1
  echo ""
  echo "=============================================="
  echo "BENCHMARK ABORTED"
  echo "  phase:     ${CURRENT_PHASE:-(not started)}"
  echo "  iteration: ${CURRENT_ITER}/${ITERATIONS}"
  echo "  in-flight: ${CURRENT_SBX:-(none)}"
  echo "  line:      $line"
  echo "  last cmd:  $cmd"
  echo "  exit rc:   $rc"
  echo "  reason:    ${ABORT_REASON:-set -e triggered (see last cmd)}"
  echo "  timestamp: $(date '+%Y-%m-%dT%H:%M:%S%z')"
  echo "  tracked:   ${CLEANUP_IDS[*]:-(none)}"
  echo "  state log: $STATE_FILE (preserved)"
  echo "=============================================="
}
trap fail_trap ERR

# Signal traps: distinguish external kill from internal set -e failure.
signal_trap() {
  ABORT_REASON="received signal $1"
  # Let the ERR+EXIT traps run with a non-zero status.
  exit 1
}
trap 'signal_trap HUP'  HUP
trap 'signal_trap INT'  INT
trap 'signal_trap TERM' TERM

cleanup() {
  if [[ "$BENCHMARK_OK" -eq 0 && "$ABORTED" -eq 0 ]]; then
    ABORTED=1
    ABORT_REASON="benchmark exited before completion"
  fi
  # Preserve state log on abort so the user can inspect after the fact.
  if [[ "$ABORTED" -eq 1 ]]; then
    echo ""
    echo "Partial results collected so far:"
    print_results 2>&1 || true
    echo "State log preserved at: $STATE_FILE"
    echo "Tracked sandbox IDs (attempting delete): ${CLEANUP_IDS[*]:-(none)}"
  fi
  if [[ ${#CLEANUP_IDS[@]} -gt 0 ]]; then
    echo ""
    echo "Cleaning up..."
    for id in "${CLEANUP_IDS[@]}"; do
      remote "$BIN/space-sbxmgr" delete "$id" --blocking 2>/dev/null || true
    done
  fi
  # Only remove tmpdir on clean exit; keep it on abort for forensics.
  if [[ "$ABORTED" -eq 0 ]]; then
    rm -rf "$TMPDIR_BENCH"
  else
    echo "Tmpdir preserved at: $TMPDIR_BENCH"
  fi
}
trap cleanup EXIT

# ── Prerequisites ───────────────────────────────────────────────────────────

check_prereqs() {
  echo "Checking prerequisites..."
  local tpl_list
  tpl_list=$(remote "$BIN/space-tmplmgr" list 2>&1) || {
    echo "ERROR: failed to list templates:"; echo "$tpl_list"; exit 1
  }
  if ! echo "$tpl_list" | grep -q "$TEMPLATE"; then
    echo "ERROR: template '$TEMPLATE' not found"; echo "$tpl_list"; exit 1
  fi
  echo "  template '$TEMPLATE': OK"

  local tpl_row
  tpl_row=$(echo "$tpl_list" | grep "$TEMPLATE" || true)

  local base_snap_cell
  base_snap_cell=$(echo "$tpl_row" | awk -F'┆' '{ cell=$(NF-1); gsub(/^[[:space:]]+|[[:space:]]+$/, "", cell); print cell }')
  if [[ "$base_snap_cell" == on* || "$base_snap_cell" == yes* ]]; then
    HAS_MEMFILE=1; echo "  base snapshot: OK"
  else
    echo "  base snapshot: NOT FOUND (base-snapshot tests will be skipped)"
  fi

  local pool_info
  pool_info=$(echo "$tpl_row" | grep -oE '[0-9]+/[0-9]+' || true)
  if [[ -n "$pool_info" ]]; then
    local pool_current
    pool_current=$(echo "$pool_info" | cut -d/ -f1)
    if [[ "$pool_current" -gt 0 ]]; then
      HAS_POOL=1; echo "  pool: OK ($pool_info)"
    else
      echo "  pool: 0 ready ($pool_info) (pool tests will be skipped)"
    fi
  else
    echo "  pool: NOT FOUND (pool tests will be skipped)"
  fi
}

# ── Benchmark Functions (17 operations) ─────────────────────────────────────

# 1. create (cold)
bench_create_cold() {
  begin_phase "create (cold)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local out ms id
    out=$(remote_timed "$BIN/space-sbxmgr create $CREATE_RUNTIME_FLAGS --template $TEMPLATE --blocking")
    ms=$(first_line "$out")
    id=$(require_sandbox_id "create (cold) output" "$out")
    record "create_cold" "$ms"
    echo "  [$i/$ITERATIONS] create (cold): ${ms}ms  ($id)"
    [[ -n "$id" ]] && remote "$BIN/space-sbxmgr" delete "$id" --blocking >/dev/null 2>&1 || true
  done
}

# 2. create (base-snapshot)
bench_create_base_snap() {
  begin_phase "create (base-snapshot)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local out ms id
    out=$(remote_timed "$BIN/space-sbxmgr create $CREATE_RUNTIME_FLAGS --template $TEMPLATE --boot-mode base-snapshot --blocking")
    ms=$(first_line "$out")
    id=$(require_sandbox_id "create (base-snapshot) output" "$out")
    record "create_base_snap" "$ms"
    echo "  [$i/$ITERATIONS] create (base-snapshot): ${ms}ms  ($id)"
    [[ -n "$id" ]] && remote "$BIN/space-sbxmgr" delete "$id" --blocking >/dev/null 2>&1 || true
  done
}

# 3. create (pool)
bench_create_pool() {
  begin_phase "create (pool)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    wait_pool_ready "$i" || { echo "  [$i/$ITERATIONS] SKIP: no pool"; continue; }
    local out ms id
    out=$(remote_timed "$BIN/space-sbxmgr create $CREATE_RUNTIME_FLAGS --template $TEMPLATE --boot-mode pool --blocking")
    ms=$(first_line "$out")
    id=$(require_sandbox_id "create (pool) output" "$out")
    record "create_pool" "$ms"
    echo "  [$i/$ITERATIONS] create (pool): ${ms}ms  ($id)"
    [[ -n "$id" ]] && remote "$BIN/space-sbxmgr" delete "$id" --blocking >/dev/null 2>&1 || true
  done
}

# 4. start (cold) — created(cold) → running+ready
bench_start_cold() {
  begin_phase "start (cold)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox)
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr start $id --blocking")
    ms=$(first_line "$out")
    record "start_cold" "$ms"
    echo "  [$i/$ITERATIONS] start (cold): ${ms}ms  ($id)"
    stop_sandbox "$id"; delete_sandbox "$id"
  done
}

# 5. start (base-snapshot) — created(base-snap) → running+ready
bench_start_base_snap() {
  begin_phase "start (base-snapshot)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    if [[ -n "$ENVIRONMENT_REFERENCE" ]]; then
      # Selected Create allocates a cold record; Start then restores the
      # template snapshot only when it was captured under that binding.
      id=$(create_sandbox)
    else
      id=$(create_sandbox "--boot-mode base-snapshot")
    fi
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr start $id --from-warm --blocking")
    ms=$(first_line "$out")
    record "start_base_snap" "$ms"
    echo "  [$i/$ITERATIONS] start (base-snapshot): ${ms}ms  ($id)"
    stop_sandbox "$id"; delete_sandbox "$id"
  done
}

# 6. stop — running+ready → stopped, with default graceful kill policy
bench_stop() {
  begin_phase "stop"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox)
    start_sandbox_blocking "$id"
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr stop $id --blocking")
    ms=$(first_line "$out")
    record "stop" "$ms"
    echo "  [$i/$ITERATIONS] stop: ${ms}ms  ($id)"
    delete_sandbox "$id"
  done
}

# 7. stop (immediate-kill) — running+ready → stopped, skipping SIGTERM grace
bench_stop_immediate() {
  begin_phase "stop (immediate-kill)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox)
    start_sandbox_blocking "$id"
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr stop $id --blocking --immediate-kill")
    ms=$(first_line "$out")
    record "stop_immediate" "$ms"
    echo "  [$i/$ITERATIONS] stop (immediate-kill): ${ms}ms  ($id)"
    delete_sandbox "$id"
  done
}

# 8. delete — created → gone
bench_delete() {
  begin_phase "delete"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox)
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr delete $id --blocking")
    ms=$(first_line "$out")
    record "delete" "$ms"
    echo "  [$i/$ITERATIONS] delete: ${ms}ms  ($id)"
  done
}

# 9. pause — running+ready → paused
bench_pause() {
  begin_phase "pause"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox)
    start_sandbox_blocking "$id"
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr pause $id --blocking")
    ms=$(first_line "$out")
    record "pause" "$ms"
    echo "  [$i/$ITERATIONS] pause: ${ms}ms  ($id)"
    stop_sandbox "$id"; delete_sandbox "$id"
  done
}

# 10. resume (paused) — manually paused → running+ready
bench_resume_paused() {
  begin_phase "resume (paused)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox)
    start_sandbox_blocking "$id"
    remote "$BIN/space-sbxmgr" pause "$id" --blocking >/dev/null 2>&1
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr resume $id --blocking")
    ms=$(first_line "$out")
    record "resume_paused" "$ms"
    echo "  [$i/$ITERATIONS] resume (paused): ${ms}ms  ($id)"
    stop_sandbox "$id"; delete_sandbox "$id"
  done
}

# 11. resume (pool) — pool-adopted paused → running+ready
bench_resume_pool() {
  begin_phase "resume (pool)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    wait_pool_ready "$i" || { echo "  [$i/$ITERATIONS] SKIP: no pool"; continue; }
    local out ms id
    # Adopt without autostart → paused sandbox
    out=$(remote "$BIN/space-sbxmgr" create "${CREATE_RUNTIME_ARGS[@]}" --template "$TEMPLATE" --boot-mode pool --blocking 2>&1)
    id=$(printf '%s\n' "$out" | parse_sandbox_id)
    [[ -z "$id" ]] && { echo "  [$i/$ITERATIONS] ERROR: adopt failed"; continue; }
    track_sandbox "$id"
    # Time the resume
    out=$(remote_timed "$BIN/space-sbxmgr resume $id --blocking")
    ms=$(first_line "$out")
    record "resume_pool" "$ms"
    echo "  [$i/$ITERATIONS] resume (pool): ${ms}ms  ($id)"
    remote "$BIN/space-sbxmgr" delete "$id" --blocking >/dev/null 2>&1 || true
  done
}

# 12. suspend — running+ready → suspended
bench_suspend() {
  begin_phase "suspend"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox "--snapshots")
    start_sandbox_blocking "$id"
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr suspend $id --blocking")
    ms=$(first_line "$out")
    record "suspend" "$ms"
    echo "  [$i/$ITERATIONS] suspend: ${ms}ms  ($id)"
    delete_sandbox "$id"
  done
}

# 13. restore — suspended → running+ready
bench_restore() {
  begin_phase "restore"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox "--snapshots")
    start_sandbox_blocking "$id"
    remote "$BIN/space-sbxmgr" suspend "$id" --blocking >/dev/null 2>&1
    wait_sandbox_state "$id" suspended
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr restore $id --blocking")
    ms=$(first_line "$out")
    record "restore" "$ms"
    echo "  [$i/$ITERATIONS] restore: ${ms}ms  ($id)"
    stop_sandbox "$id"; delete_sandbox "$id"
  done
}

# 14. checkpoint — running+ready → running+ready, with a full snapshot captured
bench_checkpoint() {
  begin_phase "checkpoint"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local id
    id=$(create_sandbox "--snapshots")
    start_sandbox_blocking "$id"
    local out ms
    out=$(remote_timed "$BIN/space-sbxmgr checkpoint $id --blocking")
    ms=$(first_line "$out")
    record "checkpoint" "$ms"
    echo "  [$i/$ITERATIONS] checkpoint: ${ms}ms  ($id)"
    stop_sandbox "$id"; delete_sandbox "$id"
  done
}

# 15. create+start (cold) — E2E
bench_e2e_cold() {
  begin_phase "create+start (cold)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local out ms id
    out=$(remote_timed "$BIN/space-sbxmgr create $CREATE_RUNTIME_FLAGS --template $TEMPLATE --autostart --boot-mode cold --blocking")
    ms=$(first_line "$out")
    id=$(require_sandbox_id "create+start (cold) output" "$out")
    record "e2e_cold" "$ms"
    echo "  [$i/$ITERATIONS] create+start (cold): ${ms}ms  ($id)"
    [[ -n "$id" ]] && remote "$BIN/space-sbxmgr" delete "$id" --blocking >/dev/null 2>&1 || true
  done
}

# 16. create+start (base-snapshot) — E2E
bench_e2e_base_snap() {
  begin_phase "create+start (base-snapshot)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    local out ms id
    out=$(remote_timed "$BIN/space-sbxmgr create $CREATE_RUNTIME_FLAGS --template $TEMPLATE --autostart --boot-mode base-snapshot --blocking")
    ms=$(first_line "$out")
    id=$(require_sandbox_id "create+start (base-snapshot) output" "$out")
    record "e2e_base_snap" "$ms"
    echo "  [$i/$ITERATIONS] create+start (base-snapshot): ${ms}ms  ($id)"
    [[ -n "$id" ]] && remote "$BIN/space-sbxmgr" delete "$id" --blocking >/dev/null 2>&1 || true
  done
}

# 17. create+resume (pool) — E2E
bench_e2e_pool() {
  begin_phase "create+resume (pool)"
  for i in $(seq 1 "$ITERATIONS"); do
    begin_iter "$i"
    wait_pool_ready "$i" || { echo "  [$i/$ITERATIONS] SKIP: no pool"; continue; }
    local out ms id
    out=$(remote_timed "$BIN/space-sbxmgr create $CREATE_RUNTIME_FLAGS --template $TEMPLATE --autostart --boot-mode pool --blocking")
    ms=$(first_line "$out")
    id=$(require_sandbox_id "create+resume (pool) output" "$out")
    record "e2e_pool" "$ms"
    echo "  [$i/$ITERATIONS] create+resume (pool): ${ms}ms  ($id)"
    [[ -n "$id" ]] && remote "$BIN/space-sbxmgr" delete "$id" --blocking >/dev/null 2>&1 || true
  done
}

# ── Results ─────────────────────────────────────────────────────────────────

print_results() {
  echo ""
  echo "=== Results ==="
  echo ""
  printf " %-26s %8s %8s %8s %8s\n" "Operation" "P95" "Median" "Min" "Max"
  printf " %-26s %8s %8s %8s %8s\n" "──────────────────────────" "──────" "──────" "──────" "──────"

  local ops=(
    "create_cold" "create_base_snap" "create_pool"
    "start_cold" "start_base_snap"
    "stop" "stop_immediate" "delete" "pause"
    "resume_paused" "resume_pool"
    "suspend" "restore" "checkpoint"
    "e2e_cold" "e2e_base_snap" "e2e_pool"
  )
  local labels=(
    "create (cold)" "create (base-snapshot)" "create (pool)"
    "start (cold)" "start (base-snapshot)"
    "stop" "stop (immediate-kill)" "delete" "pause"
    "resume (paused)" "resume (from-pool)"
    "suspend" "restore" "checkpoint"
    "create+start (cold)" "create+start (base-snap)" "create+resume (pool)"
  )

  for idx in "${!ops[@]}"; do
    local op="${ops[$idx]}" label="${labels[$idx]}"
    local file="$TMPDIR_BENCH/$op.dat"
    [[ ! -f "$file" ]] && continue
    local stats min med p95 max
    stats=$(compute_stats "$file")
    read -r min med p95 max <<< "$stats"
    printf " %-26s %6sms %6sms %6sms %6sms\n" "$label" "$p95" "$med" "$min" "$max"
  done
  echo ""
}

# ── Main ────────────────────────────────────────────────────────────────────

main() {
  echo "=== Sandbox Lifecycle Benchmark ==="
  echo "Host:       $NODE_IP"
  echo "Template:   $TEMPLATE"
  echo "Environment: ${ENVIRONMENT_REFERENCE:-legacy selection}"
  echo "Iterations: $ITERATIONS"
  echo "Started:    $(date '+%Y-%m-%dT%H:%M:%S%z')"
  echo "State log:  $STATE_FILE"
  echo ""

  CURRENT_PHASE="prereqs"
  check_prereqs

  # Create variants
  if should_run create_cold; then bench_create_cold; fi
  if should_run create_base_snap && [[ "$HAS_MEMFILE" -gt 0 && -z "$ENVIRONMENT_REFERENCE" ]]; then bench_create_base_snap; fi
  if should_run create_pool && [[ "$HAS_POOL" -gt 0 && -z "$ENVIRONMENT_REFERENCE" ]]; then bench_create_pool; fi

  # Start variants
  if should_run start_cold; then bench_start_cold; fi
  if should_run start_base_snap && [[ "$HAS_MEMFILE" -gt 0 ]]; then bench_start_base_snap; fi

  # Teardown
  if should_run stop; then bench_stop; fi
  if should_run stop_immediate; then bench_stop_immediate; fi
  if should_run delete; then bench_delete; fi
  if should_run pause; then bench_pause; fi

  # Resume variants
  if should_run resume_paused; then bench_resume_paused; fi
  if should_run resume_pool && [[ "$HAS_POOL" -gt 0 && -z "$ENVIRONMENT_REFERENCE" ]]; then bench_resume_pool; fi

  # Suspend / restore
  if should_run suspend; then bench_suspend; fi
  if should_run restore; then bench_restore; fi
  if should_run checkpoint; then bench_checkpoint; fi

  # End-to-end
  if should_run e2e_cold; then bench_e2e_cold; fi
  if should_run e2e_base_snap && [[ "$HAS_MEMFILE" -gt 0 && -z "$ENVIRONMENT_REFERENCE" ]]; then bench_e2e_base_snap; fi
  if should_run e2e_pool && [[ "$HAS_POOL" -gt 0 && -z "$ENVIRONMENT_REFERENCE" ]]; then bench_e2e_pool; fi

  CURRENT_PHASE="results"
  print_results
  BENCHMARK_OK=1
  echo "=============================================="
  echo "BENCHMARK COMPLETED"
  echo "  finished: $(date '+%Y-%m-%dT%H:%M:%S%z')"
  echo "=============================================="
}

main
