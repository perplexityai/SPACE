#!/usr/bin/env bash
# Uprev net-gateways fleet-wide: cycle every stale per-sandbox net-gateway
# sidecar so it relaunches from its node's current on-disk binary.
#
# Runs `space-sbxmgr cycle-net-gateway` on every worker node, --batch nodes
# at a time. Dry-run by default (selection
# via the node's sandbox-manager pod, no root). With --execute, each node's
# cycles run as host root in a privileged Kubernetes Job; the script waits for
# it, prints its logs, and deletes it. Cohort Jobs are retained for inspection
# if logs remain unavailable after retries (the normal TTL still applies).
# Nodes whose selection is already empty
# are skipped without a Job, so a converged fleet sweeps in seconds. Jobs clean themselves up even if the
# script dies: activeDeadlineSeconds kills a wedged run after 1h and
# ttlSecondsAfterFinished has the cluster delete finished leftovers within 1h.
# Manual sweep:
#   kubectl --context <ctx> -n default delete jobs -l app.kubernetes.io/name=netgtw-cycle-fleet
#
# Usage:
#   cycle-fleet.sh --context <ctx>                       # show what would cycle
#   cycle-fleet.sh --context <ctx> --execute
#   cycle-fleet.sh --context <ctx> --execute --batch 10 --min-age 1h
#   cycle-fleet.sh --context <ctx> --execute --hand-over-sockets
#   cycle-fleet.sh --context <ctx> --node <node-name> --refresh-config
#   cycle-fleet.sh --context <ctx> --node <node-name> --max 2 -- <sandbox-id> <sandbox-id>
#   cycle-fleet.sh --context <ctx> --node <node-name> --refresh-config --export-ids <file>
#   cycle-fleet.sh --context <ctx> --node <node-name> --ids-file <file>
#
# --batch N runs N nodes concurrently (default 10); cycles within a node stay
# sequential with the CLI's pacing. Any other extra args are passed through
# to `space-sbxmgr cycle-net-gateway` (e.g. --min-age 2h, --max 20,
# --sleep 5s). Default --max is 100000 so one run drains a node, and default
# --sleep is 0.5s — the CLI's 2s pacing is what dominates a fleet sweep.
# --hand-over-sockets, passed through the same way, keeps new guest traffic
# sent during each cycle, DNS included, on nodes that support the handover;
# other nodes stop and relaunch the gateway and drop it as without the flag.
# Connections the old gateway already relays drop either way (see
# docs/space/aws/net-gateway-cycle-runbook.md).
# Exits nonzero if any node had a failed preview or cycle.
# --node selects one exact host (including canary workers); without it the
# existing regular-worker fleet scope is unchanged. --refresh-config selects
# all live gateways regardless of SHA, to reread boot flags. It is mutually
# exclusive with --stale. Repeating it selects the same gateways again.
# IDs after -- select only named sandboxes and require --node. Put all options
# before --; do not combine IDs with --stale or --refresh-config.
# --export-ids writes a new file of full UUIDs without cycling (requires --node).
# --ids-file reads one full UUID per line as an explicit selection on --node.

set -euo pipefail

NS=space
DEBUG_NS=default
IMAGE=public.ecr.aws/docker/library/busybox:latest
CONTEXT=""
EXECUTE=0
BATCH=10
TARGET_NODE=""
REFRESH_CONFIG=0
EXPLICIT_STALE=0
EXPLICIT_IDS=0
IDS_FILE=""
EXPORT_IDS=""
ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --context) CONTEXT="$2"; shift 2 ;;
    --execute) EXECUTE=1; shift ;;
    --batch)   BATCH="$2"; shift 2 ;;
    --node) TARGET_NODE="${2:?--node requires a node name}"; shift 2 ;;
    --refresh-config) REFRESH_CONFIG=1; shift ;;
    --stale) EXPLICIT_STALE=1; shift ;;
    --ids-file) IDS_FILE="${2:?--ids-file requires a path}"; shift 2 ;;
    --export-ids) EXPORT_IDS="${2:?--export-ids requires a path}"; shift 2 ;;
    --ids-only) echo "ERROR: use --export-ids <file> to save a selection" >&2; exit 2 ;;
    --)
      shift
      [ $# -gt 0 ] || { echo "ERROR: -- requires sandbox IDs" >&2; exit 2; }
      for id in "$@"; do
        [[ "$id" =~ ^[a-fA-F0-9][a-fA-F0-9-]*$ ]] || { echo "ERROR: invalid sandbox ID" >&2; exit 2; }
      done
      EXPLICIT_IDS=1
      SANDBOX_IDS=("$@")
      break
      ;;
    -h|--help) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
[ -n "$CONTEXT" ] || { echo "ERROR: --context is required" >&2; exit 2; }
[[ "$BATCH" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --batch must be positive" >&2; exit 2; }
if [ -n "$TARGET_NODE" ]; then
  node_pattern='^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$'
  [[ "$TARGET_NODE" =~ $node_pattern ]] || { echo "ERROR: invalid --node" >&2; exit 2; }
fi
read_ids() {
  local id
  SANDBOX_IDS=()
  ID_COUNT=0
  while IFS= read -r id || [ -n "$id" ]; do
    [ -n "$id" ] || continue
    [[ "$id" =~ ^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$ ]] \
      || { echo "ERROR: expected one full sandbox UUID per line" >&2; return 1; }
    SANDBOX_IDS+=("$id")
    ID_COUNT=$((ID_COUNT + 1))
  done
  if [ "$ID_COUNT" -gt 0 ] && [ -n "$(printf '%s\n' "${SANDBOX_IDS[@]}" | tr 'A-F' 'a-f' | sort | uniq -d)" ]; then
    echo "ERROR: duplicate sandbox IDs" >&2
    return 1
  fi
}
if [ -n "$IDS_FILE" ]; then
  [ "$EXPLICIT_IDS" -eq 0 ] || { echo "ERROR: use either --ids-file or IDs after --" >&2; exit 2; }
  read_ids < "$IDS_FILE" || exit 2
  [ "$ID_COUNT" -gt 0 ] || { echo "ERROR: --ids-file is empty" >&2; exit 2; }
  EXPLICIT_IDS=1
fi
if [ -n "$EXPORT_IDS" ]; then
  [ -n "$TARGET_NODE" ] && [ "$EXECUTE" -eq 0 ] && [ "$EXPLICIT_IDS" -eq 0 ] \
    || { echo "ERROR: --export-ids requires --node and cannot execute or read explicit IDs" >&2; exit 2; }
  [ ! -e "$EXPORT_IDS" ] && [ ! -L "$EXPORT_IDS" ] \
    || { echo "ERROR: export path already exists: $EXPORT_IDS" >&2; exit 2; }
fi
if [ "$EXPLICIT_IDS" -eq 1 ]; then
  [ -n "$TARGET_NODE" ] || { echo "ERROR: explicit sandbox IDs require --node" >&2; exit 2; }
  [ "$REFRESH_CONFIG" -eq 0 ] && [ "$EXPLICIT_STALE" -eq 0 ] \
    || { echo "ERROR: explicit sandbox IDs conflict with --stale and --refresh-config" >&2; exit 2; }
fi
# Bash 3.2 treats empty arrays as unset under nounset. Apply defaults before
# prepending a selection flag so subsequent ARGS expansions are nonempty.
case " ${ARGS[*]-} " in *" --max "*|*" --max="*) ;; *) ARGS+=(--max 100000) ;; esac
# Pace each node to at most two cycles per second. SimpleDuration syntax is
# decimal + s/m/h/d — "500ms" is not a valid spelling.
case " ${ARGS[*]} " in *" --sleep "*|*" --sleep="*) ;; *) ARGS+=(--sleep 0.5s) ;; esac
if [ "$EXPLICIT_IDS" -eq 1 ]; then
  ARGS+=(-- "${SANDBOX_IDS[@]}")
elif [ "$REFRESH_CONFIG" -eq 1 ]; then
  [ "$EXPLICIT_STALE" -eq 0 ] || { echo "ERROR: --refresh-config conflicts with --stale" >&2; exit 2; }
  ARGS=(--refresh-config "${ARGS[@]}")
else
  ARGS=(--stale "${ARGS[@]}")
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CHART_DIR="$SCRIPT_DIR/cycle-job-chart"
if [ "$EXECUTE" -eq 1 ]; then
  [ -d "$CHART_DIR" ] || { echo "ERROR: missing chart $CHART_DIR" >&2; exit 1; }
  command -v helm >/dev/null || { echo "ERROR: helm is required for --execute" >&2; exit 1; }
fi

kc() { kubectl --context "$CONTEXT" "$@"; }

# Renders the checked-in per-node Job chart (see comments there for the
# privilege and self-cleanup rationale). `helm template` only — the Job is
# applied with kubectl and TTLs out on its own, so a Helm release record
# would outlive it.
job_manifest() {
  local name=$1 node=$2 values rc=0
  shift 2
  values=$(mktemp)
  {
    printf 'name: "%s"\nnamespace: "%s"\nnode: "%s"\nimage: "%s"\ncliArgs:\n' \
      "$name" "$DEBUG_NS" "$node" "$IMAGE"
    printf '  - "%s"\n' "$@"
  } > "$values"
  helm template netgtw-cycle "$CHART_DIR" --values "$values" || rc=$?
  rm -f "$values"
  return "$rc"
}

run_node() {
  local node=$1 pod=$2
  local cycle_args=("${ARGS[@]}")
  echo "== $node"

  if [ "$EXECUTE" -ne 1 ]; then
    kc exec -n "$NS" "$pod" -c sandbox-manager -- \
      space-sbxmgr cycle-net-gateway "${ARGS[@]}" </dev/null 2>&1 | sed 's/^/  /'
    return
  fi

  # Cheap selection first: a privileged Job costs pod scheduling, an image
  # pull, and poll latency per node. A failed preview cannot establish the
  # cohort, so no Job should run for that node.
  local sel
  if [ "$REFRESH_CONFIG" -eq 1 ]; then
    # Selection requires the new pod CLI; execution uses the existing host
    # explicit-ID interface, so the host need not understand refresh flags.
    sel=$(kc exec -n "$NS" "$pod" -c sandbox-manager -- \
      space-sbxmgr cycle-net-gateway --ids-only "${ARGS[@]}" </dev/null) \
      || { echo "  ERROR: cannot select refresh cohort; no Job created"; return 1; }
    read_ids <<< "$sel" \
      || { echo "  ERROR: invalid refresh selection; no Job created"; return 1; }
    if [ "$ID_COUNT" -eq 0 ]; then
      echo "  No net-gateways selected."
      return 0
    fi
    cycle_args=("${ARGS[@]:1}" -- "${SANDBOX_IDS[@]}")
  else
    if ! sel=$(kc exec -n "$NS" "$pod" -c sandbox-manager -- \
        space-sbxmgr cycle-net-gateway "${ARGS[@]}" </dev/null 2>&1); then
      printf '%s\n' "$sel" | sed 's/^/  /'
      echo "  ERROR: preview failed; no Job created"
      return 1
    fi
    case "$sel" in
      *"No net-gateways selected."*|*"No sandboxes on this node."*)
        printf '%s\n' "$sel" | sed 's/^/  /'
        [ "$EXPLICIT_IDS" -eq 0 ] || return 1
        return 0
        ;;
    esac
  fi

  if [ "$REFRESH_CONFIG" -eq 1 ] || [ "$EXPLICIT_IDS" -eq 1 ]; then
    # Older host CLIs log short labels. Keep the requested cohort in the
    # same output so operators can recover full UUIDs for a retry file.
    echo "  Requested cohort (host --max/--min-age still apply):"
    printf '    %s\n' "${SANDBOX_IDS[@]}"
  fi

  local name out phase waited logs
  name="netgtw-cycle-${node%%.*}-$$"
  out=$(job_manifest "$name" "$node" "${cycle_args[@]}" | kc create -f - 2>&1) \
    || { echo "  ERROR: creating job $name failed: $out"; return 1; }

  phase=""
  waited=0
  while :; do
    phase=$(kc get pods -n "$DEBUG_NS" -l job-name="$name" \
      -o jsonpath='{.items[0].status.phase}' </dev/null 2>/dev/null || true)
    case "$phase" in Succeeded|Failed) break ;; esac
    if [ "$waited" -ge 3600 ]; then break; fi
    sleep 2; waited=$((waited + 2))
  done
  local log_attempt logs_available=0
  for log_attempt in 1 2 3; do
    if logs=$(kc logs --request-timeout=15s -n "$DEBUG_NS" "job/$name" </dev/null 2>&1); then
      logs_available=1
      break
    fi
    [ "$log_attempt" -eq 3 ] || sleep 2
  done
  printf '%s\n' "$logs" | sed 's/^/  /'
  # Older host CLIs can exit successfully after skipping cohort members.
  # Their logs are required to establish whether the cohort completed.
  if [ "$logs_available" -eq 0 ] && { [ "$REFRESH_CONFIG" -eq 1 ] || [ "$EXPLICIT_IDS" -eq 1 ]; }; then
    echo "  ERROR: cannot retrieve Job logs after 3 attempts; cohort completion is unknown"
    echo "  Retained job/$name for inspection; automatic cleanup is due 1h after Job completion"
    printf '  Recover logs: kubectl --context %q -n %q logs %q\n' "$CONTEXT" "$DEBUG_NS" "job/$name"
    return 1
  fi
  kc delete job -n "$DEBUG_NS" "$name" --wait=false </dev/null >/dev/null 2>&1 || true

  if [ "$REFRESH_CONFIG" -eq 1 ] || [ "$EXPLICIT_IDS" -eq 1 ]; then
    case "$logs" in
      *"skipped, no live net-gateway:"*|*"No net-gateways selected."*|*"No sandboxes on this node."*)
        echo "  ERROR: selected cohort contains gateways that were not cycled"
        return 1
        ;;
    esac
  fi
  if [ "$phase" != Succeeded ]; then
    if [ "$REFRESH_CONFIG" -eq 1 ] || [ "$EXPLICIT_IDS" -eq 1 ] \
      || [ "$phase" != Failed ] || ! failed_run_tolerable "$logs"; then
      echo "  node did not converge (phase=${phase:-timeout})"
      return 1
    fi
  fi
}

# A lifecycle transition may make a stale selection non-cyclable before its RPC.
# Refresh and explicit-ID runs must instead report incomplete replacements.
failed_run_tolerable() {
  local logs=$1 errors preconditions
  errors=$(printf '%s\n' "$logs" | grep -c '^ERROR: ' || true)
  preconditions=$(printf '%s\n' "$logs" | grep '^ERROR: cycle ' \
    | grep -Eci 'failedprecondition|not in a state required' || true)
  [ "$errors" -gt 0 ] && [ "$errors" -eq "$preconditions" ]
}

# One sandbox-manager pod per node, preferring Ready: a DaemonSet surge
# rollout leaves two Running pods on a node, and a duplicate node would
# create the same Job name twice (AlreadyExists).
dedupe_node_pods() {
  sort -k1,1 -k3,3r | awk '$1 != "" && !seen[$1]++ { print $1, $2 }'
}

POD_SELECTOR=app.kubernetes.io/component=sandbox-manager,app.kubernetes.io/instance=space-worker
if [ -n "$TARGET_NODE" ]; then
  POD_SELECTOR=app.kubernetes.io/component=sandbox-manager
fi
PODS=$(kc get pods -n "$NS" \
  -l "$POD_SELECTOR" \
  --field-selector="status.phase=Running${TARGET_NODE:+,spec.nodeName=$TARGET_NODE}" \
  -o jsonpath='{range .items[*]}{.spec.nodeName} {.metadata.name} {.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
  | dedupe_node_pods)
[ -n "$PODS" ] || { echo "ERROR: no matching running sandbox-manager pods in -n $NS (node=${TARGET_NODE:-regular-workers})" >&2; exit 1; }
if [ -n "$EXPORT_IDS" ]; then
  read -r node pod <<< "$PODS"
  export_tmp=$(mktemp "${EXPORT_IDS}.tmp.XXXXXX")
  trap 'rm -f "$export_tmp"' EXIT
  kc exec -n "$NS" "$pod" -c sandbox-manager -- \
    space-sbxmgr cycle-net-gateway --ids-only "${ARGS[@]}" </dev/null > "$export_tmp"
  read_ids < "$export_tmp"
  # Publish a complete export without overwriting another saved cohort.
  ln "$export_tmp" "$EXPORT_IDS"
  echo "Exported $ID_COUNT sandbox IDs for $CONTEXT / $TARGET_NODE to $EXPORT_IDS"
  exit 0
fi
NODE_COUNT=$(printf '%s\n' "$PODS" | grep -c . || true)
echo "sweeping $NODE_COUNT worker node(s) on $CONTEXT, $BATCH at a time ($([ "$EXECUTE" -eq 1 ] && echo EXECUTE || echo dry-run))"
if [ "$REFRESH_CONFIG" -eq 1 ]; then
  echo "Configuration refresh includes current/unknown SHAs. This selection does not shrink after cycling; do not rerun capped batches to continue."
fi

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

FAILED=0
DONE=0
pids=()
files=()

# Wait for the in-flight batch, printing each node's output as it finishes —
# completion order, not launch order, so one busy node cannot gate the
# display — with a heartbeat while stragglers run.
flush() {
  local i remaining=${#pids[@]} ticks=0
  if [ "$remaining" -eq 0 ]; then return; fi
  while [ "$remaining" -gt 0 ]; do
    for i in "${!pids[@]}"; do
      if [ -z "${pids[$i]}" ]; then continue; fi
      if kill -0 "${pids[$i]}" 2>/dev/null; then continue; fi
      if ! wait "${pids[$i]}"; then FAILED=$((FAILED + 1)); fi
      cat "${files[$i]}"
      pids[$i]=""
      remaining=$((remaining - 1))
      DONE=$((DONE + 1))
      echo "-- $DONE/$NODE_COUNT node(s) done"
    done
    if [ "$remaining" -gt 0 ]; then
      sleep 1
      ticks=$((ticks + 1))
      if [ $((ticks % 30)) -eq 0 ]; then
        echo "-- still waiting on $remaining node(s) in this batch"
      fi
    fi
  done
  pids=()
  files=()
}

n=0
while read -r node pod; do
  [ -n "$node" ] || continue
  f="$WORKDIR/$n.out"
  n=$((n + 1))
  run_node "$node" "$pod" > "$f" 2>&1 &
  pids+=("$!")
  files+=("$f")
  if [ "${#pids[@]}" -ge "$BATCH" ]; then flush; fi
done <<< "$PODS"
flush

if [ "$FAILED" -gt 0 ]; then
  echo "$FAILED node(s) had failures — inspect logs before retrying; configuration-refresh selection may include gateways already cycled" >&2
  exit 1
fi
