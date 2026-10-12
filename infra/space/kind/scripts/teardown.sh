#!/usr/bin/env bash
# teardown.sh — tear down the local kind SPACE stack brought up by bootstrap.sh.
#
# Steps (best-effort, idempotent, in safe order):
#   1. Stop the host-side Tilt watcher and scripted port-forwards.
#   2. Delete the kubernetes resources bootstrap.sh applied (it uses
#      `helm template | kubectl apply`, so there are no helm releases to
#      uninstall — the namespace deletes remove everything: the sandbox
#      namespace first, while the node services its pods' teardown needs
#      still run, then the control plane's).
#   3. `kind delete cluster` for the cluster bootstrap.sh created.
#
# Each step continues on failure so a partially-torn-down stack can always
# be brought to a clean state. Re-running the script when there's nothing to
# do is a no-op.
#
# Deliberately preserved (delete by hand if you really want a from-scratch
# rebuild): $SPACE_KIND_DATA_ROOT, default ~/.kind-data (per-role containerd
# image caches, including the dedicated builder cache),
# ~/.space-devbox/space/vendor (Firecracker vendor tree),
# ~/.space-devbox/kind/seed (built seed artifacts + build cache), and
# ~/.space-devbox/kind/ssh (the per-devbox sandbox SSH keypair).
#
# Usage:
#   bash infra/space/kind/scripts/teardown.sh
#   bash infra/space/kind/scripts/teardown.sh --keep-cluster   # remove workloads, keep kind cluster
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
NAMESPACE=space
SANDBOX_NAMESPACE=sandboxes
PF_PIDFILE="${SPACE_PORT_FORWARD_PIDFILE:-/tmp/space-local-pf.pids}"
TILT_STATE_DIR="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
TILT_PIDFILE="${SPACE_TILT_PIDFILE:-$TILT_STATE_DIR/tilt-${KIND_CLUSTER}.pid}"

KEEP_CLUSTER=0
while (( $# > 0 )); do
  case "$1" in
    --keep-cluster) KEEP_CLUSTER=1 ;;
    -h|--help)
      sed -n '2,/^set/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      printf 'unknown flag: %s\n' "$1" >&2
      exit 2
      ;;
  esac
  shift
done

log() { printf '[teardown] %s\n' "$*"; }

tilt_pid_is_expected() {
  local pid="$1" command
  command="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
  [[ "$command" =~ ^[[:space:]]*([^[:space:]]*/)?tilt[[:space:]]+up([[:space:]]|$) ]]
}

if [[ -s "$TILT_PIDFILE" ]]; then
  read -r tilt_pid <"$TILT_PIDFILE"
  if [[ "$tilt_pid" =~ ^[0-9]+$ ]] && tilt_pid_is_expected "$tilt_pid"; then
    log "stopping Tilt watcher (pid $tilt_pid)"
    kill "$tilt_pid" 2>/dev/null || true
  else
    log "removing stale Tilt pidfile without signaling pid ${tilt_pid:-unknown}"
  fi
  rm -f "$TILT_PIDFILE"
fi

# ── 1. Port-forwards ─────────────────────────────────────────────────────────
if [[ -s "$PF_PIDFILE" ]]; then
  log "killing port-forwards listed in $PF_PIDFILE"
  while read -r pid; do
    [[ -z "$pid" ]] && continue
    # Group kill: bootstrap.sh starts each supervisor under `setsid`, so the
    # recorded pid leads a group containing the loop and its kubectl child. A
    # plain `kill` takes the loop and leaves the child holding its port, with
    # nothing left tracking it.
    kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  done <"$PF_PIDFILE"
  rm -f "$PF_PIDFILE"
fi

KCTX="kind-$KIND_CLUSTER"

# A `tilt up` the pidfile does not name (started before the pidfile existed,
# or from a bring-up whose state dir differs) is found through the server it
# serves. When :10350 reports this cluster's context, that Tilt is about to
# lose its apiserver and would sit bound to a dead one — the stale watcher the
# next bring-up then has to recognise and replace (it does, but a cluster
# teardown that leaves its own watcher behind is the leak). A Tilt serving
# another context is left alone.
if command -v tilt >/dev/null 2>&1 && command -v curl >/dev/null 2>&1 \
    && curl -sf -m 2 http://localhost:10350/ >/dev/null 2>&1; then
  served_context="$(timeout 5s tilt get cluster default --port 10350 \
    -o 'jsonpath={.status.connection.kubernetes.context}' 2>/dev/null || true)"
  if [[ "$served_context" == "$KCTX" ]]; then
    for tilt_pid in $(pgrep -f "tilt up" 2>/dev/null || true); do
      tilt_pid_is_expected "$tilt_pid" || continue
      log "stopping Tilt watcher serving $KCTX on :10350 (pid $tilt_pid, not in the pidfile)"
      kill "$tilt_pid" 2>/dev/null || true
    done
  fi
fi

# ── 2. Workloads (only needed with --keep-cluster) ───────────────────────────
# bootstrap.sh applies rendered manifests with kubectl (no helm release
# bookkeeping), so removing the namespace removes every SPACE workload.
# When the cluster is being deleted anyway, skip this — `kind delete
# cluster` is faster than waiting for a graceful namespace drain.
if (( KEEP_CLUSTER )) && kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  log "deleting namespace $SANDBOX_NAMESPACE (sandbox pods, while the node services run)"
  kubectl --context "$KCTX" delete namespace "$SANDBOX_NAMESPACE" \
    --ignore-not-found --timeout=180s 2>/dev/null || true
  log "deleting namespace $NAMESPACE (removes all SPACE workloads)"
  kubectl --context "$KCTX" delete namespace "$NAMESPACE" \
    --ignore-not-found --timeout=180s 2>/dev/null || true
fi

# ── 3. kind cluster ──────────────────────────────────────────────────────────
if (( KEEP_CLUSTER )); then
  log "--keep-cluster: leaving kind cluster '$KIND_CLUSTER' alive"
elif kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
  log "kind delete cluster --name $KIND_CLUSTER"
  kind delete cluster --name "$KIND_CLUSTER"
fi

log "done."
