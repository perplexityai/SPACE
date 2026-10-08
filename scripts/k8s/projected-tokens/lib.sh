#!/usr/bin/env bash
# shellcheck disable=SC2034  # config values below are read by scripts that source this file
# Shared config + helpers for the projected-token (Kubernetes TokenReview) PoC
# on a local kind cluster. Source this from the numbered scripts.
#
# Override any value via the environment, e.g.:
#   KCTX=kind-space-local NS=space ./01-apply-auth.sh
set -euo pipefail

# kube context + namespace for the local stack. KCTX defaults to your current
# kube-context — whatever `kind create` / `tilt up` selected — so it tracks the
# cluster Tilt actually brought up regardless of its name (the Tiltfile derives
# the name from SPACE_KIND_CLUSTER, default "space-local"). The mutating
# scripts call assert_local_context, which refuses any non-kind context so this
# can never touch testing/prod.
KCTX="${KCTX:-$(kubectl config current-context 2>/dev/null || echo kind-space-local)}"
NS="${NS:-space}"

# Server side: the worker node-api DaemonSet (space-node-api-local), container
# `node-api`, ServiceAccount `space-node-api`.
NODE_API_DS="${NODE_API_DS:-space-node-api}"
NODE_API_CONTAINER="${NODE_API_CONTAINER:-node-api}"
NODE_API_SA="${NODE_API_SA:-space-node-api}"
NODE_API_PORT="${NODE_API_PORT:-9800}"

# Client side: space-api dials node-api. It runs as SA `space-api`, container
# `api`.
SPACE_API_DEPLOY="${SPACE_API_DEPLOY:-space-api}"
SPACE_API_CONTAINER="${SPACE_API_CONTAINER:-api}"
SPACE_API_SA="${SPACE_API_SA:-space-api}"

# Audiences (must match the node-api server config and the client volume).
NODE_API_AUDIENCE="${NODE_API_AUDIENCE:-space-node-api}"
NODE_API_INTERNAL_AUDIENCE="${NODE_API_INTERNAL_AUDIENCE:-space-node-api-internal}"

# Which client ServiceAccounts node-api accepts on the NodeApi surface.
# space-api is the caller under test; the controller and cluster-api entries
# keep the local wiring aligned with the deployed node-api allowlist.
NODE_API_ALLOWED_SAS="${NODE_API_ALLOWED_SAS:-space-api,space-cluster-controller,space-cluster-api}"
# Exact subset authorized for tokenless SpacedWaitReady lifecycle probes.
NODE_API_TRUSTED_WAIT_READY_SAS="${NODE_API_TRUSTED_WAIT_READY_SAS:-space-api,space-cluster-controller}"
# Independent subset authorized to supply network/auto-lifecycle policy and generations.
NODE_API_TRUSTED_POLICY_SAS="${NODE_API_TRUSTED_POLICY_SAS:-space-api,space-cluster-controller}"
# nodeApiInternal accepts space-api (control-plane request path) and
# space-cluster-controller (warm-pool WorkerApiInternal/WakeSandbox). Mirrors the
# node-api chart allowlist.
NODE_API_INTERNAL_ALLOWED_SAS="${NODE_API_INTERNAL_ALLOWED_SAS:-space-api,space-cluster-controller}"

# ServiceAccount identities for the internal-surface test in 02-test.sh: one that
# is on the nodeApiInternal allowlist, one deliberately not (to prove enforcement).
INTERNAL_ALLOWED_SA="${INTERNAL_ALLOWED_SA:-space-cluster-controller}"
INTERNAL_DENIED_SA="${INTERNAL_DENIED_SA:-space-pt-unlisted}"

# Where the client mounts its normal and internal projected NodeApi tokens.
TOKEN_MOUNT="${TOKEN_MOUNT:-/var/run/space-auth}"
TOKEN_FILE="${TOKEN_FILE:-node-api.token}"
TOKEN_PATH="${TOKEN_MOUNT}/${TOKEN_FILE}"
INTERNAL_TOKEN_FILE="${INTERNAL_TOKEN_FILE:-node-api-internal.token}"
INTERNAL_TOKEN_PATH="${TOKEN_MOUNT}/${INTERNAL_TOKEN_FILE}"

# Local port for the node-api port-forward used by the test script.
LOCAL_NODE_API_PORT="${LOCAL_NODE_API_PORT:-19800}"

k() { kubectl --context "$KCTX" -n "$NS" "$@"; }

log() { printf '\033[1;34m[pt]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[pt] WARN:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[pt] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# Resolve one worker node-api pod name.
node_api_pod() {
  k get pod -l app.kubernetes.io/name=space-worker,app.kubernetes.io/component=node-api \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

require() { command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"; }

# Refuse to run cluster-mutating steps against anything that isn't a local kind
# cluster, so the harness can never touch testing/prod. The 01/02/03 scripts call
# this.
assert_local_context() {
  case "$KCTX" in
    kind-*) log "targeting kube-context: $KCTX" ;;
    "") die "no kube-context set; bring up the local stack or set KCTX=kind-<name>" ;;
    *) die "refusing to run against non-kind context '$KCTX' — set KCTX=kind-<name> if intentional" ;;
  esac
}
