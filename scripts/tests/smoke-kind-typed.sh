#!/usr/bin/env bash
# Run the portable typed smoke inventory against the local Kind/Firecracker stack.
# Usage: scripts/tests/smoke-kind-typed.sh [all|ecw|network_policy|local|native] [INDEX/COUNT]
# Each shard needs its own isolated Kind stack; scenarios within it stay serial.
#   `all` is the full inventory: every scenario on the ECW API surface, then every scenario
#   that also supports the native /v1 surface on that surface. The two passes are separate
#   runner processes because the surface is process-wide; `all` runs both, repeats both
#   passes' summaries (counts and every scenario that did not pass) at the end, and exits
#   with the first nonzero status. `native` runs only the second pass. Both need
#   SPACE_API_TOKEN (an admin session bearer token) or SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE to
#   read one from.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TARGET="${1:-ecw}"
SHARD_ARGS=()
if (( $# > 2 )); then
  echo "ERROR: expected a target and optional shard INDEX/COUNT" >&2
  exit 2
fi
if [[ -n "${2:-}" ]]; then
  SHARD_ARGS=(--shard "$2")
fi

if [[ ! -c /dev/kvm || ! -r /dev/kvm || ! -w /dev/kvm ]]; then
  echo "ERROR: the typed Kind smoke requires readable and writable /dev/kvm" >&2
  echo "       Run it on a Linux KVM devbox, not a laptop-only Kind cluster." >&2
  exit 2
fi
if ! command -v uv >/dev/null 2>&1; then
  echo "ERROR: uv is required; run infra/space/kind/scripts/install-prereqs.sh" >&2
  exit 2
fi

# As in ci/scripts/run-python-tests.sh: CI checks that uv.lock still matches
# pyproject.toml (--locked); local runs use the lock as committed (--frozen),
# which a host uv with its own resolver settings would otherwise rewrite.
UV_LOCK_MODE=--frozen
if [[ -n "${CI:-}" ]]; then
  UV_LOCK_MODE=--locked
fi

require_native_token() {
  if [[ -z "${SPACE_API_TOKEN:-}" && -n "${SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE:-}" ]]; then
    SPACE_API_TOKEN="$(<"$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE")"
    export SPACE_API_TOKEN
  fi
  if [[ -z "${SPACE_API_TOKEN:-}" ]]; then
    echo "ERROR: the native API surface needs SPACE_API_TOKEN or SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE" >&2
    exit 2
  fi
  # The native client defaults its data plane to the API URL; on Kind the public data
  # plane is port-forwarded separately (same default the ECW surface uses).
  export SPACE_DATA_PLANE_URL="${SPACE_DATA_PLANE_URL:-http://127.0.0.1:8090}"
}

# Recovery participates in the normal native inventory on this dedicated Kind
# bench. Snapshot its identity here; every mutation revalidates that identity.
prepare_native_kind() {
  require_native_token
  local target="kind-${KIND_CLUSTER:-space-local}"
  if [[ -n "${SPACE_SMOKE_NATIVE_RECOVERY_CONTEXT:-}" && "$SPACE_SMOKE_NATIVE_RECOVERY_CONTEXT" != "$target" ]]; then
    echo "ERROR: native recovery context does not match KIND_CLUSTER" >&2
    exit 2
  fi
  local uid
  uid="$(kubectl --context "$target" get namespace kube-system -o jsonpath='{.metadata.uid}')"
  if [[ -z "$uid" || ( -n "${SPACE_SMOKE_NATIVE_RECOVERY_CLUSTER_UID:-}" && "$SPACE_SMOKE_NATIVE_RECOVERY_CLUSTER_UID" != "$uid" ) ]]; then
    echo "ERROR: native recovery cluster identity does not match the Kind bench" >&2
    exit 2
  fi
  export SPACE_SMOKE_NATIVE_RECOVERY=1
  export SPACE_SMOKE_NATIVE_TENANT_FIXTURES=1
  export SPACE_SMOKE_NATIVE_RECOVERY_CONTEXT="$target"
  export SPACE_SMOKE_NATIVE_RECOVERY_CLUSTER_UID="$uid"
}

# `local.private_template_build_isolation` needs a second organization with a
# service user. Organizations are not per-run fixtures (a deleted template keeps
# its ownership record), so one stable organization per bench, found by slug or
# created on first use, serves every run. Without an operator token this does
# nothing and the scenario reports its own missing input.
BUILD_FOREIGN_ORG_SLUG="space-smoke-build-foreign"

ensure_build_foreign_org() {
  [[ -n "${SPACE_SMOKE_BUILD_FOREIGN_ORG_ID:-}" ]] && return 0
  local token="${SPACE_API_TOKEN:-}"
  if [[ -z "$token" && -n "${SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE:-}" ]]; then
    token="$(<"$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE")"
  fi
  [[ -n "$token" ]] || return 0
  for bin in curl jq; do
    if ! command -v "$bin" >/dev/null 2>&1; then
      echo "ERROR: $bin is required to provision the build test organization" >&2
      exit 2
    fi
  done
  local admin="${API_URL:-http://127.0.0.1:8080}/admin/api/v1"
  admin_call() {
    local args=(-fsS -X "$1" "$admin/$2"
      -H "Authorization: Bearer $token"
      -H 'Content-Type: application/json'
      -H "Idempotency-Key: $(cat /proc/sys/kernel/random/uuid)")
    if [[ $# -ge 3 ]]; then
      args+=(-d "$3")
    fi
    curl "${args[@]}"
  }
  local org_id
  org_id="$(admin_call GET orgs | jq -r --arg slug "$BUILD_FOREIGN_ORG_SLUG" \
    'first(.[] | select(.slug == $slug) | .id) // empty')"
  if [[ -z "$org_id" ]]; then
    org_id="$(admin_call POST orgs "$(jq -nc --arg slug "$BUILD_FOREIGN_ORG_SLUG" \
      '{name: "space-smoke build test organization", slug: $slug}')" | jq -r '.id')"
  fi
  if [[ "$(admin_call GET "orgs/$org_id/users" | jq 'length')" == 0 ]]; then
    # The scenario seeds its own API keys for this user; the password is never used.
    admin_call POST "orgs/$org_id/users" "$(jq -nc \
      --arg email "service@${BUILD_FOREIGN_ORG_SLUG}.local" \
      --arg password "$(head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')" \
      '{email: $email, password: $password, display_name: "space-smoke build service user", role: "member"}')" \
      >/dev/null
  fi
  export SPACE_SMOKE_BUILD_FOREIGN_ORG_ID="$org_id"
  echo "--- build test organization ${BUILD_FOREIGN_ORG_SLUG}: ${org_id}"
}

run_surface() {
  local surface="$1"; shift
  echo "--- typed smoke on the ${surface} API surface"
  SPACE_SMOKE_API_SURFACE="$surface" uv run "$UV_LOCK_MODE" --python 3.11 -m space_smoke run "$@" "${SHARD_ARGS[@]}"
}

cd "$REPO_ROOT"
export SPACE_SMOKE_BACKEND=kind

case "$TARGET" in
  all)
    # Check the native prerequisites before the long ECW pass so a missing token
    # fails in seconds, not after the whole inventory has run.
    prepare_native_kind
    ensure_build_foreign_org
    logs="$(mktemp -d)"
    trap 'rm -rf "$logs"' EXIT
    ecw_rc=0
    native_rc=0
    run_surface ecw --all 2>&1 | tee "$logs/ecw" || ecw_rc=$?
    run_surface native --all 2>&1 | tee "$logs/native" || native_rc=$?
    # Each pass ends on its SUITE count and a NOT PASSED block, but by now the ECW
    # pass's is above the whole native pass. Repeat both where the log ends, minus
    # the GitHub annotation lines, which would otherwise be raised twice. A pass
    # that stopped before running anything has no SUITE line; show its last lines.
    echo
    echo "=== typed smoke summary: ecw exit ${ecw_rc}, native exit ${native_rc} ==="
    for surface in ecw native; do
      echo "--- ${surface} API surface"
      if grep -q '^SUITE: ' "$logs/$surface"; then
        sed -n '/^SUITE: /,$p' "$logs/$surface" | grep -v '^::' || true
      else
        tail -n 20 "$logs/$surface"
      fi
    done
    if [[ "$ecw_rc" -ne 0 ]]; then
      exit "$ecw_rc"
    fi
    exit "$native_rc"
    ;;
  native)
    prepare_native_kind
    exec env SPACE_SMOKE_API_SURFACE=native uv run "$UV_LOCK_MODE" --python 3.11 -m space_smoke run --all "${SHARD_ARGS[@]}"
    ;;
  ecw|network_policy|local)
    if [[ "$TARGET" == local ]]; then
      ensure_build_foreign_org
    fi
    exec env SPACE_SMOKE_API_SURFACE=ecw uv run "$UV_LOCK_MODE" --python 3.11 -m space_smoke run --category "$TARGET" "${SHARD_ARGS[@]}"
    ;;
  *)
    echo "ERROR: unknown typed smoke target: $TARGET" >&2
    echo "       use all, ecw, network_policy, local, or native" >&2
    exit 2
    ;;
esac
