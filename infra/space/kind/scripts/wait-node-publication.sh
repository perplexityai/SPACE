#!/usr/bin/env bash
# wait-node-publication.sh — wait until every SPACE Kind node is published and
# schedulable through the production path.
#
# DaemonSet readiness says each node service is up; it does not say the node
# registered. Each node is published by its node-ctl through
# ClusterApi.PublishNodeState, and is usable once the API reports a fresh,
# reachable status whose scheduling verdict (derived from node-manager's
# network pool) is ready. This polls the admin node list
# (`GET /admin/api/v1/nodes`) as the local root account until every Kubernetes
# node in the sandbox and builder pools appears there with its role, a
# reachable status observed within the last five minutes, and scheduling
# status `ready`. A Kubernetes registration names its node by
# `k8s_node_name`; a Direct one (the Direct substrate lane,
# scripts/register-direct-nodes.sh) by the `name` it was registered with.
#
# The status's `stale` flag is not the test. It says only that the node
# published something recently, within a window derived from node-ctl's poll
# interval (crates/libs/node/api/client/src/node_status_freshness.rs), not that
# it sent a census. Freshness here is
# the window the API itself trusts a node's census for
# (CENSUS_INVENTORY_MAX_AGE, five minutes, which is five default polls;
# crates/libs/common/svc/src/sql/sandbox_residency.rs).
#
# Usage:
#   bash infra/space/kind/scripts/wait-node-publication.sh [timeout-seconds]
#
# Env knobs:
#   SPACE_KIND_CLUSTER      kind cluster name (default: space-local)
#   SPACE_CONSOLE_PASSWORD  root@local password (default: password)
#   SPACE_KIND_API_URL      API base URL; unset runs a private port-forward to
#                           svc/space-api for the duration of the wait
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
KCTX="kind-$KIND_CLUSTER"
TIMEOUT="${1:-600}"
PASSWORD="${SPACE_CONSOLE_PASSWORD-password}"
API_URL="${SPACE_KIND_API_URL:-}"
MAX_STATUS_AGE_SECS=300

log() { printf '[wait-node-publication] %s\n' "$*"; }
die() { printf '[wait-node-publication] ERROR: %s\n' "$*" >&2; exit 1; }

pf_pid=""
pf_log=""
cleanup() {
  [[ -z "$pf_pid" ]] || kill "$pf_pid" 2>/dev/null || true
  [[ -z "$pf_log" ]] || rm -f "$pf_log"
}
trap cleanup EXIT

if [[ -z "$API_URL" ]]; then
  pf_log="$(mktemp)"
  kubectl --context "$KCTX" -n space port-forward --address=127.0.0.1 svc/space-api :8080 \
    >"$pf_log" 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 50); do
    port="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "$pf_log" | head -n 1)"
    [[ -z "$port" ]] || break
    sleep 0.2
  done
  [[ -n "${port:-}" ]] || die "could not port-forward svc/space-api: $(cat "$pf_log")"
  API_URL="http://127.0.0.1:$port"
fi

# Kubernetes node → the SPACE role its node-ctl publishes.
expected="$(kubectl --context "$KCTX" get nodes \
  -l 'space.pplx.net/node-pool in (sandbox,builder)' \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.labels.space\.pplx\.net/node-pool}{"\n"}{end}' \
  | sed 's/ sandbox$/ worker/')"
[[ -n "$expected" ]] || die "no node labelled space.pplx.net/node-pool=sandbox or =builder in $KCTX"

token=""
deadline=$((SECONDS + TIMEOUT))
pending="$expected"
while true; do
  if [[ -z "$token" ]]; then
    token="$(jq -nc --arg password "$PASSWORD" '{email: "root@local", password: $password}' \
      | curl -fsS -m 5 -X POST "$API_URL/auth/v1/sessions" -H 'content-type: application/json' \
          --data-binary @- 2>/dev/null | jq -r '.token // empty' || true)"
  fi
  if [[ -n "$token" ]] && nodes="$(curl -fsS -m 5 -H "authorization: Bearer $token" \
      "$API_URL/admin/api/v1/nodes" 2>/dev/null)"; then
    pending="$(while read -r name role; do
      jq -e --arg name "$name" --arg role "$role" --argjson max_age "$MAX_STATUS_AGE_SECS" '
        any(.[]; (.k8s_node_name // .name) == $name and .role == $role and .deleted_at == null
            and .status.reachable == true and .status.scheduling.status == "ready"
            and ((.status.observed_at // "" | sub("\\.[0-9]+"; "") | fromdateiso8601? // 0)
                 > (now - $max_age)))' <<<"$nodes" >/dev/null \
        || printf '%s %s\n' "$name" "$role"
    done <<<"$expected")"
    [[ -n "$pending" ]] || break
  else
    token=""
  fi
  if (( SECONDS >= deadline )); then
    {
      printf 'nodes not published as ready within %ss:\n' "$TIMEOUT"
      while read -r name role; do
        printf '  %s (%s): %s\n' "$name" "$role" "$(jq -c --arg name "$name" \
          '[.[] | select((.k8s_node_name // .name) == $name) | {role, scheduling_status, status: (.status // {} | {observed_at, reachable, scheduling: .scheduling.status, error})}]' \
          <<<"${nodes:-[]}" 2>/dev/null || echo unknown)"
      done <<<"$pending"
    } >&2
    exit 1
  fi
  sleep 2
done
log "every SPACE node is published and ready: $(awk '{printf "%s(%s) ", $1, $2}' <<<"$expected")"
