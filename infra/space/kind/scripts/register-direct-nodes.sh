#!/usr/bin/env bash
# register-direct-nodes.sh — give each SPACE Kind node a Direct node identity
# and register it in the Direct node registry, for the Direct substrate lane
# (SPACE_KIND_SUBSTRATE=direct; "Direct substrate lane" in
# infra/space/kind/README.md). The Tiltfile runs both steps on that lane only.
#
#   seed-ids   Before the node services start: write a random node UUID to
#              /space/etc/node-id on every node in the sandbox and builder
#              pools that has none, as a Direct host is provisioned. On this
#              lane the node-service charts leave SPACE_NODE_ID unset
#              (space-node-service.env.identity), so node-api, node-ctl and
#              sandbox-manager report that UUID. Kept across reruns.
#   register   Once the API and each node's node-api serve: register every such
#              node through the admin API (`POST /admin/api/v1/nodes`) with
#              `verify: true`, so the API asks the node-api for its id and role
#              (GetNodeInfo) and refuses a mismatch before writing the
#              `nodes`/`node_direct` row. The address is the node's InternalIP,
#              the source address of its host-network ClusterApi calls, which
#              ClusterApi matches against the registered one; the node-api
#              endpoint is that address on :9800. A node already registered
#              under its UUID is left as it is. Registration enqueues the
#              node's first state publish; scripts/wait-node-publication.sh
#              then waits for it to report ready.
#
# Usage:
#   bash infra/space/kind/scripts/register-direct-nodes.sh seed-ids
#   bash infra/space/kind/scripts/register-direct-nodes.sh register [timeout-seconds]
#
# Env knobs:
#   SPACE_KIND_CLUSTER      kind cluster name (default: space-local)
#   SPACE_CONSOLE_PASSWORD  root@local password (default: password)
#   SPACE_KIND_API_URL      API base URL; unset runs a private port-forward to
#                           svc/space-api for the duration of the run
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
KCTX="kind-$KIND_CLUSTER"
PASSWORD="${SPACE_CONSOLE_PASSWORD-password}"
API_URL="${SPACE_KIND_API_URL:-}"
NODE_ID_PATH=/space/etc/node-id
NODE_API_PORT=9800

log() { printf '[register-direct-nodes] %s\n' "$*"; }
die() { printf '[register-direct-nodes] ERROR: %s\n' "$*" >&2; exit 1; }

# One `<node> <role> <InternalIP>` line per node in the sandbox and builder
# pools. A Kind node's name is also its container's name.
space_nodes() {
  local nodes
  nodes="$(kubectl --context "$KCTX" get nodes \
    -l 'space.pplx.net/node-pool in (sandbox,builder)' \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.labels.space\.pplx\.net/node-pool}{" "}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{end}{"\n"}{end}' \
    | sed 's/ sandbox / worker /')"
  [[ -n "$nodes" ]] || die "no node labelled space.pplx.net/node-pool=sandbox or =builder in $KCTX"
  printf '%s\n' "$nodes"
}

seed_ids() {
  local nodes node _role _address id
  # Listed first: a process substitution would lose the listing's failure.
  nodes="$(space_nodes)"
  while read -r node _role _address; do
    # The node layer (space-layout.service) owns /space/etc; this only adds
    # the identity file to it, root-owned and world-readable as the
    # services that read it run unprivileged.
    id="$(docker exec "$node" sh -c '
      set -eu
      [ -d /space/etc ] || { echo "/space/etc is missing; the node layer has not run" >&2; exit 1; }
      if [ ! -s "$1" ]; then
        cat /proc/sys/kernel/random/uuid >"$1.tmp"
        chmod 0644 "$1.tmp"
        mv -f "$1.tmp" "$1"
      fi
      cat "$1"' sh "$NODE_ID_PATH")" || die "cannot seed $NODE_ID_PATH on $node"
    log "$node: node id $id"
  done <<<"$nodes"
}

pf_pid=""
pf_log=""
cleanup() {
  [[ -z "$pf_pid" ]] || kill "$pf_pid" 2>/dev/null || true
  [[ -z "$pf_log" ]] || rm -f "$pf_log"
}

start_port_forward() {
  local port=""
  trap cleanup EXIT
  pf_log="$(mktemp)"
  kubectl --context "$KCTX" -n space port-forward --address=127.0.0.1 svc/space-api :8080 \
    >"$pf_log" 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 50); do
    port="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "$pf_log" | head -n 1)"
    [[ -z "$port" ]] || break
    sleep 0.2
  done
  [[ -n "$port" ]] || die "could not port-forward svc/space-api: $(cat "$pf_log")"
  API_URL="http://127.0.0.1:$port"
}

login() {
  jq -nc --arg password "$PASSWORD" '{email: "root@local", password: $password}' \
    | curl -fsS -m 5 -X POST "$API_URL/auth/v1/sessions" -H 'content-type: application/json' \
        --data-binary @- 2>/dev/null | jq -r '.token // empty' || true
}

register() {
  local timeout="${1:-600}" deadline token="" nodes pending node role address id body status response last=""
  [[ -n "$API_URL" ]] || start_port_forward
  deadline=$((SECONDS + timeout))
  pending="$(space_nodes)"
  while [[ -n "$pending" ]]; do
    (( SECONDS < deadline )) \
      || die "nodes not registered within ${timeout}s: $(awk '{printf "%s ", $1}' <<<"$pending")${last:+(last answer: $last)}"
    [[ -n "$token" ]] || token="$(login)"
    if [[ -z "$token" ]] || ! nodes="$(curl -fsS -m 5 -H "authorization: Bearer $token" \
        "$API_URL/admin/api/v1/nodes" 2>/dev/null)"; then
      token=""
      sleep 3
      continue
    fi
    local still=""
    while read -r node role address; do
      id="$(docker exec "$node" cat "$NODE_ID_PATH")" \
        || die "$node has no $NODE_ID_PATH; run '$0 seed-ids' before the node services start"
      if jq -e --arg id "$id" 'any(.[]; .id == $id and .deleted_at == null)' <<<"$nodes" >/dev/null; then
        log "$node: registered as $id"
        continue
      fi
      body="$(jq -nc --arg id "$id" --arg name "$node" --arg address "$address" \
        --arg endpoint "$address:$NODE_API_PORT" --arg role "$role" \
        '{id: $id, name: $name, address: $address, node_api_endpoint: $endpoint, role: $role, verify: true}')"
      response="$(curl -sS -m 15 -o /dev/stdout -w '\n%{http_code}' -X POST "$API_URL/admin/api/v1/nodes" \
        -H "authorization: Bearer $token" -H 'content-type: application/json' \
        --data-binary "$body" 2>&1)" || true
      status="${response##*$'\n'}"
      case "$status" in
        201) log "$node: registered $role $id at $address:$NODE_API_PORT" ;;
        # The verification probe refused the node (role or id mismatch, a
        # malformed endpoint): retrying cannot change the answer.
        400) die "$node: registration refused: ${response%$'\n'*}" ;;
        # 502 is the probe not reaching node-api yet; anything else (an expired
        # session, an API still starting) is retried too, until the deadline.
        *)
          [[ "$status" != 401 ]] || token=""
          last="$node: HTTP $status ${response%$'\n'*}"
          still+="$node $role $address"$'\n'
          ;;
      esac
    done <<<"$pending"
    pending="${still%$'\n'}"
    [[ -z "$pending" ]] || sleep 3
  done
  log "every SPACE node is in the Direct node registry"
}

case "${1:-}" in
  seed-ids) seed_ids ;;
  register) register "${2:-600}" ;;
  *) echo "usage: $0 seed-ids | register [timeout-seconds]" >&2; exit 2 ;;
esac
