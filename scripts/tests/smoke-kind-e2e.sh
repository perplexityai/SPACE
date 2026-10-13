#!/usr/bin/env bash
#
# End-to-end smoke for the local KinD SPACE stack (Tilt setup).
#
# Self-bootstrapping: if the KinD cluster doesn't exist it's created from a
# config rendered out of `infra/space/kind/kind-cluster.yaml.tmpl`; if Tilt
# isn't running it's started
# in the background. After bring-up the script polls the api-server's
# `/healthz` until it's reachable, then drives the public REST API:
#   1. login as root@local
#   2. create a sandbox (template = seed)
#   3. mkdir /home/user/data
#   4. write a payload to /home/user/data/hello.txt
#   5. read it back, assert content matches
#   6. delete the sandbox
#
# Env knobs:
#   API_URL              http://localhost:8080
#   ADMIN_EMAIL / ADMIN_PASSWORD   root@local / SPACE_CONSOLE_PASSWORD (or password)
#   TEMPLATE             seed:latest
#   PAYLOAD              hello, sandbox
#   KIND_CLUSTER         space-local
#   SKIP_BOOTSTRAP=1     skip the kind-create + tilt-up bring-up entirely
#                        (use when you've already brought the stack up out of band)
#   BOOTSTRAP_TIMEOUT    seconds to wait for api-server readiness (default 360)
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

API_URL="${API_URL:-http://localhost:8080}"
EMAIL="${ADMIN_EMAIL:-root@local}"
PASSWORD="${ADMIN_PASSWORD:-${SPACE_CONSOLE_PASSWORD:-password}}"
TEMPLATE="${TEMPLATE:-seed:latest}"
PAYLOAD="${PAYLOAD:-hello, sandbox}"
KIND_CLUSTER="${KIND_CLUSTER:-space-local}"
SKIP_BOOTSTRAP="${SKIP_BOOTSTRAP:-0}"
# First-run cargo-chef cook on this workspace can take 10–20 minutes; the
# default has to accommodate that. Subsequent runs are seconds.
BOOTSTRAP_TIMEOUT="${BOOTSTRAP_TIMEOUT:-1800}"

for bin in curl jq kubectl timeout; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $bin" >&2
    exit 1
  fi
done

# Idempotent kind+tilt bring-up.
# shellcheck source=lib/bootstrap-kind-stack.sh
# Shared public-API helpers. `OPERATION_POLL_ATTEMPTS` keeps this suite's
# original 60-poll budget; `CURL_TIMEOUT_SECS` is deliberately unset, so
# `api_curl` stays plain `api_curl` as it was here.
SPACE_OPS_PREFIX="smoke-kind-e2e"
OPERATION_POLL_ATTEMPTS=60
. "$SCRIPT_DIR/lib/space-api-ops.sh"
. "$SCRIPT_DIR/lib/bootstrap-kind-stack.sh"
bootstrap_kind_stack

# This is test coverage, not node readiness: rt-mgr may restart independently
# of the execution services, but a green smoke must prove its own RPC works.
# The CLI runs as root in the node's operator shell (space-node-debug), the
# local operator the runtime manager admits for reads.
check_runtime_manager() {
  local role="$1" deadline=$((SECONDS + 90)) node="" shell="" listing="" pods=""
  while (( SECONDS < deadline )); do
    if pods=$(kubectl --context "kind-${KIND_CLUSTER}" --request-timeout=10s -n space get pods \
        -l "app.kubernetes.io/name=space-${role},app.kubernetes.io/component=runtime-manager" \
        --field-selector=status.phase=Running -o json) && \
      node=$(jq -r '[.items[] | select(.metadata.deletionTimestamp == null)][0].spec.nodeName // empty' <<<"$pods") && \
      [[ -n "$node" ]] && \
      shell=$(kubectl --context "kind-${KIND_CLUSTER}" --request-timeout=10s -n space get pods \
        -l app.kubernetes.io/name=space-node-debug --field-selector="spec.nodeName=${node}" \
        -o jsonpath='{.items[0].metadata.name}') && \
      [[ -n "$shell" ]] && \
      listing=$(timeout 30s kubectl --context "kind-${KIND_CLUSTER}" --request-timeout=10s -n space \
        exec "$shell" -c shell -- timeout 25s space-rtmgr list --json) && \
      jq -e '
        .complete == true
        and any(.artifacts[];
          .kind == "runtime" and .runtime_kind == "firecracker"
          and .location == "vendor" and .status == "valid" and .version != null)
        and any(.artifacts[];
          .kind == "component" and .component_kind == "kernel"
          and .location == "cache" and .status == "valid" and .digest != null)
      ' <<<"$listing" >/dev/null; then
      echo "[rtmgr] PASS ${role}: complete listing includes a vendor runtime and staged kernel"
      return 0
    fi
    sleep 1
  done
  echo "ERROR: ${role} runtime manager did not report the baked runtime and staged kernel" >&2
  printf '%s\n' "$listing" >&2
  return 1
}

check_runtime_manager worker
check_runtime_manager builder

echo "[1/7] login as $EMAIL"
TOKEN=$(api_curl -X POST "$API_URL/auth/v1/sessions" \
  -H 'content-type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" \
  | jq -r '.token')
if [[ -z "$TOKEN" || "$TOKEN" == "null" ]]; then
  echo "ERROR: login failed" >&2
  exit 1
fi
auth=(-H "authorization: Bearer $TOKEN")

echo "[2/7] create sandbox template=$TEMPLATE"
CREATE_BODY=$(jq -nc --arg template "$TEMPLATE" '{
  template: $template,
  runtime: "fc",
  env: {},
  metadata: {},
  resources: null
}')
CREATE=$(post_operation "/v1/sandboxes" "$CREATE_BODY")
SANDBOX_ID=$(echo "$CREATE" | jq -r '.sandbox_id // empty')
if [[ -z "$SANDBOX_ID" ]]; then
  echo "ERROR: create sandbox failed: $CREATE" >&2
  exit 1
fi
echo "sandbox_id=$SANDBOX_ID"

cleanup() {
  local rc=$?
  echo "cleanup: deleting sandbox $SANDBOX_ID"
  delete_response=$(api_curl -X DELETE "$API_URL/v1/sandboxes/$SANDBOX_ID" \
    "${auth[@]}" \
    -H "idempotency-key: $(idem_key)" || true)
  delete_operation_id=$(echo "$delete_response" | jq -r '.operation_id // empty' 2>/dev/null || true)
  if [[ -n "$delete_operation_id" ]]; then
    wait_operation "$delete_operation_id" || true
  fi
  exit $rc
}
trap cleanup EXIT

echo "[3/7] start sandbox"
post_operation "/v1/sandboxes/$SANDBOX_ID:start" '{}' >/dev/null

echo "[4/7] wait until file API is ready"
ready=0
for _ in $(seq 1 30); do
  ready_status=$(api_curl -X POST "$API_URL/v1/sandboxes/$SANDBOX_ID/files:stat" \
    "${auth[@]}" \
    -H 'content-type: application/json' \
    -d '{"path":"/home/user","user":null}' \
    -o /dev/null -w '%{http_code}')
  if [[ "$ready_status" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != "1" ]]; then
  echo "ERROR: sandbox $SANDBOX_ID never reported ready (last status=$ready_status)" >&2
  exit 1
fi

echo "[5/7] mkdir /home/user/data"
api_curl -X POST "$API_URL/v1/sandboxes/$SANDBOX_ID/files:mkdir" \
  "${auth[@]}" \
  -H 'content-type: application/json' \
  -H "idempotency-key: $(idem_key)" \
  -d '{"path":"/home/user/data","parents":true,"user":null}' \
  >/dev/null

echo "[6/7] write + read /home/user/data/hello.txt"
PAYLOAD_B64=$(printf '%s' "$PAYLOAD" | base64 | tr -d '\n')
WRITE_BODY=$(jq -nc --arg data "$PAYLOAD_B64" '{
  path: "/home/user/data/hello.txt",
  data_base64: $data,
  append: false,
  user: null
}')
api_curl -X POST "$API_URL/v1/sandboxes/$SANDBOX_ID/files:write" \
  "${auth[@]}" \
  -H 'content-type: application/json' \
  -H "idempotency-key: $(idem_key)" \
  -d "$WRITE_BODY" \
  >/dev/null

GOT=$(api_curl -X POST "$API_URL/v1/sandboxes/$SANDBOX_ID/files:read" \
  "${auth[@]}" \
  -H 'content-type: application/json' \
  -d '{"path":"/home/user/data/hello.txt","offset":0,"length":16777216,"user":null}')
if [[ "$GOT" != "$PAYLOAD" ]]; then
  echo "FAIL: round-trip payload mismatch" >&2
  echo "  wrote: $PAYLOAD" >&2
  echo "  read:  $GOT" >&2
  exit 1
fi

echo "[7/7] PASS round-trip ok: '$PAYLOAD'"
