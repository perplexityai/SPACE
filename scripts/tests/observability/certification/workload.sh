#!/usr/bin/env bash
# Hold real sandboxes across several exports; reuse the public smoke API helpers.
set -Eeuo pipefail
# shellcheck source=/dev/null
source "$CERT_SOURCE/scripts/tests/lib/space-api-ops.sh"
API_URL=$CERT_API_URL
export CURL_TIMEOUT_SECS=30
export OPERATION_POLL_ATTEMPTS=180
export SPACE_OPS_PREFIX=otlp-certification
sandbox_id=''
auth=()

operation_delete() {
  local response operation_id
  response=$(api_curl --fail -X DELETE "$API_URL/v1/sandboxes/$sandbox_id" \
    "${auth[@]}" -H "idempotency-key: $(idem_key)")
  operation_id=$(jq -er .operation_id <<<"$response")
  wait_operation "$operation_id"
  sandbox_id=''
}
cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n $sandbox_id ]]; then
    operation_delete || status=1
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

login=$(jq -nc --arg password "$SPACE_CONSOLE_PASSWORD" '{email:"root@local", password:$password}')
token=$(api_curl --fail -X POST "$API_URL/auth/v1/sessions" \
  -H 'content-type: application/json' -d "$login" | jq -er .token)
auth=(-H "authorization: Bearer $token")
# Existing native journey covers process/command streams, data-plane sessions,
# and pause/resume/suspend/restore, in addition to the held filesystem rounds.
rounds=3
hold=90
if [[ $CERT_MODE == otel ]]; then
  SPACE_API_TOKEN="$token" SPACE_API_URL="$API_URL" \
    SPACE_DATA_PLANE_URL="$CERT_DATA_PLANE_URL" \
    bash "$CERT_SOURCE/scripts/tests/smoke-spaced-rest.sh"
else
  # The default-mode negative control only needs fresh sandbox activity.
  rounds=1
  hold=20
fi
for round in $(seq 1 "$rounds"); do
  create=$(post_operation /v1/sandboxes '{"template":"seed:latest","runtime":"fc","env":{},"metadata":{},"resources":null}')
  sandbox_id=$(jq -er .sandbox_id <<<"$create")
  post_operation "/v1/sandboxes/$sandbox_id:start" '{}' >/dev/null
  # Events contain only synthetic resource IDs and times, never credentials.
  jq -nc --arg id "$sandbox_id" --argjson time "$(date +%s.%N)" \
    '{event:"active", sandbox_id:$id, time:$time}' >>"$CERT_EVENTS"
  for _ in $(seq 1 60); do
    if api_curl --fail -X POST "$API_URL/v1/sandboxes/$sandbox_id/files:stat" \
      "${auth[@]}" -H 'content-type: application/json' \
      -d '{"path":"/home/user","user":null}' >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  body=$(jq -nc --arg data "$(printf 'certification-%s' "$round" | base64 | tr -d '\n')" \
    '{path:"/home/user/certification.txt",data_base64:$data,append:false,user:null}')
  api_curl --fail -X POST "$API_URL/v1/sandboxes/$sandbox_id/files:write" \
    "${auth[@]}" -H 'content-type: application/json' -H "idempotency-key: $(idem_key)" \
    -d "$body" >/dev/null
  actual=$(api_curl --fail -X POST "$API_URL/v1/sandboxes/$sandbox_id/files:read" \
    "${auth[@]}" -H 'content-type: application/json' \
    -d '{"path":"/home/user/certification.txt","offset":0,"length":1024,"user":null}')
  [[ $actual == "certification-$round" ]]
  sleep "$hold"
  jq -nc --argjson time "$(date +%s.%N)" '{event:"deleting",time:$time}' >>"$CERT_EVENTS"
  operation_delete
  jq -nc --argjson time "$(date +%s.%N)" '{event:"deleted",time:$time}' >>"$CERT_EVENTS"
  sleep 25
done
