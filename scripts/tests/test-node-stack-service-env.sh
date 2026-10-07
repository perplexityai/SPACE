#!/usr/bin/env bash
# The node-stack launcher hands single-service settings only to the services
# that read them. scope_service_environment must remove them from the exported
# environment every service inherits, and the launcher must read them only
# through the captured locals.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
launcher="$repo_root/scripts/k8s/node-stack-start.sh"
status=0
fail() {
  echo "FAIL: $*" >&2
  status=1
}

scoped_names=(
  SPACE_CREDMGR_DATABASE_URL
  SPACE_DB_CREDS_PASSWORD
  SPACE_CREDMGR_AWS_ACCESS_KEY_ID
  SPACE_CREDMGR_AWS_SECRET_ACCESS_KEY
  SPACE_CREDMGR_AWS_SESSION_TOKEN
  SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID
  SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY
  SPACE_OBJECT_STORE_STATIC_SESSION_TOKEN
  POSTGRES_CONNECTION_STRING
)

scope_fn="$(awk '/^scope_service_environment\(\) \{/,/^}/' "$launcher")"
[ -n "$scope_fn" ] || fail "scope_service_environment is missing from $launcher"

# Outside the function, a scoped name may appear only in comments, in the
# require_env check that runs before scoping, and as the variable name handed
# back to one service ("NAME=..."). Any other mention (a read, an export, a new
# launch argument) would hand it to every service again.
outside="$(awk '/^scope_service_environment\(\) \{/,/^}/ { next } { print }' "$launcher")"
for name in "${scoped_names[@]}"; do
  if printf '%s\n' "$outside" | grep -v '^[[:space:]]*#' | grep -v 'require_env' \
    | grep -vF "\"$name=" | grep -Eq "(^|[^A-Za-z0-9_])$name([^A-Za-z0-9_]|$)"; then
    fail "$name is referenced outside scope_service_environment"
  fi
done

scope_line="$(grep -n '^scope_service_environment$' "$launcher" | cut -d: -f1 || true)"
start_line="$(grep -n '^start_all$' "$launcher" | cut -d: -f1 || true)"
if [ -z "$scope_line" ] || [ -z "$start_line" ] || [ "$scope_line" -ge "$start_line" ]; then
  fail "scope_service_environment must run before start_all"
fi

grep -q 'OBJECT_STORE_STATIC_ENV.*space-volmgr-svc' "$launcher" \
  || fail "space-volmgr-svc is not launched with OBJECT_STORE_STATIC_ENV"
grep -q 'buildmgr_env=(.*OBJECT_STORE_STATIC_ENV' "$launcher" \
  && grep -q 'buildmgr_env.*space-buildmgr-svc' "$launcher" \
  || fail "space-buildmgr-svc is not launched with OBJECT_STORE_STATIC_ENV"

result="$(
  env -i PATH="$PATH" bash -c '
    set -euo pipefail
    eval "$1"
    export SPACE_CREDMGR_DATABASE_URL="postgres://u:p a*ss@db/x"
    export SPACE_DB_CREDS_PASSWORD="pw with spaces *"
    export SPACE_CREDMGR_AWS_ACCESS_KEY_ID="AKID" SPACE_CREDMGR_AWS_SECRET_ACCESS_KEY="s k*"
    export SPACE_CREDMGR_AWS_SESSION_TOKEN="tok"
    export SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID="o kid*" SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY="o sk?"
    export POSTGRES_CONNECTION_STRING="postgres://su@db/x" SPACE_OBJECT_STORE_BUCKET="bucket"
    scope_service_environment
    echo "child=$(env | grep -c -E "^(SPACE_CREDMGR_DATABASE_URL|SPACE_DB_CREDS_PASSWORD|SPACE_CREDMGR_AWS_[A-Z_]+|SPACE_OBJECT_STORE_STATIC_[A-Z_]+|POSTGRES_CONNECTION_STRING)=" || true)"
    echo "bucket=$(env | grep -c "^SPACE_OBJECT_STORE_BUCKET=bucket$")"
    echo "prefix=$(env ${OBJECT_STORE_STATIC_ENV[@]+"${OBJECT_STORE_STATIC_ENV[@]}"} env \
      | grep -c -x -e "SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID=o kid\*" \
        -e "SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY=o sk?")"
    echo "credmgr=${CREDMGR_DATABASE_URL}|${CREDMGR_DB_CREDS_PASSWORD}|${CREDMGR_AWS_SECRET_ACCESS_KEY}"
    echo "direct=${DIRECT_POSTGRES_CONNECTION_STRING}"
    OBJECT_STORE_STATIC_ENV=()
    echo "empty=$(env ${OBJECT_STORE_STATIC_ENV[@]+"${OBJECT_STORE_STATIC_ENV[@]}"} echo launched)"
  ' _ "$scope_fn"
)"

expect() {
  printf '%s\n' "$result" | grep -qxF "$1" || fail "expected '$1' in: $(printf '%s' "$result" | tr '\n' ' ')"
}
expect "child=0"
expect "bucket=1"
expect "prefix=2"
expect "credmgr=postgres://u:p a*ss@db/x|pw with spaces *|s k*"
expect "direct=postgres://su@db/x"
expect "empty=launched"

if [ "$status" -eq 0 ]; then
  echo "ok: single-service settings reach only their consumers in the node-stack launcher"
fi
exit "$status"
