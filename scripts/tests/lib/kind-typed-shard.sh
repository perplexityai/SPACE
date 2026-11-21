#!/usr/bin/env bash
# One CI smoke shard's typed run: kind-ci.yml's "Run this shard of the typed
# Kind smoke" step, and each smoke-kind-parallel.sh bench. An admin token file
# and a cold-receipts directory from the seeded root account, a wait for the
# data plane, then `smoke-kind-typed.sh all $SMOKE_SHARD`, its output kept in
# <log>. With SMOKE_TYPED_TARGET set to a scenario ID it runs just that
# scenario instead, and SMOKE_SHARD isn't needed.
#
# Usage: kind-typed-shard.sh <log>
# Exit status: smoke-kind-typed.sh's; 2 when a tool is missing, no admin token
# can be had or the data plane never answers, and a failed command's own
# status before that.
# Env:   SMOKE_SHARD or SMOKE_TYPED_TARGET, SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE (removed on exit),
#        SPACE_SMOKE_KIND_COLD_RECEIPTS, SPACE_CONSOLE_PASSWORD (default password)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LOG="${1:?usage: kind-typed-shard.sh <log>}"
: "${SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE:?}" "${SPACE_SMOKE_KIND_COLD_RECEIPTS:?}"
if [[ -n "${SMOKE_TYPED_TARGET:-}" ]]; then
  TYPED_ARGS=("$SMOKE_TYPED_TARGET")
else
  : "${SMOKE_SHARD:?}"
  export SMOKE_SHARD
  TYPED_ARGS=(all "$SMOKE_SHARD")
fi
export SKIP_BOOTSTRAP=1 SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE SPACE_SMOKE_KIND_COLD_RECEIPTS
[[ -n "${SPACE_M1_GENERATOR_MIRROR:-}" ]] || unset SPACE_M1_GENERATOR_MIRROR

# Fails the shard, with an annotation in GitHub Actions.
error() {
  if [[ "${GITHUB_ACTIONS:-}" == true ]]; then
    echo "::error::$*"
  else
    echo "ERROR: $*" >&2
  fi
  exit 2
}

for tool in aws curl jq uv; do
  command -v "$tool" >/dev/null 2>&1 || error "the typed Kind smoke needs $tool"
done
install -d -m 0700 "$SPACE_SMOKE_KIND_COLD_RECEIPTS"
# Created private before the token is written, and gone with the run.
trap 'rm -f "$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE"' EXIT
install -m 0600 /dev/null "$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE"
jq -nc --arg password "${SPACE_CONSOLE_PASSWORD:-password}" '{email: "root@local", password: $password}' \
  | curl -fsS -m 30 -X POST http://localhost:8080/auth/v1/sessions \
      -H 'content-type: application/json' --data-binary @- \
  | jq -r '.token // empty' >"$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE"
[[ -s "$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE" ]] || error "could not obtain an admin session token from the Kind API"
# The scenarios talk to the data plane from their first command.
for _ in $(seq 1 150); do
  curl -sf -m 2 http://127.0.0.1:8090/healthz >/dev/null 2>&1 && break
  sleep 2
done
curl -sf -m 2 http://127.0.0.1:8090/healthz >/dev/null 2>&1 \
  || error "the Kind data plane at 127.0.0.1:8090 never became healthy"
# The runner prints no times; <log>.events stamps each scenario's start and end
# and each pass's boundaries for smoke-kind-parallel.sh's timeline. The filter
# passes every line through to stdout (the step log), as bytes so output that
# isn't UTF-8 can't stop it; a failed events write stops only the events. It
# stays in the pipeline, so the events are complete when this script exits.
bash "$REPO_ROOT/scripts/tests/smoke-kind-typed.sh" "${TYPED_ARGS[@]}" 2>&1 \
  | tee "$LOG" \
  | python3 -u -c '
import re, sys, time
pattern = re.compile(rb"^(=== RUN |--- typed smoke on |SUITE: |SHARD )|=== END ")
try:
    events = open(sys.argv[1], "wb")
except OSError as err:
    print(f"kind-typed-shard: no timeline events: {err}", file=sys.stderr)
    events = None
for line in sys.stdin.buffer:
    sys.stdout.buffer.write(line)
    if events is not None and pattern.search(line):
        try:
            events.write(b"%.0f " % time.time() + line)
            events.flush()
        except OSError as err:
            print(f"kind-typed-shard: timeline events stopped: {err}", file=sys.stderr)
            events = None
' "$LOG.events"
