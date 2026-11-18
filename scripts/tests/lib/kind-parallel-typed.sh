#!/usr/bin/env bash
# One bench's typed smoke, as CI's "Run this shard of the typed Kind smoke"
# step runs it (kind-ci.yml): an admin token file and a cold-receipts directory
# from the seeded root account, a wait for the data plane, then
# `smoke-kind-typed.sh all $SMOKE_SHARD`, its output kept in <log>.
#
# Usage: kind-parallel-typed.sh <log>
# Exit status: smoke-kind-typed.sh's.
# Env:   SMOKE_SHARD, SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE, SPACE_SMOKE_KIND_COLD_RECEIPTS
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LOG="${1:?usage: kind-parallel-typed.sh <log>}"
: "${SMOKE_SHARD:?}" "${SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE:?}" "${SPACE_SMOKE_KIND_COLD_RECEIPTS:?}"
export SKIP_BOOTSTRAP=1 SMOKE_SHARD SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE SPACE_SMOKE_KIND_COLD_RECEIPTS
[[ -n "${SPACE_M1_GENERATOR_MIRROR:-}" ]] || unset SPACE_M1_GENERATOR_MIRROR

for tool in aws curl jq uv; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: the typed Kind smoke needs $tool" >&2
    exit 2
  fi
done
install -d -m 0700 "$SPACE_SMOKE_KIND_COLD_RECEIPTS"
jq -nc --arg password "${SPACE_CONSOLE_PASSWORD:-password}" '{email: "root@local", password: $password}' \
  | curl -fsS -m 30 -X POST http://localhost:8080/auth/v1/sessions \
      -H 'content-type: application/json' --data-binary @- \
  | jq -r '.token // empty' >"$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE"
chmod 0600 "$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE"
if [[ ! -s "$SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE" ]]; then
  echo "ERROR: could not obtain an admin session token from the Kind API" >&2
  exit 2
fi
# The scenarios talk to the data plane from their first command.
for _ in $(seq 1 150); do
  curl -sf -m 2 http://127.0.0.1:8090/healthz >/dev/null 2>&1 && break
  sleep 2
done
if ! curl -sf -m 2 http://127.0.0.1:8090/healthz >/dev/null 2>&1; then
  echo "ERROR: the Kind data plane at 127.0.0.1:8090 never became healthy" >&2
  exit 2
fi
# The runner prints no times; <log>.events stamps each scenario's start and end
# and each pass's boundaries for the run's timeline.
bash "$REPO_ROOT/scripts/tests/smoke-kind-typed.sh" all "$SMOKE_SHARD" 2>&1 \
  | tee "$LOG" \
  | python3 -u -c '
import re, sys, time
pattern = re.compile(r"^(=== (RUN|END) |--- typed smoke on |SUITE: |SHARD )")
with open(sys.argv[1], "w") as events:
    for line in sys.stdin:
        if pattern.match(line):
            events.write(f"{time.time():.0f} {line}")
            events.flush()
' "$LOG.events"
