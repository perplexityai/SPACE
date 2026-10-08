#!/usr/bin/env bash
# Volume-manager migrations must execute inside the daemon after it owns
# service.mutex. The Kind node-service charts may migrate other stores in init
# containers, but must not invoke the non-owning external volume-manager
# migrator.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if matches="$(grep -REn 'space-db-migrator[[:space:]]+volume-manager' \
    "$repo_root/infra/space/kind/charts" "$repo_root/scripts/k8s")"; then
  echo "FAIL: external volume-manager migration in the Kind node charts or node scripts:" >&2
  printf '%s\n' "$matches" >&2
  exit 1
fi
echo "ok: the Kind node services leave volume-manager migrations with the mutex owner"
