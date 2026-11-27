#!/usr/bin/env bash
# Lists the space-allow-rollback-unsafe markers in the SQLite up migrations
# that <current-ref> has and <target-ref> doesn't, one line per marker:
# store, version, rule, object and reason, newest first per store. Image-only
# rollback from <current-ref> to <target-ref> is safe past a marked migration
# only when <target-ref> is the release its reason names; db/README.md,
# "Rolling back past a newer SQLite migration", says what to run first.
#
# Exits 0 when there are none, 1 when it lists any, and 2 on an error. The
# marker syntax is the rollback guard's, scripts/check_sqlite_migrations_rollback.py.
set -euo pipefail

if (($# != 2)); then
  echo "usage: $0 <target-ref> <current-ref>" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

for ref in "$1" "$2"; do
  if ! git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
    echo "ERROR: ${ref} doesn't name a commit." >&2
    exit 2
  fi
done

exec python3 "${repo_root}/scripts/check_sqlite_migrations_rollback.py" --list-markers "$1" "$2"
