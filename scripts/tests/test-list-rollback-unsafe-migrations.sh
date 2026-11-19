#!/usr/bin/env bash
set -euo pipefail

# Keep the suite deterministic when the caller has personal Git settings or a
# repository selected through the environment.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES \
  GIT_COMMON_DIR GIT_NAMESPACE GIT_PREFIX GIT_CEILING_DIRECTORIES
export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_COUNT=0

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

repo="${tmp_dir}/repo"
marker='space-allow-rollback-unsafe'

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

git init -q --template= -b main "${repo}"
git -C "${repo}" config user.name test
git -C "${repo}" config user.email test@example.com
mkdir -p "${repo}/scripts"
cp "${repo_root}/scripts/list-rollback-unsafe-migrations.sh" \
  "${repo_root}/scripts/check_sqlite_migrations_rollback.py" "${repo}/scripts/"

# Commits each <path> <sql> pair and prints the commit.
commit() {
  while (($#)); do
    mkdir -p "$(dirname "${repo}/$1")"
    printf '%s\n' "$2" >"${repo}/$1"
    shift 2
  done
  git -C "${repo}" add -A
  git -C "${repo}" commit -q -m step
  git -C "${repo}" rev-parse HEAD
}

# The listing's first line, from <current> to <target>.
header() {
  printf '%s' "${marker} markers in SQLite migrations that $1 has and $2 doesn't, newest first per store;" \
    " a \`common\` line is the shared catalog in that store's database. Before rolling back to $2 past one" \
    " whose reason names a release newer than $2, stop that store and run \`space-db-migrator <store> ... down\`" \
    " until the marked version is gone from its histories: each run reverts the newest migration first, so" \
    " every newer migration above it goes too."
}

# Runs the script from <target> to <current>, expecting <status>, and checks
# that its output is exactly <expected>.
expect() {
  local name="$1" target="$2" current="$3" status="$4" expected="$5" actual=0
  (cd "${repo}" && bash scripts/list-rollback-unsafe-migrations.sh "${target}" "${current}") \
    >"${tmp_dir}/${name}.out" 2>&1 || actual=$?
  if ((actual != status)) || [[ "$(cat "${tmp_dir}/${name}.out")" != "${expected}" ]]; then
    cat "${tmp_dir}/${name}.out" >&2
    fail "${name}: expected status ${status} and the output above to be:"$'\n'"${expected}"
  fi
}

target="$(commit ARCHITECTURE.toml $'[rules.layers.state.shared_catalogs]\ncommon = ["sandbox-manager", "node-manager"]' \
  db/sandbox-manager/20260930000000_baseline.up.sql "CREATE TABLE sandboxes (id TEXT, legacy TEXT);
-- ${marker}: sandboxes: schema: an example in a migration both have" \
  db/node-manager/20260930000000_baseline.up.sql "CREATE TABLE slots (id TEXT);")"
unmarked="$(commit db/sandbox-manager/20261101000000_note.up.sql "ALTER TABLE sandboxes ADD COLUMN note TEXT;")"
expect none "${target}" "${unmarked}" 0 \
  "No ${marker} markers in SQLite migrations that ${unmarked} has and ${target} doesn't."

one="$(commit db/sandbox-manager/20261102000000_drop_legacy.up.sql "ALTER TABLE sandboxes DROP COLUMN legacy;
-- ${marker}: sandboxes.legacy: schema: release N stopped reading it")"
expect one "${target}" "${one}" 1 \
  "$(header "${one}" "${target}")
sandbox-manager 20261102000000 schema sandboxes.legacy: release N stopped reading it"

several="$(commit db/sandbox-manager/20261103000000_drop_note.up.sql "ALTER TABLE sandboxes DROP COLUMN note;
/* ${marker}: sandboxes.note: schema: release N+1 stopped reading it */" \
  db/node-manager/20261103000000_drop_slots.up.sql "DROP TABLE slots;
-- ${marker}: slots: schema: release M stopped using it" \
  db/postgres/20261103000000_ignored.up.sql "-- ${marker}: ignored: schema: not SQLite" \
  db/common/20261104000000_jobs.up.sql "CREATE TABLE jobs (id TEXT);
-- ${marker}: common: schema: no consumer reads it yet")"
expect several "${target}" "${several}" 1 \
  "$(header "${several}" "${target}")
node-manager 20261104000000 schema common: no consumer reads it yet
node-manager 20261103000000 schema slots: release M stopped using it
sandbox-manager 20261104000000 schema common: no consumer reads it yet
sandbox-manager 20261103000000 schema sandboxes.note: release N+1 stopped reading it
sandbox-manager 20261102000000 schema sandboxes.legacy: release N stopped reading it"

# One release back, the migrations the target already has aren't listed.
expect one-back "${one}" "${several}" 1 \
  "$(header "${several}" "${one}")
node-manager 20261104000000 schema common: no consumer reads it yet
node-manager 20261103000000 schema slots: release M stopped using it
sandbox-manager 20261104000000 schema common: no consumer reads it yet
sandbox-manager 20261103000000 schema sandboxes.note: release N+1 stopped reading it"

expect bad-ref "${target}" no-such-ref 2 "ERROR: no-such-ref doesn't name a commit."
# A migration that can't be read fails the listing rather than shortening it.
blob="$(git -C "${repo}" rev-parse "${several}:db/node-manager/20261103000000_drop_slots.up.sql")"
mv "${repo}/.git/objects/${blob:0:2}/${blob:2}" "${tmp_dir}/hidden-blob"
(cd "${repo}" && bash scripts/list-rollback-unsafe-migrations.sh "${target}" "${several}") >"${tmp_dir}/unreadable.out" 2>&1 \
  && fail "unreadable: expected failure" || status=$?
((status == 2)) && grep -qF "ERROR: can't read the migrations of ${target} or ${several}" "${tmp_dir}/unreadable.out" \
  || { cat "${tmp_dir}/unreadable.out" >&2; fail "unreadable: expected status 2, got ${status}"; }
mv "${tmp_dir}/hidden-blob" "${repo}/.git/objects/${blob:0:2}/${blob:2}"

echo "Rollback-unsafe migration listing scenarios passed."
