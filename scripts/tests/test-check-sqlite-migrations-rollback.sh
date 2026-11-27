#!/usr/bin/env bash
set -euo pipefail

# Keep the suite deterministic when the caller has personal Git settings, a
# repository selected through the environment, or a base pinned for the real
# checkout.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES \
  GIT_COMMON_DIR GIT_NAMESPACE GIT_PREFIX GIT_CEILING_DIRECTORIES
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_COUNT=0
unset GIT_ASKPASS SSH_ASKPASS GIT_SSH GIT_SSH_COMMAND GIT_CONFIG_PARAMETERS
unset MIGRATION_SQLITE_ROLLBACK_BASE_REF MIGRATION_VERSION_GUARD_BASE_REF

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

repo="${tmp_dir}/repo"
marker='space-allow-rollback-unsafe'
reason='release N stopped reading it'
store=sandbox-manager
file="db/${store}/20261101000000_change.up.sql"
landed="db/${store}/20261001000000_landed.up.sql"
landed_sql='CREATE TABLE landed (id TEXT);
UPDATE scratch SET id = id;'

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

git init -q --template= -b main "${repo}"
git -C "${repo}" config user.name test
git -C "${repo}" config user.email test@example.com
mkdir -p "${repo}/scripts/lib" "${repo}/ci/scripts" "${repo}/db/${store}" "${repo}/db/node-manager" \
  "${repo}/db/common" "${repo}/db/postgres" "${repo}/db/template-manager/history"
cp "${repo_root}/scripts/check-sqlite-migrations-rollback.sh" \
  "${repo_root}/scripts/check_sqlite_migrations_rollback.py" "${repo}/scripts/"
cp "${repo_root}/scripts/lib/migration-base.sh" "${repo}/scripts/lib/"
cp "${repo_root}/ci/scripts/resolve-migration-base.sh" \
  "${repo_root}/ci/scripts/check-sqlite-migrations-rollback-per-commit.sh" "${repo}/ci/scripts/"
printf '[rules.layers.state.shared_catalogs]\ncommon = ["%s"]\n' "${store}" >"${repo}/ARCHITECTURE.toml"
cat >"${repo}/db/${store}/20260930000000_baseline.up.sql" <<'SQL'
CREATE TABLE owners (id TEXT PRIMARY KEY, email TEXT);
CREATE TABLE sandboxes (id TEXT PRIMARY KEY, state TEXT, name TEXT, legacy TEXT, owner_id TEXT REFERENCES owners (id));
CREATE INDEX sandboxes_by_state ON sandboxes (state);
CREATE TABLE audit (id TEXT);
CREATE TRIGGER sandboxes_audit AFTER INSERT ON sandboxes BEGIN UPDATE audit SET id = NEW.id; END;
CREATE VIEW sandbox_names AS SELECT name FROM sandboxes;
CREATE TABLE scratch (id TEXT);
CREATE TABLE settings (key TEXT PRIMARY KEY ON CONFLICT REPLACE, value TEXT);
CREATE TABLE kinds (id TEXT PRIMARY KEY);
CREATE TABLE kind_notes (kind_id TEXT REFERENCES kinds (id) ON DELETE CASCADE);
CREATE TABLE odd ("(" TEXT);
CREATE TRIGGER audit AFTER UPDATE ON scratch BEGIN SELECT 1; END;
CREATE INDEX jobs_by_id ON jobs (job_id);
SQL
printf 'CREATE TABLE jobs (job_id TEXT PRIMARY KEY);\n' >"${repo}/db/common/20260930000000_baseline.up.sql"
printf 'CREATE TABLE slots (id TEXT PRIMARY KEY);\n' >"${repo}/db/node-manager/20260930000000_baseline.up.sql"
printf '%s\n' "${landed_sql}" >"${repo}/${landed}"
git -C "${repo}" add -A
git -C "${repo}" commit -q -m base
git -C "${repo}" update-ref refs/remotes/origin/main HEAD
base="$(git -C "${repo}" rev-parse HEAD)"

# Commits each <path> <sql> pair on a fresh branch from the base.
commit_case() {
  local name="$1"
  shift
  git -C "${repo}" checkout -q -B "case-${name}" "${base}"
  while (($#)); do
    mkdir -p "$(dirname "${repo}/$1")"
    printf '%s\n' "$2" >"${repo}/$1"
    shift 2
  done
  git -C "${repo}" add -A
  git -C "${repo}" commit -q -m "${name}"
}

run_guard() {
  local name="$1"
  shift
  (cd "${repo}" && env -u BUILDKITE_BUILD_ID -u BUILDKITE_BRANCH \
    MIGRATION_SQLITE_ROLLBACK_BASE_REF="${MIGRATION_SQLITE_ROLLBACK_BASE_REF:-origin/main}" "$@" \
    bash scripts/check-sqlite-migrations-rollback.sh) >"${tmp_dir}/${name}.out" 2>&1
}

expect_pass() {
  run_guard "$1" || { cat "${tmp_dir}/$1.out" >&2; fail "$1: expected pass"; }
}

# Expects the guard to fail with every given line in its output.
expect_fail() {
  local name="$1" line
  shift
  if run_guard "${name}"; then
    cat "${tmp_dir}/${name}.out" >&2
    fail "${name}: expected failure"
  fi
  for line in "$@"; do
    grep -qF -- "${line}" "${tmp_dir}/${name}.out" || { cat "${tmp_dir}/${name}.out" >&2; fail "${name}: missing '${line}'"; }
  done
}

# The allowlist: each passes with no marker.
allowed=(
  "new-tables|CREATE TABLE fresh (id TEXT PRIMARY KEY, n INTEGER NOT NULL CHECK (n > 0));
CREATE TABLE fresh_child (id TEXT REFERENCES fresh (id));"
  "indexes|CREATE INDEX sandboxes_by_name ON sandboxes (name) WHERE name IS NOT NULL;
CREATE TABLE fresh (n TEXT); CREATE UNIQUE INDEX fresh_n ON fresh (n);"
  "new-view|CREATE VIEW sandbox_states AS SELECT id, state FROM sandboxes;"
  "add-columns|ALTER TABLE sandboxes ADD COLUMN note TEXT;
ALTER TABLE sandboxes ADD COLUMN label TEXT DEFAULT 'none';
ALTER TABLE sandboxes ADD COLUMN kind TEXT NOT NULL DEFAULT 'none';
ALTER TABLE \"Sandboxes\" ADD COLUMN shown TEXT GENERATED ALWAYS AS (coalesce(name, '')) VIRTUAL;"
  "new-table-dml|CREATE TABLE fresh (id TEXT, state TEXT);
INSERT INTO fresh VALUES ('a', 'b'); UPDATE fresh SET state = 'c'; DELETE FROM fresh;
ALTER TABLE fresh ADD COLUMN m TEXT NOT NULL DEFAULT 'x' CHECK (m <> ''); ALTER TABLE fresh RENAME COLUMN state TO st;
CREATE INDEX fresh_st ON fresh (st); DROP INDEX fresh_st; DROP TABLE fresh;"
  "plain-inserts|INSERT INTO owners (id) VALUES ('o1');
INSERT INTO owners DEFAULT VALUES;
INSERT INTO sandboxes (id, owner_id) VALUES ('s1', 'o1');
INSERT INTO owners (id) VALUES ('-- ${marker}: owners: data: marker-shaped text in a string');"
  "temp-objects|CREATE TEMP TABLE work (id TEXT); INSERT INTO work SELECT id FROM sandboxes;
UPDATE work SET id = 'x'; DROP TABLE work;
CREATE TEMP TRIGGER migration_only BEFORE INSERT ON sandboxes BEGIN SELECT 1; END;"
)
for entry in "${allowed[@]}"; do
  name="${entry%%|*}"
  commit_case "${name}" "${file}" "${entry#*|}"
  expect_pass "${name}"
done

# Everything else needs a marker of its kind on its object, in its file.
needs_marker=(
  "drop-column|schema|sandboxes.legacy|ALTER TABLE sandboxes DROP COLUMN legacy;"
  "single-quoted-alter|schema|sandboxes.legacy|ALTER TABLE 'sandboxes' DROP COLUMN legacy;"
  "quoted-paren-column|schema|odd.note|ALTER TABLE odd ADD COLUMN note TEXT CHECK (note <> 'legacy');"
  "rename-column|schema|sandboxes.name|ALTER TABLE sandboxes RENAME COLUMN name TO label;"
  "rename-table|schema|scratch|ALTER TABLE scratch RENAME TO scratch_pad;"
  "add-check|schema|scratch.size|ALTER TABLE scratch ADD COLUMN size INTEGER CHECK (size > 0);"
  "add-references|schema|scratch.owner_id|ALTER TABLE scratch ADD COLUMN owner_id TEXT REFERENCES owners (id);"
  "add-not-null-generated|schema|scratch.shown|ALTER TABLE scratch ADD COLUMN shown TEXT GENERATED ALWAYS AS (id) VIRTUAL NOT NULL;"
  "add-not-null-default-null|schema|scratch.required|ALTER TABLE scratch ADD COLUMN required TEXT NOT NULL DEFAULT NULL;"
  "drop-table|schema|scratch|DROP TABLE scratch;"
  "drop-index|schema|sandboxes_by_state|DROP INDEX sandboxes_by_state;"
  "drop-view|schema|sandbox_names|DROP VIEW sandbox_names;"
  "drop-trigger|schema|sandboxes_audit|DROP TRIGGER sandboxes_audit;"
  "unique-index|schema|owners_by_email|CREATE UNIQUE INDEX owners_by_email ON owners (email);"
  "new-trigger|schema|fresh_touch|CREATE TABLE fresh (id TEXT); CREATE TRIGGER fresh_touch AFTER INSERT ON fresh BEGIN SELECT 1; END;"
  "foreign-key-cascade|schema|fresh.owner_id|CREATE TABLE fresh (id TEXT, owner_id TEXT REFERENCES owners (id) ON DELETE CASCADE);"
  "foreign-key-composite|schema|fresh|CREATE TABLE fresh (a TEXT, b TEXT, FOREIGN KEY (a, b) REFERENCES owners (id, email));"
  "pragma|schema|user_version|PRAGMA user_version = 7;"
  "other-statement|schema|analyze|ANALYZE;"
  "update|data|sandboxes|UPDATE sandboxes SET state = 'stopped' WHERE state IS NULL;"
  "table-and-trigger-names|data|audit|UPDATE audit SET id = 'new';"
  "delete|data|sandboxes|DELETE FROM sandboxes WHERE state = 'gone';"
  "insert-select|data|sandboxes|INSERT INTO sandboxes (id) SELECT id FROM owners;"
  "insert-transfer|data|scratch|CREATE TABLE staging (id TEXT); INSERT INTO scratch SELECT * FROM staging;"
  "replace|data|owners|REPLACE INTO owners (id) VALUES ('o1');"
  "or-replace|data|owners|INSERT OR REPLACE INTO owners (id) VALUES ('o1');"
  "upsert|data|owners|INSERT INTO owners (id) VALUES ('o1') ON CONFLICT (id) DO NOTHING;"
  "on-conflict-replace-table|data|settings|INSERT INTO settings (key, value) VALUES ('k', 'v');"
  "copy-into-new|data|fresh|CREATE TABLE fresh (id TEXT); INSERT INTO fresh SELECT id FROM sandboxes;"
  "create-as-select|data|fresh|CREATE TABLE fresh AS SELECT * FROM sandboxes;"
  "transfer-into-new|data|fresh|CREATE TABLE fresh (id TEXT); INSERT INTO fresh SELECT * FROM scratch;"
  "values-subquery-copy|data|fresh|CREATE TABLE fresh (id TEXT); INSERT INTO fresh VALUES ((SELECT id FROM scratch LIMIT 1));"
)
for entry in "${needs_marker[@]}"; do
  IFS='|' read -r name kind object sql <<<"${entry}"
  commit_case "${name}" "${file}" "${sql}"
  expect_fail "${name}" "ERROR: ${file}:1: ${store}: ${object}: ${kind}: " \
    "    -- ${marker}: ${object}: ${kind}: <why the previous release doesn't depend on it" \
    "1 SQLite rollback finding(s) or error(s)."
  commit_case "${name}-marked" "${file}" "${sql}
-- ${marker}: ${object}: ${kind}: ${reason}"
  expect_pass "${name}-marked"
  grep -qF "  by ${file}:2: ${reason}" "${tmp_dir}/${name}-marked.out" || fail "${name}-marked: exemption not printed"
done
# Dropping a parent deletes its cascaded child rows, which is the child's data.
commit_case drop-cascade "${file}" "DROP TABLE kinds;"
expect_fail drop-cascade "${store}: kinds: schema: " "${store}: kind_notes: data: DELETE rewrites rows"

# ATTACH, DETACH and VACUUM open or create other files, so they are refused,
# in the change and in the base replay, before any file appears.
# A transaction would refuse VACUUM first, so these opt out of one.
for denied in "ATTACH DATABASE '${tmp_dir}/attached.db' AS x;" "-- no-transaction
VACUUM INTO '${tmp_dir}/vacuumed.db';"; do
  commit_case denied "${file}" "${denied}"
  expect_fail denied "doesn't allow ATTACH, DETACH or VACUUM in migrations"
done
commit_case base-vacuum "db/${store}/20261001000001_vacuum.up.sql" "-- no-transaction
VACUUM INTO '${tmp_dir}/base-vacuumed.db';"
denied_base="$(git -C "${repo}" rev-parse HEAD)"
printf 'CREATE TABLE later (id TEXT);\n' >"${repo}/${file}"
git -C "${repo}" add -A
git -C "${repo}" commit -q -m later
MIGRATION_SQLITE_ROLLBACK_BASE_REF="${denied_base}" expect_fail base-vacuum \
  "20261001000001_vacuum.up.sql: failed to apply on the base schema: the guard doesn't allow ATTACH, DETACH or VACUUM"
for created in attached vacuumed base-vacuumed; do
  [[ ! -e "${tmp_dir}/${created}.db" ]] || fail "${created}.db was created"
done

# Marker errors.
commit_case stale "${file}" "ALTER TABLE scratch ADD COLUMN note TEXT; -- ${marker}: scratch: schema: ${reason}"
expect_fail stale "${file}:1: stale ${marker} marker: no schema finding on scratch in this file"
commit_case unknown-kind "${file}" "DROP TABLE scratch; -- ${marker}: scratch: drop-table: ${reason}"
expect_fail unknown-kind "${file}:1: ${marker} marker for scratch names unknown kind drop-table"
commit_case empty-reason "${file}" "DROP TABLE scratch; -- ${marker}: scratch: schema:"
expect_fail empty-reason "${marker} marker for scratch: schema needs a reason"
commit_case placeholder "${file}" "DROP TABLE scratch; -- ${marker}: scratch: schema: <why the previous release doesn't depend on it>"
expect_fail placeholder "${marker} marker for scratch: schema needs a reason" "${store}: scratch: schema: "
commit_case wrong-file "${file}" "DROP TABLE scratch;" \
  "db/${store}/20261102000000_other.up.sql" "-- ${marker}: scratch: schema: ${reason}"
expect_fail wrong-file "ERROR: ${file}:1: ${store}: scratch: schema: " "20261102000000_other.up.sql:1: stale ${marker} marker"
commit_case landed-marker "${landed}" "${landed_sql}
-- ${marker}: landed: schema: ${reason}" "${file}" "DROP TABLE landed;"
expect_fail landed-marker "${landed}:3: ${marker} marker in a landed migration, which never exempts" \
  "ERROR: ${file}:1: ${store}: landed: schema: "

# Landed migrations: a comment-only edit passes, an added statement is
# classified, and deleting or renaming one fails with no marker to exempt it.
commit_case landed-comment "${landed}" "-- Rewrites nothing the base didn't.
${landed_sql}"
expect_pass landed-comment
commit_case landed-statement "${landed}" "${landed_sql}
DELETE FROM sandboxes;"
expect_fail landed-statement "ERROR: ${landed}:3: ${store}: sandboxes: data: "
git -C "${repo}" checkout -q -B case-deleted "${base}"
git -C "${repo}" rm -q "${landed}"
git -C "${repo}" commit -q -m deleted
expect_fail deleted "ERROR: ${landed}: deleted or renamed, but the base has it." "wherever main is deployed" \
  "20261113190000_restore_sandbox_legacy_state"
git -C "${repo}" checkout -q -B case-renamed "${base}"
git -C "${repo}" mv "${landed}" "db/${store}/20261001000000_landed_v2.up.sql"
git -C "${repo}" commit -q -m renamed
expect_fail renamed "ERROR: ${landed}: deleted or renamed, but the base has it."
git -C "${repo}" checkout -q -B case-symlink "${base}"
ln -s nowhere.sql "${repo}/${file}"
git -C "${repo}" add -A
git -C "${repo}" commit -q -m symlink
expect_fail symlink "ERROR: ${file}: a migration must be a regular file"

# db/common: classified on its own catalog, applied on each consumer's base,
# and what it creates is new to a consumer. A store that doesn't consume it
# can have its own jobs table.
common_marker="-- ${marker}: common: schema: no consumer reads what this changes"
# Every new db/common migration needs the one shared-catalog marker, which
# covers its findings, even one that only adds a table.
commit_case common "db/common/20261101000000_jobs.up.sql" "CREATE TABLE job_links (id TEXT, owner_id TEXT REFERENCES owners (id));"
expect_fail common "ERROR: common: db/common/20261101000000_jobs.up.sql: a db/common migration needs one \`-- ${marker}: common: schema: <reason>\` whose reason covers every consumer: ${store}"
commit_case common-marked "db/common/20261101000000_jobs.up.sql" "ALTER TABLE jobs ADD COLUMN kind TEXT CHECK (kind <> '');
${common_marker}"
expect_pass common-marked
grep -qF "Exempted: db/common/20261101000000_jobs.up.sql:1: common: jobs.kind: schema: " "${tmp_dir}/common-marked.out" \
  || fail "common-marked: finding not exempted"
commit_case common-collision "db/common/20261101000000_jobs.up.sql" "CREATE INDEX sandboxes_by_state ON jobs (job_id);
${common_marker}"
expect_fail common-collision \
  "ERROR: ${store}: db/common/20261101000000_jobs.up.sql: failed to apply on the consumer's base schema: index sandboxes_by_state already exists"
# A consumer's own index on a common table: renaming the table passes the
# upgrade but fails a new node, which runs the common catalog first.
commit_case common-fresh-install "db/common/20261101000000_jobs.up.sql" "ALTER TABLE jobs RENAME TO queued_jobs;
${common_marker}"
expect_fail common-fresh-install "failed to apply on the fresh install schema: no such table: main.jobs"
commit_case common-new-table "db/common/20261101000000_jobs_v2.up.sql" "CREATE TABLE jobs_v2 (id TEXT PRIMARY KEY, lane TEXT);
${common_marker}" \
  "db/${store}/20261102000000_jobs_v2_lane.up.sql" "CREATE UNIQUE INDEX jobs_v2_lane ON jobs_v2 (lane);"
expect_pass common-new-table
commit_case not-a-consumer "db/node-manager/20261101000000_jobs.up.sql" "CREATE TABLE jobs (id TEXT PRIMARY KEY, kind TEXT);"
expect_pass not-a-consumer

# A migration that doesn't apply is an error with SQLite's message.
commit_case apply-error "${file}" "ALTER TABLE nosuch ADD COLUMN x TEXT;"
expect_fail apply-error "ERROR: ${store}: ${file}: failed to apply on the head schema: no such table: nosuch"

# Scope: postgres, down migrations and history/ are not checked.
for scoped in "db/postgres/20261101000000_drop.up.sql" "db/${store}/20261101000000_change.down.sql" \
  "db/template-manager/history/20261101000000_copy.up.sql"; do
  commit_case scope "${scoped}" "DROP TABLE sandboxes;"
  expect_pass scope
  grep -qF 'No changed SQLite migrations' "${tmp_dir}/scope.out" || fail "${scoped}: checked"
done

# An uncommitted migration is checked too.
git -C "${repo}" checkout -q -B case-untracked "${base}"
printf 'DROP TABLE scratch;\n' >"${repo}/${file}"
expect_fail untracked "ERROR: ${file}:1: ${store}: scratch: schema: "
rm "${repo}/${file}"

# Merge-queue shape: the group's base is the entry ahead of it, and only this
# entry's own migration is checked.
commit_case queue "db/${store}/20261101000000_ahead.up.sql" "DROP TABLE scratch;
-- ${marker}: scratch: schema: ${reason}"
group_base="$(git -C "${repo}" rev-parse HEAD)"
printf 'ALTER TABLE sandboxes DROP COLUMN legacy;\n' >"${repo}/db/${store}/20261102000000_entry.up.sql"
git -C "${repo}" add -A
git -C "${repo}" commit -q -m "this entry"
resolved="$(cd "${repo}" && bash ci/scripts/resolve-migration-base.sh "${group_base}" "")"
[[ "${resolved}" == "${group_base}" ]] || fail "merge group: resolved ${resolved}"
MIGRATION_SQLITE_ROLLBACK_BASE_REF="${resolved}" expect_fail merge-group \
  "ERROR: db/${store}/20261102000000_entry.up.sql:1: ${store}: sandboxes.legacy: schema: " "1 SQLite rollback finding(s)"

# The guard's own pin wins over the version guard's, and an empty one falls
# back to it. HEAD as the base leaves nothing to check.
run_guard pin-fallback MIGRATION_SQLITE_ROLLBACK_BASE_REF= MIGRATION_VERSION_GUARD_BASE_REF=HEAD \
  || { cat "${tmp_dir}/pin-fallback.out" >&2; fail "pin-fallback: expected pass"; }
grep -qF 'No changed SQLite migrations' "${tmp_dir}/pin-fallback.out" || fail "pin-fallback: checked against origin/main"
# The guard's own pin (HEAD: nothing to check) must beat the version guard's
# (the merge-group base, which has a finding), and neither may be ignored in
# favour of the origin/main default, which has findings too.
run_guard pin-own MIGRATION_SQLITE_ROLLBACK_BASE_REF=HEAD MIGRATION_VERSION_GUARD_BASE_REF="${resolved}" \
  || { cat "${tmp_dir}/pin-own.out" >&2; fail "pin-own: expected pass"; }
grep -qF 'No changed SQLite migrations' "${tmp_dir}/pin-own.out" || fail "pin-own: didn't use its own pin"

# A local branch named origin/main, here at the change itself, would leave
# nothing to check. The default base and an origin/main pin both compare with
# refs/remotes/origin/main, the ref the refresh writes, rather than letting
# that branch shadow it.
commit_case shadowed-base "${file}" "DROP TABLE scratch;"
git -C "${repo}" update-ref refs/heads/origin/main HEAD
for pin in origin/main ""; do
  if run_guard shadowed-base MIGRATION_SQLITE_ROLLBACK_BASE_REF="${pin}"; then
    cat "${tmp_dir}/shadowed-base.out" >&2
    fail "shadowed-base, pin '${pin}': expected failure"
  fi
  for line in "Checking changed SQLite migrations for rollback safety against ${base}:" \
    "ERROR: ${file}:1: ${store}: scratch: schema: "; do
    grep -qF -- "${line}" "${tmp_dir}/shadowed-base.out" \
      || { cat "${tmp_dir}/shadowed-base.out" >&2; fail "shadowed-base, pin '${pin}': missing '${line}'"; }
  done
done
git -C "${repo}" update-ref -d refs/heads/origin/main

# Main's batched push: one PR creates a table and the next drops it with a
# marker. As one change the drop is of a new table and the marker is stale;
# per first-parent commit, each passes. A walk that can't read the history fails.
commit_case batch "db/${store}/20261101000000_staged.up.sql" "CREATE TABLE staged (id TEXT);"
printf 'DROP TABLE staged;\n-- %s: staged: schema: %s\n' "${marker}" "${reason}" >"${repo}/db/${store}/20261102000000_drop_staged.up.sql"
git -C "${repo}" add -A
git -C "${repo}" commit -q -m "drop staged"
expect_fail batch-whole "stale ${marker} marker: no schema finding on staged"
per_commit() {
  (cd "${repo}" && env -u BUILDKITE_BRANCH BUILDKITE_BUILD_ID=test \
    bash ci/scripts/check-sqlite-migrations-rollback-per-commit.sh "${base}") >"${tmp_dir}/$1.out" 2>&1
}
per_commit batch || { cat "${tmp_dir}/batch.out" >&2; fail "batch: expected per-commit pass"; }
grep -qF "The SQLite rollback guard passed for each of 2 commit(s)" "${tmp_dir}/batch.out" || fail "batch: wrong output"
first="$(git -C "${repo}" rev-parse HEAD^)"
mv "${repo}/.git/objects/${first:0:2}/${first:2}" "${tmp_dir}/hidden-object"
if per_commit walk-failure; then cat "${tmp_dir}/walk-failure.out" >&2; fail "walk-failure: expected failure"; fi
grep -qF "passed" "${tmp_dir}/walk-failure.out" && fail "walk-failure: reported a pass"
mv "${tmp_dir}/hidden-object" "${repo}/.git/objects/${first:0:2}/${first:2}"
# A commit whose tree can't be read is an error, not a commit from before the guard.
tree="$(git -C "${repo}" rev-parse HEAD^{tree})"
mv "${repo}/.git/objects/${tree:0:2}/${tree:2}" "${tmp_dir}/hidden-tree"
if per_commit tree-failure; then cat "${tmp_dir}/tree-failure.out" >&2; fail "tree-failure: expected failure"; fi
grep -qF "can't read the tree of" "${tmp_dir}/tree-failure.out" || { cat "${tmp_dir}/tree-failure.out" >&2; fail "tree-failure: wrong error"; }
mv "${tmp_dir}/hidden-tree" "${repo}/.git/objects/${tree:0:2}/${tree:2}"

# In CI, a base that doesn't resolve fails closed, in the merge queue too.
git -C "${repo}" checkout -q case-drop-table
for branch in feature trunk-merge/pr-1/test; do
  if MIGRATION_SQLITE_ROLLBACK_BASE_REF=refs/heads/no-such-base run_guard no-base \
    BUILDKITE_BUILD_ID=github-actions BUILDKITE_BRANCH="${branch}"; then
    fail "no base on ${branch}: expected failure"
  fi
  grep -qF 'no base ref to compare SQLite migrations against' "${tmp_dir}/no-base.out" || fail "no base on ${branch}: wrong error"
done

echo "SQLite rollback guard scenarios passed."
