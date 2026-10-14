#!/usr/bin/env bash
set -euo pipefail

# Ignore caller-specific Git authentication. The explicit CI authentication
# scenarios below install only the mechanism they are testing.
export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_COUNT=0
unset GIT_ASKPASS SSH_ASKPASS GIT_SSH GIT_SSH_COMMAND GIT_CONFIG_PARAMETERS

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${repo_root}/scripts/check-postgres-migrations-isolated.sh"
bootstrap_roles_path="crates/libs/db/migrator/src/bootstrap_roles.rs"
postgres_bootstrap_test_path="crates/libs/db/migrator/src/bootstrap_roles/tests/postgres_bootstrap_tests.rs"
postgres_bootstrap_test_with_credential_table="fn new_credential_table_is_fenced() {}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

write_file() {
  local path="$1"
  local body="${2:-content}"

  mkdir -p "$(dirname "${path}")"
  printf '%s\n' "${body}" >"${path}"
}

append_file() {
  local path="$1"
  local body="$2"

  printf '\n%s\n' "${body}" >>"${path}"
}

rewrite_credential_tables() {
  local path="$1"
  local action="$2"
  local table="${3:-}"
  local rewritten="${path}.rewritten"

  awk -v action="${action}" -v table="${table}" '
    /^const CREDENTIAL_TABLES: &\[&str\] = &\[$/ {
      in_credential_tables = 1
      if (action == "declaration") {
        print "pub(crate) const CREDENTIAL_TABLES: &[&str] = &["
        next
      }
    }
    in_credential_tables && /^\];$/ {
      if (action == "add") {
        print "    \"" table "\","
      } else if (action == "comment") {
        print "    // " table
      } else if (action == "unclosed") {
        print "]"
        in_credential_tables = 0
        next
      }
      in_credential_tables = 0
    }
    in_credential_tables && action == "remove" \
      && $0 == "    \"" table "\"," {
      next
    }
    in_credential_tables && action == "spaces" \
      && $0 == "    \"" table "\"," {
      print $0 "  "
      next
    }
    in_credential_tables && action == "reorder" {
      if ($0 == "    \"org_master_keys\",") {
        print "    \"org_branching_keys\","
        next
      }
      if ($0 == "    \"org_branching_keys\",") {
        print "    \"org_master_keys\","
        next
      }
    }
    { print }
  ' "${path}" >"${rewritten}"
  mv "${rewritten}" "${path}"
}

rewrite_ro_column_denylist() {
  local path="$1"
  local action="$2"
  local rewritten="${path}.rewritten"

  awk -v action="${action}" '
    /^const RO_COLUMN_DENYLIST: / { in_denylist = 1 }
    in_denylist && /^\];$/ {
      if (action == "add-table") {
        print "    (\"service_keys\", &[\"key_hash\"]),"
      } else if (action == "duplicate-table") {
        print "    (\"users\", &[\"totp_secret\"]),"
      }
      in_denylist = 0
    }
    in_denylist && $0 == "    (\"users\", &[\"password_hash\"])," {
      if (action == "add-column") {
        print "    (\"users\", &[\"password_hash\", \"totp_secret\"]),"
        next
      }
      if (action == "drop-column") {
        print "    (\"users\", &[\"totp_secret\"]),"
        next
      }
      if (action == "remove-entry") {
        next
      }
    }
    in_denylist && $0 == "    (\"sessions\", &[\"token_hash\"])," \
      && action == "remove-sessions" {
      next
    }
    { print }
  ' "${path}" >"${rewritten}"
  mv "${rewritten}" "${path}"
}

write_git_auth_probe() {
  local path="$1"

  cat >"${path}" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

for arg in "$@"; do
  [[ "${arg}" == fetch ]] || continue
  [[ "${GIT_TERMINAL_PROMPT:-}" == 0 ]] || exit 91
  [[ "${GIT_ASKPASS:-}" == "${EXPECTED_GIT_ASKPASS:-}" ]] || exit 92
  [[ "${SSH_ASKPASS:-}" == "${EXPECTED_SSH_ASKPASS:-}" ]] || exit 93
  [[ "${GIT_SSH:-}" == "${EXPECTED_GIT_SSH:-}" ]] || exit 94
  [[ "${GIT_SSH_COMMAND:-}" == "${EXPECTED_GIT_SSH_COMMAND:-}" ]] || exit 95
  [[ " $* " != *" core.askPass= "* ]] || exit 96
  [[ "${GIT_CONFIG_PARAMETERS:-}" == "${EXPECTED_GIT_CONFIG_PARAMETERS:-}" ]] || exit 97
  [[ "${GIT_CONFIG_COUNT:-0}" == "${EXPECTED_GIT_CONFIG_COUNT:-0}" ]] || exit 98
  [[ "${GIT_CONFIG_KEY_0:-}" == "${EXPECTED_GIT_CONFIG_KEY_0:-}" ]] || exit 99
  [[ "${GIT_CONFIG_VALUE_0:-}" == "${EXPECTED_GIT_CONFIG_VALUE_0:-}" ]] || exit 100
  [[ "${GIT_CONFIG_KEY_1:-}" == "${EXPECTED_GIT_CONFIG_KEY_1:-}" ]] || exit 101
  [[ "${GIT_CONFIG_VALUE_1:-}" == "${EXPECTED_GIT_CONFIG_VALUE_1:-}" ]] || exit 102
  [[ " $* " != *" --deepen=50 "* ]] || exit 103
  break
done

exec "${REAL_GIT_BIN:?}" "$@"
SH
  chmod +x "${path}"
}

write_timeout_probe() {
  local path="$1"

  cat >"${path}" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" == "--kill-after=5s" ]] || exit 91
[[ "${2:-}" == "${EXPECTED_TIMEOUT_DURATION:?}" ]] || exit 92
if [[ "${TIMEOUT_PROBE_DEEPEN_ONLY:-}" == 1 ]]; then
  shift 2
  if [[ " $* " == *" --deepen=50 "* ]]; then
    printf 'deepen:%s\n' "${EXPECTED_TIMEOUT_DURATION}" >"${TIMEOUT_PROBE_LOG:?}"
    exit 124
  fi
  exec "$@"
fi
printf '%s\n' "$2" >"${TIMEOUT_PROBE_LOG:?}"
exit 124
SH
  chmod +x "${path}"
}

write_no_deepen_git_probe() {
  local path="$1"

  cat >"${path}" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" --deepen=50 "* ]]; then
  printf 'unexpected deepen\n' >>"${NO_DEEPEN_PROBE_LOG:?}"
  exit 103
fi
exec "${REAL_GIT_BIN:?}" "$@"
SH
  chmod +x "${path}"
}

run_guard() {
  local repo="$1"
  local stdout="$2"
  local stderr="$3"
  local branch="${4:-feature}"

  BUILDKITE_BRANCH="${branch}" BUILDKITE_BUILD_ID='' \
    MIGRATION_ISOLATION_BASE_REF=origin/main \
    bash "${repo}/scripts/check-postgres-migrations-isolated.sh" >"${stdout}" 2>"${stderr}"
}

expect_guard_success() {
  local repo="$1"
  local name="$2"
  local branch="${3:-feature}"

  if ! run_guard "${repo}" "${tmp_dir}/${name}.stdout" "${tmp_dir}/${name}.stderr" "${branch}"; then
    echo "expected ${name} migration isolation scenario to pass" >&2
    cat "${tmp_dir}/${name}.stdout" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
    exit 1
  fi
}

expect_guard_failure() {
  local repo="$1"
  local name="$2"

  set +e
  run_guard "${repo}" "${tmp_dir}/${name}.stdout" "${tmp_dir}/${name}.stderr"
  local exit_code=$?
  set -e

  if [[ "${exit_code}" -eq 0 ]]; then
    echo "expected ${name} migration isolation scenario to fail" >&2
    cat "${tmp_dir}/${name}.stdout" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
    exit 1
  fi
}

commit_scenario() {
  local repo="$1"
  local name="$2"

  git -C "${repo}" add .
  git -C "${repo}" commit --quiet -m "${name}"
}

work_repo="${tmp_dir}/repo"
mkdir -p "${work_repo}/scripts"
cp "${script}" "${work_repo}/scripts/check-postgres-migrations-isolated.sh"
cp "${repo_root}/scripts/check_postgres_partial_indexes.py" "${work_repo}/scripts/"
mkdir -p "${work_repo}/ci/scripts"
cp "${repo_root}/ci/scripts/ensure-uv.sh" "${work_repo}/ci/scripts/"
mkdir -p "${work_repo}/ci/scripts/lib"
cp "${repo_root}/ci/scripts/lib/uv-run.sh" "${work_repo}/ci/scripts/lib/"

git -C "${work_repo}" init --quiet --initial-branch=main
git -C "${work_repo}" config user.email test@example.com
git -C "${work_repo}" config user.name "Migration Isolation Test"

write_file "${work_repo}/README.md" "base"
existing_migration="db/postgres/20260622000000_existing.up.sql"
write_file "${work_repo}/${existing_migration}" $'-- Original explanation.\nSELECT \'-- original value\';'
mkdir -p "$(dirname "${work_repo}/${bootstrap_roles_path}")"
mkdir -p "$(dirname "${work_repo}/${postgres_bootstrap_test_path}")"
cp "${repo_root}/${bootstrap_roles_path}" "${work_repo}/${bootstrap_roles_path}"
cp "${repo_root}/${postgres_bootstrap_test_path}" \
  "${work_repo}/${postgres_bootstrap_test_path}"
commit_scenario "${work_repo}" "base"
base_commit="$(git -C "${work_repo}" rev-parse HEAD)"
git -C "${work_repo}" update-ref refs/remotes/origin/main HEAD

set +e
MIGRATION_ISOLATION_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH:-bash}" "${work_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/no-origin-ci.stdout" 2>"${tmp_dir}/no-origin-ci.stderr"
no_origin_ci_exit=$?
set -e
if [[ "${no_origin_ci_exit}" -eq 0 ]]; then
  echo "expected a cached base without an origin remote to fail closed in CI" >&2
  cat "${tmp_dir}/no-origin-ci.stdout" >&2
  cat "${tmp_dir}/no-origin-ci.stderr" >&2
  exit 1
fi
grep -F "origin remote is not configured" "${tmp_dir}/no-origin-ci.stderr" >/dev/null

for scenario in comments sql-literal transaction-mode added deleted mixed lexer-unavailable; do
  git -C "${work_repo}" checkout --quiet -B "migration-${scenario}-plus-app" "${base_commit}"
  write_file "${work_repo}/crates/libs/example/src/lib.rs" "pub fn example() {}"
  write_file "${work_repo}/${existing_migration}" $'/* Generic explanation. */\nSELECT \'-- original value\';'
  case "${scenario}" in
    sql-literal)
      write_file "${work_repo}/${existing_migration}" "SELECT '-- changed value';"
      ;;
    transaction-mode)
      write_file "${work_repo}/${existing_migration}" $'-- no-transaction\nSELECT \'-- original value\';'
      ;;
    added)
      write_file "${work_repo}/db/postgres/20260623000000_new.up.sql" "-- New migration."
      ;;
    deleted)
      rm "${work_repo}/${existing_migration}"
      ;;
    mixed)
      write_file "${work_repo}/db/postgres/20260623000000_new.up.sql" "SELECT 2;"
      ;;
    lexer-unavailable)
      rm "${work_repo}/scripts/check_postgres_partial_indexes.py"
      ;;
  esac
  commit_scenario "${work_repo}" "migration ${scenario} plus app"
  if [[ "${scenario}" == comments ]]; then
    expect_guard_success "${work_repo}" "migration-${scenario}-plus-app"
    grep -F "Verified unchanged executable SQL and transaction mode" \
      "${tmp_dir}/migration-${scenario}-plus-app.stdout" >/dev/null
  else
    expect_guard_failure "${work_repo}" "migration-${scenario}-plus-app"
    grep -F "split Postgres migrations from runtime app changes" \
      "${tmp_dir}/migration-${scenario}-plus-app.stderr" >/dev/null
  fi
done

echo "Comment-only migration isolation scenarios passed"

git -C "${work_repo}" checkout --quiet -B migration-only "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000000_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
write_file "${work_repo}/db/postgres/20260623000000_add_column.down.sql" "ALTER TABLE widgets DROP COLUMN name;"
commit_scenario "${work_repo}" "migration only"
expect_guard_success "${work_repo}" "migration-only"
grep -F "migration isolation check passed" "${tmp_dir}/migration-only.stdout" >/dev/null

git -C "${work_repo}" checkout --quiet -B app-only "${base_commit}"
write_file "${work_repo}/crates/libs/example/src/lib.rs" "pub fn example() {}"
commit_scenario "${work_repo}" "app only"
expect_guard_success "${work_repo}" "app-only"
grep -F "No Postgres migrations changed" "${tmp_dir}/app-only.stdout" >/dev/null

git -C "${work_repo}" checkout --quiet -B bootstrap-only-invalid-fence "${base_commit}"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" comment PLA-1234
commit_scenario "${work_repo}" "bootstrap only invalid fence"
expect_guard_success "${work_repo}" "bootstrap-only-invalid-fence"
grep -F "No Postgres migrations changed" \
  "${tmp_dir}/bootstrap-only-invalid-fence.stdout" >/dev/null
if [[ -s "${tmp_dir}/bootstrap-only-invalid-fence.stderr" ]]; then
  echo "expected the no-migration path not to evaluate the credential exemption" >&2
  cat "${tmp_dir}/bootstrap-only-invalid-fence.stderr" >&2
  exit 1
fi

git -C "${work_repo}" checkout --quiet -B migration-plus-credential-fence "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000008_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" add new_credential_table
append_file "${work_repo}/${postgres_bootstrap_test_path}" \
  "${postgres_bootstrap_test_with_credential_table}"
commit_scenario "${work_repo}" "migration plus credential fence"
expect_guard_success "${work_repo}" "migration-plus-credential-fence"

git -C "${work_repo}" checkout --quiet -B migration-plus-arbitrary-bootstrap-role "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000009_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" add new_credential_table
append_file "${work_repo}/${bootstrap_roles_path}" \
  'const BOOTSTRAP_MARKER: &str = "changed";'
commit_scenario "${work_repo}" "migration plus arbitrary bootstrap role change"
expect_guard_failure "${work_repo}" "migration-plus-arbitrary-bootstrap-role"
grep -F "${bootstrap_roles_path}" \
  "${tmp_dir}/migration-plus-arbitrary-bootstrap-role.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B credential-fence-plus-unrelated-code "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000010_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" add new_credential_table
append_file "${work_repo}/${postgres_bootstrap_test_path}" \
  "${postgres_bootstrap_test_with_credential_table}"
write_file "${work_repo}/crates/libs/example/src/lib.rs" "pub fn unrelated() {}"
commit_scenario "${work_repo}" "credential fence plus unrelated code"
expect_guard_failure "${work_repo}" "credential-fence-plus-unrelated-code"
grep -F "crates/libs/example/src/lib.rs" \
  "${tmp_dir}/credential-fence-plus-unrelated-code.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-credential-removal "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000011_unrelated.up.sql" "SELECT 1;"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" remove org_master_keys
commit_scenario "${work_repo}" "migration plus credential removal"
expect_guard_failure "${work_repo}" "migration-plus-credential-removal"
grep -F "may be removed only when" \
  "${tmp_dir}/migration-plus-credential-removal.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-dropped-credential "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000012_drop_credentials.up.sql" \
  "DROP TABLE public.org_master_keys;"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" remove org_master_keys
commit_scenario "${work_repo}" "migration plus dropped credential"
expect_guard_success "${work_repo}" "migration-plus-dropped-credential"

git -C "${work_repo}" checkout --quiet -B migration-plus-down-only-drop "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000012_drop_credentials.down.sql" \
  "DROP TABLE public.org_master_keys;"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" remove org_master_keys
commit_scenario "${work_repo}" "migration plus down-only credential drop"
expect_guard_failure "${work_repo}" "migration-plus-down-only-drop"
grep -F "may be removed only when" \
  "${tmp_dir}/migration-plus-down-only-drop.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-non-standalone-drop "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000012_mixed_drop.up.sql" \
  $'DROP TABLE public.org_master_keys;\nSELECT 1;'
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" remove org_master_keys
commit_scenario "${work_repo}" "migration plus non-standalone credential drop"
expect_guard_failure "${work_repo}" "migration-plus-non-standalone-drop"
grep -F "may be removed only when" \
  "${tmp_dir}/migration-plus-non-standalone-drop.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-rewritten-bootstrap-test "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000013_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" add new_credential_table
write_file "${work_repo}/${postgres_bootstrap_test_path}" "fn replacement_stub() {}"
commit_scenario "${work_repo}" "migration plus rewritten bootstrap test"
expect_guard_failure "${work_repo}" "migration-plus-rewritten-bootstrap-test"
grep -F "must remain byte-for-byte unchanged" \
  "${tmp_dir}/migration-plus-rewritten-bootstrap-test.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-bootstrap-test-only "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000014_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
append_file "${work_repo}/${postgres_bootstrap_test_path}" \
  "${postgres_bootstrap_test_with_credential_table}"
commit_scenario "${work_repo}" "migration plus bootstrap test only"
expect_guard_failure "${work_repo}" "migration-plus-bootstrap-test-only"
grep -F "allowed only with a valid CREDENTIAL_TABLES change" \
  "${tmp_dir}/migration-plus-bootstrap-test-only.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-shadowed-fence-test "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000023_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" add new_credential_table
append_file "${work_repo}/${postgres_bootstrap_test_path}" \
  'const CREDENTIAL_TABLES: &[&str] = &[];'
commit_scenario "${work_repo}" "migration plus shadowed fence test"
expect_guard_failure "${work_repo}" "migration-plus-shadowed-fence-test"
grep -F "may not shadow or replace fence constants" \
  "${tmp_dir}/migration-plus-shadowed-fence-test.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-reordered-fence "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000015_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" reorder
commit_scenario "${work_repo}" "migration plus reordered fence"
expect_guard_failure "${work_repo}" "migration-plus-reordered-fence"
grep -F "does not add or remove CREDENTIAL_TABLES entries" \
  "${tmp_dir}/migration-plus-reordered-fence.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-whitespace-fence "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000016_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" spaces org_master_keys
commit_scenario "${work_repo}" "migration plus whitespace fence"
expect_guard_failure "${work_repo}" "migration-plus-whitespace-fence"
grep -F "does not add or remove CREDENTIAL_TABLES entries" \
  "${tmp_dir}/migration-plus-whitespace-fence.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-invalid-fence-body "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000017_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" comment PLA-1234
commit_scenario "${work_repo}" "migration plus invalid fence body"
expect_guard_failure "${work_repo}" "migration-plus-invalid-fence-body"
grep -F "cannot verify a CREDENTIAL_TABLES-only change" \
  "${tmp_dir}/migration-plus-invalid-fence-body.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-invalid-fence-declaration "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000018_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" declaration
commit_scenario "${work_repo}" "migration plus invalid fence declaration"
expect_guard_failure "${work_repo}" "migration-plus-invalid-fence-declaration"
grep -F "Keep exactly one multiline" \
  "${tmp_dir}/migration-plus-invalid-fence-declaration.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-unclosed-fence "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000019_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" unclosed
commit_scenario "${work_repo}" "migration plus unclosed fence"
expect_guard_failure "${work_repo}" "migration-plus-unclosed-fence"
grep -F "cannot verify a CREDENTIAL_TABLES-only change" \
  "${tmp_dir}/migration-plus-unclosed-fence.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-duplicate-fence "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000022_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" add org_master_keys
commit_scenario "${work_repo}" "migration plus duplicate fence"
expect_guard_failure "${work_repo}" "migration-plus-duplicate-fence"
grep -F "cannot verify a CREDENTIAL_TABLES-only change" \
  "${tmp_dir}/migration-plus-duplicate-fence.stderr" >/dev/null

# RO_COLUMN_DENYLIST moves in lockstep with the migration that creates or
# renames a hash/bearer column: the bootstrap-role test requires both that every
# such column is fenced and that every fenced relation exists.
git -C "${work_repo}" checkout --quiet -B migration-plus-column-denylist "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000020_add_secret.up.sql" \
  "ALTER TABLE users ADD COLUMN totp_secret TEXT;"
rewrite_ro_column_denylist "${work_repo}/${bootstrap_roles_path}" add-column
commit_scenario "${work_repo}" "migration plus column denylist"
expect_guard_success "${work_repo}" "migration-plus-column-denylist"

git -C "${work_repo}" checkout --quiet -B migration-plus-denylist-table "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000024_rename_keys.up.sql" \
  "ALTER TABLE widgets_keys RENAME TO service_keys;"
rewrite_ro_column_denylist "${work_repo}/${bootstrap_roles_path}" add-table
append_file "${work_repo}/${postgres_bootstrap_test_path}" \
  "fn renamed_key_table_is_fenced() {}"
commit_scenario "${work_repo}" "migration plus denylist table"
expect_guard_success "${work_repo}" "migration-plus-denylist-table"

git -C "${work_repo}" checkout --quiet -B migration-plus-denylist-duplicate "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000025_add_secret.up.sql" \
  "ALTER TABLE users ADD COLUMN totp_secret TEXT;"
rewrite_ro_column_denylist "${work_repo}/${bootstrap_roles_path}" duplicate-table
commit_scenario "${work_repo}" "migration plus duplicate denylist table"
expect_guard_failure "${work_repo}" "migration-plus-denylist-duplicate"
grep -F "cannot verify a CREDENTIAL_TABLES-only change" \
  "${tmp_dir}/migration-plus-denylist-duplicate.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-denylist-column-drop "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000026_add_secret.up.sql" \
  "ALTER TABLE users ADD COLUMN totp_secret TEXT;"
rewrite_ro_column_denylist "${work_repo}/${bootstrap_roles_path}" drop-column
commit_scenario "${work_repo}" "migration plus denylist column drop"
expect_guard_failure "${work_repo}" "migration-plus-denylist-column-drop"
grep -F "may not stop fencing users.password_hash" \
  "${tmp_dir}/migration-plus-denylist-column-drop.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-denylist-removal "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000027_unrelated.up.sql" "SELECT 1;"
rewrite_ro_column_denylist "${work_repo}/${bootstrap_roles_path}" remove-sessions
commit_scenario "${work_repo}" "migration plus denylist removal"
expect_guard_failure "${work_repo}" "migration-plus-denylist-removal"
grep -F "RO_COLUMN_DENYLIST entry sessions may be removed only when" \
  "${tmp_dir}/migration-plus-denylist-removal.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-dropped-denylist-view "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000028_drop_compat_view.up.sql" \
  "DROP VIEW IF EXISTS public.sessions;"
rewrite_ro_column_denylist "${work_repo}/${bootstrap_roles_path}" remove-sessions
commit_scenario "${work_repo}" "migration plus dropped denylist view"
expect_guard_success "${work_repo}" "migration-plus-dropped-denylist-view"

git -C "${work_repo}" checkout --quiet -B migration-plus-denylist-and-code "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000029_add_secret.up.sql" \
  "ALTER TABLE users ADD COLUMN totp_secret TEXT;"
rewrite_ro_column_denylist "${work_repo}/${bootstrap_roles_path}" add-column
append_file "${work_repo}/${bootstrap_roles_path}" \
  'const BOOTSTRAP_MARKER: &str = "changed";'
commit_scenario "${work_repo}" "migration plus denylist and code"
expect_guard_failure "${work_repo}" "migration-plus-denylist-and-code"
grep -F "changes content outside CREDENTIAL_TABLES entries" \
  "${tmp_dir}/migration-plus-denylist-and-code.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-sibling-bootstrap-test "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000021_add_credentials.up.sql" \
  "CREATE TABLE new_credential_table (id BIGINT PRIMARY KEY);"
rewrite_credential_tables "${work_repo}/${bootstrap_roles_path}" add new_credential_table
write_file \
  "${work_repo}/crates/libs/db/migrator/src/bootstrap_roles/tests/other_postgres_test.rs" \
  "fn unrelated_test() {}"
commit_scenario "${work_repo}" "migration plus sibling bootstrap test"
expect_guard_failure "${work_repo}" "migration-plus-sibling-bootstrap-test"
grep -F "other_postgres_test.rs" \
  "${tmp_dir}/migration-plus-sibling-bootstrap-test.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-crates "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000001_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
write_file "${work_repo}/crates/libs/example/src/lib.rs" "pub fn example() {}"
commit_scenario "${work_repo}" "migration plus crates"
expect_guard_failure "${work_repo}" "migration-plus-crates"
grep -F "split Postgres migrations from runtime app changes" "${tmp_dir}/migration-plus-crates.stderr" >/dev/null
grep -F "db/postgres/20260623000001_add_column.up.sql" "${tmp_dir}/migration-plus-crates.stderr" >/dev/null
grep -F "crates/libs/example/src/lib.rs" "${tmp_dir}/migration-plus-crates.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B trunk-stack "${base_commit}"
write_file "${work_repo}/crates/libs/before/src/lib.rs" "pub fn before() {}"
commit_scenario "${work_repo}" "runtime PR before migration"
write_file "${work_repo}/db/postgres/20260623000007_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
commit_scenario "${work_repo}" "migration-only PR"
write_file "${work_repo}/crates/libs/after/src/lib.rs" "pub fn after() {}"
commit_scenario "${work_repo}" "runtime PR after migration"
expect_guard_success "${work_repo}" "trunk-stack" "trunk-merge/pr-123/test"

git -C "${work_repo}" checkout --quiet -B migration-plus-proto "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000002_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
write_file "${work_repo}/proto/example.proto" "syntax = \"proto3\";"
commit_scenario "${work_repo}" "migration plus proto"
expect_guard_failure "${work_repo}" "migration-plus-proto"
grep -F "proto/example.proto" "${tmp_dir}/migration-plus-proto.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-ui "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000003_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
write_file "${work_repo}/ui/package.json" "{}"
commit_scenario "${work_repo}" "migration plus ui"
expect_guard_failure "${work_repo}" "migration-plus-ui"
grep -F "ui/package.json" "${tmp_dir}/migration-plus-ui.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B migration-plus-docs "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000004_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
write_file "${work_repo}/docs/space/migration-note.md" "docs"
commit_scenario "${work_repo}" "migration plus docs"
expect_guard_success "${work_repo}" "migration-plus-docs"

git -C "${work_repo}" checkout --quiet -B migration-plus-guard-maintenance "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000005_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
write_file "${work_repo}/scripts/check-postgres-migrations-example.sh" "#!/usr/bin/env bash"
write_file "${work_repo}/scripts/tests/test-check-postgres-migrations-example.sh" "#!/usr/bin/env bash"
write_file "${work_repo}/.buildkite/pipeline.migration-version-guard.yml" "steps: []"
commit_scenario "${work_repo}" "migration plus guard maintenance"
expect_guard_success "${work_repo}" "migration-plus-guard-maintenance"

git -C "${work_repo}" checkout --quiet -B migration-plus-deploy-script "${base_commit}"
write_file "${work_repo}/db/postgres/20260623000006_add_column.up.sql" "ALTER TABLE widgets ADD COLUMN name TEXT;"
write_file "${work_repo}/scripts/space-deploy.py" "print('deploy')"
commit_scenario "${work_repo}" "migration plus deploy script"
expect_guard_failure "${work_repo}" "migration-plus-deploy-script"
grep -F "scripts/space-deploy.py" "${tmp_dir}/migration-plus-deploy-script.stderr" >/dev/null

# A present-but-stale cached origin/<base> must be freshened before diffing.
# This guard's stale-ref failure is a *false positive*, not a miss: against an
# old merge-base the diff picks up runtime files the branch merely inherited
# from real main, so a migration-only PR is reported as "migration + runtime app
# change" and fails a PR that is correct. Freshening is what keeps it honest.
stale_remote="${tmp_dir}/stale-remote"
mkdir -p "${stale_remote}/scripts"
cp "${script}" "${stale_remote}/scripts/check-postgres-migrations-isolated.sh"

git -C "${stale_remote}" init --quiet --initial-branch=main
git -C "${stale_remote}" config user.email test@example.com
git -C "${stale_remote}" config user.name "Migration Isolation Test"
write_file "${stale_remote}/README.md" "baseline"
git -C "${stale_remote}" add .
git -C "${stale_remote}" commit --quiet -m "baseline"

# Put the poisoned tracking ref on a divergent abandoned lineage so this
# fixture also requires the leading `+` in the fetch refspec.
git -C "${stale_remote}" checkout --quiet -b abandoned
git -C "${stale_remote}" commit --quiet --allow-empty -m "abandoned lineage"
stale_old_main="$(git -C "${stale_remote}" rev-parse HEAD)"
git -C "${stale_remote}" checkout --quiet main

# main moves on with a runtime app change that our branch will only inherit.
write_file "${stale_remote}/crates/foo/src/lib.rs" "pub fn f() {}"
git -C "${stale_remote}" add .
git -C "${stale_remote}" commit --quiet -m "runtime app change on main"
stale_real_main="$(git -C "${stale_remote}" rev-parse HEAD)"

stale_repo="${tmp_dir}/stale-isolated"
git clone --quiet "${stale_remote}" "${stale_repo}"
git -C "${stale_repo}" config user.email test@example.com
git -C "${stale_repo}" config user.name "Migration Isolation Test"

# Our branch sits on the real main and adds a migration and nothing else.
git -C "${stale_repo}" checkout --quiet -B feature "${stale_real_main}"
write_file "${stale_repo}/db/postgres/20260801000000_only.up.sql" "SELECT 1;"
write_file "${stale_repo}/db/postgres/20260801000000_only.down.sql" "SELECT 1;"
git -C "${stale_repo}" add .
git -C "${stale_repo}" commit --quiet -m "migration only"

# Poison the cache with the divergent lineage whose merge-base predates main's
# runtime change.
git -C "${stale_repo}" update-ref refs/remotes/origin/main "${stale_old_main}"

# Freshening runs because a remote is configured, so the guard should pass.
expect_guard_success "${stale_repo}" "stale-cached-ref"

freshened="$(git -C "${stale_repo}" rev-parse --verify --quiet \
  refs/remotes/origin/main || echo MISSING)"
if [[ "${freshened}" != "${stale_real_main}" ]]; then
  echo "expected origin/main to be freshened to ${stale_real_main}, got ${freshened}" >&2
  exit 1
fi

# CI-provided askpass and GIT_SSH helpers are authentication, not prompts for
# the guard to discard. Probe the exact environment passed to `git fetch`.
auth_probe_dir="${tmp_dir}/git-auth-probe"
mkdir -p "${auth_probe_dir}"
write_git_auth_probe "${auth_probe_dir}/git"

run_ci_auth_probe() {
  local name="$1"
  shift
  git -C "${stale_repo}" update-ref refs/remotes/origin/main "${stale_old_main}"

  if ! env \
    PATH="${auth_probe_dir}:${PATH}" \
    REAL_GIT_BIN="$(command -v git)" \
    MIGRATION_ISOLATION_BASE_REF=origin/main \
    BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
    BUILDKITE_BRANCH=feature \
    BUILDKITE_BUILD_ID=fake-build-id \
    "$@" \
    "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-isolated.sh" \
    >"${tmp_dir}/${name}.stdout" 2>"${tmp_dir}/${name}.stderr"; then
    echo "expected the CI guard to preserve ${name} authentication" >&2
    cat "${tmp_dir}/${name}.stdout" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
    exit 1
  fi
}

run_ci_auth_probe "ci-auth-git-ssh" \
  "EXPECTED_GIT_ASKPASS=${tmp_dir}/credential-askpass" \
  "EXPECTED_SSH_ASKPASS=${tmp_dir}/ssh-askpass" \
  "EXPECTED_GIT_SSH=${tmp_dir}/git-ssh" \
  "GIT_ASKPASS=${tmp_dir}/credential-askpass" \
  "SSH_ASKPASS=${tmp_dir}/ssh-askpass" \
  "GIT_SSH=${tmp_dir}/git-ssh"
run_ci_auth_probe "ci-auth-ssh-askpass" \
  "EXPECTED_SSH_ASKPASS=${tmp_dir}/ssh-askpass" \
  "SSH_ASKPASS=${tmp_dir}/ssh-askpass"
inherited_git_parameters="'core.sshCommand=${tmp_dir}/parameter-ssh --parameter-flag'"
run_ci_auth_probe "ci-auth-inherited-config" \
  "EXPECTED_GIT_CONFIG_PARAMETERS=${inherited_git_parameters}" \
  "GIT_CONFIG_PARAMETERS=${inherited_git_parameters}" \
  "EXPECTED_GIT_CONFIG_COUNT=2" \
  "GIT_CONFIG_COUNT=2" \
  "EXPECTED_GIT_CONFIG_KEY_0=credential.helper" \
  "GIT_CONFIG_KEY_0=credential.helper" \
  "EXPECTED_GIT_CONFIG_VALUE_0=${tmp_dir}/credential-helper" \
  "GIT_CONFIG_VALUE_0=${tmp_dir}/credential-helper" \
  "EXPECTED_GIT_CONFIG_KEY_1=core.askPass" \
  "GIT_CONFIG_KEY_1=core.askPass" \
  "EXPECTED_GIT_CONFIG_VALUE_1=${tmp_dir}/core-askpass" \
  "GIT_CONFIG_VALUE_1=${tmp_dir}/core-askpass"
custom_ssh_command="${tmp_dir}/custom-ssh --wrapper-flag"
run_ci_auth_probe "ci-auth-ssh-command" \
  "EXPECTED_GIT_SSH_COMMAND=${custom_ssh_command}" \
  "GIT_SSH_COMMAND=${custom_ssh_command}"
git -C "${stale_repo}" config core.sshCommand "${custom_ssh_command}"
run_ci_auth_probe "ci-auth-core-ssh-command"
git -C "${stale_repo}" config --unset core.sshCommand
run_ci_auth_probe "ci-auth-default-ssh" \
  "EXPECTED_GIT_SSH_COMMAND=ssh -oBatchMode=yes"

git -C "${stale_repo}" update-ref refs/remotes/origin/main "${stale_old_main}"
set +e
MIGRATION_ISOLATION_BASE_REF='origin/*' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/glob-override.stdout" 2>"${tmp_dir}/glob-override.stderr"
set -e
glob_override_ref="$(git -C "${stale_repo}" rev-parse refs/remotes/origin/main)"
if [[ "${glob_override_ref}" != "${stale_old_main}" ]]; then
  echo "expected origin/* override not to fetch or update origin/main" >&2
  exit 1
fi

mkdir -p "${tmp_dir}/timeout-probe-bin"
write_timeout_probe "${tmp_dir}/timeout-probe-bin/timeout"
: >"${tmp_dir}/timeout-probe.log"
set +e
PATH="${tmp_dir}/timeout-probe-bin:${PATH}" \
  EXPECTED_TIMEOUT_DURATION=7s \
  TIMEOUT_PROBE_LOG="${tmp_dir}/timeout-probe.log" \
  MIGRATION_GUARD_FETCH_TIMEOUT_SECS=7 \
  MIGRATION_ISOLATION_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/timeout-probe.stdout" 2>"${tmp_dir}/timeout-probe.stderr"
timeout_probe_exit=$?
set -e
if [[ "${timeout_probe_exit}" -eq 0 ]]; then
  echo "expected a timed-out refresh to fail closed under CI" >&2
  exit 1
fi
if ! grep -Fx "7s" "${tmp_dir}/timeout-probe.log" >/dev/null; then
  echo "expected the base-ref fetch to run under the configured timeout" >&2
  exit 1
fi

# Control: with the fetch broken, the stale ref really does misfire. `remote
# remove` would prune the ref this asserts on, so point the URL at nothing.
git -C "${stale_repo}" update-ref refs/remotes/origin/main "${stale_old_main}"
sensitive_fetch_marker="embedded-credential-must-not-leak"
git -C "${stale_repo}" remote set-url origin \
  "${tmp_dir}/no-such-remote-${sensitive_fetch_marker}"
expect_guard_failure "${stale_repo}" "stale-cached-ref-control"
grep -F "split Postgres migrations from runtime app changes" \
  "${tmp_dir}/stale-cached-ref-control.stderr" >/dev/null
grep -F "crates/foo/src/lib.rs" \
  "${tmp_dir}/stale-cached-ref-control.stderr" >/dev/null

# A resolvable cached ref is not trustworthy after its refresh fails in
# non-queue CI. Refuse to make an isolation decision from that stale snapshot.
set +e
MIGRATION_ISOLATION_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/failed-refresh-ci.stdout" 2>"${tmp_dir}/failed-refresh-ci.stderr"
failed_refresh_ci_exit=$?
set -e
if [[ "${failed_refresh_ci_exit}" -eq 0 ]]; then
  echo "expected a failed base-ref refresh to fail closed under CI" >&2
  cat "${tmp_dir}/failed-refresh-ci.stdout" >&2
  cat "${tmp_dir}/failed-refresh-ci.stderr" >&2
  exit 1
fi
grep -F "could not verify origin/main is current in CI" \
  "${tmp_dir}/failed-refresh-ci.stderr" >/dev/null
if grep -F "${sensitive_fetch_marker}" \
  "${tmp_dir}/failed-refresh-ci.stderr" >/dev/null; then
  echo "expected failed-fetch diagnostics not to expose remote credentials" >&2
  cat "${tmp_dir}/failed-refresh-ci.stderr" >&2
  exit 1
fi

# Base ref absent locally but the remote reachable, under CI. The guard must
# freshen and proceed — not fail closed. This is the ordering the fail-closed
# gate depends on: the fetch has to happen in the main shell before the gate
# reads the result, because the consumer runs inside a process substitution.
recover_repo="${tmp_dir}/recover-base"
git clone --quiet "${stale_remote}" "${recover_repo}"
git -C "${recover_repo}" config user.email test@example.com
git -C "${recover_repo}" config user.name "Migration Guard Test"

git -C "${recover_repo}" checkout --quiet -B feature "${stale_real_main}"
write_file "${recover_repo}/db/postgres/20261101000000_only.up.sql" "SELECT 1;"
write_file "${recover_repo}/db/postgres/20261101000000_only.down.sql" "SELECT 1;"
git -C "${recover_repo}" add .
git -C "${recover_repo}" commit --quiet -m "migration only"

git -C "${recover_repo}" update-ref -d --no-deref refs/remotes/origin/HEAD
git -C "${recover_repo}" update-ref -d refs/remotes/origin/main

if ! MIGRATION_ISOLATION_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH:-bash}" "${recover_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/recover-base.stdout" 2>"${tmp_dir}/recover-base.stderr"; then
  echo "expected the guard to recover a fetchable base ref under CI, not fail closed" >&2
  cat "${tmp_dir}/recover-base.stdout" >&2
  cat "${tmp_dir}/recover-base.stderr" >&2
  exit 1
fi

recovered="$(git -C "${recover_repo}" rev-parse --verify --quiet \
  refs/remotes/origin/main || echo MISSING)"
if [[ "${recovered}" != "${stale_real_main}" ]]; then
  echo "expected origin/main recovered to ${stale_real_main}, got ${recovered}" >&2
  exit 1
fi

# If neither a cached nor fetchable base exists, non-queue CI must fail closed.
git -C "${recover_repo}" update-ref -d --no-deref refs/remotes/origin/HEAD
git -C "${recover_repo}" update-ref -d refs/remotes/origin/main
git -C "${recover_repo}" remote set-url origin "${tmp_dir}/no-such-remote"

set +e
MIGRATION_ISOLATION_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH:-bash}" "${recover_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/no-base-ci.stdout" 2>"${tmp_dir}/no-base-ci.stderr"
no_base_ci_exit=$?
set -e
if [[ "${no_base_ci_exit}" -eq 0 ]]; then
  echo "expected an absent base ref to fail closed under CI" >&2
  cat "${tmp_dir}/no-base-ci.stdout" >&2
  cat "${tmp_dir}/no-base-ci.stderr" >&2
  exit 1
fi
grep -F "no base ref to compare against" "${tmp_dir}/no-base-ci.stderr" >/dev/null

if ! MIGRATION_ISOLATION_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=trunk-merge/pr-123/test \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH:-bash}" "${recover_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/no-base-trunk-merge.stdout" \
  2>"${tmp_dir}/no-base-trunk-merge.stderr"; then
  echo "expected the absent-base guard to pass on a Trunk merge-queue branch" >&2
  cat "${tmp_dir}/no-base-trunk-merge.stdout" >&2
  cat "${tmp_dir}/no-base-trunk-merge.stderr" >&2
  exit 1
fi

# A depth-1 feature checkout has its shallow root above the fork point. The
# base fetch must deepen history enough for a three-dot diff instead of falling
# back to a direct tree comparison.
shallow_remote="${tmp_dir}/shallow-remote"
mkdir -p "${shallow_remote}/scripts"
cp "${script}" "${shallow_remote}/scripts/check-postgres-migrations-isolated.sh"
git -C "${shallow_remote}" init --quiet --initial-branch=main
git -C "${shallow_remote}" config user.email test@example.com
git -C "${shallow_remote}" config user.name "Migration Isolation Test"
write_file "${shallow_remote}/README.md" "baseline"
commit_scenario "${shallow_remote}" "baseline"
for depth_filler in {1..60}; do
  git -C "${shallow_remote}" commit --quiet --allow-empty \
    -m "main commit ${depth_filler}"
done
git -C "${shallow_remote}" checkout --quiet -b shallow-feature
write_file "${shallow_remote}/db/postgres/20261201000000_only.up.sql" "SELECT 1;"
write_file "${shallow_remote}/db/postgres/20261201000000_only.down.sql" "SELECT 1;"
commit_scenario "${shallow_remote}" "migration only"
git -C "${shallow_remote}" checkout --quiet main
for depth_filler in 5 6 7 8; do
  git -C "${shallow_remote}" commit --quiet --allow-empty \
    -m "main commit ${depth_filler}"
done

shallow_repo="${tmp_dir}/shallow"
git clone --quiet --depth=1 --branch shallow-feature \
  "file://${shallow_remote}" "${shallow_repo}"

mkdir -p "${tmp_dir}/shallow-timeout-bin"
write_timeout_probe "${tmp_dir}/shallow-timeout-bin/timeout"
: >"${tmp_dir}/shallow-timeout-probe.log"
if ! PATH="${tmp_dir}/shallow-timeout-bin:${PATH}" \
  EXPECTED_TIMEOUT_DURATION=7s \
  TIMEOUT_PROBE_DEEPEN_ONLY=1 \
  TIMEOUT_PROBE_LOG="${tmp_dir}/shallow-timeout-probe.log" \
  MIGRATION_GUARD_FETCH_TIMEOUT_SECS=7 \
  MIGRATION_ISOLATION_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  "${BASH:-bash}" "${shallow_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/shallow-timeout.stdout" 2>"${tmp_dir}/shallow-timeout.stderr"; then
  echo "expected a timed-out shallow deepen to retain the local fallback" >&2
  exit 1
fi
if ! grep -Fx "deepen:7s" "${tmp_dir}/shallow-timeout-probe.log" >/dev/null; then
  echo "expected the conditional deepen to run under the configured timeout" >&2
  exit 1
fi

if ! MIGRATION_ISOLATION_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  "${BASH:-bash}" "${shallow_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/shallow.stdout" 2>"${tmp_dir}/shallow.stderr"; then
  echo "expected the shallow-checkout isolation guard to pass" >&2
  cat "${tmp_dir}/shallow.stdout" >&2
  cat "${tmp_dir}/shallow.stderr" >&2
  exit 1
fi
if grep -F "no merge-base with" "${tmp_dir}/shallow.stderr" >/dev/null; then
  echo "expected the isolation guard's base fetch to recover the fork point" >&2
  cat "${tmp_dir}/shallow.stderr" >&2
  exit 1
fi
if [[ "$(git -C "${shallow_repo}" rev-parse --is-shallow-repository)" != true ]] \
  || ! git -C "${shallow_repo}" merge-base origin/main HEAD >/dev/null; then
  echo "expected the first deepen to leave a shallow repo with a merge base" >&2
  exit 1
fi

mkdir -p "${tmp_dir}/no-deepen-git-bin"
write_no_deepen_git_probe "${tmp_dir}/no-deepen-git-bin/git"
: >"${tmp_dir}/no-deepen-git.log"
reused_shallow_commit_count="$(git -C "${shallow_repo}" rev-list --count HEAD)"
if ! env PATH="${tmp_dir}/no-deepen-git-bin:${PATH}" \
  REAL_GIT_BIN="$(command -v git)" \
  NO_DEEPEN_PROBE_LOG="${tmp_dir}/no-deepen-git.log" \
  MIGRATION_ISOLATION_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  "${BASH:-bash}" "${shallow_repo}/scripts/check-postgres-migrations-isolated.sh" \
  >"${tmp_dir}/reused-shallow.stdout" 2>"${tmp_dir}/reused-shallow.stderr"; then
  echo "expected the reused shallow-checkout isolation guard to pass" >&2
  exit 1
fi
if [[ -s "${tmp_dir}/no-deepen-git.log" ]]; then
  echo "expected a reused shallow checkout with a merge base not to deepen" >&2
  exit 1
fi
if [[ "$(git -C "${shallow_repo}" rev-list --count HEAD)" != "${reused_shallow_commit_count}" ]]; then
  echo "expected a reused shallow checkout's reachable history not to deepen" >&2
  exit 1
fi

echo "Postgres migration isolation guard tests passed"
