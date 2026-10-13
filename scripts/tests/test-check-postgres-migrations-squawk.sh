#!/usr/bin/env bash
set -euo pipefail

# Ignore caller-specific Git authentication. The explicit CI authentication
# scenarios below install only the mechanism they are testing.
export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_COUNT=0
unset GIT_ASKPASS SSH_ASKPASS GIT_SSH GIT_SSH_COMMAND GIT_CONFIG_PARAMETERS

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${repo_root}/scripts/check-postgres-migrations-squawk.sh"
partial_index_checker="${repo_root}/scripts/check_postgres_partial_indexes.py"
partial_index_checker_test="${repo_root}/scripts/tests/test_check_postgres_partial_indexes.py"
fixture_dir="${repo_root}/scripts/tests/fixtures/postgres-partial-index"
uv_bin="$(bash "${repo_root}/ci/scripts/ensure-uv.sh")"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# Run from the repository, uv would otherwise re-lock its project first. As in
# ci/scripts/run-python-tests.sh, CI checks the lock is current (--locked) and
# local runs use it as committed (--frozen).
uv_lock_mode=--frozen
if [ -n "${CI:-}" ]; then uv_lock_mode=--locked; fi
"${uv_bin}" run "${uv_lock_mode}" --with pglast==7.10 "${partial_index_checker_test}"

write_migration_pair() {
  local repo="$1"
  local version="$2"
  local description="$3"
  local up_fixture="${4:-allow/noop.sql}"
  local down_fixture="${5:-allow/noop.sql}"

  cp "${fixture_dir}/${up_fixture}" "${repo}/db/postgres/${version}_${description}.up.sql"
  cp "${fixture_dir}/${down_fixture}" "${repo}/db/postgres/${version}_${description}.down.sql"
}

write_inline_migration_pair() {
  local repo="$1"
  local version="$2"
  local description="$3"
  local up_sql="$4"
  local down_sql="${5:-SELECT 1;}"

  printf '%s\n' "${up_sql}" >"${repo}/db/postgres/${version}_${description}.up.sql"
  printf '%s\n' "${down_sql}" >"${repo}/db/postgres/${version}_${description}.down.sql"
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

write_stub_squawk() {
  local path="$1"
  local exit_code="${2:-0}"

  cat >"${path}" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" >>"${SQUAWK_ARGS_LOG}"
exit "${SQUAWK_STUB_EXIT_CODE:-0}"
SH
  chmod +x "${path}"
  SQUAWK_STUB_EXIT_CODE="${exit_code}"
  export SQUAWK_STUB_EXIT_CODE
}

run_guard() {
  local repo="$1"
  local stdout="$2"
  local stderr="$3"

  MIGRATION_SQUAWK_BASE_REF=origin/main \
    BUILDKITE_BRANCH=feature \
    BUILDKITE_BUILD_ID='' \
    SQUAWK_BIN="${tmp_dir}/squawk-stub" \
    SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
    TEST_UV_BIN="${uv_bin}" \
    bash "${repo}/scripts/check-postgres-migrations-squawk.sh" >"${stdout}" 2>"${stderr}"
}

expect_guard_success() {
  local repo="$1"
  local name="$2"

  : >"${tmp_dir}/squawk-args.log"
  if ! run_guard "${repo}" "${tmp_dir}/${name}.stdout" "${tmp_dir}/${name}.stderr"; then
    echo "expected ${name} Squawk guard scenario to pass" >&2
    cat "${tmp_dir}/${name}.stdout" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
    exit 1
  fi
}

expect_guard_failure() {
  local repo="$1"
  local name="$2"

  : >"${tmp_dir}/squawk-args.log"
  set +e
  run_guard "${repo}" "${tmp_dir}/${name}.stdout" "${tmp_dir}/${name}.stderr"
  local exit_code=$?
  set -e

  if [[ "${exit_code}" -eq 0 ]]; then
    echo "expected ${name} Squawk guard scenario to fail" >&2
    cat "${tmp_dir}/${name}.stdout" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
    exit 1
  fi
}

work_repo="${tmp_dir}/repo"
mkdir -p "${work_repo}/scripts" "${work_repo}/db/postgres" "${work_repo}/db/common"
cp "${script}" "${partial_index_checker}" "${work_repo}/scripts/"
mkdir -p "${work_repo}/ci/scripts"
cat >"${work_repo}/ci/scripts/ensure-uv.sh" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "${TEST_UV_BIN:?}"
SH
chmod +x "${work_repo}/ci/scripts/ensure-uv.sh"

git -C "${work_repo}" init --quiet --initial-branch=main
git -C "${work_repo}" config user.email test@example.com
git -C "${work_repo}" config user.name "Migration Squawk Test"

write_migration_pair "${work_repo}" "093" "baseline"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "base migrations"
base_commit="$(git -C "${work_repo}" rev-parse HEAD)"
git -C "${work_repo}" update-ref refs/remotes/origin/main HEAD

set +e
MIGRATION_SQUAWK_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${work_repo}/scripts/check-postgres-migrations-squawk.sh" \
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

write_stub_squawk "${tmp_dir}/squawk-stub" 0

git -C "${work_repo}" checkout --quiet -B no-postgres-change "${base_commit}"
cp "${fixture_dir}/allow/noop.sql" "${work_repo}/db/common/001_common.up.sql"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "common migration"
expect_guard_success "${work_repo}" "no-postgres-change"
grep -F "No changed Postgres migrations to check" "${tmp_dir}/no-postgres-change.stdout" >/dev/null
if [[ -s "${tmp_dir}/squawk-args.log" ]]; then
  echo "expected Squawk not to run when no Postgres migrations changed" >&2
  cat "${tmp_dir}/squawk-args.log" >&2
  exit 1
fi

git -C "${work_repo}" checkout --quiet -B changed-postgres "${base_commit}"
write_migration_pair "${work_repo}" "20260623000000" "changed" "allow/simple-alter.sql"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "changed postgres migration"
expect_guard_success "${work_repo}" "changed-postgres"
grep -F "Checking changed Postgres migrations" "${tmp_dir}/changed-postgres.stdout" >/dev/null
grep -F -- "--pg-version=17.0" "${tmp_dir}/squawk-args.log" >/dev/null
grep -F -- "--assume-in-transaction" "${tmp_dir}/squawk-args.log" >/dev/null
grep -F "db/postgres/20260623000000_changed.up.sql" "${tmp_dir}/squawk-args.log" >/dev/null
if grep -F "db/postgres/20260623000000_changed.down.sql" "${tmp_dir}/squawk-args.log" >/dev/null; then
  echo "expected Squawk not to lint rollback migrations" >&2
  cat "${tmp_dir}/squawk-args.log" >&2
  exit 1
fi

git -C "${work_repo}" checkout --quiet -B uuid-v4-rejected "${base_commit}"
write_inline_migration_pair "${work_repo}" "20260623000001" "uuid_v4_rejected" \
  'ALTER TABLE widgets ALTER COLUMN id SET DEFAULT gen_random_uuid();'
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "uuid v4 default"
expect_guard_failure "${work_repo}" "uuid-v4-rejected"
grep -F "generates UUIDv4 without a justified exception" \
  "${tmp_dir}/uuid-v4-rejected.stderr" >/dev/null
grep -F "space_uuid_v7()" "${tmp_dir}/uuid-v4-rejected.stderr" >/dev/null
if [[ -s "${tmp_dir}/squawk-args.log" ]]; then
  echo "expected UUIDv4 policy failure before Squawk runs" >&2
  cat "${tmp_dir}/squawk-args.log" >&2
  exit 1
fi

git -C "${work_repo}" checkout --quiet -B uuid-v4-uppercase-rejected "${base_commit}"
write_inline_migration_pair "${work_repo}" "20260623000010" "uuid_v4_uppercase_rejected" \
  'ALTER TABLE widgets ALTER COLUMN id SET DEFAULT UUID_GENERATE_V4();'
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "uppercase uuid v4 default"
expect_guard_failure "${work_repo}" "uuid-v4-uppercase-rejected"
grep -F "generates UUIDv4 without a justified exception" \
  "${tmp_dir}/uuid-v4-uppercase-rejected.stderr" >/dev/null
if [[ -s "${tmp_dir}/squawk-args.log" ]]; then
  echo "expected uppercase UUIDv4 policy failure before Squawk runs" >&2
  cat "${tmp_dir}/squawk-args.log" >&2
  exit 1
fi

git -C "${work_repo}" checkout --quiet -B uuid-v4-empty-exception "${base_commit}"
write_inline_migration_pair "${work_repo}" "20260623000002" "uuid_v4_empty_exception" \
  'ALTER TABLE widgets ALTER COLUMN id SET DEFAULT gen_random_uuid(); /* space-allow-uuid-v4: */'
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "empty uuid v4 exception"
expect_guard_failure "${work_repo}" "uuid-v4-empty-exception"
grep -F "generates UUIDv4 without a justified exception" \
  "${tmp_dir}/uuid-v4-empty-exception.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B uuid-v4-privacy-exception "${base_commit}"
write_inline_migration_pair "${work_repo}" "20260623000003" "uuid_v4_privacy_exception" \
  'ALTER TABLE widgets ALTER COLUMN id SET DEFAULT gen_random_uuid(); /* space-allow-uuid-v4: tenant-visible identifier must not disclose creation time */'
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "justified uuid v4 exception"
expect_guard_success "${work_repo}" "uuid-v4-privacy-exception"
grep -F "db/postgres/20260623000003_uuid_v4_privacy_exception.up.sql" \
  "${tmp_dir}/squawk-args.log" >/dev/null

git -C "${work_repo}" checkout --quiet -B squawk-failure "${base_commit}"
write_migration_pair "${work_repo}" "20260623000004" "bad" "allow/simple-alter.sql"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "bad postgres migration"
write_stub_squawk "${tmp_dir}/squawk-stub" 27
expect_guard_failure "${work_repo}" "squawk-failure"
grep -F "db/postgres/20260623000004_bad.up.sql" "${tmp_dir}/squawk-args.log" >/dev/null

git -C "${work_repo}" checkout --quiet -B partial-index "${base_commit}"
write_migration_pair "${work_repo}" "20260623000005" "partial_index" \
  "deny/basic-partial-index.sql" \
  "deny/multiline-unique-partial-index.sql"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "partial indexes"
write_stub_squawk "${tmp_dir}/squawk-stub" 0
expect_guard_failure "${work_repo}" "partial-index"
grep -F "idx_basic" "${tmp_dir}/partial-index.stderr" >/dev/null
grep -F "idx_unique" "${tmp_dir}/partial-index.stderr" >/dev/null
grep -F "Prefer a non-partial index or redesign the access path" "${tmp_dir}/partial-index.stderr" >/dev/null
if [[ -s "${tmp_dir}/squawk-args.log" ]]; then
  echo "expected the partial-index check to fail before Squawk runs" >&2
  cat "${tmp_dir}/squawk-args.log" >&2
  exit 1
fi

# A present-but-stale cached origin/<base> must be freshened before diffing.
# Here the symptom is a widened change set: against an old merge-base the guard
# lints migrations the branch merely inherited from real main, so it reports on
# files the author never touched. Assert the fetch actually corrects the ref, and
# that only the fetch is what corrects it.
stale_remote="${tmp_dir}/stale-remote"
mkdir -p "${stale_remote}/scripts" "${stale_remote}/db/postgres"
cp "${script}" "${partial_index_checker}" "${stale_remote}/scripts/"
mkdir -p "${stale_remote}/ci/scripts"
cat >"${stale_remote}/ci/scripts/ensure-uv.sh" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "${TEST_UV_BIN:?}"
SH
chmod +x "${stale_remote}/ci/scripts/ensure-uv.sh"

git -C "${stale_remote}" init --quiet --initial-branch=main
git -C "${stale_remote}" config user.email test@example.com
git -C "${stale_remote}" config user.name "Migration Squawk Test"
write_migration_pair "${stale_remote}" "093" "baseline"
git -C "${stale_remote}" add .
git -C "${stale_remote}" commit --quiet -m "baseline"
git -C "${stale_remote}" checkout --quiet -b abandoned
git -C "${stale_remote}" commit --quiet --allow-empty -m "abandoned lineage"
stale_old_main="$(git -C "${stale_remote}" rev-parse HEAD)"
git -C "${stale_remote}" checkout --quiet main

# main lands a migration our branch will only inherit, never author.
cp "${fixture_dir}/allow/noop.sql" "${stale_remote}/db/postgres/20260801000000_theirs.up.sql"
cp "${fixture_dir}/allow/noop.sql" "${stale_remote}/db/postgres/20260801000000_theirs.down.sql"
git -C "${stale_remote}" add .
git -C "${stale_remote}" commit --quiet -m "someone else's migration"
stale_real_main="$(git -C "${stale_remote}" rev-parse HEAD)"

stale_repo="${tmp_dir}/stale-squawk"
git clone --quiet "${stale_remote}" "${stale_repo}"
git -C "${stale_repo}" config user.email test@example.com
git -C "${stale_repo}" config user.name "Migration Squawk Test"

git -C "${stale_repo}" checkout --quiet -B feature "${stale_real_main}"
cp "${fixture_dir}/allow/noop.sql" "${stale_repo}/db/postgres/20260901000000_ours.up.sql"
cp "${fixture_dir}/allow/noop.sql" "${stale_repo}/db/postgres/20260901000000_ours.down.sql"
git -C "${stale_repo}" add .
git -C "${stale_repo}" commit --quiet -m "our migration"

git -C "${stale_repo}" update-ref refs/remotes/origin/main "${stale_old_main}"
: >"${tmp_dir}/squawk-args.log"
expect_guard_success "${stale_repo}" "stale-cached-ref"

freshened="$(git -C "${stale_repo}" rev-parse --verify --quiet \
  refs/remotes/origin/main || echo MISSING)"
if [[ "${freshened}" != "${stale_real_main}" ]]; then
  echo "expected origin/main to be freshened to ${stale_real_main}, got ${freshened}" >&2
  exit 1
fi

# CI credential helpers must reach the fetch unchanged. A PATH shim observes
# the environment without requiring a real authenticated remote.
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
    MIGRATION_SQUAWK_BASE_REF=origin/main \
    BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
    BUILDKITE_BRANCH=feature \
    BUILDKITE_BUILD_ID=fake-build-id \
    SQUAWK_BIN="${tmp_dir}/squawk-stub" \
    SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
    TEST_UV_BIN="${uv_bin}" \
    "$@" \
    "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-squawk.sh" \
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
MIGRATION_SQUAWK_BASE_REF='origin/*' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-squawk.sh" \
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
  MIGRATION_SQUAWK_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-squawk.sh" \
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

# Only our own migration should have been linted, not the inherited one.
if grep -F "20260801000000_theirs.up.sql" "${tmp_dir}/squawk-args.log" >/dev/null; then
  echo "expected Squawk not to lint a migration inherited from the base" >&2
  cat "${tmp_dir}/squawk-args.log" >&2
  exit 1
fi

# Control: with the fetch broken the stale ref survives, and the inherited
# migration is linted. `remote remove` would prune the ref under assertion.
git -C "${stale_repo}" update-ref refs/remotes/origin/main "${stale_old_main}"
sensitive_fetch_marker="embedded-credential-must-not-leak"
git -C "${stale_repo}" remote set-url origin \
  "${tmp_dir}/no-such-remote-${sensitive_fetch_marker}"
: >"${tmp_dir}/squawk-args.log"
expect_guard_success "${stale_repo}" "stale-cached-ref-control"
grep -F "20260801000000_theirs.up.sql" "${tmp_dir}/squawk-args.log" >/dev/null

# A failed refresh in non-queue CI must not let the lint target set come from a
# stale cached ref, even when that ref still resolves.
set +e
MIGRATION_SQUAWK_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${stale_repo}/scripts/check-postgres-migrations-squawk.sh" \
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
cp "${fixture_dir}/allow/noop.sql" "${recover_repo}/db/postgres/20261101000000_only.up.sql"
cp "${fixture_dir}/allow/noop.sql" "${recover_repo}/db/postgres/20261101000000_only.down.sql"
git -C "${recover_repo}" add .
git -C "${recover_repo}" commit --quiet -m "migration only"

git -C "${recover_repo}" update-ref -d --no-deref refs/remotes/origin/HEAD
git -C "${recover_repo}" update-ref -d refs/remotes/origin/main

if ! MIGRATION_SQUAWK_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${recover_repo}/scripts/check-postgres-migrations-squawk.sh" \
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

# Cover both arms when no cached base exists and the remote cannot recover it.
git -C "${recover_repo}" update-ref -d --no-deref refs/remotes/origin/HEAD
git -C "${recover_repo}" update-ref -d refs/remotes/origin/main
git -C "${recover_repo}" remote set-url origin "${tmp_dir}/no-such-remote"

set +e
MIGRATION_SQUAWK_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${recover_repo}/scripts/check-postgres-migrations-squawk.sh" \
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

if ! MIGRATION_SQUAWK_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=trunk-merge/pr-123/test \
  BUILDKITE_BUILD_ID=fake-build-id \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${recover_repo}/scripts/check-postgres-migrations-squawk.sh" \
  >"${tmp_dir}/no-base-trunk-merge.stdout" \
  2>"${tmp_dir}/no-base-trunk-merge.stderr"; then
  echo "expected the absent-base guard to pass on a Trunk merge-queue branch" >&2
  cat "${tmp_dir}/no-base-trunk-merge.stdout" >&2
  cat "${tmp_dir}/no-base-trunk-merge.stderr" >&2
  exit 1
fi

# Pin the copied guard's deepening flag with a depth-1 feature checkout whose
# shallow boundary otherwise hides the fork point.
shallow_remote="${tmp_dir}/shallow-remote"
mkdir -p "${shallow_remote}/scripts" "${shallow_remote}/db/postgres"
cp "${script}" "${partial_index_checker}" "${shallow_remote}/scripts/"
mkdir -p "${shallow_remote}/ci/scripts"
cat >"${shallow_remote}/ci/scripts/ensure-uv.sh" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "${TEST_UV_BIN:?}"
SH
chmod +x "${shallow_remote}/ci/scripts/ensure-uv.sh"
git -C "${shallow_remote}" init --quiet --initial-branch=main
git -C "${shallow_remote}" config user.email test@example.com
git -C "${shallow_remote}" config user.name "Migration Squawk Test"
write_migration_pair "${shallow_remote}" "093" "baseline"
git -C "${shallow_remote}" add .
git -C "${shallow_remote}" commit --quiet -m "baseline"
for depth_filler in {1..60}; do
  git -C "${shallow_remote}" commit --quiet --allow-empty \
    -m "main commit ${depth_filler}"
done
git -C "${shallow_remote}" checkout --quiet -b shallow-feature
write_migration_pair "${shallow_remote}" "20261201000001" "only"
git -C "${shallow_remote}" add .
git -C "${shallow_remote}" commit --quiet -m "migration only"
git -C "${shallow_remote}" checkout --quiet main
for depth_filler in 5 6 7 8; do
  git -C "${shallow_remote}" commit --quiet --allow-empty \
    -m "main commit ${depth_filler}"
done

shallow_repo="${tmp_dir}/shallow"
git clone --quiet --depth=1 --branch shallow-feature \
  "file://${shallow_remote}" "${shallow_repo}"
: >"${tmp_dir}/squawk-args.log"

mkdir -p "${tmp_dir}/shallow-timeout-bin"
write_timeout_probe "${tmp_dir}/shallow-timeout-bin/timeout"
: >"${tmp_dir}/shallow-timeout-probe.log"
if ! PATH="${tmp_dir}/shallow-timeout-bin:${PATH}" \
  EXPECTED_TIMEOUT_DURATION=7s \
  TIMEOUT_PROBE_DEEPEN_ONLY=1 \
  TIMEOUT_PROBE_LOG="${tmp_dir}/shallow-timeout-probe.log" \
  MIGRATION_GUARD_FETCH_TIMEOUT_SECS=7 \
  MIGRATION_SQUAWK_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${shallow_repo}/scripts/check-postgres-migrations-squawk.sh" \
  >"${tmp_dir}/shallow-timeout.stdout" 2>"${tmp_dir}/shallow-timeout.stderr"; then
  echo "expected a timed-out shallow deepen to retain the local fallback" >&2
  exit 1
fi
if ! grep -Fx "deepen:7s" "${tmp_dir}/shallow-timeout-probe.log" >/dev/null; then
  echo "expected the conditional deepen to run under the configured timeout" >&2
  exit 1
fi

if ! MIGRATION_SQUAWK_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${shallow_repo}/scripts/check-postgres-migrations-squawk.sh" \
  >"${tmp_dir}/shallow.stdout" 2>"${tmp_dir}/shallow.stderr"; then
  echo "expected the shallow-checkout Squawk guard to pass" >&2
  cat "${tmp_dir}/shallow.stdout" >&2
  cat "${tmp_dir}/shallow.stderr" >&2
  exit 1
fi
if grep -F "no merge-base with" "${tmp_dir}/shallow.stderr" >/dev/null; then
  echo "expected the Squawk guard's base fetch to recover the fork point" >&2
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
  MIGRATION_SQUAWK_BASE_REF='' MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  SQUAWK_BIN="${tmp_dir}/squawk-stub" \
  SQUAWK_ARGS_LOG="${tmp_dir}/squawk-args.log" \
  TEST_UV_BIN="${uv_bin}" \
  "${BASH:-bash}" "${shallow_repo}/scripts/check-postgres-migrations-squawk.sh" \
  >"${tmp_dir}/reused-shallow.stdout" 2>"${tmp_dir}/reused-shallow.stderr"; then
  echo "expected the reused shallow-checkout Squawk guard to pass" >&2
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

echo "Postgres Squawk guard tests passed"
