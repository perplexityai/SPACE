#!/usr/bin/env bash
set -euo pipefail

# The guard under test uses `declare -A`, so it needs bash 4+. macOS ships 3.2
# as /bin/bash, where the guard dies with "declare: -A: invalid option" — a
# failure that reads like a guard bug and is not one. Check up front, and invoke
# the guard with this same interpreter rather than whatever `bash` resolves to.
if ((BASH_VERSINFO[0] < 4)); then
  echo "ERROR: this suite needs bash 4+ (running ${BASH_VERSION}); the guard" >&2
  echo "       under test uses 'declare -A'. Try /opt/homebrew/bin/bash." >&2
  exit 2
fi

# Keep the suite deterministic when the caller has personal SSH or askpass
# settings. Individual authentication scenarios opt back into the exact
# mechanism they exercise.
export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_COUNT=0
unset GIT_ASKPASS SSH_ASKPASS GIT_SSH GIT_SSH_COMMAND GIT_CONFIG_PARAMETERS

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${repo_root}/scripts/check-migration-versions.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

write_migration_pair() {
  local repo="$1"
  local version="$2"
  local description="$3"

  cat >"${repo}/db/postgres/${version}_${description}.up.sql" <<'SQL'
SELECT 1;
SQL
  cat >"${repo}/db/postgres/${version}_${description}.down.sql" <<'SQL'
SELECT 1;
SQL
}

write_sqlite_migration_pair() {
  local repo="$1"
  local store="$2"
  local version="$3"
  local description="$4"

  mkdir -p "${repo}/db/${store}"
  cat >"${repo}/db/${store}/${version}_${description}.up.sql" <<'SQL'
SELECT 1;
SQL
  cat >"${repo}/db/${store}/${version}_${description}.down.sql" <<'SQL'
SELECT 1;
SQL
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

  MIGRATION_VERSION_GUARD_BASE_REF=origin/main \
    BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
    BUILDKITE_BRANCH=feature \
    BUILDKITE_BUILD_ID='' \
    "${BASH}" "${repo}/scripts/check-migration-versions.sh" >"${stdout}" 2>"${stderr}"
}

expect_guard_success() {
  local repo="$1"
  local name="$2"

  if ! run_guard "${repo}" "${tmp_dir}/${name}.stdout" "${tmp_dir}/${name}.stderr"; then
    echo "expected ${name} migration guard scenario to pass" >&2
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
    echo "expected ${name} migration guard scenario to fail" >&2
    cat "${tmp_dir}/${name}.stdout" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
    exit 1
  fi
}

work_repo="${tmp_dir}/repo"
mkdir -p "${work_repo}/scripts" "${work_repo}/db/postgres"
cp "${script}" "${work_repo}/scripts/check-migration-versions.sh"

git -C "${work_repo}" init --quiet --initial-branch=main
git -C "${work_repo}" config user.email test@example.com
git -C "${work_repo}" config user.name "Migration Version Test"

write_migration_pair "${work_repo}" "093" "baseline"

git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "base migrations"
base_commit="$(git -C "${work_repo}" rev-parse HEAD)"
git -C "${work_repo}" update-ref refs/remotes/origin/main HEAD

# A resolvable tracking ref is still untrustworthy in ordinary CI when there is
# no origin remote from which to refresh it.
set +e
MIGRATION_VERSION_GUARD_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH}" "${work_repo}/scripts/check-migration-versions.sh" \
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

git -C "${work_repo}" checkout --quiet -b valid-timestamp
write_migration_pair "${work_repo}" "20260617000000" "valid_timestamp"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "valid timestamp migration"
expect_guard_success "${work_repo}" "valid-timestamp"

git -C "${work_repo}" checkout --quiet -B too-small "${base_commit}"
write_migration_pair "${work_repo}" "092" "too_small"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "too-small migration"
expect_guard_failure "${work_repo}" "too-small"
grep -F "at or below the legacy cutoff" "${tmp_dir}/too-small.stderr" >/dev/null
grep -F "db/postgres/092_too_small.up.sql" "${tmp_dir}/too-small.stderr" >/dev/null
grep -F "db/postgres/092_too_small.down.sql" "${tmp_dir}/too-small.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B non-timestamp "${base_commit}"
write_migration_pair "${work_repo}" "094" "bad_timestamp"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "non-timestamp migration"
expect_guard_failure "${work_repo}" "non-timestamp"
grep -F "without a valid UTC YYYYMMDDHHMMSS timestamp" "${tmp_dir}/non-timestamp.stderr" >/dev/null
grep -F "db/postgres/094_bad_timestamp.up.sql" "${tmp_dir}/non-timestamp.stderr" >/dev/null
grep -F "db/postgres/094_bad_timestamp.down.sql" "${tmp_dir}/non-timestamp.stderr" >/dev/null

git -C "${work_repo}" checkout --quiet -B main "${base_commit}"
write_migration_pair "${work_repo}" "20260614000000" "merged_first"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "newer base migration"
git -C "${work_repo}" update-ref refs/remotes/origin/main HEAD

git -C "${work_repo}" checkout --quiet -B stale-branch "${base_commit}"
write_migration_pair "${work_repo}" "20260613000000" "older_timestamp"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "older timestamp migration"
expect_guard_failure "${work_repo}" "stale-branch"
grep -F "not newer than the base branch max Postgres version" "${tmp_dir}/stale-branch.stderr" >/dev/null
grep -F "Base branch max Postgres version: 20260614000000" "${tmp_dir}/stale-branch.stderr" >/dev/null
grep -F "db/postgres/20260613000000_older_timestamp.up.sql" "${tmp_dir}/stale-branch.stderr" >/dev/null
grep -F "db/postgres/20260613000000_older_timestamp.down.sql" "${tmp_dir}/stale-branch.stderr" >/dev/null

# SQLite catalogs get the same not-newer-than-base check. `down` orders a
# consumer's migrations together with db/common's, so a consumer migration is
# compared with the max of its catalog and db/common, and a db/common migration
# with the max of every catalog. history/ holds unbundled migrations that are
# not part of a catalog and must not raise its max.
git -C "${work_repo}" checkout --quiet main
write_sqlite_migration_pair "${work_repo}" "common" "20260930000000" "baseline"
write_sqlite_migration_pair "${work_repo}" "common" "20261008000000" "shared"
write_sqlite_migration_pair "${work_repo}" "sandbox-manager" "20260930000000" "baseline"
write_sqlite_migration_pair "${work_repo}" "sandbox-manager" "20261010000000" "merged_first"
write_sqlite_migration_pair "${work_repo}" "template-manager" "20260930000000" "baseline"
mkdir -p "${work_repo}/db/template-manager/history"
for kind in up down; do
  cp "${work_repo}/db/template-manager/20260930000000_baseline.${kind}.sql" \
    "${work_repo}/db/template-manager/history/20261020000000_retired.${kind}.sql"
  cp "${work_repo}/db/template-manager/20260930000000_baseline.${kind}.sql" \
    "${work_repo}/db/template-manager/history/20261001000000_withdrawn.${kind}.sql"
done
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "SQLite base migrations"
sqlite_base_commit="$(git -C "${work_repo}" rev-parse HEAD)"
git -C "${work_repo}" update-ref refs/remotes/origin/main HEAD

# Prints the SQLite section of a scenario's stderr.
sqlite_findings() {
  sed -n '/^ERROR: new SQLite/,$p' "${tmp_dir}/$1.stderr"
}

# Fails unless <scenario>'s SQLite section names <threshold> from <source> for
# <catalog> and lists <file>.
expect_sqlite_finding() {
  local name="$1"
  local catalog="$2"
  local threshold="$3"
  local source="$4"
  local file="$5"

  if ! sqlite_findings "${name}" \
    | grep -F "New ${catalog} versions must be greater than ${threshold}, the base branch max version in ${source}." >/dev/null \
    || ! sqlite_findings "${name}" | grep -F "    ${file}" >/dev/null; then
    echo "expected ${name} to report ${file} against ${threshold} from ${source}" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
    exit 1
  fi
}

git -C "${work_repo}" checkout --quiet -B sqlite-in-order "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "sandbox-manager" "20261011000000" "in_order"
# Older than sandbox-manager's max but newer than its own catalog's and
# db/common's: consumers are compared only with themselves and db/common.
write_sqlite_migration_pair "${work_repo}" "template-manager" "20261009000000" "own_catalog"
# Newer than every catalog's max; history/ does not count toward the max.
write_sqlite_migration_pair "${work_repo}" "common" "20261011000000" "shared_next"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "in-order SQLite migrations"
expect_guard_success "${work_repo}" "sqlite-in-order"

git -C "${work_repo}" checkout --quiet -B sqlite-out-of-order "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "sandbox-manager" "20261005000000" "authored_early"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "out-of-order SQLite migration"
expect_guard_failure "${work_repo}" "sqlite-out-of-order"
expect_sqlite_finding "sqlite-out-of-order" "db/sandbox-manager" "20261010000000" \
  "db/sandbox-manager" "db/sandbox-manager/20261005000000_authored_early.up.sql"
if grep -F "Postgres" "${tmp_dir}/sqlite-out-of-order.stderr" >/dev/null; then
  echo "expected a SQLite-only failure not to report a Postgres violation" >&2
  cat "${tmp_dir}/sqlite-out-of-order.stderr" >&2
  exit 1
fi

# A db/common migration newer than db/common's max but below a consumer's.
git -C "${work_repo}" checkout --quiet -B sqlite-common-below-consumer "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "common" "20261009000000" "shared_early"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "db/common migration below a consumer's max"
expect_guard_failure "${work_repo}" "sqlite-common-below-consumer"
expect_sqlite_finding "sqlite-common-below-consumer" "db/common" "20261010000000" \
  "db/sandbox-manager" "db/common/20261009000000_shared_early.up.sql"

# A consumer migration newer than its catalog's max but below db/common's.
git -C "${work_repo}" checkout --quiet -B sqlite-consumer-below-common "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "template-manager" "20261005000000" "below_common"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "consumer migration below db/common's max"
expect_guard_failure "${work_repo}" "sqlite-consumer-below-common"
expect_sqlite_finding "sqlite-consumer-below-common" "db/template-manager" "20261008000000" \
  "db/common" "db/template-manager/20261005000000_below_common.up.sql"

# A consumer migration that ties db/common's max fails on purpose. `down` would
# revert it first, but a same-second tie basically never happens, and accepting
# one would make the guard depend on the migrator's tie-break.
git -C "${work_repo}" checkout --quiet -B sqlite-equal-common "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "template-manager" "20261008000000" "equal_common"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "consumer migration equal to db/common's max"
expect_guard_failure "${work_repo}" "sqlite-equal-common"
expect_sqlite_finding "sqlite-equal-common" "db/template-manager" "20261008000000" \
  "db/common" "db/template-manager/20261008000000_equal_common.up.sql"

# A db/common migration that ties a consumer's max, and a consumer migration
# that ties its own catalog's max, are not newer and fail too.
git -C "${work_repo}" checkout --quiet -B sqlite-common-equal-consumer "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "common" "20261010000000" "equal_consumer"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "db/common migration equal to a consumer's max"
expect_guard_failure "${work_repo}" "sqlite-common-equal-consumer"
expect_sqlite_finding "sqlite-common-equal-consumer" "db/common" "20261010000000" \
  "db/sandbox-manager" "db/common/20261010000000_equal_consumer.up.sql"

git -C "${work_repo}" checkout --quiet -B sqlite-equal-own "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "sandbox-manager" "20261010000000" "equal_own"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "consumer migration equal to its own catalog's max"
expect_guard_failure "${work_repo}" "sqlite-equal-own"
expect_sqlite_finding "sqlite-equal-own" "db/sandbox-manager" "20261010000000" \
  "db/sandbox-manager" "db/sandbox-manager/20261010000000_equal_own.up.sql"

# A catalog absent from the base has no release to roll back to, so even a
# version below every base max passes.
git -C "${work_repo}" checkout --quiet -B sqlite-new-catalog "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "new-store" "20261001000000" "baseline"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "new SQLite catalog"
expect_guard_success "${work_repo}" "sqlite-new-catalog"

# A file added directly under history/ is not part of a catalog. The
# absent-catalog skip also drops it, since db/<store>/history is never a base
# catalog, so this case fails only with both that skip and the catalog-path
# filter removed.
git -C "${work_repo}" checkout --quiet -B sqlite-history-added "${sqlite_base_commit}"
for kind in up down; do
  cp "${work_repo}/db/template-manager/20260930000000_baseline.${kind}.sql" \
    "${work_repo}/db/template-manager/history/20261002000000_archived.${kind}.sql"
done
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "add a history migration"
expect_guard_success "${work_repo}" "sqlite-history-added"

# A migration moved into a catalog is an addition, committed or only staged.
git -C "${work_repo}" checkout --quiet -B sqlite-moved-in "${sqlite_base_commit}"
for kind in up down; do
  git -C "${work_repo}" mv "db/template-manager/history/20261001000000_withdrawn.${kind}.sql" \
    "db/template-manager/20261001000000_withdrawn.${kind}.sql"
done
expect_guard_failure "${work_repo}" "sqlite-moved-in-staged"
expect_sqlite_finding "sqlite-moved-in-staged" "db/template-manager" "20261008000000" \
  "db/common" "db/template-manager/20261001000000_withdrawn.up.sql"
git -C "${work_repo}" commit --quiet -m "move a history migration into its catalog"
expect_guard_failure "${work_repo}" "sqlite-moved-in"
expect_sqlite_finding "sqlite-moved-in" "db/template-manager" "20261008000000" \
  "db/common" "db/template-manager/20261001000000_withdrawn.up.sql"

# Uncommitted additions are checked as well, matching the Postgres check.
git -C "${work_repo}" checkout --quiet -B sqlite-untracked "${sqlite_base_commit}"
write_sqlite_migration_pair "${work_repo}" "sandbox-manager" "20261005000000" "untracked"
expect_guard_failure "${work_repo}" "sqlite-untracked"
expect_sqlite_finding "sqlite-untracked" "db/sandbox-manager" "20261010000000" \
  "db/sandbox-manager" "db/sandbox-manager/20261005000000_untracked.up.sql"
rm "${work_repo}/db/sandbox-manager/20261005000000_untracked."*.sql

# Postgres findings are unchanged alongside SQLite ones, and each check reports
# only its own files.
git -C "${work_repo}" checkout --quiet -B mixed-out-of-order "${sqlite_base_commit}"
write_migration_pair "${work_repo}" "20260613000000" "older_timestamp"
write_sqlite_migration_pair "${work_repo}" "sandbox-manager" "20261005000000" "authored_early"
git -C "${work_repo}" add .
git -C "${work_repo}" commit --quiet -m "out-of-order Postgres and SQLite migrations"
expect_guard_failure "${work_repo}" "mixed-out-of-order"
grep -F "Base branch max Postgres version: 20260614000000" \
  "${tmp_dir}/mixed-out-of-order.stderr" >/dev/null
expect_sqlite_finding "mixed-out-of-order" "db/sandbox-manager" "20261010000000" \
  "db/sandbox-manager" "db/sandbox-manager/20261005000000_authored_early.up.sql"
if sed -n '/^ERROR: new Postgres/,/^ERROR: new SQLite/p' \
  "${tmp_dir}/mixed-out-of-order.stderr" | grep -F "db/sandbox-manager/" >/dev/null; then
  echo "expected the Postgres check not to report SQLite files" >&2
  cat "${tmp_dir}/mixed-out-of-order.stderr" >&2
  exit 1
fi

# A present-but-stale cached origin/<base> must be freshened before diffing.
# Without the fetch, the merge-base falls back to a commit predating the legacy
# migrations, so a migration the branch merely inherited from its real base
# looks newly added and trips the legacy-cutoff check. The stale ref is placed
# on a divergent lineage, so it also covers the forcing half of the refspec: a
# fetch without the leading `+` refuses a non-fast-forward and leaves it stale.
remote_repo="${tmp_dir}/remote"
mkdir -p "${remote_repo}/scripts" "${remote_repo}/db/postgres"
cp "${script}" "${remote_repo}/scripts/check-migration-versions.sh"

git -C "${remote_repo}" init --quiet --initial-branch=main
git -C "${remote_repo}" config user.email test@example.com
git -C "${remote_repo}" config user.name "Migration Version Test"

# Commit A: no migrations yet.
git -C "${remote_repo}" add .
git -C "${remote_repo}" commit --quiet -m "scripts only"

# Commit C on an abandoned lineage from A — where the stale ref will point.
git -C "${remote_repo}" checkout --quiet -b abandoned
git -C "${remote_repo}" commit --quiet --allow-empty -m "abandoned lineage"
abandoned_commit="$(git -C "${remote_repo}" rev-parse HEAD)"

# Commit B: the real main tip, carrying a legacy-numbered migration.
git -C "${remote_repo}" checkout --quiet main
write_migration_pair "${remote_repo}" "093" "baseline"
git -C "${remote_repo}" add .
git -C "${remote_repo}" commit --quiet -m "legacy baseline migration"
real_base_commit="$(git -C "${remote_repo}" rev-parse HEAD)"

stale_repo="${tmp_dir}/stale"
git clone --quiet "${remote_repo}" "${stale_repo}"
git -C "${stale_repo}" config user.email test@example.com
git -C "${stale_repo}" config user.name "Migration Version Test"

# The branch sits on the real base and adds one properly-timestamped migration.
git -C "${stale_repo}" checkout --quiet -B feature "${real_base_commit}"
write_migration_pair "${stale_repo}" "20260620000000" "ours"
git -C "${stale_repo}" add .
git -C "${stale_repo}" commit --quiet -m "our new migration"

# Poison the cache: origin/main points at the abandoned lineage.
git -C "${stale_repo}" update-ref refs/remotes/origin/main "${abandoned_commit}"

# This scenario does not go through run_guard/expect_guard_success because it
# asserts on the fetch itself and needs the un-pinned default base, so it pins
# the environment explicitly below rather than inheriting the helper's. (The
# override no longer disables the fetch, so routing through the helper would
# still exercise it — this is about controlling the base, not dodging a skip.)
#
# Leaving the override empty means the guard derives its base from
# BUILDKITE_PULL_REQUEST_BASE_BRANCH, so that has to be pinned too: on a stacked
# PR the agent exports the parent branch, the synthetic remote has no such head,
# the fetch fails, and this scenario would go red for a reason that has nothing
# to do with the code under test.
if ! MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  "${BASH}" "${stale_repo}/scripts/check-migration-versions.sh" \
  >"${tmp_dir}/stale-cached-ref.stdout" 2>"${tmp_dir}/stale-cached-ref.stderr"; then
  echo "expected stale-cached-ref migration guard scenario to pass" >&2
  cat "${tmp_dir}/stale-cached-ref.stdout" >&2
  cat "${tmp_dir}/stale-cached-ref.stderr" >&2
  exit 1
fi

# The fetch must have corrected the poisoned ref, not merely been attempted.
freshened_ref="$(git -C "${stale_repo}" rev-parse --verify --quiet \
  refs/remotes/origin/main || echo MISSING)"
if [[ "${freshened_ref}" != "${real_base_commit}" ]]; then
  echo "expected origin/main to be freshened to ${real_base_commit}, got ${freshened_ref}" >&2
  exit 1
fi

# Buildkite may supply Git credentials through askpass or an SSH wrapper. The
# guard must preserve the selected mechanism exactly while disabling only Git's
# terminal prompts.
auth_probe_dir="${tmp_dir}/git-auth-probe"
mkdir -p "${auth_probe_dir}"
write_git_auth_probe "${auth_probe_dir}/git"

run_ci_auth_probe() {
  local name="$1"
  shift
  git -C "${stale_repo}" update-ref refs/remotes/origin/main "${abandoned_commit}"

  if ! env \
    PATH="${auth_probe_dir}:${PATH}" \
    REAL_GIT_BIN="$(command -v git)" \
    MIGRATION_VERSION_GUARD_BASE_REF='' \
    BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
    BUILDKITE_BRANCH=feature \
    BUILDKITE_BUILD_ID=fake-build-id \
    "$@" \
    "${BASH}" "${stale_repo}/scripts/check-migration-versions.sh" \
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

# SSH_ASKPASS needs interactive SSH authentication even though Git itself must
# not prompt on the terminal. Injecting OpenSSH BatchMode disables this helper.
run_ci_auth_probe "ci-auth-ssh-askpass" \
  "EXPECTED_SSH_ASKPASS=${tmp_dir}/ssh-askpass" \
  "SSH_ASKPASS=${tmp_dir}/ssh-askpass"

# Git transports credential.helper/core.askPass/core.sshCommand through config
# environment channels too. They must reach the fetch without being cleared.
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

# Command-form wrappers may not be OpenSSH and must not receive injected flags.
custom_ssh_command="${tmp_dir}/custom-ssh --wrapper-flag"
run_ci_auth_probe "ci-auth-ssh-command" \
  "EXPECTED_GIT_SSH_COMMAND=${custom_ssh_command}" \
  "GIT_SSH_COMMAND=${custom_ssh_command}"

# Repository configuration remains authoritative when no environment override
# is present; translating it into GIT_SSH_COMMAND would change its command.
git -C "${stale_repo}" config core.sshCommand "${custom_ssh_command}"
run_ci_auth_probe "ci-auth-core-ssh-command"
git -C "${stale_repo}" config --unset core.sshCommand

# With no configured transport or SSH askpass, the guard supplies its own
# noninteractive OpenSSH default.
run_ci_auth_probe "ci-auth-default-ssh" \
  "EXPECTED_GIT_SSH_COMMAND=ssh -oBatchMode=yes"

# An override tail must be one branch name, never a refspec glob that updates
# unrelated tracking refs in the shared worktree ref store.
git -C "${stale_repo}" update-ref refs/remotes/origin/main "${abandoned_commit}"
set +e
MIGRATION_VERSION_GUARD_BASE_REF='origin/*' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID='' \
  "${BASH}" "${stale_repo}/scripts/check-migration-versions.sh" \
  >"${tmp_dir}/glob-override.stdout" 2>"${tmp_dir}/glob-override.stderr"
set -e
glob_override_ref="$(git -C "${stale_repo}" rev-parse refs/remotes/origin/main)"
if [[ "${glob_override_ref}" != "${abandoned_commit}" ]]; then
  echo "expected origin/* override not to fetch or update origin/main" >&2
  exit 1
fi

# An unconditional refresh must run under a wall-clock bound. A Git transport
# or inherited auth helper can otherwise consume the whole Buildkite step.
mkdir -p "${tmp_dir}/timeout-probe-bin"
write_timeout_probe "${tmp_dir}/timeout-probe-bin/timeout"
: >"${tmp_dir}/timeout-probe.log"
set +e
PATH="${tmp_dir}/timeout-probe-bin:${PATH}" \
  EXPECTED_TIMEOUT_DURATION=7s \
  TIMEOUT_PROBE_LOG="${tmp_dir}/timeout-probe.log" \
  MIGRATION_GUARD_FETCH_TIMEOUT_SECS=7 \
  MIGRATION_VERSION_GUARD_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH}" "${stale_repo}/scripts/check-migration-versions.sh" \
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

# Guard the guard: confirm the poisoned ref really would have misfired, so this
# scenario keeps proving the fetch rather than silently passing for any reason.
# The override deliberately no longer disables the fetch, so break the fetch
# instead by pointing the remote at nothing. `remote remove` would not do: it
# prunes refs/remotes/origin/*, destroying the very ref this asserts on.
git -C "${stale_repo}" update-ref refs/remotes/origin/main "${abandoned_commit}"
sensitive_fetch_marker="embedded-credential-must-not-leak"
git -C "${stale_repo}" remote set-url origin \
  "${tmp_dir}/no-such-remote-${sensitive_fetch_marker}"
expect_guard_failure "${stale_repo}" "stale-cached-ref-control"
grep -F "at or below the legacy cutoff" "${tmp_dir}/stale-cached-ref-control.stderr" >/dev/null
grep -F "db/postgres/093_baseline.up.sql" "${tmp_dir}/stale-cached-ref-control.stderr" >/dev/null

# In non-queue CI, a failed refresh must not fall back to the poisoned cached
# ref. Even a resolvable ref can be stale enough to hide an out-of-order
# migration or invent changes the branch did not make.
set +e
MIGRATION_VERSION_GUARD_BASE_REF=origin/main \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  BUILDKITE_BUILD_ID=fake-build-id \
  "${BASH}" "${stale_repo}/scripts/check-migration-versions.sh" \
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

# A set-but-unresolvable override must still freshen rather than fail open.
# Pinning the base to a remote-tracking ref that is absent locally skipped the
# fetch, which left no base at all: the not-newer-than-base check silently
# skipped and the newly-added diff narrowed to HEAD alone. A legacy-numbered
# migration added in an earlier commit then passed the guard.
unresolvable_repo="${tmp_dir}/unresolvable-override"
git clone --quiet "${remote_repo}" "${unresolvable_repo}"
git -C "${unresolvable_repo}" config user.email test@example.com
git -C "${unresolvable_repo}" config user.name "Migration Version Test"

git -C "${unresolvable_repo}" checkout --quiet -B feature "${real_base_commit}"
write_migration_pair "${unresolvable_repo}" "092" "behind_head"
git -C "${unresolvable_repo}" add .
git -C "${unresolvable_repo}" commit --quiet -m "legacy-numbered migration"

# An unrelated commit on top, so a HEAD-only diff would not see the migration.
git -C "${unresolvable_repo}" commit --quiet --allow-empty -m "unrelated commit on top"

# The override names origin/main, which is not present locally.
# --no-deref, or the symref delete dereferences and removes origin/main too,
# leaving the next line a silent no-op.
git -C "${unresolvable_repo}" update-ref -d --no-deref refs/remotes/origin/HEAD
git -C "${unresolvable_repo}" update-ref -d refs/remotes/origin/main

expect_guard_failure "${unresolvable_repo}" "unresolvable-override"
grep -F "at or below the legacy cutoff" "${tmp_dir}/unresolvable-override.stderr" >/dev/null
grep -F "db/postgres/092_behind_head.up.sql" "${tmp_dir}/unresolvable-override.stderr" >/dev/null

# The fetch must have recovered the ref the override names.
recovered_ref="$(git -C "${unresolvable_repo}" rev-parse --verify --quiet \
  refs/remotes/origin/main || echo MISSING)"
if [[ "${recovered_ref}" != "${real_base_commit}" ]]; then
  echo "expected origin/main to be recovered to ${real_base_commit}, got ${recovered_ref}" >&2
  exit 1
fi

# In CI, an unestablishable base must fail closed rather than warn and pass.
# Without a base the not-newer-than-base check is skipped and the newly-added
# set narrows to HEAD alone, so a bad migration added in an earlier commit would
# merge green. Locally the same state degrades to WARNs on purpose, so offline
# runs stay usable — both arms are asserted here.
nobase_repo="${tmp_dir}/no-base"
git clone --quiet "${remote_repo}" "${nobase_repo}"
git -C "${nobase_repo}" config user.email test@example.com
git -C "${nobase_repo}" config user.name "Migration Version Test"

git -C "${nobase_repo}" checkout --quiet -B feature "${real_base_commit}"
write_migration_pair "${nobase_repo}" "091" "hidden_behind_head"
git -C "${nobase_repo}" add .
git -C "${nobase_repo}" commit --quiet -m "legacy-numbered migration"
git -C "${nobase_repo}" commit --quiet --allow-empty -m "unrelated commit on top"

# No cached base, and a remote that cannot be reached to recover one.
git -C "${nobase_repo}" update-ref -d --no-deref refs/remotes/origin/HEAD
git -C "${nobase_repo}" update-ref -d refs/remotes/origin/main
git -C "${nobase_repo}" remote set-url origin "${tmp_dir}/no-such-remote"

run_nobase_guard() {
  local name="$1"
  local branch="${3:-feature}"
  MIGRATION_VERSION_GUARD_BASE_REF='' \
    BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
    BUILDKITE_BRANCH="${branch}" \
    BUILDKITE_BUILD_ID="$2" \
    "${BASH}" "${nobase_repo}/scripts/check-migration-versions.sh" \
    >"${tmp_dir}/${name}.stdout" 2>"${tmp_dir}/${name}.stderr"
}

set +e
run_nobase_guard "no-base-ci" "fake-build-id"
nobase_ci_exit=$?
set -e
if [[ "${nobase_ci_exit}" -eq 0 ]]; then
  echo "expected no-base guard run to fail closed under CI" >&2
  cat "${tmp_dir}/no-base-ci.stdout" >&2
  cat "${tmp_dir}/no-base-ci.stderr" >&2
  exit 1
fi
grep -F "no base ref to compare new migrations against" \
  "${tmp_dir}/no-base-ci.stderr" >/dev/null

set +e
run_nobase_guard "no-base-local" ""
nobase_local_exit=$?
set -e
if [[ "${nobase_local_exit}" -ne 0 ]]; then
  echo "expected no-base guard run to degrade with WARNs outside CI" >&2
  cat "${tmp_dir}/no-base-local.stdout" >&2
  cat "${tmp_dir}/no-base-local.stderr" >&2
  exit 1
fi
grep -F "is unresolvable" "${tmp_dir}/no-base-local.stderr" >/dev/null

if ! run_nobase_guard \
  "no-base-trunk-merge" "fake-build-id" "trunk-merge/pr-123/test"; then
  echo "expected no-base guard run to pass on a Trunk merge-queue branch" >&2
  cat "${tmp_dir}/no-base-trunk-merge.stdout" >&2
  cat "${tmp_dir}/no-base-trunk-merge.stderr" >&2
  exit 1
fi

# A shallow checkout must still yield a usable merge-base. A Buildkite PR build
# clones the *head* branch shallowly, which puts the shallow root above the
# base's fork point; fetching the base without --deepen leaves
# `merge-base origin/<base> HEAD` unresolvable, and the guard drops to the weaker
# two-tree diff. Assert on the absence of the no-merge-base WARN, which is the
# observable difference.
shallow_remote="${tmp_dir}/shallow-remote"
mkdir -p "${shallow_remote}/scripts" "${shallow_remote}/db/postgres"
cp "${script}" "${shallow_remote}/scripts/check-migration-versions.sh"

git -C "${shallow_remote}" init --quiet --initial-branch=main
git -C "${shallow_remote}" config user.email test@example.com
git -C "${shallow_remote}" config user.name "Migration Version Test"
write_migration_pair "${shallow_remote}" "093" "baseline"
git -C "${shallow_remote}" add .
git -C "${shallow_remote}" commit --quiet -m "baseline"
for depth_filler in {1..60}; do
  git -C "${shallow_remote}" commit --quiet --allow-empty \
    -m "main commit ${depth_filler}"
done

# The head branch forks here, then main moves further on.
git -C "${shallow_remote}" checkout --quiet -b shallow-feature
write_migration_pair "${shallow_remote}" "20261001000000" "ours"
git -C "${shallow_remote}" add .
git -C "${shallow_remote}" commit --quiet -m "our migration"
git -C "${shallow_remote}" checkout --quiet main
for depth_filler in 5 6 7 8; do
  git -C "${shallow_remote}" commit --quiet --allow-empty \
    -m "main commit ${depth_filler}"
done

shallow_repo="${tmp_dir}/shallow"
git clone --quiet --depth=1 --branch shallow-feature \
  "file://${shallow_remote}" "${shallow_repo}"
git -C "${shallow_repo}" config user.email test@example.com
git -C "${shallow_repo}" config user.name "Migration Version Test"

# Let the ordinary refresh complete, then make only the conditional deepen time
# out. This proves the second network call carries the same wall-clock wrapper.
mkdir -p "${tmp_dir}/shallow-timeout-bin"
write_timeout_probe "${tmp_dir}/shallow-timeout-bin/timeout"
: >"${tmp_dir}/shallow-timeout-probe.log"
if ! PATH="${tmp_dir}/shallow-timeout-bin:${PATH}" \
  EXPECTED_TIMEOUT_DURATION=7s \
  TIMEOUT_PROBE_DEEPEN_ONLY=1 \
  TIMEOUT_PROBE_LOG="${tmp_dir}/shallow-timeout-probe.log" \
  MIGRATION_GUARD_FETCH_TIMEOUT_SECS=7 \
  MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  "${BASH}" "${shallow_repo}/scripts/check-migration-versions.sh" \
  >"${tmp_dir}/shallow-timeout.stdout" 2>"${tmp_dir}/shallow-timeout.stderr"; then
  echo "expected a timed-out shallow deepen to retain the local fallback" >&2
  exit 1
fi
if ! grep -Fx "deepen:7s" "${tmp_dir}/shallow-timeout-probe.log" >/dev/null; then
  echo "expected the conditional deepen to run under the configured timeout" >&2
  exit 1
fi

if ! MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  "${BASH}" "${shallow_repo}/scripts/check-migration-versions.sh" \
  >"${tmp_dir}/shallow.stdout" 2>"${tmp_dir}/shallow.stderr"; then
  echo "expected shallow-checkout guard run to pass" >&2
  cat "${tmp_dir}/shallow.stdout" >&2
  cat "${tmp_dir}/shallow.stderr" >&2
  exit 1
fi

if grep -F "no merge-base with" "${tmp_dir}/shallow.stderr" >/dev/null; then
  echo "expected a resolvable merge-base on a shallow checkout; the fetch must" >&2
  echo "deepen enough to reach the fork point" >&2
  cat "${tmp_dir}/shallow.stderr" >&2
  exit 1
fi
if [[ "$(git -C "${shallow_repo}" rev-parse --is-shallow-repository)" != true ]] \
  || ! git -C "${shallow_repo}" merge-base origin/main HEAD >/dev/null; then
  echo "expected the first deepen to leave a shallow repo with a merge base" >&2
  exit 1
fi

# The successful run above left this clone shallow but exposed its fork point.
# A reused Buildkite worktree in that state must refresh without deepening again.
mkdir -p "${tmp_dir}/no-deepen-git-bin"
write_no_deepen_git_probe "${tmp_dir}/no-deepen-git-bin/git"
: >"${tmp_dir}/no-deepen-git.log"
reused_shallow_commit_count="$(git -C "${shallow_repo}" rev-list --count HEAD)"
if ! env PATH="${tmp_dir}/no-deepen-git-bin:${PATH}" \
  REAL_GIT_BIN="$(command -v git)" \
  NO_DEEPEN_PROBE_LOG="${tmp_dir}/no-deepen-git.log" \
  MIGRATION_VERSION_GUARD_BASE_REF='' \
  BUILDKITE_PULL_REQUEST_BASE_BRANCH=main \
  BUILDKITE_BRANCH=feature \
  "${BASH}" "${shallow_repo}/scripts/check-migration-versions.sh" \
  >"${tmp_dir}/reused-shallow.stdout" 2>"${tmp_dir}/reused-shallow.stderr"; then
  echo "expected the reused shallow-checkout guard run to pass" >&2
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

echo "migration version guard tests passed"
