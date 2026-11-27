#!/usr/bin/env bash
set -euo pipefail

unset MIGRATION_ISOLATION_BASE_REF MIGRATION_VERSION_GUARD_BASE_REF BUILDKITE_BRANCH

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
checker="${repo_root}/ci/scripts/check-migration-isolation-per-commit.sh"
guard="${repo_root}/scripts/check-postgres-migrations-isolated.sh"
base_helper="${repo_root}/scripts/lib/migration-base.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fixture="${tmp_dir}/repo"
git init -q -b main "${fixture}"
git -C "${fixture}" config user.name test
git -C "${fixture}" config user.email test@example.com
mkdir -p "${fixture}/scripts/lib"
cp "${guard}" "${fixture}/scripts/check-postgres-migrations-isolated.sh"
cp "${base_helper}" "${fixture}/scripts/lib/migration-base.sh"
git -C "${fixture}" add .
git -C "${fixture}" commit -q -m base

runtime_file=crates/app/src/lib.rs

commit_change() {
    local message="$1"
    shift
    local path

    for path in "$@"; do
        mkdir -p "$(dirname "${fixture}/${path}")"
        printf '%s\n' "${message}" >>"${fixture}/${path}"
    done
    git -C "${fixture}" add .
    git -C "${fixture}" commit -q -m "${message}"
}

run_checker() {
    local name="$1"
    local base="$2"

    (cd "${fixture}" && BUILDKITE_BUILD_ID=test bash "${checker}" "${base}") \
        >"${tmp_dir}/${name}.stdout" 2>"${tmp_dir}/${name}.stderr"
}

show_output() {
    local name="$1"

    cat "${tmp_dir}/${name}.stdout" >&2
    cat "${tmp_dir}/${name}.stderr" >&2
}

expect_pass() {
    local name="$1"
    local base="$2"

    if ! run_checker "${name}" "${base}"; then
        echo "${name}: expected per-commit migration isolation to pass" >&2
        show_output "${name}"
        exit 1
    fi
}

expect_failure_naming() {
    local name="$1"
    local base="$2"
    local subject="$3"

    if run_checker "${name}" "${base}"; then
        echo "${name}: expected per-commit migration isolation to fail" >&2
        show_output "${name}"
        exit 1
    fi
    if ! grep -Fq "${subject}" "${tmp_dir}/${name}.stderr"; then
        echo "${name}: expected the failure to name '${subject}'" >&2
        show_output "${name}"
        exit 1
    fi
}

# A batch whose commits each keep migrations and runtime code apart passes,
# even though the whole-range guard rejects the same batch.
batch_base="$(git -C "${fixture}" rev-parse HEAD)"
commit_change "runtime only" "${runtime_file}"
commit_change "migration only" db/postgres/20260925000001_split.up.sql
expect_pass split-batch "${batch_base}"

if (cd "${fixture}" && BUILDKITE_BUILD_ID=test MIGRATION_ISOLATION_BASE_REF="${batch_base}" \
    bash scripts/check-postgres-migrations-isolated.sh) \
    >"${tmp_dir}/whole-range.stdout" 2>"${tmp_dir}/whole-range.stderr" \
    || ! grep -Fq "split Postgres migrations from runtime app changes" \
        "${tmp_dir}/whole-range.stderr"; then
    echo "whole-range: expected the guard to reject the batch as one diff" >&2
    show_output whole-range
    exit 1
fi

# A single landed commit that mixes both still fails, and only it is named.
mixed_base="$(git -C "${fixture}" rev-parse HEAD)"
commit_change "runtime before mixed" "${runtime_file}"
commit_change "mixed change" "${runtime_file}" db/postgres/20260925000002_mixed.up.sql
expect_failure_naming mixed-commit "${mixed_base}" "mixed change"
if grep -Fq "runtime before mixed" "${tmp_dir}/mixed-commit.stderr"; then
    echo "mixed-commit: the clean commit was reported as failing" >&2
    show_output mixed-commit
    exit 1
fi

# A merge commit is one landed change: its branch's commits would pass one by
# one, but the merge's first-parent diff mixes both.
git -C "${fixture}" commit -q --allow-empty -m "clean main tip"
merge_base="$(git -C "${fixture}" rev-parse HEAD)"
git -C "${fixture}" switch -q -c feature
commit_change "feature migration" db/postgres/20260925000003_feature.up.sql
commit_change "feature runtime" "${runtime_file}"
git -C "${fixture}" switch -q main
git -C "${fixture}" merge -q --no-ff -m "merge feature" feature
head="$(git -C "${fixture}" rev-parse HEAD)"
expect_failure_naming merge-commit "${merge_base}" "merge feature"
if grep -Fq -e "feature migration" -e "feature runtime" "${tmp_dir}/merge-commit.stdout"; then
    echo "merge-commit: the walk left the first-parent chain" >&2
    show_output merge-commit
    exit 1
fi

expect_pass empty-range HEAD

if [[ "$(git -C "${fixture}" rev-parse HEAD)" != "${head}" ]] \
    || [[ -n "$(git -C "${fixture}" status --porcelain)" ]]; then
    echo "per-commit migration isolation changed the checkout" >&2
    exit 1
fi
if [[ "$(git -C "${fixture}" worktree list --porcelain | grep -c '^worktree ')" != 1 ]]; then
    echo "per-commit migration isolation left worktrees behind:" >&2
    git -C "${fixture}" worktree list >&2
    exit 1
fi

echo "Per-commit migration isolation scenarios passed."
