#!/usr/bin/env bash
# Regression test for ci/scripts/kind-pull-needed.sh against the real path list.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
selector="${repo_root}/ci/scripts/kind-pull-needed.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fixture="${tmp_dir}/repo"
git init -q -b main "${fixture}"
git -C "${fixture}" config user.name test
git -C "${fixture}" config user.email test@example.com
git -C "${fixture}" commit -q --allow-empty -m base
base="$(git -C "${fixture}" rev-parse HEAD)"

# Build GitHub's synthetic merge of a one-commit PR that touches $@, and leave
# it checked out. Prints the PR head.
synthetic_merge() {
    git -C "${fixture}" checkout -q --detach "${base}"
    local path
    for path in "$@"; do
        mkdir -p "${fixture}/$(dirname "${path}")"
        printf 'change\n' >"${fixture}/${path}"
        git -C "${fixture}" add -- "${path}"
    done
    git -C "${fixture}" commit -q -m pr
    local head
    head="$(git -C "${fixture}" rev-parse HEAD)"
    # A base commit the PR does not contain, as when main moved on.
    git -C "${fixture}" checkout -q --detach "${base}"
    mkdir -p "${fixture}/space_smoke"
    printf 'landed\n' >"${fixture}/space_smoke/landed-on-main.py"
    git -C "${fixture}" add space_smoke/landed-on-main.py
    git -C "${fixture}" commit -q -m main-moved
    git -C "${fixture}" merge -q --no-ff --no-edit "${head}" >/dev/null
    printf '%s\n' "${head}"
}

assert_needed() {
    local expected="$1"
    shift
    local head actual
    head="$(synthetic_merge "$@")"
    actual="$(cd "${fixture}" && bash "${selector}" "${head}" 2>"${tmp_dir}/stderr")"
    if [[ "${actual}" != "${expected}" ]]; then
        echo "$*: expected ${expected}, got ${actual}" >&2
        cat "${tmp_dir}/stderr" >&2
        exit 1
    fi
}

# Directory globs cross directories; file globs stay in one. Each `false`
# below matches its entry's prefix, so a selector without glob magic, where
# `*` also crosses `/`, would answer `true`.
assert_needed true space_smoke/adapters.py
assert_needed true crates/libs/build/mgr/src/deep/nested.rs
assert_needed true crates/apps/cluster/controller/svc/src/main.rs
assert_needed true crates/libs/cluster/controller/server/src/leader_election/mod.rs
assert_needed true crates/libs/internal/api/server/src/template_builds.rs
assert_needed true shim/telemetry_export.go
assert_needed true docs/unrelated.md crates/libs/template/mgr/src/volume.rs
assert_needed true .github/workflows/pull-ci.yml
assert_needed false crates/libs/internal/api/server/src/template_x/nested.rs
assert_needed false shim/telemetry/nested.go
assert_needed false scripts/tests/smoke-kind-x/nested.sh
# Every fixture's base also gains a space_smoke file the PR does not contain,
# so these also prove base-side changes are not counted as the PR's.
assert_needed false docs/unrelated.md
assert_needed false crates/libs/template/mgr/src/service.rs

# Run a command in the fixture; it must fail with $1 on stderr, so an unrelated
# crash cannot pass for a refusal.
assert_refused() {
    local message="$1" description="$2"
    shift 2
    if (cd "${fixture}" && "$@" >/dev/null 2>"${tmp_dir}/stderr"); then
        echo "expected ${description} to fail" >&2
        exit 1
    fi
    if ! grep -qF -- "${message}" "${tmp_dir}/stderr"; then
        echo "expected ${description} to fail with: ${message}" >&2
        cat "${tmp_dir}/stderr" >&2
        exit 1
    fi
}

# A checkout that is not the merge of the named head must fail, not answer.
guard="requires the synthetic merge of the expected PR head"
head="$(synthetic_merge space_smoke/adapters.py)"
moved="$(git -C "${fixture}" rev-parse HEAD^1)"
assert_refused "${guard}" "an unrelated PR head" bash "${selector}" "${base}"
assert_refused "${guard}" "the merge's base parent as PR head" bash "${selector}" "${moved}"
git -C "${fixture}" checkout -q --detach "${head}"
assert_refused "${guard}" "a non-merge checkout" bash "${selector}" "${head}"
# The same two parents in the wrong order: the PR head first.
git -C "${fixture}" merge -q --no-ff --no-edit "${moved}" >/dev/null
assert_refused "${guard}" "a merge with its parents swapped" bash "${selector}" "${head}"
assert_refused "usage:" "a missing PR head" bash "${selector}"

# An empty path list must fail rather than select nothing.
printf '# nothing\n\n' >"${tmp_dir}/empty-paths.txt"
head="$(synthetic_merge space_smoke/adapters.py)"
assert_refused "lists no paths" "an empty path list" \
    env SPACE_KIND_PULL_PATHS="${tmp_dir}/empty-paths.txt" bash "${selector}" "${head}"

echo "kind-pull-needed: ok"
