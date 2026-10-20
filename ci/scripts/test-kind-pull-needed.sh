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

# Directory globs cross directories; file globs stay in one.
assert_needed true space_smoke/adapters.py
assert_needed true crates/libs/build/mgr/src/deep/nested.rs
assert_needed true crates/libs/internal/api/server/src/template_builds.rs
assert_needed true shim/telemetry_export.go
assert_needed true docs/unrelated.md crates/libs/template/mgr/src/volume.rs
assert_needed true .github/workflows/pull-ci.yml
assert_needed false crates/libs/internal/api/server/src/template/nested.rs
# Every fixture's base also gains a space_smoke file the PR does not contain,
# so these also prove base-side changes are not counted as the PR's.
assert_needed false docs/unrelated.md
assert_needed false crates/libs/template/mgr/src/service.rs

# A checkout that is not the merge of the named head must fail, not answer.
head="$(synthetic_merge docs/unrelated.md)"
if (cd "${fixture}" && bash "${selector}" "${base}" >/dev/null 2>&1); then
    echo "expected a mismatched PR head to fail" >&2
    exit 1
fi
git -C "${fixture}" checkout -q --detach "${head}"
if (cd "${fixture}" && bash "${selector}" "${head}" >/dev/null 2>&1); then
    echo "expected a non-merge checkout to fail" >&2
    exit 1
fi
if (cd "${fixture}" && bash "${selector}" >/dev/null 2>&1); then
    echo "expected a missing PR head to fail" >&2
    exit 1
fi

# An empty path list must fail rather than select nothing.
printf '# nothing\n\n' >"${tmp_dir}/empty-paths.txt"
head="$(synthetic_merge space_smoke/adapters.py)"
if (cd "${fixture}" && SPACE_KIND_PULL_PATHS="${tmp_dir}/empty-paths.txt" bash "${selector}" "${head}" >/dev/null 2>&1); then
    echo "expected an empty path list to fail" >&2
    exit 1
fi

echo "kind-pull-needed: ok"
