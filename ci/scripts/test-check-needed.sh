#!/usr/bin/env bash
# Regression test for ci/scripts/check-needed.sh against the real check scopes.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
selector="${repo_root}/ci/scripts/check-needed.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fixture="${tmp_dir}/repo"
git init -q -b main "${fixture}"
git -C "${fixture}" config user.name test
git -C "${fixture}" config user.email test@example.com
git -C "${fixture}" commit -q --allow-empty -m base
base="$(git -C "${fixture}" rev-parse HEAD)"

# Check out a commit on the base that touches $@.
change() {
    git -C "${fixture}" checkout -q --detach "${base}"
    local path
    for path in "$@"; do
        mkdir -p "${fixture}/$(dirname "${path}")"
        printf 'change\n' >"${fixture}/${path}"
        git -C "${fixture}" add -- "${path}"
    done
    git -C "${fixture}" commit -q -m change
}

assert_needed() {
    local expected="$1" scope="$2"
    shift 2
    change "$@"
    local actual
    actual="$(cd "${fixture}" && env -u GITHUB_ACTIONS bash "${selector}" "${scope}" "${base}" 2>"${tmp_dir}/stderr")"
    if [[ "${actual}" != "needed=${expected}" ]]; then
        echo "${scope} for $*: expected needed=${expected}, got ${actual}" >&2
        cat "${tmp_dir}/stderr" >&2
        exit 1
    fi
}

# A console-only change needs none of the Rust, Python, shell, Go or
# migration checks.
for scope in rust python shell go migrations; do
    assert_needed false "${scope}" ui/src/app/page.tsx ui/package.json
done

# Documentation needs neither Rust nor the shell tests, but the Python tests
# discover docs/site/test_build.py and read docs/configuration.md.
assert_needed false rust docs/spaced-upgrades.md docs/book/SUMMARY.md README.md
assert_needed false shell docs/spaced-upgrades.md
assert_needed true python docs/spaced-upgrades.md
assert_needed true python docs/configuration.md

# The Go shim needs only shim/ and proto/; Rust needs the shim's fixtures.
assert_needed true go shim/identity.go
assert_needed true go proto/net/policy/v1/policy.proto
assert_needed false go crates/l40_api/api/src/lib.rs
assert_needed false rust shim/identity.go
assert_needed true rust shim/testdata/network-policy-v1.json

# Anything not listed as unread runs the check: Rust reads infra/, db/,
# templates/ and scripts/.
assert_needed true rust infra/space/identity/node-identities.sh
assert_needed true rust db/postgres/0001_init.sql
assert_needed true rust scripts/generate-env-contract.sh
assert_needed true rust ui/src/app/page.tsx crates/l40_api/api/src/lib.rs
assert_needed true rust some-new-directory/file.txt

# A change to CI itself runs every check.
assert_needed true rust ui/src/app/page.tsx .github/workflows/pull-ci.yml
assert_needed true go ci/scripts/run-shim-checks.sh

# Always means always, and so does no base.
assert_needed true always ui/src/app/page.tsx
change ui/src/app/page.tsx
no_base="$(cd "${fixture}" && bash "${selector}" rust "" 2>/dev/null)"
[[ "${no_base}" == needed=true ]] || { echo "no base: got ${no_base}" >&2; exit 1; }

# An unreadable base runs the check instead of failing it.
missing="$(cd "${fixture}" && bash "${selector}" rust 0000000000000000000000000000000000000000 2>/dev/null)"
[[ "${missing}" == needed=true ]] || { echo "missing base: got ${missing}" >&2; exit 1; }

# An unknown scope is an error, not a skip.
if (cd "${fixture}" && bash "${selector}" no-such-scope "${base}" >/dev/null 2>&1); then
    echo "an unknown scope must fail" >&2
    exit 1
fi

echo "check-needed.sh: all cases pass"
