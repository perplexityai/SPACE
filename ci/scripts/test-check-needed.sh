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
    actual="${actual%%$'\n'*}"
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
# pytest would still collect Python files under ui/.
assert_needed true python ui/src/widget_test.py
assert_needed true python ui/conftest.py

# The Go shim needs only shim/ and proto/; Rust needs the shim's fixtures.
assert_needed true go shim/identity.go
assert_needed true go proto/net/policy/v1/policy.proto
assert_needed false go crates/l40_api/api/src/lib.rs
assert_needed false rust shim/identity.go
assert_needed true rust shim/testdata/network-policy-v1.json
# Rust format compares the shim's environment contract with its generator.
assert_needed true rust shim/env_contract_generated.go
# The shim's commands run without GOWORK=off.
assert_needed true go go.work

# Anything not listed as unread runs the check: Rust reads infra/, db/,
# templates/ and scripts/.
assert_needed true rust infra/space/identity/node-identities.sh
assert_needed true rust db/postgres/0001_init.sql
assert_needed true rust scripts/generate-env-contract.sh
assert_needed true rust ui/src/app/page.tsx crates/l40_api/api/src/lib.rs
assert_needed true rust some-new-directory/file.txt

# A rename counts both of its paths: moving a shim source into docs/ still
# changes the shim.
git -C "${fixture}" checkout -q --detach "${base}"
mkdir -p "${fixture}/shim"
printf 'package shim\n' >"${fixture}/shim/identity.go"
git -C "${fixture}" add shim/identity.go
git -C "${fixture}" commit -q -m "add a shim source"
with_shim="$(git -C "${fixture}" rev-parse HEAD)"
mkdir -p "${fixture}/docs"
git -C "${fixture}" mv shim/identity.go docs/identity.go
git -C "${fixture}" commit -q -m "move it to docs"
renamed="$(cd "${fixture}" && env -u GITHUB_ACTIONS bash "${selector}" go "${with_shim}" 2>/dev/null)"
renamed="${renamed%%$'\n'*}"
[[ "${renamed}" == needed=true ]] || { echo "rename out of shim/: got ${renamed}" >&2; exit 1; }

# A name git would quote, or one with a space, still matches its scope.
assert_needed true go "shim/café.go"
assert_needed false rust "ui/src/app/café.tsx"
assert_needed false rust "docs/release notes.md"

# A change to CI itself, or to the CI image's inputs, runs every check: the
# Go scope would otherwise skip all of these.
assert_needed true go .github/workflows/pull-ci.yml
assert_needed true go ci/scripts/run-shim-checks.sh
assert_needed true go rust-toolchain.toml
assert_needed true go docker/build-btrfs-progs.sh

# The change is tree to tree: a later console-only commit does not hide an
# earlier Rust one.
git -C "${fixture}" checkout -q --detach "${base}"
mkdir -p "${fixture}/crates/x" "${fixture}/ui"
printf 'change\n' >"${fixture}/crates/x/lib.rs"
git -C "${fixture}" add crates/x/lib.rs
git -C "${fixture}" commit -q -m rust
printf 'change\n' >"${fixture}/ui/page.tsx"
git -C "${fixture}" add ui/page.tsx
git -C "${fixture}" commit -q -m console
two="$(cd "${fixture}" && env -u GITHUB_ACTIONS bash "${selector}" rust "${base}" 2>/dev/null)"
two="${two%%$'\n'*}"
[[ "${two}" == needed=true ]] || { echo "two commits: got ${two}" >&2; exit 1; }

# No change at all runs the check.
git -C "${fixture}" checkout -q --detach "${base}"
empty="$(cd "${fixture}" && bash "${selector}" rust "${base}" 2>/dev/null)"
empty="${empty%%$'\n'*}"
[[ "${empty}" == needed=true ]] || { echo "empty change: got ${empty}" >&2; exit 1; }

# A pull request decides against its synthetic merge's first parent, not the
# event's base. The PR changes only the console; main then gained a Rust
# change, so the stale event base would count it as the PR's.
git -C "${fixture}" checkout -q --detach "${base}"
mkdir -p "${fixture}/ui"
printf 'pr\n' >"${fixture}/ui/pr.tsx"
git -C "${fixture}" add ui/pr.tsx
git -C "${fixture}" commit -q -m pr
pr_head="$(git -C "${fixture}" rev-parse HEAD)"
git -C "${fixture}" checkout -q --detach "${base}"
mkdir -p "${fixture}/crates/y"
printf 'main\n' >"${fixture}/crates/y/lib.rs"
git -C "${fixture}" add crates/y/lib.rs
git -C "${fixture}" commit -q -m main-moved
git -C "${fixture}" merge -q --no-ff --no-edit "${pr_head}" >/dev/null
merge_base="$(cd "${fixture}" && env -u GITHUB_ACTIONS bash "${selector}" rust "${base}" "${pr_head}" 2>/dev/null)"
merge_base="${merge_base%%$'\n'*}"
[[ "${merge_base}" == needed=false ]] || { echo "synthetic merge: got ${merge_base}" >&2; exit 1; }
# A pull request's answer names its synthetic merge's first parent.
first_parent="$(git -C "${fixture}" rev-parse HEAD^1)"
named="$(cd "${fixture}" && bash "${selector}" rust "${base}" "${pr_head}" 2>/dev/null | sed -n 2p)"
[[ "${named}" == "base=${first_parent}" ]] || { echo "synthetic merge base line: got ${named}" >&2; exit 1; }
event_base="$(cd "${fixture}" && env -u GITHUB_ACTIONS bash "${selector}" rust "${base}" 2>/dev/null)"
event_base="${event_base%%$'\n'*}"
[[ "${event_base}" == needed=true ]] || { echo "event base: got ${event_base}" >&2; exit 1; }
# A merge of some other head runs the check.
wrong="$(cd "${fixture}" && bash "${selector}" rust "${base}" "${base}" 2>/dev/null)"
wrong="${wrong%%$'\n'*}"
[[ "${wrong}" == needed=true ]] || { echo "wrong PR head: got ${wrong}" >&2; exit 1; }

# With a base and a change, the answer names the commit it compared with; an
# empty change names none.
change crates/l40_api/api/src/lib.rs
named="$(cd "${fixture}" && bash "${selector}" rust "${base}" 2>/dev/null | sed -n 2p)"
[[ "${named}" == "base=${base}" ]] || { echo "base line: got ${named}" >&2; exit 1; }
git -C "${fixture}" checkout -q --detach "${base}"
unnamed="$(cd "${fixture}" && bash "${selector}" rust "${base}" 2>/dev/null | sed -n 2p)"
[[ -z "${unnamed}" ]] || { echo "an empty change named ${unnamed}" >&2; exit 1; }

# Always means always, and so does no base.
assert_needed true always ui/src/app/page.tsx
change ui/src/app/page.tsx
no_base="$(cd "${fixture}" && bash "${selector}" rust "" 2>/dev/null)"
no_base="${no_base%%$'\n'*}"
[[ "${no_base}" == needed=true ]] || { echo "no base: got ${no_base}" >&2; exit 1; }

# An unreadable base runs the check instead of failing it.
missing="$(cd "${fixture}" && bash "${selector}" rust 0000000000000000000000000000000000000000 2>/dev/null)"
missing="${missing%%$'\n'*}"
[[ "${missing}" == needed=true ]] || { echo "missing base: got ${missing}" >&2; exit 1; }

# An unknown scope, a scope without patterns and a missing run-all.txt are
# errors, not skips: the first two would skip the check for every change.
change ui/src/app/page.tsx
if (cd "${fixture}" && bash "${selector}" no-such-scope "${base}" >/dev/null 2>&1); then
    echo "an unknown scope must fail" >&2
    exit 1
fi
scopes="${tmp_dir}/scopes"
mkdir -p "${scopes}"
printf '.github/**\n' >"${scopes}/run-all.txt"
printf '# comments only\n' >"${scopes}/empty.run"
if (cd "${fixture}" && SPACE_CHECK_SCOPES_DIR="${scopes}" bash "${selector}" empty "${base}" >/dev/null 2>&1); then
    echo "a scope without patterns must fail" >&2
    exit 1
fi
rm "${scopes}/run-all.txt"
printf 'shim/**\n' >"${scopes}/go.run"
if (cd "${fixture}" && SPACE_CHECK_SCOPES_DIR="${scopes}" bash "${selector}" go "${base}" >/dev/null 2>&1); then
    echo "a missing run-all.txt must fail" >&2
    exit 1
fi

echo "check-needed.sh: all cases pass"
