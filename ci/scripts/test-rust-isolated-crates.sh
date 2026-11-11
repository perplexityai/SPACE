#!/usr/bin/env bash
# Regression test for affected_lint_crates in
# ci/scripts/lib/rust-isolated-crates.sh, on a synthetic workspace: `a`, `b`
# depending on `a`, `e` depending on `b`, `c` on its own, and `d`, which
# includes a file from `a`'s directory by a literal path and another through
# CARGO_MANIFEST_DIR.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# shellcheck source=ci/scripts/lib/rust-isolated-crates.sh
source "${repo_root}/ci/scripts/lib/rust-isolated-crates.sh"

fixture="${tmp_dir}/workspace"
mkdir -p "${fixture}/ci/check-scopes"
cp "${repo_root}/ci/check-scopes/rust.skip" "${fixture}/ci/check-scopes/"
cat >"${fixture}/Cargo.toml" <<'EOF'
[workspace]
members = ["crates/a", "crates/b", "crates/c", "crates/d", "crates/e"]
resolver = "2"
EOF
crate() {
    local name="$1" dependency="${2:-}"
    mkdir -p "${fixture}/crates/${name}/src"
    {
        printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n' "${name}"
        if [[ -n "${dependency}" ]]; then
            printf '\n[dependencies]\n%s = { path = "../%s" }\n' "${dependency}" "${dependency}"
        fi
    } >"${fixture}/crates/${name}/Cargo.toml"
    printf '// %s\n' "${name}" >"${fixture}/crates/${name}/src/lib.rs"
}
crate a
crate b a
crate c
crate d
crate e b
printf 'data\n' >"${fixture}/crates/a/data.txt"
printf 'other\n' >"${fixture}/crates/a/other.txt"
printf 'const DATA: &str = include_str!(\n    "../../a/data.txt"\n);\n' >>"${fixture}/crates/d/src/lib.rs"
printf 'const OTHER: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../a/other.txt"));\n' \
    >>"${fixture}/crates/d/src/lib.rs"
git -C "${fixture}" init -q -b main
git -C "${fixture}" config user.name test
git -C "${fixture}" config user.email test@example.com
(cd "${fixture}" && cargo generate-lockfile --offline -q)
git -C "${fixture}" add -A
git -C "${fixture}" commit -q -m base
base="$(git -C "${fixture}" rev-parse HEAD)"
(cd "${fixture}" && cargo metadata --offline --locked --format-version 1 --no-deps) >"${tmp_dir}/metadata.json"

listed=$'a\nb\nc\nd\ne'

assert_affected() {
    local expected="$1"
    shift
    git -C "${fixture}" checkout -q --detach "${base}"
    local path
    for path in "$@"; do
        mkdir -p "${fixture}/$(dirname "${path}")"
        printf 'change\n' >>"${fixture}/${path}"
        git -C "${fixture}" add -- "${path}"
    done
    git -C "${fixture}" commit -q -m change
    local actual
    actual="$(cd "${fixture}" && affected_lint_crates "${tmp_dir}/metadata.json" "${base}" "${listed}" 2>"${tmp_dir}/stderr" | tr '\n' ' ')"
    if [[ "${actual% }" != "${expected}" ]]; then
        echo "$*: expected '${expected}', got '${actual% }'" >&2
        cat "${tmp_dir}/stderr" >&2
        exit 1
    fi
}

# A crate and every crate that depends on it, however indirectly.
assert_affected "a b e" crates/a/src/lib.rs
assert_affected "b e" crates/b/src/lib.rs
assert_affected "c" crates/c/src/lib.rs
# A file another crate includes affects that crate too, whether by a literal
# path or through CARGO_MANIFEST_DIR.
assert_affected "a b d e" crates/a/data.txt
assert_affected "a b d e" crates/a/other.txt
# Paths no crate's lint reads affect none.
assert_affected "" docs/notes.md ui/page.tsx .github/workflows/x.yml architecture-baseline.toml
assert_affected "c" docs/notes.md crates/c/src/lib.rs
# A path Cargo, a build script or an include may read lints every crate, as
# does a change to this lint itself and any path nothing names.
assert_affected "a b c d e" Cargo.lock
assert_affected "a b c d e" proto/x/v1/x.proto
assert_affected "a b c d e" ci/scripts/lib/rust-isolated-crates.sh
assert_affected "a b c d e" .github/workflows/rust-static-ci.yml
assert_affected "a b c d e" some-new-directory/file.txt
# An include of a path the scan cannot resolve could read anything.
git -C "${fixture}" checkout -q --detach "${base}"
printf 'const COMPUTED: &str = include_str!(env!("SOME_PATH"));\n' >>"${fixture}/crates/c/src/lib.rs"
git -C "${fixture}" commit -q -am computed
computed_base="$(git -C "${fixture}" rev-parse HEAD)"
printf 'change\n' >>"${fixture}/crates/b/src/lib.rs"
git -C "${fixture}" commit -q -am b
computed="$(cd "${fixture}" && affected_lint_crates "${tmp_dir}/metadata.json" "${computed_base}" "${listed}" 2>/dev/null | tr '\n' ' ')"
[[ "${computed% }" == "a b c d e" ]] || { echo "a computed include: got '${computed% }'" >&2; exit 1; }

# lint_affects_any answers for a set of crates, and always without a base.
git -C "${fixture}" checkout -q --detach "${base}"
printf 'change\n' >>"${fixture}/crates/c/src/lib.rs"
git -C "${fixture}" commit -q -am c
(cd "${fixture}" && SPACE_RUST_LINT_BASE="${base}" lint_affects_any "${tmp_dir}/lint" a b 2>/dev/null) &&
    { echo "a change to c must not affect a or b" >&2; exit 1; }
(cd "${fixture}" && SPACE_RUST_LINT_BASE="${base}" lint_affects_any "${tmp_dir}/lint" a c 2>/dev/null) ||
    { echo "a change to c must affect c" >&2; exit 1; }
(cd "${fixture}" && SPACE_RUST_LINT_BASE="" lint_affects_any "${tmp_dir}/lint" a 2>/dev/null) ||
    { echo "without a base every crate is affected" >&2; exit 1; }

# A failed selection runs the checks: here the scope file it reads for a path
# outside every crate is gone.
git -C "${fixture}" checkout -q --detach "${base}"
mkdir -p "${fixture}/docs"
printf 'change\n' >"${fixture}/docs/notes.md"
git -C "${fixture}" add docs/notes.md
git -C "${fixture}" commit -q -m docs
mv "${fixture}/ci/check-scopes/rust.skip" "${tmp_dir}/rust.skip"
(cd "${fixture}" && SPACE_RUST_LINT_BASE="${base}" lint_affects_any "${tmp_dir}/lint" a 2>/dev/null) ||
    { echo "a failed selection must answer affected" >&2; exit 1; }
mv "${tmp_dir}/rust.skip" "${fixture}/ci/check-scopes/rust.skip"

echo "rust-isolated-crates.sh: all cases pass"
