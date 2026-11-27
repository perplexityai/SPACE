#!/usr/bin/env bash
# Regression test for affected_lint_crates and lint_affects_any in
# ci/scripts/lib/rust-affected-crates.sh, on a synthetic workspace: `a`, `b`
# depending on `a`, `e` depending on `b`, `c` on its own, and `d`, which
# includes a file from `a`'s directory by a literal path and another through
# CARGO_MANIFEST_DIR. A local directory registry stands in for crates.io: `b`
# uses its `leaf` and `c` its `mid`, which uses `leaf`.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# shellcheck source=ci/scripts/lib/rust-affected-crates.sh
source "${repo_root}/ci/scripts/lib/rust-affected-crates.sh"

# A crate in the local registry: NAME VERSION [DEPENDENCY], which it requires
# at version 1.
registry_crate() {
    local name="$1" version="$2" dependency="${3:-}"
    local crate_dir="${tmp_dir}/registry/${name}-${version}"
    mkdir -p "${crate_dir}/src"
    {
        printf '[package]\nname = "%s"\nversion = "%s"\nedition = "2021"\n' "${name}" "${version}"
        if [[ -n "${dependency}" ]]; then
            printf '\n[dependencies]\n%s = "1"\n' "${dependency}"
        fi
    } >"${crate_dir}/Cargo.toml"
    : >"${crate_dir}/src/lib.rs"
    printf '{"files":{},"package":"%s"}' "$(printf '%s %s' "${name}" "${version}" | sha256sum | cut -d' ' -f1)" \
        >"${crate_dir}/.cargo-checksum.json"
}
registry_crate leaf 1.0.0
registry_crate mid 1.0.0 leaf
registry_crate fresh 1.0.0

fixture="${tmp_dir}/workspace"
mkdir -p "${fixture}/ci/check-scopes" "${fixture}/.cargo"
printf '[source.crates-io]\nreplace-with = "local"\n\n[source.local]\ndirectory = "%s"\n' \
    "${tmp_dir}/registry" >"${fixture}/.cargo/config.toml"
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
printf 'leaf = "1"\n' >>"${fixture}/crates/b/Cargo.toml"
printf '\n[dependencies]\nmid = "1"\n' >>"${fixture}/crates/c/Cargo.toml"
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

# Every case runs; those that fail are named at the end.
failures=()

# Commits, on the base, what COMMAND changes in the fixture, and checks that
# affected_lint_crates names EXPECTED for it; its log stays in stderr.
assert_change_affects() {
    local expected="$1" label="$2"
    shift 2
    git -C "${fixture}" checkout -q --detach "${base}"
    (cd "${fixture}" && "$@")
    git -C "${fixture}" add -A
    git -C "${fixture}" commit -q -m "${label}"
    local actual
    actual="$(cd "${fixture}" && affected_lint_crates "${tmp_dir}/metadata.json" "${base}" "${listed}" 2>"${tmp_dir}/stderr" | tr '\n' ' ')"
    if [[ "${actual% }" != "${expected}" ]]; then
        echo "${label}: expected '${expected}', got '${actual% }'" >&2
        cat "${tmp_dir}/stderr" >&2
        failures+=("${label}")
    fi
}

append_change() {
    local path
    for path in "$@"; do
        mkdir -p "$(dirname "${path}")"
        printf 'change\n' >>"${path}"
    done
}

assert_affected() {
    local expected="$1"
    shift
    assert_change_affects "${expected}" "$*" append_change "$@"
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
# A Cargo.lock change affects the crates whose resolved tree holds a package
# it changes, and their dependents: a bump of leaf reaches b directly and c
# through mid, and a member that gains a dependency changes its own entry.
bump_leaf() {
    [[ -d "${tmp_dir}/registry/leaf-1.0.1" ]] || registry_crate leaf 1.0.1
    cargo update --offline -q -p leaf
}
assert_change_affects "b c e" "a leaf bump" bump_leaf
grep -qF 'leaf 1.0.0 -> 1.0.1: in the tree of 3: b c e' "${tmp_dir}/stderr" ||
    failures+=("a leaf bump names the bump and the crates it reaches")
gain_fresh() {
    printf '\n[dependencies]\nfresh = "1"\n' >>crates/a/Cargo.toml
    cargo update --offline -q -w
}
assert_change_affects "a b e" "a gaining a dependency" gain_fresh
# A lock that does not parse, and the files that configure every build, lint
# every crate.
assert_affected "a b c d e" Cargo.lock
assert_affected "a b c d e" rust-toolchain.toml
assert_affected "a b c d e" .cargo/config.toml
assert_affected "a b c d e" Cargo.toml
bump_leaf_and_proto() {
    bump_leaf
    append_change proto/x/v1/x.proto
}
assert_change_affects "a b c d e" "a leaf bump with a proto change" bump_leaf_and_proto
# Any other path Cargo, a build script or an include may read lints every
# crate, as does a change to this lint itself and any path nothing names.
assert_affected "a b c d e" proto/x/v1/x.proto
assert_affected "a b c d e" ci/scripts/lib/rust-affected-crates.sh
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
# Only an include in code counts: one escaped in a string literal, as in a
# test fixture, is text, while an include of a variable is computed.
assert_source_affects() {
    local expected="$1" line="$2"
    git -C "${fixture}" checkout -q --detach "${base}"
    printf '%s\n' "${line}" >>"${fixture}/crates/c/src/lib.rs"
    git -C "${fixture}" commit -q -am source
    local actual
    actual="$(cd "${fixture}" && affected_lint_crates "${tmp_dir}/metadata.json" "${base}" "${listed}" 2>/dev/null | tr '\n' ' ')"
    [[ "${actual% }" == "${expected}" ]] ||
        { echo "${line}: expected '${expected}', got '${actual% }'" >&2; exit 1; }
}
assert_source_affects "c" 'const FIXTURE: &str = "#[doc = include_str!(\"README.md\")]\n";'
assert_source_affects "a b c d e" 'const VARIABLE: &str = include_str!(some_path);'

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

if [[ "${#failures[@]}" -ne 0 ]]; then
    printf 'rust-affected-crates.sh: failed: %s\n' "${failures[@]}" >&2
    exit 1
fi
echo "rust-affected-crates.sh: all cases pass"
