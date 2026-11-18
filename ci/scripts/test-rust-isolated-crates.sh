#!/usr/bin/env bash
# Regression test for affected_lint_crates and isolated_crate_shard in
# ci/scripts/lib/rust-isolated-crates.sh, on a synthetic workspace: `a`, `b`
# depending on `a`, `e` depending on `b`, `c` on its own, and `d`, which
# includes a file from `a`'s directory by a literal path and another through
# CARGO_MANIFEST_DIR. A local directory registry stands in for crates.io: `b`
# uses its `leaf` and `c` its `mid`, which uses `leaf`. The shards are also
# checked on a synthetic cost table and against the workflow's entries.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# shellcheck source=ci/scripts/lib/rust-isolated-crates.sh
source "${repo_root}/ci/scripts/lib/rust-isolated-crates.sh"

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

# isolated_crate_shard, on the fixture's crates. For every shard count,
# more shards than crates included, the shards together list each crate
# exactly once, each shard in the listed order; the same crates give the same
# split whatever their order.
fail() { echo "isolated_crate_shard: $*" >&2; failures+=("isolated_crate_shard: $*"); }
shard() { isolated_crate_shard "$1" "$2" "$3" 2>/dev/null; }
reversed="$(sort -r <<<"${listed}")"
for count in 1 2 3 5 7; do
    union=""
    for ((index = 1; index <= count; index++)); do
        part="$(shard "${tmp_dir}/metadata.json" "${index}/${count}" "${listed}")"
        [[ "${part}" == "$(shard "${tmp_dir}/metadata.json" "${index}/${count}" "${listed}")" ]] ||
            fail "shard ${index}/${count} differs between two runs"
        [[ "$(sort <<<"${part}")" == "$(shard "${tmp_dir}/metadata.json" "${index}/${count}" "${reversed}" | sort)" ]] ||
            fail "shard ${index}/${count} depends on the order of the list"
        [[ "${part}" == "$(grep -Fx -f <(printf '%s\n' "${part}") <<<"${listed}")" ]] ||
            fail "shard ${index}/${count} is not in the listed order: '${part}'"
        union+="${part:+${part}$'\n'}"
    done
    [[ "$(sort <<<"${union%$'\n'}")" == "$(sort <<<"${listed}")" ]] ||
        fail "the ${count} shards list '$(tr '\n' ' ' <<<"${union}")', not each library once"
done
# Five crates in seven shards: each of the first five gets one, the last two none.
[[ "$(shard "${tmp_dir}/metadata.json" 5/7 "${listed}")" != "" ]] || fail "shard 5/7 is empty"
[[ "$(shard "${tmp_dir}/metadata.json" 7/7 "${listed}")" == "" ]] || fail "shard 7/7 is not empty"
# No crate, as when a pull request affects none: every shard is empty.
for spec in 1/1 2/3; do
    part="$(isolated_crate_shard "${tmp_dir}/metadata.json" "${spec}" "" 2>/dev/null)" ||
        fail "shard ${spec} of no crate fails"
    [[ -z "${part}" ]] || fail "shard ${spec} of no crate lists '${part}'"
done
# A shard that is not K/N with 1 <= K <= N fails rather than lint nothing.
for spec in 0/2 3/2 1/0 2 x/y 1/2/3 ""; do
    if isolated_crate_shard "${tmp_dir}/metadata.json" "${spec}" "${listed}" >/dev/null 2>&1; then
        fail "shard '${spec}' is accepted"
    fi
done

# Balance by cost, on a synthetic cost table. A crate costs one plus the
# workspace packages its lint builds: its own dependencies of every kind,
# optional ones included, and their normal and build dependencies, not their
# dev-dependencies; a package outside the workspace counts for nothing.
python3 - "${tmp_dir}/costs.json" <<'PY'
import json
import sys

packages = {f"leaf{i:02}": [] for i in range(1, 12)}


def dep(name, kind=None, optional=False):
    return {"name": name, "kind": kind, "optional": optional}


for name, cost in {"big": 12, "large": 10, "mid1": 6, "mid2": 5, "mid3": 4}.items():
    packages[name] = [dep(f"leaf{i:02}") for i in range(1, cost)]
for i in range(1, 7):
    packages[f"s{i}"] = [dep("serde")]
packages["helper"] = [dep("leaf01"), dep("leaf02", "build"), dep("leaf03", "dev")]
packages["tested"] = [dep("helper", "dev"), dep("leaf04", optional=True), dep("serde")]
with open(sys.argv[1], "w") as out:
    json.dump({"packages": [{"name": n, "dependencies": d} for n, d in packages.items()]}, out)
PY
costs_of() {
    isolated_crate_shard "${tmp_dir}/costs.json" "$1" "$2" 2>&1 >/dev/null | sed -n 's/^  //p' | tr '\n' ';'
}
[[ "$(costs_of 1/1 tested)" == "1/1: 1 crates, cost 5;" ]] ||
    fail "tested should cost 5 (itself, helper, leaf01, leaf02, leaf04): $(costs_of 1/1 tested)"
table=$'big\nlarge\nmid1\nmid2\nmid3\ns1\ns2\ns3\ns4\ns5\ns6'
# Largest first, each to the cheapest shard so far: no shard costs more than
# another by more than its largest crate. Three by count would put big,
# large and mid1, costing 28, in one.
for expected in "1/3=big s2 s4 s6" "2/3=large mid3" "3/3=mid1 mid2 s1 s3 s5"; do
    part="$(shard "${tmp_dir}/costs.json" "${expected%%=*}" "${table}" | tr '\n' ' ')"
    [[ "${part% }" == "${expected#*=}" ]] || fail "shard ${expected%%=*}: expected '${expected#*=}', got '${part% }'"
done
[[ "$(costs_of 1/3 "${table}")" == "1/3: 4 crates, cost 15;2/3: 2 crates, cost 14;3/3: 5 crates, cost 14;" ]] ||
    fail "unexpected shard costs: $(costs_of 1/3 "${table}")"

if [[ "${#failures[@]}" -ne 0 ]]; then
    printf 'rust-isolated-crates.sh: failed: %s\n' "${failures[@]}" >&2
    exit 1
fi
echo "rust-isolated-crates.sh: all cases pass"
