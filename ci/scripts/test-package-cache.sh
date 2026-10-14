#!/usr/bin/env bash
# Regression test for ci/scripts/package-cache.sh: a missed or failed restore
# leaves an empty cache, a restored one is kept, and the report counts only the
# packages added after the restore.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${repo_root}/ci/scripts/package-cache.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

expect_output() {
    local expected="$1" output
    shift
    output="$(bash "${script}" "$@")" || fail "$* exited $?"
    [[ "${output}" == *"${expected}"* ]] || fail "$*: expected '${expected}' in: ${output}"
}

# An archive saved by an earlier run, with that run's mtimes.
old() {
    mkdir -p "$(dirname "$1")"
    echo old >"$1"
    touch -d '2000-01-01' "$1"
}

new() {
    mkdir -p "$(dirname "$1")"
    echo new >"$1"
}

# A failed restore can leave part of an archive behind; it must not survive.
registry="${tmp_dir}/cargo-home/registry"
new "${registry}/cache/index.example/partial-0.1.0.crate"
expect_output "fetches its packages from upstream" restored cargo "${registry}/" ""
[[ -d "${registry}" ]] || fail "the cleared registry was not recreated"
[[ -z "$(ls -A "${registry}")" ]] || fail "a failed restore left files behind"
[[ -e "${registry}.restored" ]] || fail "restored did not mark the time"

# A restored registry is kept, and only crates fetched afterwards count.
old "${registry}/cache/index.example/kept-1.0.0.crate"
old "${registry}/index/index.example/.cache/ke/pt/kept"
expect_output "Restored the cargo packages from space-packages-v1-cargo-abc" \
    restored cargo "${registry}" space-packages-v1-cargo-abc
[[ -e "${registry}/cache/index.example/kept-1.0.0.crate" ]] || fail "a restored crate was deleted"
expect_output "added 0 crates" report cargo "${registry}"
new "${registry}/cache/index.example/added-2.0.0.crate"
new "${registry}/src/index.example/added-2.0.0/Cargo.toml"
expect_output "added 1 crates" report cargo "${registry}"

go_cache="${tmp_dir}/go-mod"
old "${go_cache}/cache/download/example.com/a/@v/v1.0.0.zip"
bash "${script}" restored go "${go_cache}" space-packages-v1-go-abc >/dev/null
new "${go_cache}/cache/download/example.com/b/@v/v1.0.0.zip"
new "${go_cache}/cache/download/example.com/b/@v/v1.0.0.mod"
new "${go_cache}/example.com/b@v1.0.0/b.go"
expect_output "added 1 module archives" report go "${go_cache}"

uv_cache="${tmp_dir}/uv-cache"
old "${uv_cache}/archive-v0/restored/pkg/__init__.py"
touch -d '2000-01-01' "${uv_cache}/archive-v0/restored"
bash "${script}" restored uv "${uv_cache}" space-packages-v1-uv-python-abc >/dev/null
new "${uv_cache}/archive-v0/downloaded/pkg/__init__.py"
new "${uv_cache}/simple-v22/pypi/pkg.rkyv"
expect_output "added 1 distributions" report uv "${uv_cache}"

npm_cache="${tmp_dir}/npm"
bash "${script}" restored npm "${npm_cache}" "" >/dev/null
new "${npm_cache}/_cacache/content-v2/sha512/aa/bb/one"
new "${npm_cache}/_cacache/content-v2/sha512/cc/dd/two"
new "${npm_cache}/_cacache/index-v5/aa/bb/one"
expect_output "added 2 cache entries" report npm "${npm_cache}"

# Reporting never fails a job, even for a cache that was never prepared or has
# since disappeared.
expect_output "never prepared" report go "${tmp_dir}/absent"
rm -rf "${uv_cache}"
expect_output "added 0 distributions" report uv "${uv_cache}"

if bash "${script}" restored pip "${tmp_dir}/pip" "" 2>/dev/null; then
    fail "an unknown kind was accepted"
fi

echo "package-cache.sh: all checks passed"
