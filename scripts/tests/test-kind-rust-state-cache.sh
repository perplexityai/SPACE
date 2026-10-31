#!/usr/bin/env bash
# Regression test for infra/space/kind/scripts/rust-state-cache.sh: the packed
# transport restores the incremental build state exactly, mtimes included, drops
# the uplifted outputs a later step could stage by path, and leaves an empty
# state with restored=false when the parts are missing or damaged.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${repo_root}/infra/space/kind/scripts/rust-state-cache.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

run() {
  SPACE_KIND_RUST_STATE_PARTS=3 SPACE_KIND_RUST_STATE_ZSTD_LEVEL=3 GITHUB_OUTPUT="${tmp_dir}/output" \
    bash "${script}" "$@" >/dev/null
}

restored() {
  grep '^restored=' "${tmp_dir}/output" | tail -1 | cut -d= -f2
}

state="${tmp_dir}/state"
release="${state}/target/release"
mkdir -p "${release}/.fingerprint/crate-0123456789abcdef" "${release}/deps" \
  "${state}/src/crates/a/src" "${state}/cargo-home/registry/cache/index"
echo fingerprint >"${release}/.fingerprint/crate-0123456789abcdef/lib-crate"
echo library >"${release}/deps/libcrate-0123456789abcdef.rlib"
head -c 3000000 /dev/urandom >"${release}/deps/big-0123456789abcdef.rlib"
echo uplifted >"${release}/space-api-svc"
ln -s space-api-svc "${release}/space-api-svc-link"
echo 'pub fn a() {}' >"${state}/src/crates/a/src/lib.rs"
echo crate >"${state}/cargo-home/registry/cache/index/crate-1.0.0.crate"
touch -d '2001-02-03T04:05:06Z' "${state}/src/crates/a/src/lib.rs"
touch -d '2002-03-04T05:06:07Z' "${release}/deps/libcrate-0123456789abcdef.rlib"
# A build script's rerun-if-changed=<dir> reads the newest mtime in the walk,
# directories included, so a directory restored newer than its build reruns it.
touch -d '2003-04-05T06:07:08Z' "${state}/src/crates/a/src" "${state}/src/crates/a"
checksum="$(sha256sum "${release}/deps/big-0123456789abcdef.rlib" | cut -d' ' -f1)"

run pack "${state}" "${tmp_dir}/parts"
[[ -f "${tmp_dir}/parts/dirs.tar.zst" ]] || fail "pack wrote no directory archive"
compgen -G "${tmp_dir}/parts/part-*.tar.zst" >/dev/null || fail "pack wrote no file parts"

run unpack "${tmp_dir}/parts" "${tmp_dir}/out"
[[ "$(restored)" == true ]] || fail "unpack of a packed state did not report restored=true"
out_release="${tmp_dir}/out/target/release"
[[ "$(stat -c %Y "${tmp_dir}/out/src/crates/a/src/lib.rs")" == "$(date -d '2001-02-03T04:05:06Z' +%s)" ]] \
  || fail "the source mirror's mtime was not kept"
[[ "$(stat -c %Y "${out_release}/deps/libcrate-0123456789abcdef.rlib")" == "$(date -d '2002-03-04T05:06:07Z' +%s)" ]] \
  || fail "a build output's mtime was not kept"
for dir in src/crates/a src/crates/a/src; do
  [[ "$(stat -c %Y "${tmp_dir}/out/${dir}")" == "$(date -d '2003-04-05T06:07:08Z' +%s)" ]] \
    || fail "the mtime of directory ${dir} was not kept"
done
[[ "$(sha256sum "${out_release}/deps/big-0123456789abcdef.rlib" | cut -d' ' -f1)" == "${checksum}" ]] \
  || fail "a build output's content changed"
[[ -f "${out_release}/.fingerprint/crate-0123456789abcdef/lib-crate" ]] || fail "a fingerprint was dropped"
[[ -f "${tmp_dir}/out/cargo-home/registry/cache/index/crate-1.0.0.crate" ]] || fail "the Cargo home was dropped"
[[ ! -e "${out_release}/space-api-svc" && ! -L "${out_release}/space-api-svc-link" ]] \
  || fail "an uplifted output survived the round trip"
[[ ! -d "${tmp_dir}/parts" ]] || fail "unpack left the parts behind"

run unpack "${tmp_dir}/missing" "${tmp_dir}/none"
[[ "$(restored)" == false ]] || fail "unpack without parts did not report restored=false"
[[ -z "$(ls -A "${tmp_dir}/none")" ]] || fail "unpack without parts left a state behind"

mkdir -p "${tmp_dir}/damaged"
echo junk >"${tmp_dir}/damaged/dirs.tar.zst"
echo junk >"${tmp_dir}/damaged/part-00.tar.zst"
mkdir -p "${tmp_dir}/stale/target"
run unpack "${tmp_dir}/damaged" "${tmp_dir}/stale" 2>/dev/null
[[ "$(restored)" == false ]] || fail "unpack of damaged parts did not report restored=false"
[[ -z "$(ls -A "${tmp_dir}/stale")" ]] || fail "unpack of damaged parts left a state behind"

echo "PASS: rust-state-cache.sh restores, prunes and falls back as documented"
