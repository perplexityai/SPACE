#!/usr/bin/env bash
# Regression test for infra/space/kind/scripts/rust-state-cache.sh: the packed
# transport restores the incremental build state exactly, mtimes included, drops
# the uplifted outputs a later step could stage by path, and leaves an empty
# state with restored=false when any part is missing or damaged. Its key crosses
# Cargo.lock but none of its hard inputs.
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
  : >"${tmp_dir}/output"
  SPACE_KIND_RUST_STATE_PARTS=3 SPACE_KIND_RUST_STATE_ZSTD_LEVEL=3 GITHUB_OUTPUT="${tmp_dir}/output" \
    bash "${script}" "$@" >/dev/null
}

restored() {
  grep '^restored=' "${tmp_dir}/output" | cut -d= -f2
}

repack() {
  rm -rf "${tmp_dir}/parts"
  cp -a "${tmp_dir}/pristine" "${tmp_dir}/parts"
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
cp -a "${tmp_dir}/parts" "${tmp_dir}/pristine"

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

for damage in directory-archive part missing-part; do
  repack
  case "${damage}" in
    directory-archive) echo junk >"${tmp_dir}/parts/dirs.tar.zst" ;;
    part) head -c 64 "${tmp_dir}/parts/part-01.tar.zst" >"${tmp_dir}/truncated" &&
      mv "${tmp_dir}/truncated" "${tmp_dir}/parts/part-01.tar.zst" ;;
    missing-part) rm "${tmp_dir}/parts/part-01.tar.zst" ;;
  esac
  rm -rf "${tmp_dir}/stale"
  mkdir -p "${tmp_dir}/stale/target"
  run unpack "${tmp_dir}/parts" "${tmp_dir}/stale" 2>/dev/null
  [[ "$(restored)" == false ]] || fail "unpack with a damaged ${damage} did not report restored=false"
  [[ -z "$(ls -A "${tmp_dir}/stale")" ]] || fail "unpack with a damaged ${damage} left a state behind"
done

# The key: a hard input changes `hard`; Cargo.lock changes only `lock`.
repo="${tmp_dir}/repo"
mkdir -p "${repo}/infra/space/kind/scripts" "${repo}/.cargo" "${repo}/ci" "${repo}/docker"
cp "${script}" "${repo}/infra/space/kind/scripts/rust-state-cache.sh"
for path in rust-toolchain.toml .cargo/config.toml ci/mirrored-images.txt docker/Dockerfile.rust-build-common \
  docker/Dockerfile.rust-build-kind docker/run-cargo-with-sccache.sh \
  infra/space/kind/scripts/build-rust-artifacts.sh infra/space/kind/scripts/sync-rust-sources.py Cargo.lock; do
  echo "${path}" >"${repo}/${path}"
done
key_of() {
  GITHUB_OUTPUT="${tmp_dir}/key" GITHUB_RUN_ID=7 GITHUB_RUN_ATTEMPT=1 RUNNER_OS=Linux RUNNER_ARCH=X64 \
    bash "${repo}/infra/space/kind/scripts/rust-state-cache.sh" key >/dev/null
  grep "^$1=" "${tmp_dir}/key" | tail -1 | cut -d= -f2
}
first="$(key_of key)"
[[ "${first}" =~ ^space-target-v2-Linux-X64-kind-test-hooks-[0-9a-f]{16}-[0-9a-f]{16}-7-1$ ]] \
  || fail "key ${first} does not match the target-cache key format"
echo changed >>"${repo}/Cargo.lock"
[[ "$(key_of hard)" == "${first%-*-*-*}-" ]] || fail "a Cargo.lock change moved the hard prefix"
[[ "$(key_of key)" != "${first}" ]] || fail "a Cargo.lock change kept the key"
echo changed >>"${repo}/docker/Dockerfile.rust-build-kind"
[[ "$(key_of hard)" != "${first%-*-*-*}-" ]] || fail "a build-list change kept the hard prefix"

echo "PASS: rust-state-cache.sh restores, prunes and falls back as documented"
