#!/usr/bin/env bash
# Carry the Kind Rust producer's incremental build state from main to pull
# requests.
#
# The incremental mode of build-rust-artifacts.sh keeps three directories
# between builds: the Cargo target directory, the content-synced mirror of the
# build inputs it compiles from (sync-rust-sources.py), and the Cargo home. The
# mirror is what makes the target directory reusable: it keeps the mtimes of
# files whose content did not change, so Cargo rebuilds only the crates whose
# inputs changed since the state was saved. A fresh CI runner has none of the
# three. Main CI's kind_rust_state job builds them cold and saves them; a pull
# request's Kind producer restores the newest and builds incrementally
# (.github/workflows/kind-ci.yml). The merge queue runs no Kind smoke.
#
# key
#   Writes hard=, lock= and key= for the Actions cache. `hard` names the family
#   and the inputs a restore must never cross (HARD_INPUTS: the toolchain, the
#   Cargo config, the builder image and its pinned base, the build lists the
#   source sync walks, and the scripts that drive the build); `lock` adds
#   Cargo.lock, whose changes Cargo's own fingerprints track, so a restore may
#   cross it; `key` adds this run, so every save is new. Main CI's save and the
#   pull-request restore both call this, so they cannot drift apart.
#
# pack <state-dir> <parts-dir>
#   Deletes what a later step could read by path instead of through Cargo: the
#   uplifted binaries and libraries at the top of each profile directory, which
#   Cargo recreates for every target it builds, fresh or not, so a removed or
#   renamed binary cannot survive as a stale, staged file. Then packs
#   <state-dir> into zstd tar parts in
#   <parts-dir>, as ci/scripts/rust-target-cache.sh does: a small archive of the
#   directories first, so no two parts race to create one, then PARTS archives
#   that unpack in parallel. Tar keeps every mtime.
#
# unpack <parts-dir> <state-dir>
#   Unpacks the parts into an empty <state-dir>, mtimes included, after
#   checking that every part pack wrote is there: the
#   directory archive goes last again, because extracting a file into a
#   directory restamps the directory, and a build script that watches a
#   directory would otherwise rerun on every restore. Writes
#   restored=true, or restored=false with <state-dir> left empty when the parts
#   are missing or any of them fails to unpack.
#
#   Unlike ci/scripts/rust-target-cache.sh, this does not wait out a saving
#   runner whose clock ran ahead. A restored output stamped in the future would
#   look newer than a source the pull request changes; that needs the saving
#   runner's clock to have run ahead by more than the state's age, at least the
#   cold build's 8 minutes, which is not guarded against.
#
# Every subcommand writes its outputs as GitHub step outputs when GITHUB_OUTPUT
# is set.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
PARTS="${SPACE_KIND_RUST_STATE_PARTS:-4}"
# What a restored state must have been built with. A change to any of these
# can change a clean build without Cargo noticing: a toolchain or Cargo config
# Cargo does fingerprint but a new one makes the old state useless, a builder
# image whose gcc, protoc or headers Cargo does not fingerprint, and a build
# list whose dropped path would survive in a restored mirror.
HARD_INPUTS=(
  rust-toolchain.toml
  .cargo/config.toml
  ci/mirrored-images.txt
  docker/Dockerfile.rust-build-common
  docker/Dockerfile.rust-build-kind
  docker/run-cargo-with-sccache.sh
  infra/space/kind/scripts/build-rust-artifacts.sh
  infra/space/kind/scripts/sync-rust-sources.py
  infra/space/kind/scripts/rust-state-cache.sh
)
ZSTD_LEVEL="${SPACE_KIND_RUST_STATE_ZSTD_LEVEL:-6}"
ZSTD_WINDOW_LOG=31

output() {
  echo "$1=$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "$1=$2" >>"${GITHUB_OUTPUT}"
  fi
}

delete_path_readable_outputs() {
  local state_dir="$1" profile
  rm -rf "${state_dir}/target/tmp"
  for profile in "${state_dir}"/target/*/; do
    [[ -d "${profile}.fingerprint" ]] || continue
    find "${profile}" -maxdepth 1 \( -type f -o -type l \) -delete
  done
}

pack() {
  local state_dir="$1" parts_dir="$2"
  local threads list pid pids=() status=0 start="${SECONDS}"
  [[ -d "${state_dir}/target" ]] || { echo "${state_dir} has no target directory" >&2; exit 1; }
  delete_path_readable_outputs "${state_dir}"
  threads=$(( ($(nproc) + PARTS - 1) / PARTS ))
  rm -rf "${parts_dir}"
  mkdir -p "${parts_dir}"
  (cd "${state_dir}" && find . -mindepth 1 -type d -printf '%P\n') | LC_ALL=C sort \
    >"${parts_dir}/dirs.list"
  tar --format=posix --no-recursion -C "${state_dir}" -cf - -T "${parts_dir}/dirs.list" |
    zstd -q -T0 -"${ZSTD_LEVEL}" -o "${parts_dir}/dirs.tar.zst"
  (cd "${state_dir}" && find . \( -type f -o -type l \) -printf '%s\t%P\n') |
    LC_ALL=C sort -t $'\t' -k2 |
    awk -F '\t' -v parts="${PARTS}" -v dir="${parts_dir}" '
      { size[NR] = $1; path[NR] = $2; total += $1 }
      END {
        part = 0
        for (i = 1; i <= NR; i++) {
          list = sprintf("%s/part-%02d.list", dir, part)
          print path[i] >> list
          done += size[i]
          if (part < parts - 1 && done >= total * (part + 1) / parts) {
            close(list)
            part++
          }
        }
      }'
  for list in "${parts_dir}"/part-*.list; do
    tar --format=posix --hard-dereference --no-recursion -C "${state_dir}" -cf - -T "${list}" |
      zstd -q -T"${threads}" -"${ZSTD_LEVEL}" --long="${ZSTD_WINDOW_LOG}" \
        -o "${list%.list}.tar.zst" &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    wait "${pid}" || status=$?
  done
  rm -f "${parts_dir}"/*.list
  (( status == 0 )) || { echo "packing ${state_dir} failed" >&2; exit "${status}"; }
  # unpack refuses a state with a part missing, rather than restoring the rest.
  { compgen -G "${parts_dir}/part-*.tar.zst" || true; } | wc -l | tr -d ' ' >"${parts_dir}/parts.count"
  echo "Packed $(du -sh "${state_dir}" | cut -f1) into $(du -sh "${parts_dir}" | cut -f1)" \
    "in $((SECONDS - start)) s."
}

key() {
  local family hard lock
  cd "${REPO_ROOT}"
  family="space-target-v2-${RUNNER_OS:-Linux}-${RUNNER_ARCH:-X64}-kind-test-hooks"
  hard="$(for path in "${HARD_INPUTS[@]}"; do
    printf '%s %s\n' "${path}" "$(sha256sum "${path}" | cut -d' ' -f1)"
  done | sha256sum | cut -c1-16)"
  lock="$(sha256sum Cargo.lock | cut -c1-16)"
  output hard "${family}-${hard}-"
  output lock "${family}-${hard}-${lock}-"
  output key "${family}-${hard}-${lock}-${GITHUB_RUN_ID:-0}-${GITHUB_RUN_ATTEMPT:-0}"
}

extract() {
  zstd -q -d --long="${ZSTD_WINDOW_LOG}" -c "$1" | tar -xf - -C "$2"
}

unpack() {
  local parts_dir="$1" state_dir="$2"
  local part pid pids=() status=0 start="${SECONDS}"
  rm -rf "${state_dir}"
  mkdir -p "${state_dir}"
  local expected found
  expected="$(cat "${parts_dir}/parts.count" 2>/dev/null || true)"
  found="$({ compgen -G "${parts_dir}/part-*.tar.zst" || true; } | wc -l | tr -d ' ')"
  if [[ ! -f "${parts_dir}/dirs.tar.zst" ]] || (( found == 0 )) || [[ "${expected}" != "${found}" ]]; then
    echo "No saved Kind Rust state in ${parts_dir}; the producer builds without it."
    output restored false
    return 0
  fi
  extract "${parts_dir}/dirs.tar.zst" "${state_dir}" || status=$?
  if (( status == 0 )); then
    for part in "${parts_dir}"/part-*.tar.zst; do
      extract "${part}" "${state_dir}" &
      pids+=("$!")
    done
    for pid in "${pids[@]}"; do
      wait "${pid}" || status=$?
    done
  fi
  # Extracting the files into a directory stamped it with the extraction time.
  # A build script's rerun-if-changed=<dir> reads the newest mtime in the walk,
  # directories included, so a directory newer than its build would rerun the
  # script, and rebuild every crate above it, on every restore. The directory
  # archive again, now that every file is in place, puts their mtimes back.
  if (( status == 0 )); then
    extract "${parts_dir}/dirs.tar.zst" "${state_dir}" || status=$?
  fi
  rm -rf "${parts_dir}"
  if (( status != 0 )) || [[ ! -d "${state_dir}/target" || ! -d "${state_dir}/src" ]]; then
    echo "::warning::Unpacking the saved Kind Rust state failed; the producer builds without it."
    rm -rf "${state_dir}"
    mkdir -p "${state_dir}"
    output restored false
    return 0
  fi
  echo "Unpacked $(du -sh "${state_dir}" | cut -f1) of Kind Rust state in $((SECONDS - start)) s."
  output restored true
}

case "${1:-}" in
  key) (( $# == 1 )) || { echo "usage: $0 key" >&2; exit 2; }; key ;;
  pack) (( $# == 3 )) || { echo "usage: $0 pack <state-dir> <parts-dir>" >&2; exit 2; }; pack "$2" "$3" ;;
  unpack) (( $# == 3 )) || { echo "usage: $0 unpack <parts-dir> <state-dir>" >&2; exit 2; }; unpack "$2" "$3" ;;
  *) echo "usage: $0 key | pack <state-dir> <parts-dir> | unpack <parts-dir> <state-dir>" >&2; exit 2 ;;
esac
