#!/usr/bin/env bash
# rust-build.sh — which Rust binaries the Kind harness builds and deploys,
# selected by SPACE_KIND_RUST_BUILD for every bring-up path (the Tiltfile,
# build-rust-artifacts.sh and the bring-up up.sh and the smoke suites share):
#
#   test-hooks  (default) every crate's `test-hooks` feature is compiled in, so
#               the hooks the charts set (`pplx_space_common_core::env::test_hook`)
#               take effect, and LTO is off for build time.
#   release     the release lane: no crate's `test-hooks` feature and thin LTO.
#               The charts render no test hook, and the typed smoke skips the
#               scenarios declared `test_hooks=`. See "Release-build lane" in
#               infra/space/kind/README.md.
#
# The artifact image and every Rust runtime image built from it record their
# build in the space.pplx.net/kind-rust-build label
# (scripts/runtime-image-cache.sh), which scripts/deployed-rust-build.py reads
# back from the images the pods run.
#
# Source it for the functions, or run it to print the mode.

# Prints the validated mode; fails on any other value.
space_kind_rust_build() {
  local mode="${SPACE_KIND_RUST_BUILD:-test-hooks}"
  case "$mode" in
    test-hooks|release) printf '%s\n' "$mode" ;;
    *)
      printf 'SPACE_KIND_RUST_BUILD must be test-hooks or release, not %q\n' "$mode" >&2
      return 1
      ;;
  esac
}

# Prints the build a host Docker image ($1) records; an image without the
# label predates the release lane, whose images were all built with test hooks.
# Fails when the image is missing.
space_kind_image_rust_build() {
  local label
  label="$(docker image inspect -f '{{index .Config.Labels "space.pplx.net/kind-rust-build"}}' "$1")" \
    || return 1
  printf '%s\n' "${label:-test-hooks}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  space_kind_rust_build
fi
