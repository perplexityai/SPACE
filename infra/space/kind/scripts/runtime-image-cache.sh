#!/usr/bin/env bash
# runtime-image-cache.sh — the Kind runtime images' part of the registry layer
# cache (infra/space/kind/lib/build-cache.sh): which stages of each image the
# cache keeps, the options a build reads them with, and main CI's export.
#
# Each runtime Dockerfile ends its dependency layers at a named stage: the
# base, the packages, the downloaded tools and the installed language
# dependencies, before any SPACE binary, script or seed. The cache keeps each
# such stage under its own scope. A build that reads them reuses every layer
# whose inputs are unchanged, so it downloads packages only when it changes
# what they install. The copies of a commit's binaries are never exported: the
# next commit replaces them, and they would make every scope grow. Without a
# repository, or when the cache misses or cannot be reached, a build fetches
# from upstream as before. The seed builder's scope belongs to build-seed.sh.
#
# Usage:
#   bash infra/space/kind/scripts/runtime-image-cache.sh cache-from <image>
#       Print the options that import <image>'s stages, one per line; nothing
#       without SPACE_KIND_BUILD_CACHE_REPOSITORY. Never exports.
#   bash infra/space/kind/scripts/runtime-image-cache.sh publish
#       Main CI (SPACE_KIND_BUILD_CACHE_WRITE=1): build every cached stage,
#       and the seed builder's, and export them. With writes off it only
#       builds them, which warms the local build cache.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=../lib/build-cache.sh
source "$REPO_ROOT/infra/space/kind/lib/build-cache.sh"

log() { printf '[runtime-image-cache] %s\n' "$*" >&2; }
die() { printf '[runtime-image-cache] ERROR: %s\n' "$*" >&2; exit 1; }

# <image> <dockerfile> <stage>...: the stages each image's build imports and
# main exports, in build order. The key broker installs nothing.
IMAGES="
space-api docker/Dockerfile.api smoke-build runtime-deps
space-data-plane docker/Dockerfile.data-plane runtime-deps
space-cluster-controller docker/Dockerfile.cluster-controller runtime-deps
space-worker docker/Dockerfile.worker runtime-deps
space-seed-publish docker/Dockerfile.seed-publish runtime-deps
space-ui docker/Dockerfile.ui deps
space-kind-key-broker docker/Dockerfile.kind-key-broker
"

image_entry() {
  awk -v image="$1" '$1 == image { print; found = 1 } END { exit !found }' <<<"$IMAGES"
}

scope() { # image stage
  printf 'kind-%s-%s\n' "${1#space-}" "$2"
}

cache_from() {
  local image=$1 entry stage
  local -a fields
  entry="$(image_entry "$image")" || die "no cached stages are declared for $image"
  read -r -a fields <<<"$entry"
  for stage in "${fields[@]:2}"; do
    SPACE_KIND_BUILD_CACHE_WRITE=0 space_kind_cache_flags "$(scope "$image" "$stage")" \
      || die "invalid build cache configuration"
    # Pairs of option and value, printed as one word each for $(...) expansion.
    while (( ${#CACHE_FLAGS[@]} )); do
      printf '%s=%s\n' "${CACHE_FLAGS[0]}" "${CACHE_FLAGS[1]}"
      CACHE_FLAGS=("${CACHE_FLAGS[@]:2}")
    done
  done
}

# Build each stage on its own, with no output but the cache. A failure leaves
# that scope as main last wrote it and does not stop the others; readers of a
# stale or missing scope build those layers cold.
publish() {
  local entry dockerfile stage failures=0
  local -a fields mirror
  [[ "${SPACE_KIND_BUILD_CACHE_WRITE:-0}" == 1 ]] \
    || log "SPACE_KIND_BUILD_CACHE_WRITE is not 1: building the cached stages without exporting them"
  while read -r entry; do
    read -r -a fields <<<"$entry"
    (( ${#fields[@]} > 2 )) || continue
    dockerfile=${fields[1]}
    # In GitHub CI the same mirrored bases the readers' builds take, so both
    # compute the same cache keys; elsewhere none.
    mapfile -t mirror < <(bash ci/scripts/mirror-images.sh build-contexts "$dockerfile")
    for stage in "${fields[@]:2}"; do
      space_kind_cache_flags "$(scope "${fields[0]}" "$stage")" || die "invalid build cache configuration"
      log "${fields[0]}: $stage → $(scope "${fields[0]}" "$stage")"
      if ! docker build -f "$dockerfile" --target "$stage" --output type=cacheonly \
          "${mirror[@]}" "${CACHE_FLAGS[@]}" . </dev/null; then
        log "WARNING: could not build $stage of $dockerfile; its scope keeps main's previous layers"
        failures=$((failures + 1))
      fi
    done
  done <<<"$IMAGES"
  log "seed builder"
  if ! bash infra/space/kind/scripts/build-seed.sh --cache-only; then
    log "WARNING: could not build the seed builder's cached stages"
    failures=$((failures + 1))
  fi
  (( failures == 0 )) || die "$failures cached build(s) failed"
}

case "${1:-}" in
  cache-from)
    (( $# == 2 )) || die "usage: $0 cache-from <image>"
    cache_from "$2"
    ;;
  publish)
    (( $# == 1 )) || die "usage: $0 publish"
    publish
    ;;
  *) die "usage: $0 cache-from <image> | publish" ;;
esac
