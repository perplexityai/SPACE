#!/usr/bin/env bash
# runtime-image-cache.sh — the Kind runtime images' part of the registry layer
# cache (infra/space/kind/lib/build-cache.sh): which stages of each image the
# cache keeps, the build every bring-up runs to read them, and main CI's export.
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
# A layer's cache key includes where its base image came from: a base taken
# from the GHCR mirror (SPACE_CI_IMAGE_MIRROR set, ci/mirrored-images.txt) has
# a different key from the same image pulled from its source. Main exports
# with the mirror, so every build here takes its bases the same way, and reads
# main's layers only when SPACE_CI_IMAGE_MIRROR is set as well.
#
# Usage:
#   bash infra/space/kind/scripts/runtime-image-cache.sh build <image> <docker build args>...
#       Build <image> from its Dockerfile with its mirrored bases and the
#       options that import its stages (never an export), then the given
#       arguments, which are relative to the repository root. A build that
#       fails reading the cache or a mirrored base is built again without
#       either, from the Dockerfile's own FROM sources.
#   bash infra/space/kind/scripts/runtime-image-cache.sh publish
#       Main CI (SPACE_KIND_BUILD_CACHE_WRITE=1): build every cached stage,
#       and the seed builder's, and export them; a failed export fails. With
#       writes off it only builds them, which warms the local build cache.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=../lib/build-cache.sh
source "$REPO_ROOT/infra/space/kind/lib/build-cache.sh"

log() { printf '[runtime-image-cache] %s\n' "$*" >&2; }
die() { printf '[runtime-image-cache] ERROR: %s\n' "$*" >&2; exit 1; }
warn() {
  log "WARNING: $*"
  [[ "${GITHUB_ACTIONS:-}" != true ]] || printf '::warning::%s\n' "$*" >&2
}

# <image> <dockerfile> <stage>...: the stages each image's build imports and
# main exports, in build order. The key broker and the STS bridge install
# nothing.
IMAGES="
space-api docker/Dockerfile.api smoke-build runtime-deps
space-data-plane docker/Dockerfile.data-plane runtime-deps
space-cluster-controller docker/Dockerfile.cluster-controller runtime-deps
space-worker docker/Dockerfile.worker runtime-deps
space-seed-publish docker/Dockerfile.seed-publish runtime-deps
space-ui docker/Dockerfile.ui deps
space-kind-key-broker docker/Dockerfile.kind-key-broker
space-kind-sts-bridge docker/Dockerfile.kind-sts-bridge
"

image_entry() {
  awk -v image="$1" '$1 == image { print; found = 1 } END { exit !found }' <<<"$IMAGES"
}

scope() { # image stage
  space_kind_layer_scope "${1#space-}-$2"
}

# In GitHub CI the --build-context options that take the Dockerfile's bases
# from the mirror; a base the mirror lacks, or every base outside CI, comes
# from its source.
mirror_flags() {
  space_kind_mirror_contexts "$1"
}

build() {
  local image=$1 entry stage
  local -a fields imports=()
  shift
  entry="$(image_entry "$image")" || die "no cached stages are declared for $image"
  read -r -a fields <<<"$entry"
  for stage in "${fields[@]:2}"; do
    SPACE_KIND_BUILD_CACHE_WRITE=0 space_kind_cache_flags "$(scope "$image" "$stage")" \
      || die "invalid build cache configuration"
    imports+=("${CACHE_FROM_FLAGS[@]}")
  done
  mirror_flags "${fields[1]}"
  space_kind_build_reading_cache $(( ${#imports[@]} + ${#MIRROR_FLAGS[@]} )) \
    "${imports[@]}" "${MIRROR_FLAGS[@]}" -f "${fields[1]}" "$@"
}

# Build each stage on its own, with no output but the cache. A failure leaves
# that scope as main last wrote it and does not stop the others; readers of a
# stale or missing scope build those layers cold. A broken import is dropped
# on a retry, but the mirrored bases are kept: layers built from the bases'
# sources would carry keys no reader computes.
publish() {
  local entry dockerfile stage failures=0
  local -a fields
  [[ "${SPACE_KIND_BUILD_CACHE_WRITE:-0}" == 1 ]] \
    || log "SPACE_KIND_BUILD_CACHE_WRITE is not 1: building the cached stages without exporting them"
  while read -r entry; do
    read -r -a fields <<<"$entry"
    (( ${#fields[@]} > 2 )) || continue
    dockerfile=${fields[1]}
    mirror_flags "$dockerfile"
    # Exporting layers built from a base's source would replace main's tags
    # with layers that no mirrored build matches; keep the old tags instead.
    if (( MIRROR_MISSING )); then
      warn "a mirrored base of $dockerfile was unavailable; its stages keep main's previous layers"
      failures=$((failures + 1))
      continue
    fi
    for stage in "${fields[@]:2}"; do
      space_kind_cache_flags "$(scope "${fields[0]}" "$stage")" --fail-on-export-error \
        || die "invalid build cache configuration"
      log "${fields[0]}: $stage → $(scope "${fields[0]}" "$stage")"
      if ! space_kind_build_reading_cache "${#CACHE_FROM_FLAGS[@]}" "${CACHE_FROM_FLAGS[@]}" \
          -f "$dockerfile" --target "$stage" --output type=cacheonly \
          "${MIRROR_FLAGS[@]}" "${CACHE_TO_FLAGS[@]}" . </dev/null; then
        warn "could not build or export $stage of $dockerfile; $(scope "${fields[0]}" "$stage") keeps main's previous layers"
        failures=$((failures + 1))
      fi
    done
  done <<<"$IMAGES"
  log "seed builder"
  if ! bash infra/space/kind/scripts/build-seed.sh --cache-only; then
    warn "could not build or export the seed builder's cached stages"
    failures=$((failures + 1))
  fi
  (( failures == 0 )) || die "$failures cached build(s) failed"
}

case "${1:-}" in
  build)
    (( $# >= 2 )) || die "usage: $0 build <image> <docker build args>..."
    shift
    build "$@"
    ;;
  publish)
    (( $# == 1 )) || die "usage: $0 publish"
    publish
    ;;
  *) die "usage: $0 build <image> <docker build args>... | publish" ;;
esac
