#!/usr/bin/env bash
# build-images.sh — build every SPACE image the local kind stack needs and
# `kind load` it into the cluster.
#
# Mirrors the docker-build chain in the Tiltfile so a scripted bring-up and a
# `tilt up` run produce byte-identical images. Every Rust binary comes from
# one artifact image (space-rust-build-kind:local — the union of the
# production artifact sets, built once by build-rust-artifacts.sh on the host
# or inside Docker); re-runs are fast because that build is incremental or
# layer-cached and every runtime stage sits in the docker layer cache.
#
# Usage:
#   bash infra/space/kind/scripts/build-images.sh                # build all
#   bash infra/space/kind/scripts/build-images.sh space-api      # build one
#   bash infra/space/kind/scripts/build-images.sh --no-load all  # skip kind load
#
# Env knobs:
#   SPACE_KIND_CLUSTER       kind cluster name to load images into (default: space-local)
#   SPACE_KIND_PARALLEL=1    build all images in parallel via background jobs
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SEED_OUTPUT_DIR="${SPACE_SEED_OUTPUT_DIR:-$HOME/.space-devbox/kind/seed/build}"
cd "$REPO_ROOT"

LOAD_INTO_KIND=1
if [[ "${1:-}" == "--no-load" ]]; then
  LOAD_INTO_KIND=0
  shift
fi

log() { printf '[build-images] %s\n' "$*"; }

kind_load() {
  local ref=$1
  if (( LOAD_INTO_KIND )); then
    log "kind load: $ref → $KIND_CLUSTER"
    kind load docker-image "$ref" --name "$KIND_CLUSTER"
  fi
}

RUST_ARTIFACT_IMAGE="space-rust-build-kind:local"
# The build SHA every SPACE image records, matching the version the workloads
# report; see telemetry_config.py.
KIND_GIT_SHA="$(python3 "$REPO_ROOT/infra/space/kind/scripts/telemetry_config.py" commit)"

# The one Rust build every runtime image below copies from. Owned by
# build-rust-artifacts.sh: an incremental cargo build (inside the CI builder
# image, host target dir) staged into the /artifacts layout on Linux/x86_64,
# the in-Docker union build otherwise (SPACE_KIND_RUST_ARTIFACTS=incremental|docker forces either). Idempotent
# and lock-serialized; an up-to-date build returns in seconds.
build_rust_artifacts() {
  log "building rust artifacts: $RUST_ARTIFACT_IMAGE (every production set, one build)"
  bash "$REPO_ROOT/infra/space/kind/scripts/build-rust-artifacts.sh"
}

# `docker build` for one runtime image, as the Tiltfile runs it: its Dockerfile,
# its bases from the GHCR mirror when SPACE_CI_IMAGE_MIRROR is set, and its
# dependency stages from the registry layer cache when
# SPACE_KIND_BUILD_CACHE_REPOSITORY is set (see runtime-image-cache.sh).
image_build() {
  bash "$REPO_ROOT/infra/space/kind/scripts/runtime-image-cache.sh" build "$@"
}

# Build the runtime image that copies its binaries out of the shared artifact
# image into a debian-slim base.
build_rust_runtime() {
  local name=$1 dockerfile=$2
  local runtime_ref="${name}:local"

  build_rust_artifacts

  log "building runtime image: $runtime_ref ($dockerfile)"
  image_build "$name" --provenance=false --sbom=false -t "$runtime_ref" \
    --build-arg "SPACE_RUST_BUILD_IMAGE=${RUST_ARTIFACT_IMAGE}" \
    --build-arg "SPACE_GIT_SHA=${KIND_GIT_SHA:-unknown}" \
    .

  kind_load "$runtime_ref"
}

build_space_api() {
  build_rust_runtime space-api docker/Dockerfile.api
}

build_space_data_plane() {
  build_rust_runtime space-data-plane docker/Dockerfile.data-plane
}

# Kind-only: the intermediate-key broker fixture the credential-manager
# authority uses as its external key provider (charts/space-kind-key-broker).
build_space_kind_key_broker() {
  build_rust_runtime space-kind-key-broker docker/Dockerfile.kind-key-broker
}

# Kind-only: the STS bridge fixture the credential-manager authority's storage
# exchange calls in place of AWS STS (charts/space-kind-sts-bridge). Go, so it
# needs no Rust artifact.
build_space_kind_sts_bridge() {
  log "building runtime image: space-kind-sts-bridge:local (docker/Dockerfile.kind-sts-bridge)"
  image_build space-kind-sts-bridge --provenance=false --sbom=false -t space-kind-sts-bridge:local \
    --build-arg "SPACE_GIT_SHA=${KIND_GIT_SHA:-unknown}" \
    .
  kind_load space-kind-sts-bridge:local
}

build_space_cluster_controller() {
  build_rust_runtime space-cluster-controller docker/Dockerfile.cluster-controller
}

# The production worker image (docker/Dockerfile.worker — the binaries, cargo
# features, identities, init scripts and nftables policies of the production
# node DaemonSets), which every node-service chart, the operator shell and
# space-internal-api run unchanged. LTO stays off locally.
build_space_worker() {
  build_rust_runtime space-worker docker/Dockerfile.worker
}

# seed-publish carries the locally built production seed (see build-seed.sh)
# plus the publisher that uploads it to the object store and registers the release
# through the real ClusterApi RPCs. build-seed.sh is idempotent and skips the
# privileged rootfs build when its inputs are unchanged.
build_space_seed_publish() {
  build_rust_artifacts
  SPACE_SEED_OUTPUT_DIR="$SEED_OUTPUT_DIR" \
  SPACE_RUST_BUILD_IMAGE="$RUST_ARTIFACT_IMAGE" \
    bash "$REPO_ROOT/infra/space/kind/scripts/build-seed.sh"

  log "building runtime image: space-seed-publish:local (docker/Dockerfile.seed-publish)"
  image_build space-seed-publish --provenance=false --sbom=false -t space-seed-publish:local \
    --build-context "seed=$SEED_OUTPUT_DIR" \
    .

  kind_load space-seed-publish:local
}

# UI is a vanilla node:22-based build. NEXT_PUBLIC_API_URL is baked in at
# build time because Next.js freezes server-resolved env in
# .next/required-server-files.json; the local stack rendezvouses on the
# in-cluster Service DNS (Tilt uses the same value).
build_space_ui() {
  log "building runtime image: space-ui:local (docker/Dockerfile.ui)"
  image_build space-ui --provenance=false --sbom=false -t space-ui:local \
    --build-arg NEXT_PUBLIC_API_URL=http://space-api:8080 \
    --build-arg NEXT_PUBLIC_DATA_PLANE_URL=http://space-data-plane:8090 \
    .
  kind_load space-ui:local
}

# One list with the superseded-image prune (lib/space-images.sh), so every
# image built here also has its old builds removed from the nodes.
# shellcheck source=../lib/space-images.sh
. "$REPO_ROOT/infra/space/kind/lib/space-images.sh"
ALL_IMAGES=("${SPACE_KIND_LOCAL_IMAGES[@]}")

dispatch() {
  case $1 in
    space-api)                    build_space_api ;;
    space-data-plane)             build_space_data_plane ;;
    space-cluster-controller)     build_space_cluster_controller ;;
    space-worker)                 build_space_worker ;;
    space-seed-publish)           build_space_seed_publish ;;
    space-kind-key-broker)        build_space_kind_key_broker ;;
    space-kind-sts-bridge)        build_space_kind_sts_bridge ;;
    space-ui)                     build_space_ui ;;
    *)
      printf 'unknown image: %s\nknown: %s\n' "$1" "${ALL_IMAGES[*]}" >&2
      exit 2
      ;;
  esac
}

if (( LOAD_INTO_KIND )); then
  if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
    printf 'kind cluster %s not found; create it first or pass --no-load\n' "$KIND_CLUSTER" >&2
    exit 1
  fi
fi

TARGETS=()
if (( $# == 0 )) || [[ "$1" == "all" ]]; then
  TARGETS=("${ALL_IMAGES[@]}")
else
  TARGETS=("$@")
fi

if [[ "${SPACE_KIND_PARALLEL:-0}" == "1" ]]; then
  log "parallel build of ${#TARGETS[@]} images (logs interleaved)"
  pids=()
  for img in "${TARGETS[@]}"; do
    (dispatch "$img") &
    pids+=($!)
  done
  fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=1
  done
  exit "$fail"
fi

for img in "${TARGETS[@]}"; do
  dispatch "$img"
done

log "all done."
