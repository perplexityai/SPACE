#!/usr/bin/env bash
# build-rust-artifacts.sh — produce `space-rust-build-kind:local`, the one
# artifact image every SPACE runtime image of the kind stack copies its Rust
# binaries from (docker/Dockerfile.{api,data-plane,cluster-controller,worker}
# via SPACE_RUST_BUILD_IMAGE, and build-seed.sh for the guest binaries).
#
# Two ways to fill it, same layout, same tag:
#
#   incremental  ONE cargo build of the union of the production artifact sets,
#           run inside the production Dockerfile's own `chef-base` builder
#           image (bookworm, the rust-toolchain.toml pin, the CI linker) with
#           a content-synced copy of the build inputs, a persistent `target/`
#           and a persistent cargo home bind-mounted from the host. The copy
#           (see sync-rust-sources.py) is what makes the target dir reusable
#           across checkouts: cargo judges freshness by mtime, and a fresh
#           worktree's new mtimes otherwise recompile every workspace crate.
#           Nothing is layered into BuildKit: the
#           outputs live under $HOME, an edit recompiles only the crates it
#           touched, and the Docker data root stops growing by ~20 GB per cold
#           run. The features, bins, supervisor invocation and /artifacts layout
#           are read from docker/Dockerfile.rust-build-kind so the two paths
#           cannot drift; the staged binaries are packaged by
#           docker/Dockerfile.rust-artifacts-host (scratch, no compile).
#           Compiling inside the runtime images' own libc generation is the
#           point of the container: a build with the devbox's newer glibc
#           produced binaries the bookworm images could not load (bench
#           2026-09-22), so the assembly also refuses any binary whose required
#           GLIBC symbol version exceeds the runtime base's libc.
#
#   docker  the round-1 path: `chef-base` + docker/Dockerfile.rust-build-kind as
#           BuildKit stages. Works anywhere Docker does and is what the GitHub
#           registry layer cache (SPACE_KIND_BUILD_CACHE_REPOSITORY) warms.
#
#   prebuilt  CI-only handoff: use the already loaded image, requiring its exact
#           image ID, and either this checkout's SHA or, for an image built
#           from another commit, this checkout's Rust input key (--input-key).
#           No compilation or automatic fallback.
#
# Mode: SPACE_KIND_RUST_ARTIFACTS=auto (default) picks incremental on
# Linux/x86_64 with Docker and says why when it falls back; =incremental
# fails closed when its requirements do not hold (never a silent switch to a
# five-minute BuildKit compile); =docker forces the Docker path.
#
# Idempotent and safe to run concurrently: Tilt runs it from every runtime
# image's custom_build and its rust-artifacts resource at once. A file lock
# serializes the builds (the Docker path's too, wherever flock exists); later
# callers then find everything up to date.
#
# Usage:
#   bash infra/space/kind/scripts/build-rust-artifacts.sh
#   bash infra/space/kind/scripts/build-rust-artifacts.sh --mode     # print the mode decision, build nothing
#   bash infra/space/kind/scripts/build-rust-artifacts.sh --input-key  # print the Docker build's input key, build nothing
#
# Env knobs:
#   SPACE_KIND_RUST_ARTIFACTS       auto | incremental | docker | prebuilt (default: auto)
#   SPACE_KIND_PREBUILT_RUST_IMAGE_ID  prebuilt mode: verified Docker image ID
#   SPACE_KIND_PREBUILT_RUST_SHA       prebuilt mode: source commit of that image
#   SPACE_KIND_PREBUILT_RUST_KEY       prebuilt mode: that image's Rust input key,
#                                      checked when the commit is not this checkout's
#   SPACE_KIND_RUST_BUILD           test-hooks (default) or release: the release
#                                   lane builds without any `test-hooks` feature
#                                   and with thin LTO
#                                   (infra/space/kind/lib/rust-build.sh); the
#                                   image's space.pplx.net/kind-rust-build label
#                                   records which, and prebuilt mode checks it.
#                                   The release lane never exports to the layer
#                                   cache, whatever SPACE_KIND_BUILD_CACHE_WRITE says
#   SPACE_KIND_RUST_PROFILE         incremental mode cargo profile: release
#                                   (default, LTO off — the Docker path's shape)
#                                   or dev-release (incremental codegen, debug=1;
#                                   refused for the release lane)
#   SPACE_KIND_RUST_TARGET_DIR      incremental mode target dir
#                                   (default: ~/.space-devbox/kind/rust-target)
#   SPACE_KIND_CARGO_HOME           incremental mode cargo home (registry + git
#                                   caches; default: ~/.space-devbox/kind/cargo-home)
#   SPACE_KIND_RUST_SRC_DIR         incremental mode copy of the build inputs cargo
#                                   compiles from (default: ~/.space-devbox/kind/rust-src)
#   SPACE_KIND_ARTIFACT_STAGE_DIR   staging dir for the /artifacts layout
#                                   (default: ~/.space-devbox/kind/rust-artifacts)
#   SPACE_KIND_RUNTIME_LIBC_IMAGE   image whose glibc the guard compares against
#                                   (default: the runtime Dockerfiles' bookworm base)
#   SPACE_KIND_BUILD_CACHE_REPOSITORY  optional registry layer cache; unset leaves
#                                     local builds unchanged
#   SPACE_KIND_BUILD_CACHE_WRITE       1 exports layers (trusted main CI only);
#                                     default 0 imports without exporting
#                                     (both: infra/space/kind/lib/build-cache.sh)
# In Actions, the exposed runtime also imports existing GHA layers until the
# registry cache is seeded and public. It never exports to the Actions cache.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$REPO_ROOT"

ARTIFACT_IMAGE="space-rust-build-kind:local"
CHEF_IMAGE="space-rust-chef-base:local"
HEALTH_PROBE_IMAGE="space-grpc-health-probe:local"
KIND_DOCKERFILE="docker/Dockerfile.rust-build-kind"
COMMON_DOCKERFILE="docker/Dockerfile.rust-build-common"
HOST_DOCKERFILE="docker/Dockerfile.rust-artifacts-host"
# A Docker-mode image built from pinned bases carries its input key and commit.
INPUT_KEY_LABEL="space.pplx.net/kind-rust-input-key"
REVISION_LABEL="org.opencontainers.image.revision"
STATE_DIR="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
STAGE_DIR="${SPACE_KIND_ARTIFACT_STAGE_DIR:-$STATE_DIR/rust-artifacts}"
TARGET_DIR="${SPACE_KIND_RUST_TARGET_DIR:-$STATE_DIR/rust-target}"
CARGO_HOME_DIR="${SPACE_KIND_CARGO_HOME:-$STATE_DIR/cargo-home}"
SRC_DIR="${SPACE_KIND_RUST_SRC_DIR:-$STATE_DIR/rust-src}"
LOCK_FILE="$STATE_DIR/rust-artifacts.lock"
MODE_REQUEST="${SPACE_KIND_RUST_ARTIFACTS:-auto}"
PROFILE="${SPACE_KIND_RUST_PROFILE:-release}"
# The libc generation the runtime images run on. Three of the four runtime
# Dockerfiles are debian:bookworm-slim (glibc 2.36); the worker is ubuntu:24.04
# (2.39), so bookworm is the binding constraint for the shared artifact set.
RUNTIME_LIBC_IMAGE="${SPACE_KIND_RUNTIME_LIBC_IMAGE:-public.ecr.aws/docker/library/debian:bookworm-slim}"

# shellcheck source=../lib/build-cache.sh
source "$REPO_ROOT/infra/space/kind/lib/build-cache.sh"
# shellcheck source=../lib/shared-build-inputs.sh
source "$REPO_ROOT/infra/space/kind/lib/shared-build-inputs.sh"
# shellcheck source=../lib/rust-build.sh
source "$REPO_ROOT/infra/space/kind/lib/rust-build.sh"

log() { printf '[rust-artifacts] %s\n' "$*"; }
die() { printf '[rust-artifacts] ERROR: %s\n' "$*" >&2; exit 1; }

RUST_BUILD="$(space_kind_rust_build)" || die "invalid SPACE_KIND_RUST_BUILD"
# The release lane links with thin LTO (SPACE_CARGO_RELEASE_LTO in
# docker/Dockerfile.rust-build-common); the default keeps LTO off.
RELEASE_LTO=false
# The workspace member the host builds leave out (SPACE_KIND_CARGO_EXCLUDE in
# docker/Dockerfile.rust-build-kind): the release lane leaves out the
# test-support member, so that no selected crate turns a `test-hooks` feature on.
CARGO_EXCLUDE=""
CARGO_SELECTION=()
if [[ "$RUST_BUILD" == release ]]; then
  RELEASE_LTO=thin
  CARGO_EXCLUDE=pplx-space-db-test-fixtures
  CARGO_SELECTION=(--workspace --exclude "$CARGO_EXCLUDE")
  [[ "$PROFILE" == release ]] \
    || die "SPACE_KIND_RUST_BUILD=release builds the release profile; unset SPACE_KIND_RUST_PROFILE (got '$PROFILE')"
  # The shared layer cache holds the default build's layers; the release lane
  # reads it but never replaces them with its own, in either mode and for
  # every image this script builds (chef-base and the health probe included).
  if [[ "${SPACE_KIND_BUILD_CACHE_WRITE:-0}" == 1 ]]; then
    log "the release lane does not export to the layer cache; importing only"
  fi
  export SPACE_KIND_BUILD_CACHE_WRITE=0
fi

# ── Mode decision ─────────────────────────────────────────────────────────────

# Why the incremental path is unavailable, one reason per line; empty when it
# is. The compiler lives in the builder image, so no host toolchain is
# consulted: a devbox whose rustc drifted from the pin builds what CI builds.
incremental_mode_blockers() {
  [[ "$(uname -s)" == "Linux" && "$(uname -m)" == "x86_64" ]] \
    || echo "incremental builds need Linux/x86_64 (this is $(uname -s)/$(uname -m)); the builder image is amd64 and bind-mounts a host target dir"
  command -v docker >/dev/null 2>&1 || echo "docker is not on PATH"
  command -v flock >/dev/null 2>&1 || echo "flock is not on PATH (util-linux)"
  command -v python3 >/dev/null 2>&1 || echo "python3 is not on PATH (it syncs the build inputs)"
  [[ -f "$HOST_DOCKERFILE" ]] || echo "$HOST_DOCKERFILE is missing"
}

# Prints the mode and, on stderr, the reasons behind an auto fallback.
decide_mode() {
  local blockers
  case "$MODE_REQUEST" in
    docker|prebuilt) echo "$MODE_REQUEST"; return ;;
    incremental)
      blockers="$(incremental_mode_blockers)"
      if [[ -n "$blockers" ]]; then
        die "SPACE_KIND_RUST_ARTIFACTS=incremental but the incremental path is unavailable:"$'\n'"$blockers"
      fi
      echo incremental; return ;;
    auto)
      blockers="$(incremental_mode_blockers)"
      if [[ -z "$blockers" ]]; then
        echo incremental
      else
        while IFS= read -r line; do log "auto: $line"; done <<<"$blockers" >&2
        log "auto: falling back to the Docker artifact build (set SPACE_KIND_RUST_ARTIFACTS=incremental to require the incremental path)" >&2
        echo docker
      fi
      return ;;
    *) die "SPACE_KIND_RUST_ARTIFACTS must be auto, incremental, docker or prebuilt (got '$MODE_REQUEST')" ;;
  esac
}

# ── Shared images ─────────────────────────────────────────────────────────────

# The registry layer cache options (CACHE_FLAGS) for one scope; see the library
# for the repository and write rules. Each image owns one stable tag.
docker_cache_flags() {
  space_kind_cache_flags "$1" || die "invalid build cache configuration"
}

# In GitHub CI (SPACE_CI_IMAGE_MIRROR set), the --build-context options that
# take the common Dockerfile's registry bases from the repository's GHCR
# mirror (ci/mirrored-images.txt); elsewhere none. Clears BASES_PINNED when
# any registry base of the build would come from its source instead, whose tag
# can move away from the digest the input key records.
BASES_PINNED=1
mirror_build_flags() {
  MIRROR_FLAGS=()
  local flag base
  while IFS= read -r flag; do
    MIRROR_FLAGS+=("$flag")
  done < <(bash "$REPO_ROOT/ci/scripts/mirror-images.sh" build-contexts "$COMMON_DOCKERFILE")
  while read -r base; do
    printf '%s\n' "${MIRROR_FLAGS[@]}" | grep -qF -- "--build-context=$base=" || BASES_PINNED=0
  done < <(registry_bases)
}

# The production Dockerfile's toolchain stage: bookworm, the rust-toolchain.toml
# pin installed in rust-base, the CI linker. Both modes compile inside it, so
# the kind binaries are built with exactly the CI image's rustc and libc.
build_chef_base_image() {
  docker_cache_flags kind-rust-chef-base
  mirror_build_flags
  docker build -t "$CHEF_IMAGE" -f "$COMMON_DOCKERFILE" \
    --target chef-base "${CACHE_FLAGS[@]}" "${MIRROR_FLAGS[@]}" .
}

build_health_probe_image() {
  docker_cache_flags kind-grpc-health-probe
  mirror_build_flags
  docker build -t "$HEALTH_PROBE_IMAGE" -f "$COMMON_DOCKERFILE" \
    --target grpc-health-probe-downloader "${CACHE_FLAGS[@]}" "${MIRROR_FLAGS[@]}" .
}

# ── Docker mode ───────────────────────────────────────────────────────────────

# The Dockerfile's build arguments for this build: the release lane empties
# both test-hook feature lists and narrows the package selection.
rust_build_args() {
  BUILD_ARGS=(--build-arg "SPACE_KIND_RUST_BUILD=$RUST_BUILD"
    --build-arg "SPACE_CARGO_RELEASE_LTO=$RELEASE_LTO")
  if [[ "$RUST_BUILD" == release ]]; then
    BUILD_ARGS+=(--build-arg SPACE_KIND_TEST_HOOK_FEATURES=
      --build-arg SPACE_KIND_GUEST_TEST_HOOK_FEATURES=
      --build-arg "SPACE_KIND_CARGO_EXCLUDE=$CARGO_EXCLUDE")
  fi
}

build_docker() {
  log "docker mode: $ARTIFACT_IMAGE ($RUST_BUILD) from $KIND_DOCKERFILE (one cook, one build, as BuildKit stages)"
  build_chef_base_image
  build_health_probe_image
  rust_build_args
  docker_cache_flags kind-rust-build
  if [[ "${SPACE_KIND_BUILD_CACHE_WRITE:-0}" == 1 ]]; then
    # Publish the reusable dependency graph, before adding this commit's
    # sources and compiled workspace. Exporting those large, per-commit layers
    # delays the artifact handoff without helping the next source build.
    # Keep the existing scope so readers can reuse already-published cook layers.
    docker build -f "$KIND_DOCKERFILE" --target cook --output type=cacheonly \
      "${BUILD_ARGS[@]}" "${CACHE_FLAGS[@]}" .
  fi
  SPACE_KIND_BUILD_CACHE_WRITE=0 docker_cache_flags kind-rust-build
  # Label the image with what it was built from, so main CI can publish it for
  # later runs with the same inputs (rust-artifact-handoff.py). Only when the
  # key is exact: committed inputs and every base at its pinned digest.
  local key labels=()
  if (( BASES_PINNED )) && key="$(rust_input_key 2>/dev/null)"; then
    labels=(--label "$INPUT_KEY_LABEL=$key" --label "$REVISION_LABEL=$(git rev-parse HEAD)")
    log "Rust input key $key"
  else
    log "no Rust input key label: an input is uncommitted or a base is not at its mirrored digest"
  fi
  # One manifest, no attestation: the provenance attestation carries the build
  # time, and an index would split into several registry package versions.
  docker build -t "$ARTIFACT_IMAGE" -f "$KIND_DOCKERFILE" --provenance=false \
    "${BUILD_ARGS[@]}" ${labels[@]+"${labels[@]}"} "${CACHE_FLAGS[@]}" .
}

# ── Incremental mode ──────────────────────────────────────────────────────────

# The build stage of docker/Dockerfile.rust-build-kind is the single source of
# truth for what the artifact image contains. These readers pull the feature
# list, the bin list, the runtime-supervisor invocation and the /artifacts
# layout out of it, so a set change made there reaches this path unedited.
build_stage() {
  awk '/^FROM cook AS build$/ { s = 1; next } /^FROM / { s = 0 } s' "$KIND_DOCKERFILE"
}

# The build stage's `RUN cargo build` invocations, numbered from 1: the host
# services, then the guest's spaced, built on its own feature set.
cargo_build_block() {
  build_stage | awk -v n="$1" '/^RUN cargo build/ { b++; on = b == n } on { print } on && /^$/ { on = 0 }'
}

# Its literal -F arguments; the test-hook list it expands is test_hook_features.
main_build_features() {
  cargo_build_block 1 | grep -oE -- '-F [^ \\]+' | awk '$2 !~ /\$/ { print $2 }'
}

# One feature per line from the default of a test-hook ARG ($1), and nothing
# for the release lane, which builds with the list empty.
test_hook_features() {
  [[ "$RUST_BUILD" == test-hooks ]] || return 0
  awk -F= -v name="ARG $1" '$1 == name && NF > 1 { print $2 }' "$KIND_DOCKERFILE" | tr ',' '\n'
}

main_build_bins() {
  cargo_build_block 1 | grep -oE -- '--bin [^ \\]+' | awk '{print $2}'
}

# The guest invocation's literal package, feature and bin arguments.
guest_build_args() {
  cargo_build_block 2 | grep -oE -- '(-p|-F|--bin) [^ \\]+' | awk '!/\$/'
}

supervisor_features() {
  build_stage | awk '/^RUN bash scripts\/build-runtime-supervisor.sh/,/^$/' \
    | grep -oE -- '-F [^ \\]+' | awk '{print $2}'
}

# The checkout paths the COPY instructions on stdin take from the build context.
copy_sources() {
  awk '/^COPY / && !/--from=/ { for (i = 2; i < NF; i++) if ($i !~ /^--/) print $i }'
}

# The checkout paths the build stage COPYs in: exactly what the production
# image compiles from, so exactly what the synced copy needs.
build_inputs() {
  build_stage | copy_sources
}

# The registry images the Docker build's stages start from; the other FROMs
# name a stage, a build argument (an image built here from these) or scratch.
registry_bases() {
  awk '/^FROM / { from[$2] = 1; if (toupper($3) == "AS") stage[$4] = 1 }
       END { for (f in from) if (!(f in stage) && f !~ /^\$/ && f != "scratch") print f }' \
    "$KIND_DOCKERFILE" "$COMMON_DOCKERFILE" | sort
}

# Why the Rust Dockerfiles may read context bytes the input key cannot list,
# one reason per line; empty when they cannot. Instructions are matched in any
# case and indentation, as BuildKit reads them. A mount reads the context
# unless it is a secret, tmpfs, ssh or plain cache mount (an untyped mount is
# a bind mount).
uncovered_context_reads() {
  local dockerfile mount
  for dockerfile in "$KIND_DOCKERFILE" "$COMMON_DOCKERFILE"; do
    grep -qiE '^[[:space:]]*(ADD|ONBUILD)([[:space:]]|$)' "$dockerfile" \
      && echo "$dockerfile uses ADD or ONBUILD"
    awk 'toupper($0) ~ /^[[:space:]]*COPY([[:space:]]|$)/ && !/^COPY / { found = 1 } END { exit !found }' \
      "$dockerfile" && echo "$dockerfile has a COPY not written as an unindented uppercase instruction"
    grep -qE '^COPY .*\\$' "$dockerfile" && echo "$dockerfile continues a COPY onto another line"
    while read -r mount; do
      [[ "$mount" =~ (^|,)type=(secret|tmpfs|ssh)(,|$) ]] && continue
      [[ "$mount" =~ (^|,)type=cache(,|$) && ! "$mount" =~ (^|,)from= ]] && continue
      echo "$dockerfile mounts --mount=$mount"
    done < <(grep -oiE -- '--mount=[^[:space:]]+' "$dockerfile" | cut -d= -f2-)
  done
}

# The content key of the Docker-mode artifact image: equal keys mean the
# build reads the same bytes. It covers:
#   - both Dockerfiles: the stages, their build-argument defaults, and the
#     feature and bin lists;
#   - every path they COPY from the context, by mode and blob ID;
#   - the ignore files that filter those (.dockerignore and each Dockerfile's
#     own <Dockerfile>.dockerignore, which BuildKit prefers), and every
#     .gitattributes from the root down to each input, which can change the
#     checked-out bytes;
#   - this script, which passes the build no build arguments;
#   - the digest the effective mirrored-image list (SPACE_MIRRORED_IMAGES, as
#     ci/scripts/mirror-images.sh reads it) pins for each registry base;
#   - the architecture.
# Commits, times and file stat data are not part of it. A key exists only for
# committed inputs: it fails when any input is modified or untracked, or when
# a Dockerfile reads the context in a way it cannot list.
rust_input_key() {
  local paths=() optional=() path dir base pin changes listing uncovered
  local pins="${SPACE_MIRRORED_IMAGES:-$REPO_ROOT/ci/mirrored-images.txt}"
  uncovered="$(uncovered_context_reads)"
  [[ -z "$uncovered" ]] || die "the Rust Dockerfiles read context files the input key cannot list:"$'\n'"$uncovered"
  mapfile -t paths < <(cat "$KIND_DOCKERFILE" "$COMMON_DOCKERFILE" | copy_sources | sort -u)
  (( ${#paths[@]} > 0 )) || die "could not read the COPY sources of the Rust Dockerfiles"
  paths+=("$KIND_DOCKERFILE" "$COMMON_DOCKERFILE" .dockerignore infra/space/kind/scripts/build-rust-artifacts.sh)
  # Inputs that change the build only when present; absent, they list nothing.
  optional=("$KIND_DOCKERFILE.dockerignore" "$COMMON_DOCKERFILE.dockerignore" .gitattributes)
  for path in "${paths[@]}"; do
    dir="$(dirname "$path")"
    while [[ "$dir" != . ]]; do
      optional+=("$dir/.gitattributes")
      dir="$(dirname "$dir")"
    done
  done
  mapfile -t optional < <(printf '%s\n' "${optional[@]}" | sort -u)
  # No index refresh: Tilt's concurrent builds read the same checkout.
  changes="$(git --no-optional-locks status --porcelain --untracked-files=all -- "${paths[@]}" "${optional[@]}")" \
    || die "could not read the Rust build inputs' git status"
  [[ -z "$changes" ]] || die "the Rust build inputs have uncommitted changes; they have no input key"
  listing="$(git ls-files -s --error-unmatch -- "${paths[@]}")" \
    || die "a COPY source of the Rust Dockerfiles is not tracked"
  listing+=$'\n'"$(git ls-files -s -- "${optional[@]}")" \
    || die "could not list the Rust build's ignore and attribute files"
  {
    echo "space-kind-rust-artifacts docker v1 $(uname -m)"
    # The release lane builds other binaries from the same inputs; name it so its
    # key never matches a test-hooks image's. The default build's key is unchanged.
    [[ "$RUST_BUILD" == test-hooks ]] || echo "rust-build $RUST_BUILD"
    while read -r base; do
      pin="$(awk -v name="$base" '$1 == name { print $2 }' "$pins")"
      echo "base $base ${pin:-unpinned}"
    done < <(registry_bases)
    printf '%s\n' "$listing"
  } | sha256sum | awk '{ print $1 }'
}

# "<target-relative-source> <artifact-path>" per line, from the scratch stage.
artifact_layout() {
  awk '/^FROM scratch AS build-artifacts-kind$/ { s = 1; next } s && /^COPY --from=build / {
         sub("/workspace/target/release/", "", $3); print $3, $4 }' "$KIND_DOCKERFILE"
}

profile_dir() {
  case "$PROFILE" in
    release) echo release ;;
    dev-release) echo dev-release ;;
    *) die "SPACE_KIND_RUST_PROFILE must be release or dev-release (got '$PROFILE')" ;;
  esac
}

cargo_profile_flag() {
  case "$PROFILE" in
    release) echo "--release" ;;
    *) echo "--profile=$PROFILE" ;;
  esac
}

# Run a command inside the builder image as the invoking user, with the
# synced build inputs at /workspace, the persistent target dir at /workspace/target and
# the persistent cargo home at /cargo-home. The image's RUSTUP_HOME stays
# readable for the pinned toolchain; CARGO_HOME moves so registry and git
# caches land on the host and survive the container. The uid mapping keeps
# every output owned by the developer, never root.
in_builder() {
  docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$SRC_DIR:/workspace" \
    -v "$TARGET_DIR:/workspace/target" \
    -v "$CARGO_HOME_DIR:/cargo-home" \
    -e HOME=/cargo-home \
    -e CARGO_HOME=/cargo-home \
    -e CARGO_TARGET_DIR=/workspace/target \
    -e CARGO_PROFILE_RELEASE_LTO="$RELEASE_LTO" \
    -e CARGO_TERM_COLOR=never \
    -w /workspace \
    "$CHEF_IMAGE" "$@"
}

# "<name> <version>" per binary: the highest GLIBC_x.y symbol version it
# requires, or "none" for a static binary. objdump is the builder image's.
required_glibc() {
  in_builder bash -c '
    for bin in "$@"; do
      v=$(objdump -T "$bin" 2>/dev/null | grep -oE "GLIBC_[0-9]+\.[0-9]+" | sed "s/GLIBC_//" | sort -V | tail -1)
      printf "%s %s\n" "$(basename "$bin")" "${v:-none}"
    done' _ "$@"
}

runtime_glibc() {
  docker run --rm --entrypoint sh "$RUNTIME_LIBC_IMAGE" -c 'ldd --version 2>&1 | head -1' \
    | grep -oE '[0-9]+\.[0-9]+$'
}

version_le() { # a <= b, dotted numerics
  [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" == "$1" ]]
}

# Fail the assembly, not the healthz wait: a binary linked against a newer
# glibc than the runtime image carries dies at exec with a symbol-version
# error that surfaces, thirty minutes later, as "api-server never came up".
# Takes the in-container paths of the staged binaries.
assert_glibc_compatible() {
  local runtime max name ver bad=()
  runtime="$(runtime_glibc)"
  [[ -n "$runtime" ]] || die "could not read the runtime image's glibc version from $RUNTIME_LIBC_IMAGE"
  max="none"
  while read -r name ver; do
    [[ "$ver" == none ]] && continue
    version_le "$ver" "$runtime" || bad+=("$name needs GLIBC_$ver")
    if [[ "$max" == none ]] || ! version_le "$ver" "$max"; then max="$ver"; fi
  done < <(required_glibc "$@")
  log "glibc guard: binaries require ≤ GLIBC_$max; runtime image $RUNTIME_LIBC_IMAGE provides $runtime"
  (( ${#bad[@]} == 0 )) || die "these binaries would not load on the runtime images (glibc $runtime):"$'\n'"$(printf '  %s\n' "${bad[@]}")"$'\n'"The build ran outside the builder image, or the image's base changed. Rebuild with SPACE_KIND_RUST_ARTIFACTS=docker to compare."
}

build_incremental() {
  local release_dir host_release_dir features bins sup_features stamp
  mapfile -t features < <(main_build_features; test_hook_features SPACE_KIND_TEST_HOOK_FEATURES)
  mapfile -t bins < <(main_build_bins)
  mapfile -t sup_features < <(supervisor_features)
  (( ${#features[@]} > 0 && ${#bins[@]} > 0 && ${#sup_features[@]} > 0 )) \
    || die "could not read the feature/bin lists from $KIND_DOCKERFILE build stage"

  local inputs=() synced
  mapfile -t inputs < <(build_inputs)
  (( ${#inputs[@]} > 0 )) || die "could not read the build inputs from $KIND_DOCKERFILE build stage"

  mkdir -p "$TARGET_DIR" "$CARGO_HOME_DIR" "$SRC_DIR"

  # Every runtime image's custom_build runs this script, so one edit starts six
  # runs; the first builds, and the other five used to repeat the builder image,
  # cargo, the supervisor, staging, the glibc guard and the assembly with nothing
  # to do, 6-19 s each in series on the lock (2026-09-23 bench). The record of
  # the last assembly, "<key> <image id>", is valid exactly while the synced
  # sources are what that assembly built from. It is taken out before the sync
  # touches the copy and put back only when the sync changed nothing: a sync that
  # rewrote some files and then failed must not leave a record vouching for them,
  # or a retry that finds nothing left to write would reuse the old image. Only a
  # completed assembly writes a new one.
  local assembled="$STAGE_DIR/.assembled" record key image_id
  record="$(cat "$assembled" 2>/dev/null || true)"
  rm -f "$assembled"
  local phase_started=$SECONDS
  synced="$(python3 "$REPO_ROOT/infra/space/kind/scripts/sync-rust-sources.py" "$REPO_ROOT" "$SRC_DIR" "${inputs[@]}")" \
    || die "could not sync the build inputs into $SRC_DIR"
  log "source sync: ${synced% *} written, ${synced#* } removed in $SRC_DIR ($((SECONDS - phase_started))s)"
  key="$(assembly_key)"
  if [[ "$synced" == "0 0" && -n "$record" ]]; then
    image_id="$(docker image inspect -f '{{.Id}}' "$ARTIFACT_IMAGE" 2>/dev/null || true)"
    if [[ -n "$image_id" && "$record" == "$key $image_id" ]]; then
      printf '%s\n' "$record" >"$assembled"
      log "sources unchanged since the last assembly; reusing $ARTIFACT_IMAGE (${image_id:7:12})"
      return 0
    fi
  fi

  phase_started=$SECONDS
  build_chef_base_image
  log "builder image: $((SECONDS - phase_started))s"
  release_dir="/workspace/target/$(profile_dir)"
  host_release_dir="$TARGET_DIR/$(profile_dir)"

  log "incremental mode: cargo $(cargo_profile_flag) ($RUST_BUILD, LTO $RELEASE_LTO) for ${#bins[@]} bins + runtime-supervisor inside $CHEF_IMAGE (target $TARGET_DIR)"
  local cargo_args=(cargo build --locked "$(cargo_profile_flag)" ${CARGO_SELECTION[@]+"${CARGO_SELECTION[@]}"})
  local f b
  for f in "${features[@]}"; do cargo_args+=(-F "$f"); done
  for b in "${bins[@]}"; do cargo_args+=(--bin "$b"); done
  phase_started=$SECONDS
  in_builder bash -c 'rustc --version && exec "$@"' _ "${cargo_args[@]}"
  log "cargo build: $((SECONDS - phase_started))s"

  local guest_args=(cargo build --locked "$(cargo_profile_flag)") arg
  while read -r arg; do
    read -ra arg <<<"$arg"
    guest_args+=("${arg[@]}")
  done < <(guest_build_args)
  while read -r f; do guest_args+=(-F "$f"); done < <(test_hook_features SPACE_KIND_GUEST_TEST_HOOK_FEATURES)
  (( ${#guest_args[@]} > 4 )) || die "could not read the guest build from $KIND_DOCKERFILE build stage"
  phase_started=$SECONDS
  in_builder "${guest_args[@]}"
  log "guest cargo build: $((SECONDS - phase_started))s"

  # As the production worker stage does: the supervisor is built through the
  # helper that writes its compatibility hash next to it.
  local sup_args=(cargo build --locked "$(cargo_profile_flag)" ${CARGO_SELECTION[@]+"${CARGO_SELECTION[@]}"})
  for f in "${sup_features[@]}"; do sup_args+=(-F "$f"); done
  sup_args+=(--bin space-runtime-supervisor-svc)
  phase_started=$SECONDS
  in_builder bash scripts/build-runtime-supervisor.sh \
    "$release_dir/space-runtime-supervisor-svc" \
    "$release_dir/space-runtime-supervisor-compatibility" \
    "${sup_args[@]}"
  log "runtime-supervisor build: $((SECONDS - phase_started))s"

  # Stage the /artifacts layout the Dockerfile declares.
  log "staging /artifacts layout into $STAGE_DIR"
  rm -rf "$STAGE_DIR"
  local src dst
  local -a staged_bins=()
  while read -r src dst; do
    [[ -f "$host_release_dir/$src" ]] || die "expected build output missing: $host_release_dir/$src"
    install -D -m 0755 "$host_release_dir/$src" "$STAGE_DIR$dst"
    [[ "$dst" == /artifacts/bin/* ]] && staged_bins+=("$release_dir/$src")
  done < <(artifact_layout)
  chmod 0644 "$STAGE_DIR/artifacts/etc/space-runtime-supervisor-compatibility"

  assert_glibc_compatible "${staged_bins[@]}"

  phase_started=$SECONDS
  build_health_probe_image
  log "assembling $ARTIFACT_IMAGE from the staged build"
  docker build -t "$ARTIFACT_IMAGE" -f "$HOST_DOCKERFILE" \
    --build-arg "SPACE_GRPC_HEALTH_PROBE_IMAGE=$HEALTH_PROBE_IMAGE" \
    --build-arg "SPACE_KIND_RUST_BUILD=$RUST_BUILD" \
    --build-context "artifacts=$STAGE_DIR/artifacts" .
  log "assembly: $((SECONDS - phase_started))s"
  stamp="$STAGE_DIR/.built-from"
  printf 'mode=incremental profile=%s build=%s builder=%s target=%s at=%s\n' \
    "$PROFILE" "$RUST_BUILD" "$CHEF_IMAGE" "$TARGET_DIR" "$(date -u +%FT%TZ)" >"$stamp"
  printf '%s %s\n' "$key" "$(docker image inspect -f '{{.Id}}' "$ARTIFACT_IMAGE")" >"$assembled"
}

# What an assembly depends on besides the synced sources, by content: the
# profile and Rust build, the Dockerfiles that define the builder, the build lists, the
# packaging and the health probe, the wrapper the builder image carries, the
# scripts that drive the build, and the libc image the glibc guard checked
# against. File identity (inode, mtime) is deliberately not part of it: the
# supervisor helper rewrites its compatibility file on every run.
assembly_key() {
  {
    echo "$PROFILE $RUST_BUILD $RUNTIME_LIBC_IMAGE"
    sha256sum "$KIND_DOCKERFILE" "$COMMON_DOCKERFILE" "$HOST_DOCKERFILE" \
      docker/run-cargo-with-sccache.sh infra/space/kind/scripts/build-rust-artifacts.sh \
      infra/space/kind/scripts/sync-rust-sources.py
  } | sha256sum | awk '{ print $1 }'
}

# ── Entry ─────────────────────────────────────────────────────────────────────

if [[ "${1:-}" == "--input-key" ]]; then
  rust_input_key
  exit 0
fi
MODE="$(decide_mode)"
if [[ "${1:-}" == "--mode" ]]; then
  echo "$MODE"
  exit 0
fi
(( $# == 0 )) || die "usage: $0 [--mode | --input-key]"

mkdir -p "$STATE_DIR"
# The artifact image's tag is shared by every checkout on the host; see the
# library for who reads this mark.
space_kind_mark_shared_input_writer "$REPO_ROOT"
# One build at a time, in either mode; concurrent callers wait, then find the
# build up to date. Tilt starts several at once: the rust-artifacts resource and
# every runtime image's custom_build. The incremental build needs this for its
# shared target dir. Without it the Docker path runs identical BuildKit solves
# side by side (248 s of overlap in a 2026-09-23 check), each exporting to the
# same cache scope, and is correct only because BuildKit merges them. Off Linux
# there may be no flock; the Docker path then runs unguarded, as it always has
# (incremental mode requires flock).
if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    log "another artifact build holds $LOCK_FILE; waiting for it"
    flock 9
    log "lock acquired after ${SECONDS}s"
  fi
else
  log "flock is not on PATH; concurrent callers of this build are not serialized"
fi
case "$MODE" in
  prebuilt)
    [[ -n "${SPACE_KIND_PREBUILT_RUST_IMAGE_ID:-}" && -n "${SPACE_KIND_PREBUILT_RUST_SHA:-}" ]] \
      || die "prebuilt mode requires an image ID and source SHA from the CI artifact handoff"
    [[ "$(docker image inspect -f '{{.Id}}' "$ARTIFACT_IMAGE")" == "$SPACE_KIND_PREBUILT_RUST_IMAGE_ID" ]] \
      || die "prebuilt Rust image is missing or does not match the verified image ID"
    prebuilt_build="$(docker image inspect -f '{{index .Config.Labels "space.pplx.net/kind-rust-build"}}' "$ARTIFACT_IMAGE")"
    [[ "$prebuilt_build" == "$RUST_BUILD" ]] \
      || die "prebuilt Rust image is the '${prebuilt_build:-unlabelled}' build; SPACE_KIND_RUST_BUILD asks for $RUST_BUILD"
    # An image main CI built from another commit is this checkout's build when
    # the image, the handoff and this checkout all name the same input key.
    if [[ "$(git rev-parse HEAD)" != "$SPACE_KIND_PREBUILT_RUST_SHA" ]]; then
      [[ -n "${SPACE_KIND_PREBUILT_RUST_KEY:-}" ]] \
        || die "prebuilt Rust source SHA does not match this checkout, and the handoff names no input key"
      [[ "$(docker image inspect -f "{{index .Config.Labels \"$INPUT_KEY_LABEL\"}}" "$ARTIFACT_IMAGE")" == "$SPACE_KIND_PREBUILT_RUST_KEY" ]] \
        || die "prebuilt Rust image does not carry the input key its handoff names"
      [[ "$(rust_input_key)" == "$SPACE_KIND_PREBUILT_RUST_KEY" ]] \
        || die "prebuilt Rust source SHA does not match this checkout, and neither does its input key"
      log "prebuilt image built from ${SPACE_KIND_PREBUILT_RUST_SHA:0:12}: its Rust input key matches this checkout's"
    fi
    ;;
  docker) build_docker ;;
  incremental) build_incremental ;;
esac
log "ready: $ARTIFACT_IMAGE ($MODE, $RUST_BUILD, ${SECONDS}s)"
