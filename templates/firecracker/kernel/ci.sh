#!/usr/bin/env bash
# Build and QEMU-smoke the default guest kernel for Main CI, unless these exact
# inputs already passed.
#
# Every kernel input is pinned in this directory (source commit, base config
# digest, Ubuntu base digest and package snapshot), and this script is the
# whole check, so a build that passed for one hash of the directory passes
# again. A pass is recorded as a marker image in GHCR: one small file naming
# the commit that passed, with that commit in its labels. Its tag is the first
# 16 characters of the SHA-256 over the directory's files (each hashed with its
# path and executable bit) and a format salt, as ci/scripts/ci-image.sh tags the
# CI image. A run that finds the marker skips the ~30-minute rebuild and every
# download it needs.
#
#   hash     print the input hash
#   check    skip if the marker exists, else build and smoke-boot; never writes
#   publish  as check, and after a build also save the toolchain cache and,
#            after a pass, the marker (Main CI only)
#
# Nothing optional can fail or stall the check. Every registry call outside the
# builds has a deadline; any failed or timed-out marker lookup builds, and a
# failed or timed-out write is a warning. A failing build never writes the
# marker, so a broken change fails every run until it is fixed.
#
# Builds run on a docker-container builder, started from the pinned BuildKit
# image in the third-party image mirror, else from Docker Hub, else on Docker's
# own builder without saving the cache. They read the base, build-toolchain and
# smoke-toolchain layers from the repository's toolchain-cache and smoke-cache
# tags; publish writes those tags. The cache leaves out the kernel source and
# compile, which the marker covers, so it stays the size of the packages. The
# Ubuntu base comes from the image mirror when SPACE_CI_IMAGE_MIRROR is set and
# the mirror has it (ci/scripts/mirror-images.sh build-contexts). A build that
# fails fetching from the mirror or the cache, or that stalls while importing
# the cache, runs once more without either.
#
# GHCR_TOKEN, when set, logs in to the registry first.
# SPACE_GUEST_KERNEL_CI_REPOSITORY overrides the repository, and
# SPACE_GUEST_KERNEL_BUILDER the builder, which is used as it is if it exists.
# SPACE_GUEST_KERNEL_REGISTRY_TIMEOUT_SECONDS, ..._WRITE_TIMEOUT_SECONDS and
# ..._BUILD_TIMEOUT_SECONDS override the deadlines. Without GNU timeout (as
# `timeout` or `gtimeout`) nothing has a deadline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
KERNEL_DIR="templates/firecracker/kernel"
REPOSITORY="${SPACE_GUEST_KERNEL_CI_REPOSITORY:-ghcr.io/perplexityai/space-guest-kernel-ci}"
SOURCE_URL="https://github.com/perplexityai/SPACE"
# Part of the tag, so that a change to what a recorded pass means produces a
# new tag instead of trusting the old markers.
MARKER_FORMAT="v1"
BUILDER="${SPACE_GUEST_KERNEL_BUILDER:-space-guest-kernel}"
# The BuildKit daemon of the builder, pinned rather than pulled by a mutable
# tag; ci/mirrored-images.txt lists it under this name.
BUILDKIT_IMAGE="moby/buildkit:v0.32.2@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8"
# Each lookup, login and mirror probe; each cache export and the marker push;
# and each build that imports the cache, which a cache hit keeps far shorter
# (the smoke image's build gets half).
REGISTRY_TIMEOUT_SECONDS="${SPACE_GUEST_KERNEL_REGISTRY_TIMEOUT_SECONDS:-60}"
WRITE_TIMEOUT_SECONDS="${SPACE_GUEST_KERNEL_WRITE_TIMEOUT_SECONDS:-300}"
BUILD_TIMEOUT_SECONDS="${SPACE_GUEST_KERNEL_BUILD_TIMEOUT_SECONDS:-3600}"
# The last line of a build that failed fetching from a registry rather than in
# one of its own steps: an unreachable, refused, truncated or corrupt transfer.
FETCH_FAILURE='^ERROR:.*(httpReadSeeker|could not fetch content descriptor|failed to copy:|unexpected status|failed to verify|blob unknown|failed to do request|failed to resolve source metadata|i/o timeout|connection reset|connection refused|TLS handshake timeout|unexpected EOF|no such host|context deadline exceeded)'
TIMEOUT_BIN="$(command -v timeout || command -v gtimeout || true)"
BUILDER_OPTION=""
CACHE_EXPORT=false
OUTPUT_ROOT=""
trap '[ -z "${OUTPUT_ROOT}" ] || rm -rf "${OUTPUT_ROOT}"' EXIT

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@"
    else
        shasum -a 256 "$@"
    fi
}

# with_deadline <seconds> <command>...: exits 124 when the command overruns.
with_deadline() {
    local seconds="$1"
    shift
    if [ -n "${TIMEOUT_BIN}" ]; then
        "${TIMEOUT_BIN}" --kill-after=10 "${seconds}" "$@"
    else
        "$@"
    fi
}

# The directory's files, tracked or not, without ignored build output.
inputs() {
    git -C "${REPO_ROOT}" ls-files --cached --others --exclude-standard -- "${KERNEL_DIR}" | LC_ALL=C sort -u
}

# A file's executable bit counts: the build runs the scripts it copies.
input_hash() {
    (cd "${REPO_ROOT}" && {
        inputs | while IFS= read -r input; do
            if [ -x "${input}" ]; then mode=executable; else mode=regular; fi
            printf '%s %s\n' "$(sha256 "${input}")" "${mode}"
        done
        echo "format: ${MARKER_FORMAT}"
    } | sha256 | cut -c1-16)
}

registry_login() {
    [ -n "${GHCR_TOKEN:-}" ] || return 0
    printf '%s' "${GHCR_TOKEN}" |
        with_deadline "${REGISTRY_TIMEOUT_SECONDS}" docker login "${REPOSITORY%%/*}" \
            --username "${GITHUB_ACTOR:-space-ci}" --password-stdin >/dev/null
}

# lookup <ref> <what>: succeed if the registry serves the reference. Otherwise
# say why not, as a warning unless the registry answered that it is missing.
lookup() {
    local ref="$1" what="$2" error status=0
    error="$(with_deadline "${REGISTRY_TIMEOUT_SECONDS}" \
        docker buildx imagetools inspect --raw "${ref}" 2>&1 >/dev/null)" || status=$?
    [ "${status}" -ne 0 ] || return 0
    if [ "${status}" -eq 124 ]; then
        echo "::warning::${what}; ${ref} gave no answer within ${REGISTRY_TIMEOUT_SECONDS} s"
    elif [ "${status}" -ne 127 ] && [[ "${error}" == *"${ref}: not found"* ]]; then
        echo "${what}; ${ref} does not exist"
    else
        echo "::warning::${what}; could not look up ${ref}: ${error%%$'\n'*}"
    fi
    return 1
}

# The pinned BuildKit image in the image mirror, if the mirror has it. A file
# naming the image makes build-contexts look it up as it does a FROM image.
mirrored_buildkit() {
    local listing line
    listing="$(mktemp)"
    printf '%s\n' "${BUILDKIT_IMAGE}" >"${listing}"
    while IFS= read -r line; do
        [[ "${line}" != "--build-context=${BUILDKIT_IMAGE}=docker-image://"* ]] ||
            printf '%s\n' "${line#*=docker-image://}"
    done < <(with_deadline "$((2 * REGISTRY_TIMEOUT_SECONDS))" \
        bash "${REPO_ROOT}/ci/scripts/mirror-images.sh" build-contexts "${listing}" || true)
    rm -f "${listing}"
}

# Choose the builder: an existing one by that name; else one started from the
# mirrored BuildKit image, then from its source; else Docker's own builder,
# which cannot export a registry cache.
select_builder() {
    local image
    if docker buildx inspect "${BUILDER}" >/dev/null 2>&1; then
        BUILDER_OPTION="--builder=${BUILDER}"
        CACHE_EXPORT=true
        return 0
    fi
    for image in $(mirrored_buildkit) "${BUILDKIT_IMAGE}"; do
        if with_deadline 60 docker buildx create --name "${BUILDER}" --driver docker-container \
            --driver-opt "image=${image}" >/dev/null &&
            with_deadline 180 docker buildx inspect --bootstrap "${BUILDER}" >/dev/null; then
            echo "Started the ${BUILDER} builder from ${image}"
            BUILDER_OPTION="--builder=${BUILDER}"
            CACHE_EXPORT=true
            return 0
        fi
        echo "::warning::Could not start the ${BUILDER} builder from ${image}"
        with_deadline 60 docker buildx rm "${BUILDER}" >/dev/null 2>&1 || true
    done
    echo "::warning::Building on Docker's own builder, which reads the toolchain cache but cannot save it"
}

# The docker build options of every build here, one per line: the builder, the
# mirrored base, and each cache tag that exists.
build_options() {
    local target
    [ -z "${BUILDER_OPTION}" ] || printf '%s\n' "${BUILDER_OPTION}"
    with_deadline "$((2 * REGISTRY_TIMEOUT_SECONDS))" \
        bash "${REPO_ROOT}/ci/scripts/mirror-images.sh" build-contexts "${SCRIPT_DIR}/Dockerfile" ||
        echo "::warning::Building from the Dockerfile's own base; the image mirror lookup failed or overran" >&2
    for target in toolchain smoke; do
        if lookup "${REPOSITORY}:${target}-cache" "Building the ${target} layers without the cache" >&2; then
            printf -- '--cache-from=type=registry,ref=%s\n' "${REPOSITORY}:${target}-cache"
        fi
    done
}

# attempt <deadline> <options> <command>...: run a build command with
# KERNEL_DOCKER_BUILD_OPTIONS=<options>, within the deadline if it imports the
# cache: a cache hit is predictable, a cold build (a new snapshot) is not. If it
# took anything from the mirror or the cache and then failed fetching from a
# registry or overran, it runs once more without either and without a deadline.
attempt() {
    local deadline="$1" options="$2" log status=0
    shift 2
    log="$(mktemp)"
    if [[ "${options}" == *--cache-from=* ]]; then
        with_deadline "${deadline}" env KERNEL_DOCKER_BUILD_OPTIONS="${options}" "$@" 2>&1 | tee "${log}"
    else
        env KERNEL_DOCKER_BUILD_OPTIONS="${options}" "$@" 2>&1 | tee "${log}"
    fi
    status="${PIPESTATUS[0]}"
    if [ "${status}" -ne 0 ] && [[ "${options}" == *--cache-from=* || "${options}" == *--build-context=* ]] &&
        { [ "${status}" -eq 124 ] || grep -Eq "${FETCH_FAILURE}" "${log}"; }; then
        echo "::warning::The build failed or stalled taking layers from the image mirror or the toolchain cache; building once more without them"
        status=0
        env KERNEL_DOCKER_BUILD_OPTIONS="$(grep -Ev '^--(cache-from|build-context)=' <<<"${options}")" "$@" ||
            status=$?
    fi
    rm -f "${log}"
    return "${status}"
}

# Export the given toolchain stages' layers to their cache tags, then check that
# each tag exists. The layers are already built, so a deadline only cuts off a
# stalled push; an export failure is reported, not fatal.
save_toolchain_caches() {
    local options="$1" target
    shift
    for target in "$@"; do
        echo "Saving the ${target} layers to ${REPOSITORY}:${target}-cache"
        # shellcheck disable=SC2086 # one option per line, none containing spaces
        if ! with_deadline "${WRITE_TIMEOUT_SECONDS}" docker buildx build ${options} \
            --platform linux/amd64 --target "${target}" \
            --cache-to "type=registry,ref=${REPOSITORY}:${target}-cache,mode=max,oci-mediatypes=true,ignore-error=true" \
            --output type=cacheonly "${SCRIPT_DIR}"; then
            echo "::warning::Could not save the ${target} layers to ${REPOSITORY}:${target}-cache"
        elif with_deadline "${REGISTRY_TIMEOUT_SECONDS}" \
            docker buildx imagetools inspect --raw "${REPOSITORY}:${target}-cache" >/dev/null 2>&1; then
            echo "Saved the ${target} layers to ${REPOSITORY}:${target}-cache"
        else
            echo "::warning::The ${target} layers were not saved: ${REPOSITORY}:${target}-cache does not exist after the export"
        fi
    done
}

# The marker holds one file naming the commit that passed: an image without
# layers has a null layer list, which registries need not accept. Docker pushes
# it, whichever builder made it.
record_pass() {
    local ref="$1" revision
    revision="${GITHUB_SHA:-$(git -C "${REPO_ROOT}" rev-parse HEAD)}"
    mkdir "${OUTPUT_ROOT}/marker"
    printf '%s\n' "${revision}" >"${OUTPUT_ROOT}/marker/revision"
    # shellcheck disable=SC2086 # at most one option, without spaces
    if printf 'FROM scratch\nCOPY revision /\n' | docker buildx build ${BUILDER_OPTION} \
        --provenance=false --sbom=false --file - --load --tag "${ref}" \
        --label "org.opencontainers.image.source=${SOURCE_URL}" \
        --label "org.opencontainers.image.revision=${revision}" \
        "${OUTPUT_ROOT}/marker" &&
        with_deadline "${WRITE_TIMEOUT_SECONDS}" docker push --quiet "${ref}" >/dev/null; then
        echo "Recorded the passing build of these kernel inputs as ${ref}"
    else
        echo "::warning::Could not record the passing build as ${ref}; the next run rebuilds it"
    fi
}

run() {
    local write="$1" ref options artifact_dir kernel_version built=false status=0
    ref="${REPOSITORY}:$(input_hash)"
    if ! registry_login; then
        echo "::warning::Could not log in to ${REPOSITORY%%/*}; the lookup below may fail"
    fi
    if lookup "${ref}" "No passing build of these kernel inputs is recorded"; then
        echo "These kernel inputs already built and smoke-booted (${ref}); skipping the rebuild."
        return 0
    fi

    select_builder
    options="$(build_options)"
    # The default profile only: the hash does not cover the overrides.
    OUTPUT_ROOT="$(mktemp -d)"
    attempt "${BUILD_TIMEOUT_SECONDS}" "${options}" env -u ARCH -u KERNEL_VERSION -u FIRECRACKER_CI_VERSION \
        -u KERNEL_SOURCE_COMMIT -u BASE_CONFIG_SHA256 -u CONFIG_FRAGMENT \
        OUTPUT_DIR="${OUTPUT_ROOT}" "${SCRIPT_DIR}/build.sh" || status=$?
    if [ "${status}" -eq 0 ]; then
        built=true
        artifact_dir="$(echo "${OUTPUT_ROOT}"/vmlinux-*)"
        kernel_version="${artifact_dir##*/vmlinux-}"
        if [ -f "${artifact_dir}/vmlinux.bin" ] && [ -f "${artifact_dir}/config" ] &&
            (cd "${artifact_dir}" && sha256 --check --strict vmlinux.bin.sha256); then
            # The container builder keeps its images; the boot runs from Docker's.
            attempt "$((BUILD_TIMEOUT_SECONDS / 2))" "${options}"$'\n'--load \
                "${SCRIPT_DIR}/smoke-boot.sh" "${artifact_dir}/vmlinux.bin" "${kernel_version}" ||
                status=$?
        else
            echo "ERROR: the build did not produce a verified vmlinux.bin and config" >&2
            status=1
        fi
    fi

    # After the builds, so that their layers are in the builder and only the
    # upload remains. The toolchain is saved once the kernel built, even if it
    # then fails to boot.
    if [ "${write}" = true ] && [ "${built}" = true ]; then
        if [ "${CACHE_EXPORT}" != true ]; then
            echo "::warning::Not saving the toolchain cache: Docker's own builder cannot export it"
        elif [ "${status}" -eq 0 ]; then
            save_toolchain_caches "${options}" toolchain smoke
        else
            save_toolchain_caches "${options}" toolchain
        fi
    fi
    [ "${status}" -eq 0 ] || return "${status}"

    if [ "${write}" = true ]; then
        record_pass "${ref}"
    fi
}

case "${1:-}" in
    hash) input_hash ;;
    check) run false ;;
    publish) run true ;;
    *)
        echo "usage: $0 hash|check|publish" >&2
        exit 2
        ;;
esac
