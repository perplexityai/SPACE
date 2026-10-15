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
# Registry trouble alone costs time, not the check, while Docker Hub and
# snapshot.ubuntu.com work. Every registry call outside the builds has a
# deadline; any failed or timed-out marker lookup builds, and a failed or
# timed-out write is a warning. A failing build never writes the marker, so a
# broken change fails every run until it is fixed.
#
# Builds run on a docker-container builder, started from the pinned BuildKit
# image in the third-party image mirror, else from Docker Hub, else on Docker's
# own builder without saving the cache. They read the base, build-toolchain and
# smoke-toolchain layers from the repository's toolchain-cache and smoke-cache
# tags; publish writes those tags. The cache leaves out the kernel source and
# compile, which the marker covers, so it stays the size of the packages. The
# Ubuntu base comes from the image mirror when SPACE_CI_IMAGE_MIRROR is set and
# the mirror has it (ci/scripts/mirror-images.sh build-contexts). A build that
# takes layers from the mirror or the cache has a deadline. If it fails
# fetching from them or overruns, it runs once more without either, on a new
# builder, and so does every later build and export of the run.
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
# Deadlines: each lookup, login and mirror probe; each cache export and the
# marker push; and each kernel build that takes layers from the mirror or the
# cache (the smoke image's build gets half), and its retry if that runs on
# Docker's own builder, which may still hold the stalled download. A kernel
# build normally takes about 30 minutes cold and far less from the cache.
#
# The budget against the job's timeout-minutes (180, main-ci.yml), for the worst
# case: GHCR answers manifests but stalls every download, while Docker Hub and
# snapshot.ubuntu.com work, and the builder for the retry does not start.
#   login, marker lookup, mirror probes, cache-tag probes   8 min at most
#   builder: the mirrored BuildKit image stalls (60 + 180 + 60 s), the source
#   starts                                                   7 min
#   kernel build with the mirror and cache, cut off         60 min
#   the old builder removed, a new one fails to start
#   (60 + 60 + 180 + 60 s)                                   6 min
#   the same build, cold, on Docker's own builder, at most  60 min
#   smoke build, without them since the fallback           ~10 min
#   the marker push (Docker's own builder exports nothing)   5 min
# About 156 minutes. When neither BuildKit image starts, the builder costs 10
# minutes and there is no builder to replace: about 153. When the retry builder
# starts (5 min), the cold build takes about 45 minutes without a deadline, the
# two cache exports, two tag checks and the push 17, and removing the builder
# at the end 1: about 153.
REGISTRY_TIMEOUT_SECONDS="${SPACE_GUEST_KERNEL_REGISTRY_TIMEOUT_SECONDS:-60}"
WRITE_TIMEOUT_SECONDS="${SPACE_GUEST_KERNEL_WRITE_TIMEOUT_SECONDS:-300}"
BUILD_TIMEOUT_SECONDS="${SPACE_GUEST_KERNEL_BUILD_TIMEOUT_SECONDS:-3600}"
# The final error of a build that failed fetching from a registry rather than in
# one of its own steps: an unreachable, refused, truncated or corrupt transfer.
FETCH_FAILURE='^ERROR:.*(httpReadSeeker|could not fetch content descriptor|failed to copy:|unexpected status|failed to verify|blob unknown|failed to do request|failed to resolve source metadata|i/o timeout|connection reset|connection refused|TLS handshake timeout|unexpected EOF|no such host|context deadline exceeded)'
TIMEOUT_BIN="$(command -v timeout || command -v gtimeout || true)"
BUILDER_OPTION=""
CACHE_EXPORT=false
# Whether this run started the builder, and so may remove it.
OWN_BUILDER=false
# The BuildKit image of the builder this run started.
BUILDER_IMAGE=""
# Set once a build has fallen back; every later build and export then goes
# without the mirror and the cache too.
WITHOUT_EXTERNAL=false
OUTPUT_ROOT=""
cleanup() {
    [ -z "${OUTPUT_ROOT}" ] || rm -rf "${OUTPUT_ROOT}"
    # The builder this run started (after a fallback, the retry's), so that no
    # later run adopts it by name; never a caller's.
    [ "${OWN_BUILDER}" != true ] ||
        with_deadline 60 docker buildx rm "${BUILDER_OPTION#--builder=}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

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

# The directory's files, tracked or not, without ignored build output. The
# marker covers what is built, and nothing outside this directory: the base is
# pinned by digest and the image mirror serves the same content, so an edit to
# the mirror (ci/scripts/mirror-images.sh, ci/mirrored-images.txt) or to the
# workflow does not change the kernel. Hashing the workflow made every
# unrelated edit to it force a cold rebuild through the Ubuntu snapshot mirror.
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

# start_builder <name> <image>...: start a docker-container builder of that
# name from the first image that starts, removing each attempt that does not.
start_builder() {
    local name="$1" image
    shift
    for image in "$@"; do
        if with_deadline 60 docker buildx create --name "${name}" --driver docker-container \
            --driver-opt "image=${image}" >/dev/null &&
            with_deadline 180 docker buildx inspect --bootstrap "${name}" >/dev/null; then
            echo "Started the ${name} builder from ${image}"
            BUILDER_OPTION="--builder=${name}"
            BUILDER_IMAGE="${image}"
            CACHE_EXPORT=true
            OWN_BUILDER=true
            return 0
        fi
        echo "::warning::Could not start the ${name} builder from ${image}"
        with_deadline 60 docker buildx rm "${name}" >/dev/null 2>&1 || true
    done
    return 1
}

use_docker_builder() {
    BUILDER_OPTION=""
    CACHE_EXPORT=false
    OWN_BUILDER=false
    echo "::warning::Building on Docker's own builder, which reads the toolchain cache but cannot save it"
}

# Choose the builder: an existing one by that name; else one started from the
# mirrored BuildKit image, then from its source; else Docker's own builder,
# which cannot export a registry cache.
select_builder() {
    if docker buildx inspect "${BUILDER}" >/dev/null 2>&1; then
        BUILDER_OPTION="--builder=${BUILDER}"
        CACHE_EXPORT=true
        OWN_BUILDER=false
        return 0
    fi
    # shellcheck disable=SC2046 # one reference per line, none containing spaces
    start_builder "${BUILDER}" $(mirrored_buildkit) "${BUILDKIT_IMAGE}" || use_docker_builder
}

# A build that stalled or failed fetching can leave BuildKit holding the
# download, and the same digests built again on that builder wait for it, with
# no deadline. So the retry, and the rest of the run, go to a new builder under
# a name of this run's own, which nothing can have left behind, started from
# the BuildKit image the first one ran (already local), else Docker's own
# builder. Removing the old builder only frees it; nothing depends on that. The
# new builder has no layers yet, but neither had the first: the retry is cold
# either way.
replace_builder() {
    [ -n "${BUILDER_OPTION}" ] || return 0
    local old="${BUILDER_OPTION#--builder=}"
    echo "::warning::Leaving the ${old} builder, which may still hold the failed download"
    if [ "${OWN_BUILDER}" = true ]; then
        with_deadline 60 docker buildx rm "${old}" >/dev/null 2>&1 ||
            echo "::warning::Could not remove the ${old} builder"
    fi
    start_builder "${BUILDER}-retry-$$" "${BUILDER_IMAGE:-${BUILDKIT_IMAGE}}" || use_docker_builder
}

# The docker build options of every build here, one per line, but the
# builder's: the mirrored base, and each cache tag that exists.
build_options() {
    local target
    with_deadline "$((2 * REGISTRY_TIMEOUT_SECONDS))" \
        bash "${REPO_ROOT}/ci/scripts/mirror-images.sh" build-contexts "${SCRIPT_DIR}/Dockerfile" ||
        echo "::warning::Building from the Dockerfile's own base; the image mirror lookup failed or overran" >&2
    for target in toolchain smoke; do
        if lookup "${REPOSITORY}:${target}-cache" "Building the ${target} layers without the cache" >&2; then
            printf -- '--cache-from=type=registry,ref=%s\n' "${REPOSITORY}:${target}-cache"
        fi
    done
}

# without_external <options>: the options without the mirror and the cache.
without_external() {
    grep -Ev '^--(cache-from|build-context)=' <<<"$1" || true
}

# The options of a build or export: the current builder's, then the given
# ones, without the mirror and the cache once a build has fallen back.
effective() {
    [ -z "${BUILDER_OPTION}" ] || printf '%s\n' "${BUILDER_OPTION}"
    if [ "${WITHOUT_EXTERNAL}" = true ]; then without_external "$1"; else printf '%s\n' "$1"; fi
}

# overran <status> <started> <deadline>: whether a command run with_deadline
# since SECONDS was <started> was cut off. timeout exits 124, or 137 once it
# has to kill, but the command can exit with either itself (smoke-boot.sh
# passes on QEMU's status), so they count only once the deadline has passed,
# give or take a few seconds.
overran() {
    [ -n "${TIMEOUT_BIN}" ] && { [ "$1" -eq 124 ] || [ "$1" -eq 137 ]; } &&
        [ "$((SECONDS - $2))" -ge "$(($3 - 3))" ]
}

# attempt <deadline> <options> <command>...: run a build command with
# KERNEL_DOCKER_BUILD_OPTIONS=<options>, within the deadline if it takes layers
# from the mirror or the cache. If it then failed fetching from them, or
# overran, it runs once more without either, on a new builder, and so does
# every later build of the run. That retry has no deadline unless it runs on
# Docker's own builder, which may still hold the stalled download. A failure in
# one of the build's own steps fails without a retry.
#
# The output goes to a file that a follower prints, not through a pipe: a
# docker client that outlives the deadline would hold a pipe open, and the wait
# for it would have none.
attempt() {
    local deadline="$1" requested="$2" options log follower started status=0
    options="$(effective "${requested}")"
    shift 2
    log="$(mktemp)"
    if [[ "${options}" == *--cache-from=* || "${options}" == *--build-context=* ]]; then
        tail -f "${log}" &
        follower=$!
        started="${SECONDS}"
        with_deadline "${deadline}" env KERNEL_DOCKER_BUILD_OPTIONS="${options}" "$@" >>"${log}" 2>&1 ||
            status=$?
        sleep 1
        kill "${follower}" 2>/dev/null || true
        wait "${follower}" 2>/dev/null || true
        if [ "${status}" -ne 0 ] && { overran "${status}" "${started}" "${deadline}" ||
            grep '^ERROR:' "${log}" | tail -n 1 | grep -Eq "${FETCH_FAILURE}"; }; then
            echo "::warning::The build failed or stalled taking layers from the image mirror or the toolchain cache; building once more, and for the rest of this run, without them"
            WITHOUT_EXTERNAL=true
            replace_builder
            status=0
            if [ -n "${BUILDER_OPTION}" ]; then
                env KERNEL_DOCKER_BUILD_OPTIONS="$(effective "${requested}")" "$@" || status=$?
            else
                started="${SECONDS}"
                with_deadline "${deadline}" env KERNEL_DOCKER_BUILD_OPTIONS="$(effective "${requested}")" "$@" ||
                    status=$?
                ! overran "${status}" "${started}" "${deadline}" ||
                    echo "::error::The cold retry on Docker's own builder overran its ${deadline} s deadline, probably waiting on the stalled download"
            fi
        fi
    else
        env KERNEL_DOCKER_BUILD_OPTIONS="${options}" "$@" || status=$?
    fi
    rm -f "${log}"
    return "${status}"
}

# Export the given toolchain stages' layers to their cache tags, then check that
# each tag exists, which only a first save can fail. The layers are already
# built, so a deadline only cuts off a stalled push. An export failure fails the
# export, without ignore-error, and is reported, not fatal.
save_toolchain_caches() {
    local options target
    options="$(effective "$1")"
    shift
    for target in "$@"; do
        echo "Saving the ${target} layers to ${REPOSITORY}:${target}-cache"
        # shellcheck disable=SC2086 # one option per line, none containing spaces
        if ! with_deadline "${WRITE_TIMEOUT_SECONDS}" docker buildx build ${options} \
            --platform linux/amd64 --target "${target}" \
            --cache-to "type=registry,ref=${REPOSITORY}:${target}-cache,mode=max,oci-mediatypes=true" \
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

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    case "${1:-}" in
        hash) input_hash ;;
        check) run false ;;
        publish) run true ;;
        *)
            echo "usage: $0 hash|check|publish" >&2
            exit 2
            ;;
    esac
fi
