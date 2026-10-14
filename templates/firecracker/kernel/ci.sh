#!/usr/bin/env bash
# Build and QEMU-smoke the default guest kernel for Main CI, unless these exact
# inputs already passed.
#
# Every kernel input is pinned in this directory (source commit, base config
# digest, Ubuntu base digest and package snapshot), and this script is the
# whole check, so a build that passed for one hash of the directory passes
# again. A pass is recorded as a marker image in GHCR, an empty image labelled
# with the commit that passed. Its tag is the first 16 characters of the
# SHA-256 over the directory's files (each hashed with its path) and a format
# salt, as ci/scripts/ci-image.sh tags the CI image. A run that finds the
# marker skips the ~30-minute rebuild and every download it needs.
#
#   hash     print the input hash
#   check    skip if the marker exists, else build and smoke-boot; never writes
#   publish  as check, but also save the toolchain cache before building and
#            the marker after a pass (Main CI only)
#
# Any failed marker lookup builds, so an unreachable registry costs a rebuild,
# never a failed check. A failing build never writes the marker, so a broken
# change fails every run until it is fixed. A failed write is a warning.
#
# Builds run on a docker-container builder, which can export a registry cache.
# They read the base, build-toolchain and smoke-toolchain layers from the
# repository's toolchain-cache and smoke-cache tags, ignoring errors; publish
# writes those tags. The cache leaves out the kernel source and compile, which
# the marker covers, so it stays the size of the packages. The Ubuntu base
# comes from the third-party image mirror when SPACE_CI_IMAGE_MIRROR is set
# and the mirror has it (ci/scripts/mirror-images.sh build-contexts).
#
# GHCR_TOKEN, when set, logs in to the registry first.
# SPACE_GUEST_KERNEL_CI_REPOSITORY overrides the repository, and
# SPACE_GUEST_KERNEL_BUILDER the builder, which is created if it is absent.
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
LOOKUP_TIMEOUT_SECONDS=60
OUTPUT_ROOT=""
trap '[ -z "${OUTPUT_ROOT}" ] || rm -rf "${OUTPUT_ROOT}"' EXIT

# The directory's files, tracked or not, without ignored build output.
inputs() {
    git -C "${REPO_ROOT}" ls-files --cached --others --exclude-standard -- "${KERNEL_DIR}" | LC_ALL=C sort -u
}

input_hash() {
    (cd "${REPO_ROOT}" && {
        inputs | while IFS= read -r input; do sha256sum "${input}"; done
        echo "format: ${MARKER_FORMAT}"
    } | sha256sum | cut -c1-16)
}

registry_login() {
    [ -n "${GHCR_TOKEN:-}" ] || return 0
    printf '%s' "${GHCR_TOKEN}" |
        docker login "${REPOSITORY%%/*}" --username "${GITHUB_ACTOR:-space-ci}" \
            --password-stdin >/dev/null
}

# Succeed if the marker exists. Otherwise say why not and fail.
find_marker() {
    local ref="$1" error
    if error="$(timeout "${LOOKUP_TIMEOUT_SECONDS}" \
        docker buildx imagetools inspect --raw "${ref}" 2>&1 >/dev/null)"; then
        return 0
    fi
    if [[ "${error}" == *"not found"* ]]; then
        echo "No passing build of these kernel inputs is recorded as ${ref}"
    else
        echo "::warning::Building the guest kernel; could not look up ${ref}: ${error%%$'\n'*}"
    fi
    return 1
}

ensure_builder() {
    docker buildx inspect "${BUILDER}" >/dev/null 2>&1 ||
        docker buildx create --name "${BUILDER}" --driver docker-container >/dev/null
}

# The docker build options of every build here, one per line.
build_options() {
    printf -- '--builder=%s\n' "${BUILDER}"
    bash "${REPO_ROOT}/ci/scripts/mirror-images.sh" build-contexts "${SCRIPT_DIR}/Dockerfile"
    printf -- '--cache-from=type=registry,ref=%s\n' "${REPOSITORY}:toolchain-cache" "${REPOSITORY}:smoke-cache"
}

# Export the toolchain stages' layers to their cache tags. Done before the
# kernel build so that a failing kernel change still saves them; a failed
# export is ignored.
save_toolchain_caches() {
    local options="$1" target
    for target in toolchain smoke; do
        echo "Saving the ${target} layers to ${REPOSITORY}:${target}-cache"
        # shellcheck disable=SC2086 # one option per line, none containing spaces
        docker buildx build ${options} --platform linux/amd64 --target "${target}" \
            --cache-to "type=registry,ref=${REPOSITORY}:${target}-cache,mode=max,oci-mediatypes=true,ignore-error=true" \
            --output type=cacheonly "${SCRIPT_DIR}"
    done
}

# The marker holds one file naming the commit that passed: an image without
# layers has a null layer list, which registries need not accept.
record_pass() {
    local ref="$1" revision
    revision="${GITHUB_SHA:-$(git -C "${REPO_ROOT}" rev-parse HEAD)}"
    mkdir "${OUTPUT_ROOT}/marker"
    printf '%s\n' "${revision}" >"${OUTPUT_ROOT}/marker/revision"
    if printf 'FROM scratch\nCOPY revision /\n' | docker buildx build --builder "${BUILDER}" \
        --provenance=false --sbom=false --file - \
        --label "org.opencontainers.image.source=${SOURCE_URL}" \
        --label "org.opencontainers.image.revision=${revision}" \
        --output "type=registry,name=${ref},oci-mediatypes=true" "${OUTPUT_ROOT}/marker"; then
        echo "Recorded the passing build of these kernel inputs as ${ref}"
    else
        echo "::warning::Could not record the passing build as ${ref}; the next run rebuilds it"
    fi
}

run() {
    local write="$1" ref options artifact_dir kernel_version
    ref="${REPOSITORY}:$(input_hash)"
    if ! registry_login; then
        echo "::warning::Could not log in to ${REPOSITORY%%/*}; the lookup below may fail"
    fi
    if find_marker "${ref}"; then
        echo "These kernel inputs already built and smoke-booted (${ref}); skipping the rebuild."
        return 0
    fi

    ensure_builder
    options="$(build_options)"
    if [ "${write}" = true ]; then
        save_toolchain_caches "${options}"
    fi

    # The default profile only: the hash does not cover the overrides.
    OUTPUT_ROOT="$(mktemp -d)"
    env -u ARCH -u KERNEL_VERSION -u FIRECRACKER_CI_VERSION -u KERNEL_SOURCE_COMMIT \
        -u BASE_CONFIG_SHA256 -u CONFIG_FRAGMENT \
        OUTPUT_DIR="${OUTPUT_ROOT}" KERNEL_DOCKER_BUILD_OPTIONS="${options}" \
        "${SCRIPT_DIR}/build.sh"
    artifact_dir="$(echo "${OUTPUT_ROOT}"/vmlinux-*)"
    kernel_version="${artifact_dir##*/vmlinux-}"
    test -f "${artifact_dir}/vmlinux.bin"
    test -f "${artifact_dir}/config"
    (cd "${artifact_dir}" && sha256sum --check --strict vmlinux.bin.sha256)
    # The container builder keeps its images; the boot runs from Docker's.
    KERNEL_DOCKER_BUILD_OPTIONS="${options}"$'\n'--load \
        "${SCRIPT_DIR}/smoke-boot.sh" "${artifact_dir}/vmlinux.bin" "${kernel_version}"

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
