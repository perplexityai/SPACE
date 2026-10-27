#!/usr/bin/env bash
# Provide the CI image to a job without rebuilding it on every run.
#
# The image is content-addressed. Its tag is the first 16 characters of the
# SHA-256 over the Dockerfile, toolchain and shared build scripts. Main pushes
# publish each tag to GHCR once. Every other job pulls
# its tag and retags it locally. A tag that does not exist yet (a change to
# an input file) or cannot be pulled is built locally, as every job did before,
# so a registry problem slows a check down but never fails it.
#
# Usage: ci/scripts/ci-image.sh ref
#        ci/scripts/ci-image.sh ensure <local-tag>
#        ci/scripts/ci-image.sh publish
#
# GHCR_TOKEN, when set, logs in to the registry first; the package is private
# while the repository is. SPACE_CI_IMAGE_REPOSITORY overrides the repository.
# Both builds take the Dockerfile's base from the repository's third-party
# image mirror when it has it (ci/mirrored-images.txt); SPACE_CI_IMAGE_MIRROR=
# (empty) builds from the Dockerfile's own FROM image instead.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REPOSITORY="${SPACE_CI_IMAGE_REPOSITORY:-ghcr.io/perplexityai/space-ci}"
SOURCE_URL="https://github.com/perplexityai/SPACE"
INPUTS=(ci/Dockerfile.ci rust-toolchain.toml docker/build-btrfs-progs.sh)
# Part of the tag, so that publishing the same inputs in a new form (layer
# compression) produces a new tag instead of reusing the old one.
PUBLISH_FORMAT="zstd"
BUILDER="space-ci-image"
export SPACE_CI_IMAGE_MIRROR="${SPACE_CI_IMAGE_MIRROR-ghcr}"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@"
    else
        shasum -a 256 "$@"
    fi
}

image_ref() {
    local digest
    digest="$(cd "${REPO_ROOT}" &&
        { sha256 "${INPUTS[@]}"; echo "format: ${PUBLISH_FORMAT}"; } | sha256 | cut -c1-16)"
    printf '%s:%s\n' "${REPOSITORY}" "${digest}"
}

registry_login() {
    [ -n "${GHCR_TOKEN:-}" ] || return 0
    printf '%s' "${GHCR_TOKEN}" |
        docker login "${REPOSITORY%%/*}" --username "${GITHUB_ACTOR:-space-ci}" \
            --password-stdin >/dev/null
}

# The --build-context options that take the Dockerfile's base from the mirror.
mirrored_base() {
    bash "${SCRIPT_DIR}/mirror-images.sh" build-contexts "${REPO_ROOT}/ci/Dockerfile.ci"
}

build_local() {
    local tag="$1"
    shift
    # shellcheck disable=SC2046 # one option per line, none containing spaces
    docker build --file "${REPO_ROOT}/ci/Dockerfile.ci" --tag "${tag}" $(mirrored_base) "$@" "${REPO_ROOT}"
}

ensure() {
    local tag="$1"
    local ref error
    ref="$(image_ref)"
    if ! registry_login; then
        echo "::warning::Could not log in to ${REPOSITORY%%/*}; the pull below may fail"
    fi
    if error="$(docker pull --quiet "${ref}" 2>&1 >/dev/null)"; then
        docker tag "${ref}" "${tag}"
        echo "Using the published CI image ${ref}"
        return 0
    fi
    echo "::warning::Building the CI image locally; ${ref} is unavailable: ${error%%$'\n'*}"
    build_local "${tag}"
}

publish() {
    local ref
    ref="$(image_ref)"
    registry_login
    if docker manifest inspect "${ref}" >/dev/null 2>&1; then
        echo "${ref} is already published"
        return 0
    fi
    # zstd layers unpack much faster than gzip; with gzip the standard runners
    # spent 42-69 s pulling this image. The docker driver cannot choose layer
    # compression, so build on a container builder and push from it.
    if ! docker buildx inspect "${BUILDER}" >/dev/null 2>&1; then
        docker buildx create --name "${BUILDER}" --driver docker-container >/dev/null
    fi
    # shellcheck disable=SC2046 # one option per line, none containing spaces
    docker buildx build --builder "${BUILDER}" \
        --file "${REPO_ROOT}/ci/Dockerfile.ci" $(mirrored_base) \
        --label "org.opencontainers.image.source=${SOURCE_URL}" \
        --output "type=registry,name=${ref},oci-mediatypes=true,compression=${PUBLISH_FORMAT},force-compression=true" \
        "${REPO_ROOT}"
}

case "${1:-}" in
    ref)
        image_ref
        ;;
    ensure)
        [ $# -eq 2 ] || { echo "usage: $0 ensure <local-tag>" >&2; exit 2; }
        ensure "$2"
        ;;
    publish)
        publish
        ;;
    *)
        echo "usage: $0 ref|ensure <local-tag>|publish" >&2
        exit 2
        ;;
esac
