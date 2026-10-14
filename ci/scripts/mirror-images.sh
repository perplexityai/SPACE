#!/usr/bin/env bash
# Mirror the third-party images GitHub CI uses into GHCR, and take them from
# there.
#
# ci/mirrored-images.txt lists each image: its name as the Dockerfile or tool
# writes it, the digest it is pinned to, its GHCR destination, and optionally
# another repository that serves the same digest. Main pushes copy every digest
# the mirror lacks, from the name's repository or else that other one. GitHub
# CI then uses the mirror in two ways, and never fails because of it:
#
# - `pull` pulls an image from the mirror by digest and tags it with its name,
#   so the tools that use that name find it locally. If the mirror lacks the
#   digest or cannot be reached, it pulls the same digest from the sources; if
#   that fails too, it leaves the image to the tools' own pulls, as before.
# - `build-contexts` prints a `--build-context <name>=docker-image://<mirror>`
#   option for each listed image the given Dockerfiles name, which makes a
#   build take that FROM image from the mirror without changing the
#   Dockerfile. An image the mirror lacks gets no option, so its build pulls
#   the source as before. It prints nothing unless SPACE_CI_IMAGE_MIRROR is
#   set, so builders outside GitHub CI are unchanged.
# - `pinned` prints the name `pull` tags, pinned to the digest it pulls. On the
#   containerd image store a tag with a digest resolves to a local image of
#   that name and digest, so a tool that is given this reference and pulls only
#   what is missing, as `kind create` does, uses the copy `pull` took. The
#   listed name's own digest can pin a multi-platform index, which that copy
#   does not answer to.
#
# Usage: ci/scripts/mirror-images.sh publish
#        ci/scripts/mirror-images.sh pull <name>...
#        ci/scripts/mirror-images.sh build-contexts <dockerfile>...
#        ci/scripts/mirror-images.sh listed <name>
#        ci/scripts/mirror-images.sh pinned <name>
#
# GHCR_TOKEN, when set, logs in to ghcr.io first; the package is private while
# the repository is. SPACE_MIRRORED_IMAGES overrides the list. The login and
# each pull end after SPACE_DOCKER_PULL_TIMEOUT_SECONDS (default 300), so a
# stalled mirror falls back to the source instead of holding the job.
#
# The mirror holds each listed digest for linux/amd64, the platform GitHub CI
# runs. Before copying a digest, and on every run for a name pinned by digest,
# publish checks that the name serves that digest for linux/amd64, so that a
# name bumped without its digest does not leave CI on the old image.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LIST="${SPACE_MIRRORED_IMAGES:-${REPO_ROOT}/ci/mirrored-images.txt}"
COPY_ATTEMPTS="${SPACE_MIRROR_COPY_ATTEMPTS:-5}"
COPY_RETRY_DELAY_SECONDS="${SPACE_MIRROR_COPY_RETRY_DELAY_SECONDS:-10}"
PULL_TIMEOUT_SECONDS="${SPACE_DOCKER_PULL_TIMEOUT_SECONDS:-300}"
# shellcheck source=lib/test-deps.sh
source "${SCRIPT_DIR}/lib/test-deps.sh"

# Print "<name> <mirror> <destination> <source>..." for each listed image: its
# name, the GHCR repository at the pinned digest, the tagged destination, and
# each repository that serves the digest, at the digest. Jobs use the mirror by
# digest; the tag only names the image in the package. Fail on a line whose
# name has no tag, whose digest is not a SHA-256 digest, whose destination is
# not tagged, or whose other repository carries a tag or digest.
entries() {
    local name digest destination other extra repository
    while read -r name digest destination other extra; do
        case "${name}" in "" | "#"*) continue ;; esac
        repository="${name%%@*}"
        if [[ "${repository##*/}" != *:* || ! "${digest}" =~ ^sha256:[0-9a-f]{64}$ ||
            "${destination##*/}" != *:* || "${destination}" == *@* ||
            "${other}" == *@* || "${other##*/}" == *:* || -n "${extra}" ]]; then
            echo "Malformed line in ${LIST}: ${name} ${digest} ${destination} ${other} ${extra}" >&2
            return 1
        fi
        printf '%s %s %s %s' "${name}" "${destination%:*}@${digest}" "${destination}" \
            "${repository%:*}@${digest}"
        [ -z "${other}" ] || printf ' %s' "${other}@${digest}"
        printf '\n'
    done <"${LIST}"
}

registry_login() {
    [ -n "${GHCR_TOKEN:-}" ] || return 0
    printf '%s' "${GHCR_TOKEN}" |
        space_with_deadline 60 docker login ghcr.io --username "${GITHUB_ACTOR:-space-ci}" \
            --password-stdin >/dev/null
}

mirrored() {
    docker buildx imagetools inspect --raw "$1" >/dev/null 2>&1
}

pull() {
    local listed image entry name mirror sources source error status started
    listed="$(entries)"
    if ! registry_login; then
        echo "::warning::Could not log in to ghcr.io; the mirror pulls below may fail"
    fi
    for image in "$@"; do
        entry="$(awk -v image="${image}" '$1 == image { print; exit }' <<<"${listed}")"
        if [ -z "${entry}" ]; then
            echo "::warning::${image} is not listed in ${LIST#"${REPO_ROOT}/"}; the job pulls it itself"
            continue
        fi
        read -r name mirror _ sources <<<"${entry}"
        # A tag cannot carry a digest; a name pinned by digest keeps its tag.
        name="${name%%@*}"
        started="${SECONDS}"
        status=0
        error="$(space_with_deadline "${PULL_TIMEOUT_SECONDS}" docker pull --quiet "${mirror}" 2>&1 >/dev/null)" ||
            status=$?
        if [ "${status}" -eq 0 ]; then
            docker tag "${mirror}" "${name}"
            echo "Pulled ${name} from ${mirror} in $((SECONDS - started)) s"
            continue
        fi
        [ "${status}" -ne 124 ] || error="no answer within ${PULL_TIMEOUT_SECONDS} s"
        echo "::warning::Pulling ${name} from its source; ${mirror} is unavailable: ${error%%$'\n'*}"
        for source in ${sources}; do
            started="${SECONDS}"
            if space_pull_docker_image "${source}"; then
                docker tag "${source}" "${name}"
                echo "Pulled ${name} from ${source} in $((SECONDS - started)) s"
                continue 2
            fi
        done
        echo "::warning::Could not pull ${sources// / or }; the job pulls ${name} itself"
    done
}

build_contexts() {
    local listed tokens name mirror
    [ -n "${SPACE_CI_IMAGE_MIRROR:-}" ] || return 0
    listed="$(entries)"
    if ! registry_login >&2; then
        echo "::warning::Could not log in to ghcr.io; builds may pull their bases from the source" >&2
    fi
    # Whitespace- and =-separated words, so that FROM lines and ARG defaults
    # both name their images exactly.
    tokens="$(awk -F '[[:space:]=]+' '{ for (i = 1; i <= NF; i++) print $i }' "$@")"
    while read -r name mirror _; do
        [ -n "${name}" ] || continue
        grep -qxF -- "${name}" <<<"${tokens}" || continue
        if mirrored "${mirror}"; then
            printf -- '--build-context=%s=docker-image://%s\n' "${name}" "${mirror}"
        else
            echo "::warning::Building from ${name} at its source; ${mirror} is unavailable" >&2
        fi
    done <<<"${listed}"
}

# Copy the pinned digest to the destination from the first source that gives
# it. Each attempt tries every source, so a source that has hit its rate limit
# does not hold up the others.
copy_with_retries() {
    local destination="$1" attempt source delay="${COPY_RETRY_DELAY_SECONDS}"
    shift
    for ((attempt = 1; ; attempt++)); do
        for source in "$@"; do
            # With one source and --prefer-index=false, imagetools pushes the
            # pinned manifest byte for byte, so the mirror keeps its digest.
            if docker buildx imagetools create --prefer-index=false --tag "${destination}" "${source}"; then
                echo "Copied ${source}"
                return 0
            fi
        done
        if [ "${attempt}" -ge "${COPY_ATTEMPTS}" ]; then
            echo "::error::Could not copy $* to ${destination} after ${attempt} attempts"
            return 1
        fi
        echo "Copy to ${destination} failed (attempt ${attempt}/${COPY_ATTEMPTS}); retrying in ${delay}s." >&2
        sleep "${delay}"
        delay=$((delay * 2))
    done
}

# The digest a name serves for linux/amd64: its entry for that platform when
# the name is an index, else the name's own manifest.
platform_digest() {
    docker buildx imagetools inspect --format '{{json .Manifest}}' "$1" |
        jq -r 'if .manifests then
                ([.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64")
                  | .digest][0] // empty)
            else .digest end'
}

# Fail when a name pinned by digest does not serve the listed digest for
# linux/amd64: the line was bumped without it. A tag can legitimately move
# after its line was written, and an unreadable name proves nothing, so those
# only warn.
check_listed_digest() {
    local name="$1" digest="$2" served
    if ! served="$(platform_digest "${name}" 2>/dev/null)" || [ -z "${served}" ]; then
        echo "::warning::Could not check that ${name} serves ${digest} for linux/amd64"
        return 0
    fi
    [ "${served}" != "${digest}" ] || return 0
    if [[ "${name}" == *@* ]]; then
        echo "::error::${name} serves ${served} for linux/amd64, not the listed ${digest}"
        return 1
    fi
    echo "::warning::${name} now serves ${served} for linux/amd64, not the listed ${digest}; its tag has moved"
}

publish() {
    local listed name mirror destination sources failures=0
    listed="$(entries)"
    registry_login
    while read -r name mirror destination sources; do
        [ -n "${name}" ] || continue
        if [[ "${name}" == *@* ]] && ! check_listed_digest "${name}" "${mirror#*@}"; then
            failures=$((failures + 1))
            continue
        fi
        if mirrored "${mirror}"; then
            echo "${name} is already mirrored as ${mirror}"
            continue
        fi
        if [[ "${name}" != *@* ]]; then
            check_listed_digest "${name}" "${mirror#*@}"
        fi
        # shellcheck disable=SC2086 # sources are digest references without spaces
        if ! copy_with_retries "${destination}" ${sources}; then
            failures=$((failures + 1))
            continue
        fi
        if ! mirrored "${mirror}"; then
            echo "::error::Copied ${name}, but ${mirror} does not exist"
            failures=$((failures + 1))
            continue
        fi
        echo "Mirrored ${name} as ${mirror}"
    done <<<"${listed}"
    [ "${failures}" -eq 0 ]
}

case "${1:-}" in
    publish)
        [ $# -eq 1 ] || { echo "usage: $0 publish" >&2; exit 2; }
        publish
        ;;
    pull)
        [ $# -ge 2 ] || { echo "usage: $0 pull <name>..." >&2; exit 2; }
        shift
        pull "$@"
        ;;
    build-contexts)
        [ $# -ge 2 ] || { echo "usage: $0 build-contexts <dockerfile>..." >&2; exit 2; }
        shift
        build_contexts "$@"
        ;;
    listed)
        [ $# -eq 2 ] || { echo "usage: $0 listed <name>" >&2; exit 2; }
        entries | awk -v name="$2" '$1 == name { found = 1 } END { exit !found }'
        ;;
    pinned)
        [ $# -eq 2 ] || { echo "usage: $0 pinned <name>" >&2; exit 2; }
        entries | awk -v name="$2" '$1 == name && !found {
            found = 1
            sub(/@.*/, "", $1)
            sub(/^[^@]*@/, "", $2)
            print $1 "@" $2
        } END { exit !found }'
        ;;
    *)
        echo "usage: $0 publish|pull <name>...|build-contexts <dockerfile>...|listed <name>|pinned <name>" >&2
        exit 2
        ;;
esac
