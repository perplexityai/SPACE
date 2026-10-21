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
# - `pull` pulls an image from the mirror and tags it with its name, so the
#   tools that use that name find it locally. If the mirror lacks it or cannot
#   be reached, it pulls the same digest from the sources, or for a name
#   pinned by digest the index it pins; if that fails too, it leaves the image
#   to the tools' own pulls, as before.
# - `build-contexts` prints a `--build-context <name>=docker-image://<mirror>`
#   option for each listed image the given Dockerfiles name, which makes a
#   build take that FROM image from the mirror without changing the
#   Dockerfile. An image the mirror lacks gets no option, so its build pulls
#   the source as before. It prints nothing unless SPACE_CI_IMAGE_MIRROR is
#   set, so builders outside GitHub CI are unchanged.
# - `pinned` prints the name `pull` tags, pinned to the listed digest. On the
#   containerd image store a tag with a digest resolves to a local image of
#   that name and digest, so a tool that is given this reference and pulls only
#   what is missing, as `kind create` does, uses the copy `pull` took from the
#   mirror. The listed name's own digest can pin a multi-platform index, which
#   that copy does not answer to.
# - `verified` succeeds when jobs take the name from the mirror: for a name
#   pinned by digest, when its pairing's record is there and names the listed
#   digest. Only then does `pull` leave the listed digest, so only then should
#   a tool be given `pinned`'s reference; otherwise give it the name itself.
#
# Usage: ci/scripts/mirror-images.sh publish
#        ci/scripts/mirror-images.sh pull <name>...
#        ci/scripts/mirror-images.sh build-contexts <dockerfile>...
#        ci/scripts/mirror-images.sh listed <name>
#        ci/scripts/mirror-images.sh pinned <name>
#        ci/scripts/mirror-images.sh verified <name>
#
# GHCR_TOKEN, when set, logs in to ghcr.io first; the package is private while
# the repository is. SPACE_MIRRORED_IMAGES overrides the list. Logins and
# manifest reads end after SPACE_REGISTRY_READ_TIMEOUT_SECONDS (default 60), and
# each pull or copy after SPACE_DOCKER_PULL_TIMEOUT_SECONDS (default 300), so a
# stalled registry falls back to the source instead of holding the job.
#
# The mirror holds each listed digest for linux/amd64, the platform GitHub CI
# runs. A name pinned by digest pins a multi-platform index, and its line
# lists the index's linux/amd64 manifest; nothing but the index itself says
# the two belong together. So publish reads the index once, when it first sees
# that pairing, and records the pairing it verified as a `pin-<hash>` tag on
# the mirrored manifest. Jobs take such an image from the mirror only through
# that tag, and only while it names the listed digest: a name bumped without
# its digest has no record, so it is neither copied nor substituted, and jobs
# and builds keep its pinned index from the source. An index publish cannot
# read is not recorded either, and is tried again on the next run; a record
# that names another digest is a miss, which publish repairs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LIST="${SPACE_MIRRORED_IMAGES:-${REPO_ROOT}/ci/mirrored-images.txt}"
COPY_ATTEMPTS="${SPACE_MIRROR_COPY_ATTEMPTS:-5}"
COPY_RETRY_DELAY_SECONDS="${SPACE_MIRROR_COPY_RETRY_DELAY_SECONDS:-10}"
PULL_TIMEOUT_SECONDS="${SPACE_DOCKER_PULL_TIMEOUT_SECONDS:-300}"
READ_TIMEOUT_SECONDS="${SPACE_REGISTRY_READ_TIMEOUT_SECONDS:-60}"
# shellcheck source=lib/test-deps.sh
source "${SCRIPT_DIR}/lib/test-deps.sh"

# Print "<name> <mirror> <destination> <use> <source>..." for each listed
# image: its name, the GHCR repository at the pinned digest, the tagged
# destination, the reference jobs take from the mirror, and each repository
# that serves the digest, at the digest. Jobs use the mirror by digest, or for
# a name pinned by digest through the tag that records its verified pairing;
# the destination tag only names the image in the package. Fail on a line
# whose name has no tag, whose digest is not a SHA-256 digest, whose
# destination is not tagged, or whose other repository carries a tag or digest.
entries() {
    local name digest destination other extra repository use
    while read -r name digest destination other extra; do
        case "${name}" in "" | "#"*) continue ;; esac
        repository="${name%%@*}"
        if [[ "${repository##*/}" != *:* || ! "${digest}" =~ ^sha256:[0-9a-f]{64}$ ||
            "${destination##*/}" != *:* || "${destination}" == *@* ||
            "${other}" == *@* || "${other##*/}" == *:* || -n "${extra}" ]]; then
            echo "Malformed line in ${LIST}: ${name} ${digest} ${destination} ${other} ${extra}" >&2
            return 1
        fi
        use="${destination%:*}@${digest}"
        [[ "${name}" != *@* ]] || use="${destination%:*}:$(pin_tag "${name}" "${digest}")"
        printf '%s %s %s %s %s' "${name}" "${destination%:*}@${digest}" "${destination}" "${use}" \
            "${repository%:*}@${digest}"
        [ -z "${other}" ] || printf ' %s' "${other}@${digest}"
        printf '\n'
    done <"${LIST}"
}

# The tag that records a verified pairing of a name pinned by digest with its
# listed linux/amd64 digest: a hash of both, so a new pairing has none.
pin_tag() {
    local hash
    if command -v sha256sum >/dev/null 2>&1; then
        hash="$(printf '%s %s' "$1" "$2" | sha256sum)"
    else
        hash="$(printf '%s %s' "$1" "$2" | shasum -a 256)"
    fi
    printf 'pin-%s\n' "${hash:0:40}"
}

registry_login() {
    [ -n "${GHCR_TOKEN:-}" ] || return 0
    printf '%s' "${GHCR_TOKEN}" |
        space_with_deadline "${READ_TIMEOUT_SECONDS}" docker login ghcr.io --username "${GITHUB_ACTOR:-space-ci}" \
            --password-stdin >/dev/null
}

mirrored() {
    space_with_deadline "${READ_TIMEOUT_SECONDS}" docker buildx imagetools inspect --raw "$1" >/dev/null 2>&1
}

# Records are read with jq. Without it, say once per run that images pinned by
# digest come from their sources, rather than skip the mirror silently.
JQ_WARNED=0
have_jq() {
    command -v jq >/dev/null 2>&1 && return 0
    if [ "${JQ_WARNED}" -eq 0 ]; then
        echo "::warning::jq is not installed, so the mirror's records cannot be read; images pinned by digest come from their sources" >&2
        JQ_WARNED=1
    fi
    return 1
}

# Whether a pairing's record exists and names the given digest.
record_names() {
    local named
    have_jq || return 1
    named="$(space_with_deadline "${READ_TIMEOUT_SECONDS}" \
        docker buildx imagetools inspect --format '{{json .Manifest}}' "$1" 2>/dev/null | jq -r '.digest // empty')" ||
        return 1
    [ "${named}" = "$2" ]
}

# Whether jobs take a listed image from the mirror: <name> <mirror> <use>.
on_mirror() {
    if [[ "$1" == *@* ]]; then
        record_names "$3" "${2#*@}"
    else
        mirrored "$2"
    fi
}

pull() {
    local listed image entry name mirror use sources source error status started
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
        read -r name mirror _ use sources <<<"${entry}"
        # Without its verified record, a name pinned by digest falls back to
        # the index it pins, never to the listed digest the index may not hold.
        if [[ "${name}" == *@* ]]; then
            sources="$(for source in ${sources}; do printf '%s@%s ' "${source%@*}" "${name##*@}"; done)"
        fi
        # A tag cannot carry a digest; a name pinned by digest keeps its tag.
        name="${name%%@*}"
        started="${SECONDS}"
        status=0
        error="$(space_with_deadline "${PULL_TIMEOUT_SECONDS}" docker pull --quiet "${use}" 2>&1 >/dev/null)" ||
            status=$?
        # Through a pairing's tag, take only the listed digest.
        if [ "${status}" -eq 0 ] && ! docker image inspect "${mirror}" >/dev/null 2>&1; then
            status=1
            error="${use} is not ${mirror#*@}"
        fi
        if [ "${status}" -eq 0 ]; then
            docker tag "${mirror}" "${name}"
            echo "Pulled ${name} from ${mirror} in $((SECONDS - started)) s"
            continue
        fi
        ! space_deadline_expired "${status}" || error="no answer within ${PULL_TIMEOUT_SECONDS} s"
        echo "::warning::Pulling ${name} from its source; ${use} is unavailable: ${error%%$'\n'*}"
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
    local listed tokens name mirror use
    [ -n "${SPACE_CI_IMAGE_MIRROR:-}" ] || return 0
    listed="$(entries)"
    if ! registry_login >&2; then
        echo "::warning::Could not log in to ghcr.io; builds may pull their bases from the source" >&2
    fi
    # Whitespace- and =-separated words, so that FROM lines and ARG defaults
    # both name their images exactly.
    tokens="$(awk -F '[[:space:]=]+' '{ for (i = 1; i <= NF; i++) print $i }' "$@")"
    while read -r name mirror _ use _; do
        [ -n "${name}" ] || continue
        grep -qxF -- "${name}" <<<"${tokens}" || continue
        if on_mirror "${name}" "${mirror}" "${use}"; then
            printf -- '--build-context=%s=docker-image://%s\n' "${name}" "${mirror}"
        else
            echo "::warning::Building from ${name} at its source; ${use} is unavailable" >&2
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
            if space_with_deadline "${PULL_TIMEOUT_SECONDS}" \
                docker buildx imagetools create --prefer-index=false --tag "${destination}" "${source}"; then
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
    space_with_deadline "${READ_TIMEOUT_SECONDS}" docker buildx imagetools inspect --format '{{json .Manifest}}' "$1" |
        jq -r 'if .manifests then
                ([.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64")
                  | .digest][0] // empty)
            else .digest end'
}

# Whether a name serves the listed digest for linux/amd64: 0 when it does, 1
# when it serves another, 2 when that cannot be read. Says which, and why.
check_listed_digest() {
    local name="$1" digest="$2" served error_file error status=0
    error_file="$(mktemp)"
    served="$(platform_digest "${name}" 2>"${error_file}")" || status=$?
    error="$(head -n 1 "${error_file}")"
    rm -f "${error_file}"
    if [ "${status}" -ne 0 ] || [ -z "${served}" ]; then
        ! space_deadline_expired "${status}" || error="no answer within ${READ_TIMEOUT_SECONDS} s"
        echo "::warning::Could not check that ${name} serves ${digest} for linux/amd64: ${error:-it lists no linux/amd64 manifest}"
        return 2
    fi
    if [ "${served}" = "${digest}" ]; then
        echo "Verified that ${name} serves ${digest} for linux/amd64"
        return 0
    fi
    if [[ "${name}" == *@* ]]; then
        echo "::error::${name} serves ${served} for linux/amd64, not the listed ${digest}"
    else
        echo "::warning::${name} now serves ${served} for linux/amd64, not the listed ${digest}; its tag has moved"
    fi
    return 1
}

publish() {
    local listed name mirror destination use sources digest failures=0
    if ! command -v jq >/dev/null 2>&1; then
        echo "::error::publish needs jq to verify and record the pairings of images pinned by digest"
        return 1
    fi
    listed="$(entries)"
    registry_login
    while read -r name mirror destination use sources; do
        [ -n "${name}" ] || continue
        digest="${mirror#*@}"
        if [[ "${name}" == *@* ]]; then
            # Its pairing is read from the index once, then from its record.
            if record_names "${use}" "${digest}"; then
                echo "${name} is already mirrored as ${mirror}, verified as ${use##*:}"
                continue
            fi
            if mirrored "${use}"; then
                echo "::warning::${use} does not name ${digest}; verifying ${name} again to repair it"
            fi
            check_listed_digest "${name}" "${digest}" || case $? in
                1)
                    failures=$((failures + 1))
                    continue
                    ;;
                *)
                    echo "::warning::Not mirroring ${name} until its pairing with ${digest} is verified; jobs take it from its source"
                    continue
                    ;;
            esac
        else
            if mirrored "${mirror}"; then
                echo "${name} is already mirrored as ${mirror}"
                continue
            fi
            # A tag can move after its line was written; the digest is the pin.
            check_listed_digest "${name}" "${digest}" || true
        fi
        if ! mirrored "${mirror}"; then
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
        fi
        if [[ "${name}" == *@* ]]; then
            if ! space_with_deadline "${READ_TIMEOUT_SECONDS}" \
                docker buildx imagetools create --prefer-index=false --tag "${use}" "${mirror}" ||
                ! record_names "${use}" "${digest}"; then
                echo "::error::Could not record ${name}'s verified pairing as ${use}"
                failures=$((failures + 1))
                continue
            fi
            echo "Mirrored ${name} as ${mirror}, verified as ${use##*:}"
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
    verified)
        [ $# -eq 2 ] || { echo "usage: $0 verified <name>" >&2; exit 2; }
        # Listed in full before awk stops at the match, as in pull. Piped, an
        # awk that exits at the match while entries is still writing ends
        # entries by SIGPIPE, or fails its writes where SIGPIPE is ignored,
        # and pipefail turns either into a failed verify.
        listed="$(entries)"
        entry="$(awk -v name="$2" '$1 == name { print; exit }' <<<"${listed}")"
        [ -n "${entry}" ] || exit 1
        read -r name mirror _ use _ <<<"${entry}"
        registry_login || true
        on_mirror "${name}" "${mirror}" "${use}"
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
        echo "usage: $0 publish|pull <name>...|build-contexts <dockerfile>...|listed <name>|pinned <name>|verified <name>" >&2
        exit 2
        ;;
esac
