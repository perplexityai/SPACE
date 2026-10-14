#!/usr/bin/env bash
# Regression test for ci/scripts/mirror-images.sh against a fake docker: jobs
# take the mirror by digest, fall back to the source's digest, and never fail
# for a missing image; builds get a build context only for a mirrored image
# their Dockerfile names; pinned names the copy a pull leaves by the digest it
# pulled; publish copies only missing digests, from another
# repository when the first fails, and fails when the copy does not keep the
# source's digest.
set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/mirror-images.sh"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

DIGEST="sha256:$(printf 'a%.0s' {1..64})"
NAME="registry.example/library/postgres:16-alpine"
ORIGIN="registry.example/library/postgres@${DIGEST}"
MIRROR="ghcr.io/example/mirror@${DIGEST}"
OTHER="other.example/postgres@${DIGEST}"
DESTINATION="ghcr.io/example/mirror:postgres-16-alpine"
# A name pinned by its index digest, as a FROM line can be.
PINNED_DIGEST="sha256:$(printf 'c%.0s' {1..64})"
PINNED_NAME="registry.example/library/rust:1-bookworm@sha256:$(printf 'd%.0s' {1..64})"
PINNED_MIRROR="ghcr.io/example/mirror@${PINNED_DIGEST}"

mkdir -p "${WORK_DIR}/bin"
cat >"${WORK_DIR}/bin/docker" <<'SH'
#!/usr/bin/env bash
# Pullable references are listed in FAKE_PULLABLE; the registry's manifests,
# which imagetools inspects and creates, are the lines of FAKE_REGISTRY.
printf '%s\n' "$*" >>"${FAKE_CALLS}"
case "$1 ${2:-} ${3:-}" in
    "login "*) cat >/dev/null ;;
    "image inspect "*) exit 1 ;;
    "pull "*)
        ref="${*: -1}"
        [[ " ${FAKE_PULLABLE:-} " == *" ${ref} "* ]] || { echo "manifest unknown: ${ref}" >&2; exit 1; }
        ;;
    "tag "*) [[ "${*: -1}" != *@* ]] || { echo "refusing to create a tag with a digest reference" >&2; exit 1; } ;;
    "buildx imagetools inspect") grep -qxF "${*: -1}" "${FAKE_REGISTRY}" ;;
    "buildx imagetools create")
        [ -z "${FAKE_CREATE_FAILS:-}" ] || exit 1
        [[ " ${FAKE_UNREACHABLE:-} " != *" ${*: -1} "* ]] || exit 1
        destination="${*: -2:1}"
        digest="${FAKE_CREATE_DIGEST:-${*: -1}}"
        printf '%s@%s\n' "${destination%:*}" "${digest#*@}" >>"${FAKE_REGISTRY}"
        ;;
    *) echo "unexpected docker call: $*" >&2; exit 99 ;;
esac
SH
chmod +x "${WORK_DIR}/bin/docker"
printf '# comment\n\n%s %s %s %s\n%s %s %s\n' \
    "${NAME}" "${DIGEST}" "${DESTINATION}" "${OTHER%@*}" \
    "${PINNED_NAME}" "${PINNED_DIGEST}" ghcr.io/example/mirror:rust-1-bookworm >"${WORK_DIR}/list"
# shellcheck disable=SC2016 # the Dockerfile expands ${BASE}, not this shell
printf 'FROM %s AS build\nARG BASE=%s\nFROM ${BASE}\n' "${PINNED_NAME}" "${NAME}" >"${WORK_DIR}/Dockerfile"
printf 'FROM %s-slim\n' "${NAME}" >"${WORK_DIR}/Dockerfile.other"

FAILURES=0
fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

# run <pullable refs> <registry manifests> <args>...: runs the script, leaving
# its stdout in ${WORK_DIR}/out, its stderr in ${WORK_DIR}/err, and its docker
# calls in ${WORK_DIR}/calls.
run() {
    local pullable="$1" registry="$2"
    shift 2
    : >"${WORK_DIR}/calls"
    printf '%s\n' "${registry}" >"${WORK_DIR}/registry"
    PATH="${WORK_DIR}/bin:${PATH}" \
        FAKE_CALLS="${WORK_DIR}/calls" FAKE_REGISTRY="${WORK_DIR}/registry" FAKE_PULLABLE="${pullable}" \
        SPACE_MIRRORED_IMAGES="${SPACE_MIRRORED_IMAGES:-${WORK_DIR}/list}" \
        SPACE_CI_IMAGE_MIRROR="${SPACE_CI_IMAGE_MIRROR-ghcr}" \
        SPACE_DOCKER_PULL_ATTEMPTS=1 SPACE_MIRROR_COPY_ATTEMPTS=2 SPACE_MIRROR_COPY_RETRY_DELAY_SECONDS=0 \
        GHCR_TOKEN=token bash "${SCRIPT}" "$@" >"${WORK_DIR}/out" 2>"${WORK_DIR}/err"
}
called() { grep -qxF "$1" "${WORK_DIR}/calls"; }

run "${MIRROR}" "" pull "${NAME}" || fail "pull from the mirror exited nonzero"
called "tag ${MIRROR} ${NAME}" || fail "the mirrored digest was not tagged as ${NAME}"
called "pull ${ORIGIN}" && fail "a mirror hit still pulled the source"

run "${ORIGIN}" "" pull "${NAME}" || fail "the source fallback exited nonzero"
called "tag ${ORIGIN} ${NAME}" || fail "the fallback did not tag the source's pinned digest"
grep -q "Could not pull" "${WORK_DIR}/out" && fail "a successful fallback still reported a failed pull"

run "${OTHER}" "" pull "${NAME}" || fail "the fallback to another repository exited nonzero"
called "tag ${OTHER} ${NAME}" || fail "the fallback did not try the other repository's digest"

run "" "" pull "${NAME}" || fail "a pull with every registry down failed the job"
grep -q "^tag " "${WORK_DIR}/calls" && fail "an image was tagged although no pull succeeded"

run "" "" pull registry.example/unlisted:1 || fail "an unlisted image failed the job"
grep -q "^pull " "${WORK_DIR}/calls" && fail "an unlisted image was pulled"

run "${PINNED_MIRROR}" "" pull "${PINNED_NAME}" || fail "pull of a digest-pinned name exited nonzero"
called "tag ${PINNED_MIRROR} ${PINNED_NAME%%@*}" || fail "a digest-pinned name was not tagged by its tag"

run "" "${MIRROR}"$'\n'"${PINNED_MIRROR}" build-contexts "${WORK_DIR}/Dockerfile" ||
    fail "build-contexts exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}
--build-context=${PINNED_NAME}=docker-image://${PINNED_MIRROR}" ] ||
    fail "build-contexts did not override both of the Dockerfile's bases: $(cat "${WORK_DIR}/out")"

run "" "${MIRROR}" build-contexts "${WORK_DIR}/Dockerfile" || fail "build-contexts with a missing mirror exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}" ] ||
    fail "build-contexts overrode a base the mirror lacks: $(cat "${WORK_DIR}/out")"

run "" "${MIRROR}" build-contexts "${WORK_DIR}/Dockerfile.other" || fail "build-contexts exited nonzero"
[ -s "${WORK_DIR}/out" ] && fail "build-contexts matched a name that only prefixes a FROM image"

SPACE_CI_IMAGE_MIRROR="" run "" "${MIRROR}" build-contexts "${WORK_DIR}/Dockerfile" ||
    fail "build-contexts without SPACE_CI_IMAGE_MIRROR exited nonzero"
[ -s "${WORK_DIR}/out" ] && fail "build-contexts overrode a base outside GitHub CI"
[ -s "${WORK_DIR}/calls" ] && fail "build-contexts called docker outside GitHub CI"

run "" "" listed "${NAME}" || fail "listed rejected a listed name"
run "" "" listed "${NAME%:*}:17" && fail "listed accepted an unlisted name"

run "" "" pinned "${NAME}" || fail "pinned rejected a listed name"
[ "$(cat "${WORK_DIR}/out")" = "${NAME}@${DIGEST}" ] || fail "pinned did not pin the tag: $(cat "${WORK_DIR}/out")"
run "" "" pinned "${PINNED_NAME}" || fail "pinned rejected a digest-pinned name"
[ "$(cat "${WORK_DIR}/out")" = "${PINNED_NAME%%@*}@${PINNED_DIGEST}" ] ||
    fail "pinned did not replace the name's digest with the pulled one: $(cat "${WORK_DIR}/out")"
run "" "" pinned "${NAME%:*}:17" && fail "pinned accepted an unlisted name"
[ -s "${WORK_DIR}/out" ] && fail "pinned printed a reference for an unlisted name"

run "" "${MIRROR}"$'\n'"${PINNED_MIRROR}" publish || fail "publish of mirrored digests exited nonzero"
grep -q "imagetools create" "${WORK_DIR}/calls" && fail "publish copied a digest already mirrored"

run "" "${PINNED_MIRROR}" publish || fail "publish of a missing digest exited nonzero"
called "buildx imagetools create --prefer-index=false --tag ${DESTINATION} ${ORIGIN}" ||
    fail "publish did not copy the pinned digest to the destination"

FAKE_UNREACHABLE="${ORIGIN}" run "" "${PINNED_MIRROR}" publish || fail "publish failed although another repository had the digest"
called "buildx imagetools create --prefer-index=false --tag ${DESTINATION} ${OTHER}" ||
    fail "publish did not copy from the other repository when the first failed"

FAKE_CREATE_DIGEST="sha256:$(printf 'b%.0s' {1..64})" run "" "${PINNED_MIRROR}" publish &&
    fail "publish accepted a copy that changed the digest"
FAKE_CREATE_FAILS=1 run "" "${PINNED_MIRROR}" publish && fail "publish succeeded although every copy failed"
[ "$(grep -c "imagetools create" "${WORK_DIR}/calls")" -eq 4 ] || fail "publish did not retry both repositories"

printf '%s %s %s\n' "${NAME}" "${DIGEST#sha256:}" "${DESTINATION}" >"${WORK_DIR}/unpinned"
SPACE_MIRRORED_IMAGES="${WORK_DIR}/unpinned" run "" "" pull "${NAME}" &&
    fail "a line without a SHA-256 digest was accepted"

if [ "${FAILURES}" -ne 0 ]; then
    exit 1
fi
echo "mirror-images tests passed"
