#!/usr/bin/env bash
# Regression test for ci/scripts/mirror-images.sh against a fake docker: jobs
# take the mirror by digest, or a name pinned by digest only through the tag
# that records its verified pairing, and only while it names the listed
# digest; they fall back to the source's digest, or to the index a name pins,
# never fail for a missing image, and give up on a registry that does not
# answer; builds get a build context only for a mirrored image their
# Dockerfile names; pinned names the copy a pull leaves by the digest it
# pulled; publish copies only missing digests, from another repository when
# the first fails, fails when the copy does not keep the source's digest, and
# reads a pinned index only for a pairing it has not recorded, recording it
# when the index serves the listed digest, refusing it when the index serves
# another, leaving it unmirrored when the index cannot be read, and repairing
# a record that names another digest.
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
# A name pinned by its index digest, as a FROM line can be, and the tag that
# records its verified pairing with the listed digest.
PINNED_DIGEST="sha256:$(printf 'c%.0s' {1..64})"
PINNED_NAME="registry.example/library/rust:1-bookworm@sha256:$(printf 'd%.0s' {1..64})"
PINNED_MIRROR="ghcr.io/example/mirror@${PINNED_DIGEST}"
PINNED_ORIGIN="registry.example/library/rust@${PINNED_DIGEST}"
PINNED_INDEX_ORIGIN="registry.example/library/rust@${PINNED_NAME##*@}"
PINNED_DESTINATION="ghcr.io/example/mirror:rust-1-bookworm"
PINNED_RECORD="ghcr.io/example/mirror:pin-$(printf '%s %s' "${PINNED_NAME}" "${PINNED_DIGEST}" | sha256sum | cut -c1-40)"
STALE="sha256:$(printf 'e%.0s' {1..64})"

mkdir -p "${WORK_DIR}/bin"
cat >"${WORK_DIR}/bin/docker" <<'SH'
#!/usr/bin/env bash
# FAKE_PULLABLE lists what pulls give: `ref`, or `tag=digest-ref` for a tag
# that resolves to that digest. What was pulled or tagged is local
# (FAKE_LOCAL). A pull or read of a reference in FAKE_HANG never answers. The
# registry's manifests (`repo@digest`) and tags (`repo:tag=digest`, the last
# line for a tag winning), which imagetools inspects and creates, are the
# lines of FAKE_REGISTRY. FAKE_SERVES lists name=digest pairs: the linux/amd64
# digest each upstream name serves, as an index for a name pinned by digest
# and as a single manifest otherwise.
printf '%s\n' "$*" >>"${FAKE_CALLS}"
ref="${*: -1}"
[[ " ${FAKE_HANG:-} " != *" ${ref} "* ]] || exec sleep 60
case "$1 ${2:-} ${3:-}" in
    "login "*) cat >/dev/null ;;
    "image inspect "*) grep -qxF -- "${ref}" "${FAKE_LOCAL}" ;;
    "pull "*)
        for entry in ${FAKE_PULLABLE:-}; do
            [ "${entry%%=*}" = "${ref}" ] || continue
            printf '%s\n' "${entry%%=*}" "${entry#*=}" >>"${FAKE_LOCAL}"
            exit 0
        done
        echo "manifest unknown: ${ref}" >&2
        exit 1
        ;;
    "tag "*)
        [[ "${ref}" != *@* ]] || { echo "refusing to create a tag with a digest reference" >&2; exit 1; }
        echo "${ref}" >>"${FAKE_LOCAL}"
        ;;
    "buildx imagetools inspect")
        if [ "${4:-}" = --format ]; then
            for pair in ${FAKE_SERVES:-}; do
                [ "${pair%=*}" = "${ref}" ] || continue
                if [[ "${ref}" == *@* ]]; then
                    printf '{"manifests":[{"digest":"sha256:arm","platform":{"os":"linux","architecture":"arm64"}},'
                    printf '{"digest":"%s","platform":{"os":"linux","architecture":"amd64"}}]}\n' "${pair##*=}"
                else
                    printf '{"digest":"%s"}\n' "${pair##*=}"
                fi
                exit 0
            done
            named="$(grep -F -- "${ref}=" "${FAKE_REGISTRY}" | tail -n 1)"
            if [ -n "${named}" ]; then
                printf '{"digest":"%s"}\n' "${named#*=}"
                exit 0
            fi
            if grep -qxF -- "${ref}" "${FAKE_REGISTRY}"; then
                printf '{"digest":"%s"}\n' "${ref#*@}"
                exit 0
            fi
            echo "manifest unknown: ${ref}" >&2
            exit 1
        fi
        grep -qxF -- "${ref}" "${FAKE_REGISTRY}" || grep -qF -- "${ref}=" "${FAKE_REGISTRY}"
        ;;
    "buildx imagetools create")
        [ -z "${FAKE_CREATE_FAILS:-}" ] || exit 1
        [[ " ${FAKE_UNREACHABLE:-} " != *" ${ref} "* ]] || exit 1
        destination="${*: -2:1}"
        digest="${FAKE_CREATE_DIGEST:-${ref}}"
        printf '%s@%s\n%s=%s\n' "${destination%:*}" "${digest#*@}" "${destination}" "${digest#*@}" >>"${FAKE_REGISTRY}"
        ;;
    *) echo "unexpected docker call: $*" >&2; exit 99 ;;
esac
SH
chmod +x "${WORK_DIR}/bin/docker"
printf '# comment\n\n%s %s %s %s\n%s %s %s\n' \
    "${NAME}" "${DIGEST}" "${DESTINATION}" "${OTHER%@*}" \
    "${PINNED_NAME}" "${PINNED_DIGEST}" "${PINNED_DESTINATION}" >"${WORK_DIR}/list"
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
# calls in ${WORK_DIR}/calls. Nothing is local at the start.
run() {
    local pullable="$1" registry="$2"
    shift 2
    : >"${WORK_DIR}/calls"
    : >"${WORK_DIR}/local"
    # shellcheck disable=SC2086 # one manifest or tag per word
    printf '%s\n' ${registry} >"${WORK_DIR}/registry"
    PATH="${WORK_DIR}/bin:${PATH}" \
        FAKE_CALLS="${WORK_DIR}/calls" FAKE_REGISTRY="${WORK_DIR}/registry" FAKE_LOCAL="${WORK_DIR}/local" \
        FAKE_PULLABLE="${pullable}" \
        SPACE_MIRRORED_IMAGES="${SPACE_MIRRORED_IMAGES:-${WORK_DIR}/list}" \
        SPACE_CI_IMAGE_MIRROR="${SPACE_CI_IMAGE_MIRROR-ghcr}" \
        SPACE_DOCKER_PULL_ATTEMPTS=1 SPACE_MIRROR_COPY_ATTEMPTS=2 SPACE_MIRROR_COPY_RETRY_DELAY_SECONDS=0 \
        GHCR_TOKEN=token bash "${SCRIPT}" "$@" >"${WORK_DIR}/out" 2>"${WORK_DIR}/err"
}
called() { grep -qxF -- "$1" "${WORK_DIR}/calls"; }
calls_matching() { grep -c -- "$1" "${WORK_DIR}/calls" || true; }
can_time_out() { command -v timeout >/dev/null 2>&1; }

# ── pull ─────────────────────────────────────────────────────────────────────
run "${MIRROR}" "" pull "${NAME}" || fail "pull from the mirror exited nonzero"
called "tag ${MIRROR} ${NAME}" || fail "the mirrored digest was not tagged as ${NAME}"
called "pull ${ORIGIN}" && fail "a mirror hit still pulled the source"

run "${ORIGIN}" "" pull "${NAME}" || fail "the source fallback exited nonzero"
called "tag ${ORIGIN} ${NAME}" || fail "the fallback did not tag the source's pinned digest"
grep -q "Could not pull" "${WORK_DIR}/out" && fail "a successful fallback still reported a failed pull"

run "${OTHER}" "" pull "${NAME}" || fail "the fallback to another repository exited nonzero"
called "tag ${OTHER} ${NAME}" || fail "the fallback did not try the other repository's digest"

if can_time_out; then
    started="${SECONDS}"
    FAKE_HANG="${MIRROR}" SPACE_DOCKER_PULL_TIMEOUT_SECONDS=1 run "${ORIGIN}" "" pull "${NAME}" ||
        fail "a mirror that never answers failed the job"
    [ $((SECONDS - started)) -lt 15 ] || fail "a mirror that never answers held the pull for $((SECONDS - started)) s"
    called "tag ${ORIGIN} ${NAME}" || fail "a mirror that never answers did not fall back to the source"
    grep -q "no answer within 1 s" "${WORK_DIR}/out" || fail "the fallback did not say the mirror timed out"
fi

run "" "" pull "${NAME}" || fail "a pull with every registry down failed the job"
grep -q "^tag " "${WORK_DIR}/calls" && fail "an image was tagged although no pull succeeded"

run "" "" pull registry.example/unlisted:1 || fail "an unlisted image failed the job"
grep -q "^pull " "${WORK_DIR}/calls" && fail "an unlisted image was pulled"

run "${PINNED_RECORD}=${PINNED_MIRROR}" "" pull "${PINNED_NAME}" || fail "pull of a digest-pinned name exited nonzero"
called "pull --quiet ${PINNED_RECORD}" || fail "a digest-pinned name was not pulled through its pairing's record"
called "tag ${PINNED_MIRROR} ${PINNED_NAME%%@*}" || fail "a digest-pinned name was not tagged by its tag"

# An index bumped without its digest: the listed digest is stale, and an
# unverified pairing never runs it, from the mirror or the source; it falls
# back to the index the name pins.
run "${PINNED_MIRROR} ${PINNED_ORIGIN} ${PINNED_INDEX_ORIGIN}" "" pull "${PINNED_NAME}" ||
    fail "pull of an unverified pairing exited nonzero"
called "tag ${PINNED_MIRROR} ${PINNED_NAME%%@*}" && fail "a digest publish never verified was taken from the mirror"
called "pull ${PINNED_ORIGIN}" && fail "an unverified pairing pulled the listed digest from the source"
called "tag ${PINNED_INDEX_ORIGIN} ${PINNED_NAME%%@*}" || fail "an unverified pairing did not fall back to the pinned index"

run "${PINNED_RECORD}=ghcr.io/example/mirror@${STALE} ${PINNED_ORIGIN} ${PINNED_INDEX_ORIGIN}" "" pull "${PINNED_NAME}" ||
    fail "pull through a record of another digest exited nonzero"
called "tag ${PINNED_INDEX_ORIGIN} ${PINNED_NAME%%@*}" || fail "a record of another digest was not refused"
grep -q "${PINNED_RECORD} is not ${PINNED_DIGEST}" "${WORK_DIR}/out" || fail "the refusal did not say why"

# ── build-contexts ───────────────────────────────────────────────────────────
run "" "${MIRROR} ${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" build-contexts "${WORK_DIR}/Dockerfile" ||
    fail "build-contexts exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}
--build-context=${PINNED_NAME}=docker-image://${PINNED_MIRROR}" ] ||
    fail "build-contexts did not override both of the Dockerfile's bases: $(cat "${WORK_DIR}/out")"

run "" "${MIRROR} ${PINNED_MIRROR}" build-contexts "${WORK_DIR}/Dockerfile" ||
    fail "build-contexts without a recorded pairing exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}" ] ||
    fail "build-contexts substituted a digest whose pairing publish never verified: $(cat "${WORK_DIR}/out")"

run "" "${MIRROR} ${PINNED_RECORD}=${STALE}" build-contexts "${WORK_DIR}/Dockerfile" ||
    fail "build-contexts with a record of another digest exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}" ] ||
    fail "build-contexts trusted a record that names another digest: $(cat "${WORK_DIR}/out")"

run "" "${MIRROR}" build-contexts "${WORK_DIR}/Dockerfile" || fail "build-contexts with a missing mirror exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}" ] ||
    fail "build-contexts overrode a base the mirror lacks: $(cat "${WORK_DIR}/out")"

if can_time_out; then
    started="${SECONDS}"
    FAKE_HANG="${PINNED_RECORD}" SPACE_REGISTRY_READ_TIMEOUT_SECONDS=1 \
        run "" "${MIRROR} ${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" build-contexts "${WORK_DIR}/Dockerfile" ||
        fail "build-contexts with a mirror that never answers exited nonzero"
    [ $((SECONDS - started)) -lt 15 ] || fail "a mirror that never answers held build-contexts for $((SECONDS - started)) s"
    [ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}" ] ||
        fail "build-contexts overrode a base whose mirror never answered: $(cat "${WORK_DIR}/out")"
fi

run "" "${MIRROR}" build-contexts "${WORK_DIR}/Dockerfile.other" || fail "build-contexts exited nonzero"
[ -s "${WORK_DIR}/out" ] && fail "build-contexts matched a name that only prefixes a FROM image"

SPACE_CI_IMAGE_MIRROR="" run "" "${MIRROR}" build-contexts "${WORK_DIR}/Dockerfile" ||
    fail "build-contexts without SPACE_CI_IMAGE_MIRROR exited nonzero"
[ -s "${WORK_DIR}/out" ] && fail "build-contexts overrode a base outside GitHub CI"
[ -s "${WORK_DIR}/calls" ] && fail "build-contexts called docker outside GitHub CI"

# ── listed, pinned ───────────────────────────────────────────────────────────
run "" "" listed "${NAME}" || fail "listed rejected a listed name"
run "" "" listed "${NAME%:*}:17" && fail "listed accepted an unlisted name"

run "" "" pinned "${NAME}" || fail "pinned rejected a listed name"
[ "$(cat "${WORK_DIR}/out")" = "${NAME}@${DIGEST}" ] || fail "pinned did not pin the tag: $(cat "${WORK_DIR}/out")"
run "" "" pinned "${PINNED_NAME}" || fail "pinned rejected a digest-pinned name"
[ "$(cat "${WORK_DIR}/out")" = "${PINNED_NAME%%@*}@${PINNED_DIGEST}" ] ||
    fail "pinned did not replace the name's digest with the pulled one: $(cat "${WORK_DIR}/out")"
run "" "" pinned "${NAME%:*}:17" && fail "pinned accepted an unlisted name"
[ -s "${WORK_DIR}/out" ] && fail "pinned printed a reference for an unlisted name"

run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" verified "${PINNED_NAME}" ||
    fail "verified rejected a recorded pairing"
run "" "${PINNED_MIRROR}" verified "${PINNED_NAME}" && fail "verified accepted a pairing without a record"
run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${STALE}" verified "${PINNED_NAME}" &&
    fail "verified accepted a record that names another digest"
run "" "${MIRROR}" verified "${NAME}" || fail "verified rejected a mirrored tag-only name"
run "" "" verified "${NAME}" && fail "verified accepted a name the mirror lacks"
run "" "" verified "${NAME%:*}:17" && fail "verified accepted an unlisted name"

# ── publish ──────────────────────────────────────────────────────────────────
run "" "${MIRROR} ${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" publish || fail "publish of mirrored digests exited nonzero"
grep -q "imagetools create" "${WORK_DIR}/calls" && fail "publish copied a digest already mirrored"
grep -qF -- "--format {{json .Manifest}} ${PINNED_NAME}" "${WORK_DIR}/calls" &&
    fail "publish read an upstream name it had already verified"

# A record that names another digest is a miss, which publish repairs.
FAKE_SERVES="${PINNED_NAME}=${PINNED_DIGEST}" run "" "${MIRROR} ${PINNED_MIRROR} ${PINNED_RECORD}=${STALE}" publish ||
    fail "publish could not repair a record that names another digest: $(cat "${WORK_DIR}/out")"
grep -q "::warning::${PINNED_RECORD} does not name ${PINNED_DIGEST}" "${WORK_DIR}/out" ||
    fail "publish did not say the record named another digest"
called "buildx imagetools create --prefer-index=false --tag ${PINNED_RECORD} ${PINNED_MIRROR}" ||
    fail "publish did not rewrite the record"
grep -q "Mirrored ${PINNED_NAME} as ${PINNED_MIRROR}, verified" "${WORK_DIR}/out" || fail "publish did not verify the repaired record"

run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" publish || fail "publish of a missing digest exited nonzero"
called "buildx imagetools create --prefer-index=false --tag ${DESTINATION} ${ORIGIN}" ||
    fail "publish did not copy the pinned digest to the destination"

FAKE_UNREACHABLE="${ORIGIN}" run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" publish ||
    fail "publish failed although another repository had the digest"
called "buildx imagetools create --prefer-index=false --tag ${DESTINATION} ${OTHER}" ||
    fail "publish did not copy from the other repository when the first failed"

FAKE_SERVES="${NAME}=${STALE}" run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" publish ||
    fail "publish refused a digest whose tag has since moved"
grep -q "::warning::${NAME} now serves ${STALE}" "${WORK_DIR}/out" || fail "publish did not warn that the tag moved"
called "buildx imagetools create --prefer-index=false --tag ${DESTINATION} ${ORIGIN}" ||
    fail "publish did not copy a pinned digest whose tag has since moved"

# A pairing publish has not recorded, whose digest is already mirrored.
FAKE_SERVES="${PINNED_NAME}=${PINNED_DIGEST}" run "" "${MIRROR} ${PINNED_MIRROR}" publish ||
    fail "publish refused an index that serves the listed digest: $(cat "${WORK_DIR}/out")"
called "buildx imagetools create --prefer-index=false --tag ${PINNED_RECORD} ${PINNED_MIRROR}" ||
    fail "publish did not record a pairing it verified"
[ "$(calls_matching "imagetools create")" -eq 1 ] || fail "publish copied a digest already mirrored to record its pairing"

FAKE_SERVES="${PINNED_NAME}=${STALE}" run "" "${MIRROR} ${PINNED_MIRROR}" publish &&
    fail "publish accepted a name pinned to an index that does not serve the listed digest"
grep -q "::error::${PINNED_NAME} serves ${STALE}" "${WORK_DIR}/out" ||
    fail "publish did not name the digest the bumped index serves: $(cat "${WORK_DIR}/out")"
grep -q "imagetools create" "${WORK_DIR}/calls" && fail "publish recorded or copied a pairing the index refutes"

run "" "${MIRROR}" publish || fail "publish failed because an index could not be read"
grep -q "::warning::Could not check that ${PINNED_NAME} serves ${PINNED_DIGEST} for linux/amd64: manifest unknown" \
    "${WORK_DIR}/out" || fail "publish did not say why it could not check a pairing: $(cat "${WORK_DIR}/out")"
grep -q "imagetools create" "${WORK_DIR}/calls" && fail "publish copied or recorded a pairing it could not verify"

if can_time_out; then
    started="${SECONDS}"
    FAKE_HANG="${PINNED_NAME}" SPACE_REGISTRY_READ_TIMEOUT_SECONDS=1 run "" "${MIRROR}" publish ||
        fail "publish failed because an index never answered"
    [ $((SECONDS - started)) -lt 15 ] || fail "an index that never answers held publish for $((SECONDS - started)) s"
    grep -q "no answer within 1 s" "${WORK_DIR}/out" || fail "publish did not say the index timed out"
    grep -q "imagetools create" "${WORK_DIR}/calls" && fail "publish copied a pairing whose index never answered"
fi

FAKE_SERVES="${PINNED_NAME}=${PINNED_DIGEST}" run "" "${MIRROR}" publish ||
    fail "publish of a new verified pairing exited nonzero: $(cat "${WORK_DIR}/out")"
called "buildx imagetools create --prefer-index=false --tag ${PINNED_DESTINATION} ${PINNED_ORIGIN}" ||
    fail "publish did not copy a new verified pairing's digest"
called "buildx imagetools create --prefer-index=false --tag ${PINNED_RECORD} ${PINNED_MIRROR}" ||
    fail "publish did not record a new verified pairing"

FAKE_CREATE_DIGEST="sha256:$(printf 'b%.0s' {1..64})" run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" publish &&
    fail "publish accepted a copy that changed the digest"
FAKE_CREATE_FAILS=1 run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" publish && fail "publish succeeded although every copy failed"
[ "$(calls_matching "imagetools create")" -eq 4 ] || fail "publish did not retry both repositories"

# ── without jq ───────────────────────────────────────────────────────────────
# Every tool on PATH but jq: records cannot be read, so digest-pinned images
# come from their sources, and the run says so once; publish refuses.
mkdir -p "${WORK_DIR}/no-jq"
IFS=: read -ra path_dirs <<<"${PATH}"
for dir in "${path_dirs[@]}"; do
    for tool in "${dir}"/*; do
        [ -x "${tool}" ] && [ "${tool##*/}" != jq ] && [ ! -e "${WORK_DIR}/no-jq/${tool##*/}" ] &&
            ln -s "${tool}" "${WORK_DIR}/no-jq/${tool##*/}"
    done
done
SECOND_PINNED_NAME="registry.example/library/rust:1-slim@sha256:$(printf 'f%.0s' {1..64})"
SECOND_PINNED_RECORD="ghcr.io/example/mirror:pin-$(printf '%s %s' "${SECOND_PINNED_NAME}" "${PINNED_DIGEST}" | sha256sum | cut -c1-40)"
cp "${WORK_DIR}/list" "${WORK_DIR}/two-pinned"
printf '%s %s ghcr.io/example/mirror:rust-1-slim\n' "${SECOND_PINNED_NAME}" "${PINNED_DIGEST}" >>"${WORK_DIR}/two-pinned"
printf 'FROM %s\nFROM %s\nFROM %s\n' "${PINNED_NAME}" "${SECOND_PINNED_NAME}" "${NAME}" >"${WORK_DIR}/Dockerfile.two"
without_jq() { PATH="${WORK_DIR}/no-jq" "$@"; }
command -v jq >/dev/null 2>&1 && PATH="${WORK_DIR}/no-jq" command -v jq >/dev/null 2>&1 &&
    fail "the test could not remove jq from PATH"

SPACE_MIRRORED_IMAGES="${WORK_DIR}/two-pinned" without_jq run "" \
    "${MIRROR} ${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST} ${SECOND_PINNED_RECORD}=${PINNED_DIGEST}" \
    build-contexts "${WORK_DIR}/Dockerfile.two" || fail "build-contexts without jq exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "--build-context=${NAME}=docker-image://${MIRROR}" ] ||
    fail "build-contexts without jq overrode a digest-pinned base or dropped a tag-only one: $(cat "${WORK_DIR}/out")"
[ "$(grep -c "jq is not installed" "${WORK_DIR}/err")" -eq 1 ] ||
    fail "build-contexts without jq did not warn exactly once: $(cat "${WORK_DIR}/err")"
grep -q "images pinned by digest come from their sources" "${WORK_DIR}/err" || fail "the warning did not say what it costs"

without_jq run "" "${PINNED_MIRROR} ${PINNED_RECORD}=${PINNED_DIGEST}" verified "${PINNED_NAME}" &&
    fail "verified without jq accepted a record it could not read"
grep -q "jq is not installed" "${WORK_DIR}/err" || fail "verified without jq did not say why"
[ -s "${WORK_DIR}/out" ] && fail "verified without jq wrote to stdout, which callers capture"

without_jq run "${PINNED_RECORD}=${PINNED_MIRROR}" "" pull "${PINNED_NAME}" || fail "pull without jq exited nonzero"
called "tag ${PINNED_MIRROR} ${PINNED_NAME%%@*}" || fail "pull, which needs no jq, stopped taking the mirror without it"

without_jq run "" "${MIRROR}" publish && fail "publish ran without jq"
grep -q "::error::publish needs jq" "${WORK_DIR}/out" || fail "publish without jq did not say why"
grep -q "imagetools create" "${WORK_DIR}/calls" && fail "publish without jq copied or recorded something"

printf '%s %s %s\n' "${NAME}" "${DIGEST#sha256:}" "${DESTINATION}" >"${WORK_DIR}/unpinned"
SPACE_MIRRORED_IMAGES="${WORK_DIR}/unpinned" run "" "" pull "${NAME}" &&
    fail "a line without a SHA-256 digest was accepted"

if [ "${FAILURES}" -ne 0 ]; then
    exit 1
fi
echo "mirror-images tests passed"
