#!/usr/bin/env bash
# Regression test for templates/firecracker/kernel/ci.sh against a fake docker:
# a recorded pass skips the build; a missing one builds, smoke-boots and, only
# when publishing, saves the toolchain cache and records the pass after it; a
# failing build records nothing; a lookup, export or push that fails or stalls
# never fails the check; the builder starts from the mirrored BuildKit image,
# else its source, else Docker's own builder; a build that fails fetching from
# the mirror or the cache, or stalls importing the cache, runs once more without
# them, as does the rest of the run, on a new builder, or within a deadline on
# Docker's own builder; a command's own exit 137 is not an overrun; the run
# removes the builder it started, never a caller's; and the input hash covers
# each file's executable bit.
set -euo pipefail

KERNEL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../templates/firecracker/kernel" && pwd)"
SCRIPT="${KERNEL_DIR}/ci.sh"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

REPOSITORY="registry.example/space-guest-kernel-ci"
BASE="ubuntu:24.04@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4"
MIRROR="ghcr.io/example/mirror@sha256:$(printf 'b%.0s' {1..64})"
BUILDKIT="moby/buildkit:v0.32.2@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8"
BUILDKIT_MIRROR="ghcr.io/example/mirror@sha256:$(printf 'e%.0s' {1..64})"

mkdir -p "${WORK_DIR}/bin"
cat >"${WORK_DIR}/bin/docker" <<'SH'
#!/usr/bin/env bash
# The registry's references are the lines of FAKE_REGISTRY, a tag that names a
# digest as "<tag>=<digest>", the builders those of FAKE_BUILDERS
# ("<name> <image>"). FAKE_LOOKUP_ERROR is the error of every
# lookup; FAKE_STALL lists the operations that never answer (lookup, export,
# push, external-build: any build that uses the mirror or the cache, and
# own-builder-build: any build on Docker's own builder).
# FAKE_BAD_BUILDKIT lists BuildKit images that do not start, FAKE_BAD_BUILDER
# is the name prefix of builders that do not, and FAKE_RM_FAILS fails builder
# removals. FAKE_BUILD_FAILS fails the kernel build in its own step, after an
# earlier error line that reads like a fetch failure; FAKE_FETCH_FAILS fails
# each build that uses the mirror or the cache; FAKE_EXPORT_FAILS fails cache
# exports, and FAKE_EXPORT_DROPS makes them succeed without writing the tag.
printf '%s\n' "$*" >>"${FAKE_CALLS}"
stalls() { [[ " ${FAKE_STALL:-} " == *" $1 "* ]]; }
# option <name> <args>...: prints the value of --name=value or --name value.
option() {
    local name="$1"
    shift
    while [ $# -gt 0 ]; do
        case "$1" in
            "${name}="*) printf '%s\n' "${1#*=}"; return ;;
            "${name}") printf '%s\n' "$2"; return ;;
        esac
        shift
    done
}
external() { [[ " $* " == *" --cache-from="* || " $* " == *" --build-context="* ]]; }
case "$1 ${2:-} ${3:-}" in
    "login "*) cat >/dev/null ;;
    "buildx imagetools inspect")
        ref="${*: -1}"
        ! stalls lookup || exec sleep 30
        [ -z "${FAKE_LOOKUP_ERROR:-}" ] || { echo "ERROR: ${FAKE_LOOKUP_ERROR}" >&2; exit 1; }
        named="$(awk -v ref="${ref}=" 'index($0, ref) == 1 { digest = substr($0, length(ref) + 1) } END { print digest }' \
            "${FAKE_REGISTRY}")"
        if [ -n "${named}" ]; then
            [ "$4" != --format ] || printf '{"digest":"%s"}\n' "${named}"
            exit 0
        fi
        grep -qxF "${ref}" "${FAKE_REGISTRY}" || { echo "ERROR: ${ref}: not found" >&2; exit 1; }
        [ "$4" != --format ] || printf '{"digest":"%s"}\n' "${ref#*@}"
        ;;
    "buildx inspect --bootstrap")
        image="$(awk -v name="$4" '$1 == name { print $2 }' "${FAKE_BUILDERS}")"
        [[ " ${FAKE_BAD_BUILDKIT:-} " != *" ${image} "* ]] || exit 1
        [[ -z "${FAKE_BAD_BUILDER:-}" || "$4" != "${FAKE_BAD_BUILDER}"* ]] || exit 1
        ;;
    "buildx inspect "*) grep -q "^$3 " "${FAKE_BUILDERS}" ;;
    "buildx create "*)
        image="$(option --driver-opt "$@")"
        printf '%s %s\n' "$(option --name "$@")" "${image#image=}" >>"${FAKE_BUILDERS}"
        ;;
    "buildx rm "*)
        [ -z "${FAKE_RM_FAILS:-}" ] || exit 1
        sed -i "/^$3 /d" "${FAKE_BUILDERS}"
        ;;
    "buildx build "*)
        [[ " $* " != *" --file - "* ]] || cat >/dev/null
        cache_to="$(option --cache-to "$@")"
        if [ -n "${cache_to}" ]; then
            ! stalls export || exec sleep 30
            # As BuildKit does, ignore-error=true turns a failed export into success.
            if [ -n "${FAKE_EXPORT_FAILS:-}" ]; then
                [[ "${cache_to}" == *ignore-error=true* ]] && exit 0
                echo "ERROR: failed to push cache" >&2
                exit 1
            fi
            [ -n "${FAKE_EXPORT_DROPS:-}" ] || { ref="${cache_to#*ref=}"; printf '%s\n' "${ref%%,*}" >>"${FAKE_REGISTRY}"; }
        fi
        ;;
    "push "*)
        ! stalls push || exec sleep 30
        [ -z "${FAKE_PUSH_FAILS:-}" ] || exit 1
        printf '%s\n' "${*: -1}" >>"${FAKE_REGISTRY}"
        ;;
    "build "*)
        [[ " $* " == *" --builder="* ]] || ! stalls own-builder-build || exec sleep 30
        if external "$@"; then
            ! stalls external-build || exec sleep 30
            if [ -n "${FAKE_FETCH_FAILS:-}" ]; then
                echo "ERROR: failed to build: failed to solve: failed to copy: httpReadSeeker: failed open: unexpected status code 502 Bad Gateway" >&2
                exit 1
            fi
        fi
        if [[ " $* " == *" --target export "* ]]; then
            if [ -n "${FAKE_BUILD_FAILS:-}" ]; then
                echo "ERROR: failed to do request: an earlier, recovered fetch" >&2
                echo 'ERROR: failed to build: failed to solve: process "/bin/sh -c ./build-kernel.sh" did not complete successfully: exit code: 2' >&2
                exit 1
            fi
            dest="$(option --output "$@")"
            dir="${dest#type=local,dest=}/vmlinux-6.1.155"
            mkdir -p "${dir}"
            printf 'kernel\n' >"${dir}/vmlinux.bin"
            printf 'CONFIG_TEST=y\n' >"${dir}/config"
            (cd "${dir}" && sha256sum vmlinux.bin >vmlinux.bin.sha256)
        fi
        ;;
    "create "*) echo container ;;
    "cp "* | "rm "*) ;;
    "start "*)
        echo "Linux version 6.1.155"
        echo "Kernel panic - not syncing: VFS: Unable to mount root fs on unknown-block(0,0)"
        ;;
    *) echo "unexpected docker call: $*" >&2; exit 99 ;;
esac
SH
chmod +x "${WORK_DIR}/bin/docker"
printf '%s %s %s\n%s %s %s\n' \
    "${BASE}" "${MIRROR#*@}" ghcr.io/example/mirror:ubuntu-24.04 \
    "${BUILDKIT}" "${BUILDKIT_MIRROR#*@}" ghcr.io/example/mirror:buildkit >"${WORK_DIR}/list"

# A PATH without GNU timeout, as on macOS: every other command it had.
mkdir -p "${WORK_DIR}/no-timeout"
IFS=: read -r -a path_dirs <<<"${WORK_DIR}/bin:${PATH}"
for dir in "${path_dirs[@]}"; do
    for command in "${dir}"/*; do
        name="${command##*/}"
        case "${name}" in timeout | gtimeout) continue ;; esac
        if [ -x "${command}" ] && [ ! -e "${WORK_DIR}/no-timeout/${name}" ]; then
            ln -s "${command}" "${WORK_DIR}/no-timeout/${name}"
        fi
    done
done

FAILURES=0
fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

# run <registry references> <args>...: runs the script with no builder yet, or
# with the caller's when PRESET_BUILDER is set, leaving its output in
# ${WORK_DIR}/out and its docker calls in ${WORK_DIR}/calls.
run() {
    local registry="$1"
    shift
    : >"${WORK_DIR}/calls"
    : >"${WORK_DIR}/builders"
    [ -z "${PRESET_BUILDER:-}" ] || echo "space-guest-kernel caller-image" >"${WORK_DIR}/builders"
    printf '%s\n' "${registry}" >"${WORK_DIR}/registry"
    PATH="${TEST_PATH:-${WORK_DIR}/bin:${PATH}}" FAKE_CALLS="${WORK_DIR}/calls" \
        FAKE_REGISTRY="${WORK_DIR}/registry" FAKE_BUILDERS="${WORK_DIR}/builders" \
        SPACE_GUEST_KERNEL_CI_REPOSITORY="${REPOSITORY}" SPACE_MIRRORED_IMAGES="${WORK_DIR}/list" \
        SPACE_CI_IMAGE_MIRROR="${SPACE_CI_IMAGE_MIRROR-ghcr}" GITHUB_SHA=0123abcd GHCR_TOKEN=token \
        SPACE_GUEST_KERNEL_REGISTRY_TIMEOUT_SECONDS=1 SPACE_GUEST_KERNEL_WRITE_TIMEOUT_SECONDS=1 \
        SPACE_GUEST_KERNEL_BUILD_TIMEOUT_SECONDS=2 \
        bash "${SCRIPT}" "$@" >"${WORK_DIR}/out" 2>&1
}
calls() { grep -c -e "$1" "${WORK_DIR}/calls" || true; }
# without_external_after <pattern>: the calls matching it all came after the
# fallback, without the mirror and the cache.
without_external_after() {
    [ "$(grep -e "$1" "${WORK_DIR}/calls" | grep -cE -e '--(cache-from|build-context)=')" = 0 ]
}
# The retry ran on the builder started for it.
retry_on_new_builder() {
    grep -e '--target export' "${WORK_DIR}/calls" | tail -n 1 | grep -q -e '--builder=space-guest-kernel-retry-'
}
said() { grep -qF -e "$1" "${WORK_DIR}/out"; }
recorded() { grep -qxF "${MARKER}" "${WORK_DIR}/registry"; }

hash="$(bash "${SCRIPT}" hash)"
[[ "${hash}" =~ ^[0-9a-f]{16}$ ]] || fail "the input hash is not 16 hex digits: ${hash}"
[ "$(KERNEL_VERSION=9.9.9 bash "${SCRIPT}" hash)" = "${hash}" ] || fail "the input hash depends on the environment"
MARKER="${REPOSITORY}:${hash}"
CACHES="${REPOSITORY}:toolchain-cache"$'\n'"${REPOSITORY}:smoke-cache"
# The mirror takes a name pinned by digest only through the tag that records
# its verified pairing with the listed digest (ci/scripts/mirror-images.sh).
record() {
    printf 'ghcr.io/example/mirror:pin-%s=%s' "$(printf '%s %s' "$1" "$2" | sha256sum | cut -c1-40)" "$2"
}
MIRRORS="${MIRROR}"$'\n'"${BUILDKIT_MIRROR}"$'\n'"$(record "${BASE}" "${MIRROR#*@}")"$'\n'"$(record "${BUILDKIT}" "${BUILDKIT_MIRROR#*@}")"

# A copy of the directory in its own repository, to change a file's mode.
copy="${WORK_DIR}/repo/templates/firecracker/kernel"
mkdir -p "${copy}"
git -C "${KERNEL_DIR}" ls-files | while IFS= read -r file; do cp -p "${KERNEL_DIR}/${file}" "${copy}/"; done
git -C "${WORK_DIR}/repo" init --quiet
copy_hash="$(bash "${copy}/ci.sh" hash)"
chmod -x "${copy}/build-kernel.sh"
[ "$(bash "${copy}/ci.sh" hash)" != "${copy_hash}" ] || fail "the input hash ignores a lost executable bit"

run "${MARKER}" publish || fail "a recorded pass failed the check: $(cat "${WORK_DIR}/out")"
said "skipping the rebuild" || fail "a recorded pass did not say it skipped the rebuild"
[ "$(calls 'build ')$(calls 'buildx create')" = 00 ] || fail "a recorded pass still built or started a builder"

TEST_PATH="${WORK_DIR}/no-timeout" run "${MARKER}" publish ||
    fail "a recorded pass without GNU timeout failed: $(cat "${WORK_DIR}/out")"
said "skipping the rebuild" || fail "without GNU timeout the marker was not found: $(cat "${WORK_DIR}/out")"

run "${MIRRORS}" publish || fail "a passing build failed the check: $(cat "${WORK_DIR}/out")"
said "No passing build of these kernel inputs is recorded; ${MARKER} does not exist" ||
    fail "a missing marker was not reported"
said "::warning::No passing build" && fail "a missing marker was reported as a warning"
[ "$(calls "buildx create ")" = 1 ] && [ "$(calls "image=${BUILDKIT_MIRROR}")" = 1 ] ||
    fail "the builder did not start from the mirrored BuildKit image: $(grep 'buildx create' "${WORK_DIR}/calls")"
[ "$(calls '^build .*--builder=space-guest-kernel')" = 2 ] || fail "the builds did not use the started builder"
[ "$(calls "^build .*--build-context=${BASE}=docker-image://${MIRROR}")" = 2 ] ||
    fail "the kernel and smoke builds did not take the Ubuntu base from the mirror"
[ "$(calls 'KERNEL_VERSION=6.1.155')" = 1 ] || fail "the kernel build did not take the default version"
[ "$(calls '^build .*--target smoke.* --load')" = 1 ] || fail "the smoke image was not loaded into Docker"
[ "$(calls '^start ')" = 1 ] || fail "the kernel was not smoke-booted"
said "Saved the toolchain layers to ${REPOSITORY}:toolchain-cache" || fail "the build toolchain cache was not saved"
said "Saved the smoke layers to ${REPOSITORY}:smoke-cache" || fail "the smoke toolchain cache was not saved"
recorded || fail "a passing build was not recorded"
[ "$(grep -v '^buildx rm ' "${WORK_DIR}/calls" | tail -n 1)" = "push --quiet ${MARKER}" ] ||
    fail "the pass was not recorded last: $(tail -n 1 "${WORK_DIR}/calls")"
[ "$(tail -n 1 "${WORK_DIR}/calls")" = "buildx rm space-guest-kernel" ] && [ ! -s "${WORK_DIR}/builders" ] ||
    fail "the run did not remove the builder it started: $(grep 'buildx rm' "${WORK_DIR}/calls")"

PRESET_BUILDER=1 run "${MIRRORS}" publish || fail "a build on the caller's builder failed: $(cat "${WORK_DIR}/out")"
[ "$(calls '^buildx rm ')" = 0 ] || fail "the run removed the caller's builder: $(grep 'buildx rm' "${WORK_DIR}/calls")"

run "${MIRRORS}"$'\n'"${CACHES}" publish || fail "a cached build failed the check: $(cat "${WORK_DIR}/out")"
[ "$(calls "^build .*--cache-from=type=registry,ref=${REPOSITORY}:toolchain-cache")" = 2 ] ||
    fail "the builds did not read the toolchain cache"

FAKE_BAD_BUILDKIT="${BUILDKIT_MIRROR}" run "${MIRRORS}" publish ||
    fail "a broken mirrored BuildKit image failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::Could not start the space-guest-kernel builder from ${BUILDKIT_MIRROR}" ||
    fail "a broken mirrored BuildKit image was not reported"
[ "$(calls "buildx create .*image=${BUILDKIT}")" = 1 ] || fail "the builder did not fall back to BuildKit's source"
recorded || fail "a build on the source BuildKit image was not recorded"

FAKE_BAD_BUILDKIT="${BUILDKIT_MIRROR} ${BUILDKIT}" run "${MIRRORS}"$'\n'"${CACHES}" publish ||
    fail "a BuildKit outage failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::Building on Docker's own builder" || fail "the fallback to Docker's own builder was not reported"
[ "$(calls '^build .*--builder=')" = 0 ] || fail "a build used a builder that did not start"
[ "$(calls '^build .*--cache-from=')" = 2 ] || fail "Docker's own builder did not read the toolchain cache"
[ "$(calls 'cache-to')" = 0 ] || fail "Docker's own builder tried to export the cache"
recorded || fail "a build on Docker's own builder was not recorded"

KERNEL_VERSION=9.9.9 CONFIG_FRAGMENT=custom.config run "${MIRRORS}" check ||
    fail "check failed a passing build: $(cat "${WORK_DIR}/out")"
[ "$(calls 'KERNEL_VERSION=6.1.155')" = 1 ] || fail "check built a kernel profile the hash does not cover"
[ "$(calls 'cache-to')$(calls '^push ')" = 00 ] || fail "check wrote to the registry"

FAKE_BUILD_FAILS=1 run "${MIRRORS}"$'\n'"${CACHES}" publish && fail "a failing build passed the check"
recorded && fail "a failing build recorded a pass"
[ "$(calls '--target export')" = 1 ] || fail "a failing build step was retried"
[ "$(calls '^start ')$(calls 'cache-to')" = 00 ] || fail "a failing build was smoke-booted or saved"

FAKE_LOOKUP_ERROR="failed to do request: dial tcp: connect: connection refused" run "" publish ||
    fail "an unreachable registry failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::No passing build of these kernel inputs is recorded; could not look up ${MARKER}: ERROR: failed to do request" ||
    fail "an unreachable registry was not reported as a warning"
[ "$(calls '^start ')" = 1 ] || fail "an unreachable registry did not build and boot the kernel"

FAKE_LOOKUP_ERROR="docker-credential-ghcr: executable file not found" run "" check ||
    fail "a broken credential helper failed the check"
said "::warning::No passing build of these kernel inputs is recorded; could not look up" ||
    fail "an error that merely says 'not found' was taken for a missing marker"

FAKE_STALL="lookup export push" run "" publish ||
    fail "a registry that accepts and then stalls failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::No passing build of these kernel inputs is recorded; ${MARKER} gave no answer within 1 s" ||
    fail "a stalled lookup was not reported"
said "::warning::Could not save the toolchain layers" || fail "a stalled cache export was not reported"
said "::warning::Could not record the passing build as ${MARKER}" || fail "a stalled marker push was not reported"
said "reached the VFS handoff" || fail "a stalled registry stopped the build"

FAKE_FETCH_FAILS=1 run "${MIRRORS}"$'\n'"${CACHES}" publish ||
    fail "a broken mirror copy failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::The build failed or stalled taking layers from the image mirror or the toolchain cache" ||
    fail "a failed fetch from the mirror was not reported"
[ "$(calls '--target export')" = 2 ] || fail "a failed fetch from the mirror was not retried once"
retry_on_new_builder || fail "the retry after a failed fetch did not run on a new builder"
[ "$(grep -e '--target export' "${WORK_DIR}/calls" | tail -n 1 | grep -cE -e '--(cache-from|build-context)=')" = 0 ] ||
    fail "the retry still used the mirror or the cache"
recorded || fail "a build retried without the mirror was not recorded"

FAKE_STALL=external-build run "${MIRRORS}"$'\n'"${CACHES}" publish ||
    fail "a stalled mirror and cache failed the check: $(cat "${WORK_DIR}/out")"
[ "$(calls '--target export')" = 2 ] || fail "a stalled kernel build was not retried without the mirror and cache"
[ "$(calls '^buildx rm space-guest-kernel$')" = 1 ] || fail "the builder that held the stalled build was not removed"
retry_on_new_builder || fail "the retry did not run on a new builder: $(grep -e '--target export' "${WORK_DIR}/calls")"
[ "$(grep -e '--target smoke' "${WORK_DIR}/calls" | grep -cE -e '--builder=space-guest-kernel( |$)')" = 0 ] ||
    fail "the smoke build went back to the builder that held the stalled build"
[ "$(calls '^buildx rm space-guest-kernel-retry-')" = 1 ] || fail "the run did not remove the builder it started for the retry"
[ "$(calls '^build .*--target smoke')" = 1 ] || fail "the smoke build stalled again instead of going without them"
without_external_after '^build .*--target smoke' || fail "the smoke build after the fallback used the mirror or cache"
without_external_after 'cache-to' || fail "the exports after the fallback used the mirror or cache"
said "Saved the smoke layers" || fail "the run did not finish its exports after the fallback"
recorded || fail "a run that fell back was not recorded"

PRESET_BUILDER=1 FAKE_STALL=external-build run "${MIRRORS}"$'\n'"${CACHES}" publish ||
    fail "a stall on the caller's builder failed the check: $(cat "${WORK_DIR}/out")"
[ "$(calls '^buildx rm space-guest-kernel$')" = 0 ] || fail "the caller's builder was removed"
retry_on_new_builder || fail "the retry stayed on the caller's builder that held the stalled build"
[ "$(grep -e '--target smoke' "${WORK_DIR}/calls" | grep -cE -e '--builder=space-guest-kernel( |$)')" = 0 ] ||
    fail "the smoke build went back to the caller's builder"
recorded || fail "a run that left the caller's builder was not recorded"

# The removal fails, and the stalled builder keeps its name: the retry must not
# take it back by that name.
FAKE_RM_FAILS=1 FAKE_STALL=external-build run "${MIRRORS}"$'\n'"${CACHES}" publish ||
    fail "a stall whose builder could not be removed failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::Could not remove the space-guest-kernel builder" || fail "a failed builder removal was not reported"
retry_on_new_builder || fail "the retry reused the builder that held the stalled build after its removal failed"

FAKE_BAD_BUILDER=space-guest-kernel-retry FAKE_STALL=external-build run "${MIRRORS}"$'\n'"${CACHES}" publish ||
    fail "a stall whose new builder did not start failed the check: $(cat "${WORK_DIR}/out")"
[ "$(grep -e '--target export' "${WORK_DIR}/calls" | tail -n 1 | grep -c -e '--builder=')" = 0 ] ||
    fail "the retry did not fall back to Docker's own builder when the new one did not start"
[ "$(calls 'cache-to')" = 0 ] || fail "Docker's own builder tried to export the cache after the fallback"
recorded || fail "a retry on Docker's own builder was not recorded"

# Docker's own builder may still hold the stalled download, so the retry on it
# has a deadline too.
started="${SECONDS}"
FAKE_BAD_BUILDKIT="${BUILDKIT_MIRROR} ${BUILDKIT}" FAKE_STALL=own-builder-build \
    run "${MIRRORS}"$'\n'"${CACHES}" publish && fail "a retry that stalled on Docker's own builder passed the check"
said "::error::The cold retry on Docker's own builder overran its 2 s deadline, probably waiting on the stalled download" ||
    fail "a stalled retry on Docker's own builder was not reported: $(cat "${WORK_DIR}/out")"
[ "$((SECONDS - started))" -lt 20 ] || fail "the retry on Docker's own builder had no deadline"
[ "$(calls '--target export')" = 2 ] || fail "the stalled build on Docker's own builder was not retried once"
recorded && fail "a retry that overran recorded a pass"

FAKE_STALL=external-build run "${MIRRORS}" publish ||
    fail "a stalled mirror without a cache failed the check: $(cat "${WORK_DIR}/out")"
[ "$(calls '--target export')" = 2 ] || fail "a build with only the mirror had no deadline"
without_external_after '^build .*--target smoke' || fail "the smoke build after a mirror-only fallback used the mirror"

# attempt_with <command> [<deadline>]: runs the script's attempt on the
# command with the cache and the deadline (one second by default), in a
# subshell, leaving its output in ${WORK_DIR}/out and the seconds it took in
# ${WORK_DIR}/took.
attempt_with() {
    local started="${SECONDS}" status=0
    (
        # shellcheck disable=SC1090,SC1091 # the script under test
        source "${SCRIPT}"
        attempt "${2:-1}" "--cache-from=type=registry,ref=${REPOSITORY}:toolchain-cache" bash -c "$1"
    ) >"${WORK_DIR}/out" 2>&1 || status=$?
    echo "$((SECONDS - started))" >"${WORK_DIR}/took"
    return "${status}"
}
# Each stalls only while the cache is in use, as a stalled import does.
# shellcheck disable=SC2016 # expanded by the command's own shell
CACHED_ONLY='[[ "${KERNEL_DOCKER_BUILD_OPTIONS}" != *--cache-from=* ]] ||'

# A build that ignores SIGTERM is killed 10 s later, and timeout exits 137.
attempt_with "${CACHED_ONLY} { trap '' TERM; sleep 30; }; echo built" ||
    fail "a build killed after its deadline failed: $(cat "${WORK_DIR}/out")"
said "built" || fail "a build killed after its deadline (exit 137) was not retried"

# A command that exits 137 itself well before its deadline, as smoke-boot.sh
# does when QEMU is killed, fails as it is, without the fallback.
attempt_with 'echo attempted; exit 137' 30 && fail "a command that exited 137 passed"
[ "$(grep -c attempted "${WORK_DIR}/out")" = 1 ] ||
    fail "a command that exited 137 before its deadline was taken for an overrun and rebuilt"
said "::warning::The build failed or stalled" && fail "a command that exited 137 before its deadline fell back"

# A docker client that outlives its shell must not hold the attempt.
attempt_with "${CACHED_ONLY} { (trap '' TERM; exec sleep 30) & sleep 30; }; echo built" ||
    fail "a build whose client outlived it failed: $(cat "${WORK_DIR}/out")"
said "built" || fail "a build whose client outlived it was not retried"
[ "$(cat "${WORK_DIR}/took")" -lt 10 ] ||
    fail "a client that outlived its build held the attempt for $(cat "${WORK_DIR}/took") s"

FAKE_EXPORT_FAILS=1 run "${MIRRORS}"$'\n'"${CACHES}" publish || fail "a failing cache export failed the check"
said "::warning::Could not save the toolchain layers to ${REPOSITORY}:toolchain-cache" ||
    fail "a failed export over an existing tag was not reported"
said "Saved the toolchain layers" && fail "a failed export over an existing tag was reported as saved"

FAKE_EXPORT_DROPS=1 run "${MIRRORS}" publish || fail "a dropped cache export failed the check"
said "::warning::The toolchain layers were not saved: ${REPOSITORY}:toolchain-cache does not exist after the export" ||
    fail "a cache export that wrote nothing was not reported"

if [ "${FAILURES}" -ne 0 ]; then
    echo "${FAILURES} guest kernel CI check(s) failed" >&2
    exit 1
fi
echo "Guest kernel CI checks passed"
