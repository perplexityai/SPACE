#!/usr/bin/env bash
# Regression test for templates/firecracker/kernel/ci.sh against a fake docker:
# a recorded pass skips the build; a missing one builds, smoke-boots and, only
# when publishing, records the pass after it; a failing build records nothing;
# a lookup or write that fails never fails the check; and the builds take the
# Ubuntu base from the mirror when it has it.
set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../templates/firecracker/kernel" && pwd)/ci.sh"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

REPOSITORY="registry.example/space-guest-kernel-ci"
BASE="ubuntu:24.04@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4"
MIRROR="ghcr.io/example/mirror@sha256:$(printf 'b%.0s' {1..64})"

mkdir -p "${WORK_DIR}/bin"
cat >"${WORK_DIR}/bin/docker" <<'SH'
#!/usr/bin/env bash
# The registry's references are the lines of FAKE_REGISTRY. FAKE_LOOKUP_ERROR
# makes lookups fail as an unreachable registry does, FAKE_BUILD_FAILS fails
# the kernel build, and FAKE_PUSH_FAILS fails the marker push.
printf '%s\n' "$*" >>"${FAKE_CALLS}"
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
case "$1 ${2:-} ${3:-}" in
    "login "*) cat >/dev/null ;;
    "buildx imagetools inspect")
        ref="${*: -1}"
        [ -z "${FAKE_LOOKUP_ERROR:-}" ] || { echo "ERROR: dial tcp: connect: connection refused" >&2; exit 1; }
        grep -qxF "${ref}" "${FAKE_REGISTRY}" || { echo "ERROR: ${ref}: not found" >&2; exit 1; }
        ;;
    "buildx inspect "*) ;;
    "buildx build "*)
        [[ " $* " != *" --file - "* ]] || cat >/dev/null
        output="$(option --output "$@")"
        if [[ "${output}" == type=registry,name=* ]]; then
            [ -z "${FAKE_PUSH_FAILS:-}" ] || exit 1
            name="${output#type=registry,name=}"
            printf '%s\n' "${name%%,*}" >>"${FAKE_REGISTRY}"
        fi
        ;;
    "build "*)
        if [[ " $* " == *" --target export "* ]]; then
            [ -z "${FAKE_BUILD_FAILS:-}" ] || exit 1
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
printf '%s %s %s\n' "${BASE}" "${MIRROR#*@}" ghcr.io/example/mirror:ubuntu-24.04 >"${WORK_DIR}/list"

FAILURES=0
fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

# run <registry references> <args>...: runs the script, leaving its output in
# ${WORK_DIR}/out and its docker calls in ${WORK_DIR}/calls.
run() {
    local registry="$1"
    shift
    : >"${WORK_DIR}/calls"
    printf '%s\n' "${registry}" >"${WORK_DIR}/registry"
    PATH="${WORK_DIR}/bin:${PATH}" FAKE_CALLS="${WORK_DIR}/calls" FAKE_REGISTRY="${WORK_DIR}/registry" \
        SPACE_GUEST_KERNEL_CI_REPOSITORY="${REPOSITORY}" SPACE_MIRRORED_IMAGES="${WORK_DIR}/list" \
        SPACE_CI_IMAGE_MIRROR="${SPACE_CI_IMAGE_MIRROR:-}" GITHUB_SHA=0123abcd GHCR_TOKEN=token \
        bash "${SCRIPT}" "$@" >"${WORK_DIR}/out" 2>&1
}
calls() { grep -c -e "$1" "${WORK_DIR}/calls" || true; }
said() { grep -qF -e "$1" "${WORK_DIR}/out"; }

hash="$(bash "${SCRIPT}" hash)"
[[ "${hash}" =~ ^[0-9a-f]{16}$ ]] || fail "the input hash is not 16 hex digits: ${hash}"
[ "$(KERNEL_VERSION=9.9.9 bash "${SCRIPT}" hash)" = "${hash}" ] || fail "the input hash depends on the environment"
MARKER="${REPOSITORY}:${hash}"

run "${MARKER}" publish || fail "a recorded pass failed the check: $(cat "${WORK_DIR}/out")"
said "skipping the rebuild" || fail "a recorded pass did not say it skipped the rebuild"
[ "$(calls ' build ')$(calls '^build ')" = 00 ] || fail "a recorded pass still built"

run "" publish || fail "a passing build failed the check: $(cat "${WORK_DIR}/out")"
said "No passing build of these kernel inputs is recorded as ${MARKER}" || fail "a missing marker was not reported"
said "::warning::" && fail "a missing marker was reported as a warning"
[ "$(calls "--cache-to type=registry,ref=${REPOSITORY}:toolchain-cache,")" = 1 ] ||
    fail "publish did not save the build toolchain cache"
[ "$(calls "--cache-to type=registry,ref=${REPOSITORY}:smoke-cache,")" = 1 ] ||
    fail "publish did not save the smoke toolchain cache"
[ "$(calls "--cache-from=type=registry,ref=${REPOSITORY}:toolchain-cache")" = 4 ] ||
    fail "not every build read the toolchain cache"
[ "$(calls 'KERNEL_VERSION=6.1.155')" = 1 ] || fail "the kernel build did not take the default version"
[ "$(calls '^build .*--target smoke.* --load')" = 1 ] || fail "the smoke image was not loaded into Docker"
[ "$(calls '^start ')" = 1 ] || fail "the kernel was not smoke-booted"
grep -qxF "${MARKER}" "${WORK_DIR}/registry" || fail "a passing build was not recorded"
[ "$(tail -n 1 "${WORK_DIR}/calls" | grep -c 'type=registry,name=')" = 1 ] ||
    fail "the pass was not recorded last: $(tail -n 1 "${WORK_DIR}/calls")"

KERNEL_VERSION=9.9.9 CONFIG_FRAGMENT=custom.config run "" check ||
    fail "check failed a passing build: $(cat "${WORK_DIR}/out")"
[ "$(calls 'KERNEL_VERSION=6.1.155')" = 1 ] || fail "check built a kernel profile the hash does not cover"
[ "$(calls 'cache-to')" = 0 ] || fail "check wrote a cache"
grep -qxF "${MARKER}" "${WORK_DIR}/registry" && fail "check recorded a pass"

FAKE_BUILD_FAILS=1 run "" publish && fail "a failing build passed the check"
grep -qxF "${MARKER}" "${WORK_DIR}/registry" && fail "a failing build recorded a pass"
[ "$(calls '^start ')" = 0 ] || fail "a failing build was smoke-booted"

FAKE_LOOKUP_ERROR=1 run "" publish || fail "an unreachable registry failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::Building the guest kernel; could not look up ${MARKER}: ERROR: dial tcp" ||
    fail "an unreachable registry was not reported as a warning"
[ "$(calls '^start ')" = 1 ] || fail "an unreachable registry did not build and boot the kernel"

FAKE_PUSH_FAILS=1 run "" publish || fail "a failed marker write failed the check: $(cat "${WORK_DIR}/out")"
said "::warning::Could not record the passing build as ${MARKER}" || fail "a failed marker write was not reported"

SPACE_CI_IMAGE_MIRROR=ghcr run "${MIRROR}" check || fail "a mirrored build failed: $(cat "${WORK_DIR}/out")"
[ "$(calls "--build-context=${BASE}=docker-image://${MIRROR}")" = 2 ] ||
    fail "the kernel and smoke builds did not take the Ubuntu base from the mirror"

if [ "${FAILURES}" -ne 0 ]; then
    echo "${FAILURES} guest kernel CI check(s) failed" >&2
    exit 1
fi
echo "Guest kernel CI checks passed"
