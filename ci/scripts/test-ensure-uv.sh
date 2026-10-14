#!/usr/bin/env bash
# Regression test for ci/scripts/ensure-uv.sh against a fake curl: it installs
# the pinned release only at its checksum, takes it from GitHub when Astral's
# mirror fails, installs nothing from an archive that fails the checksum,
# replaces a ci/bin/uv of another release, and refuses a version override
# without its checksum.
set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/ensure-uv.sh"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

VERSION=9.9.9
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) TARGET=x86_64-unknown-linux-gnu ;;
    Linux-aarch64 | Linux-arm64) TARGET=aarch64-unknown-linux-gnu ;;
    Darwin-arm64) TARGET=aarch64-apple-darwin ;;
    Darwin-x86_64) TARGET=x86_64-apple-darwin ;;
    *) echo "SKIP: no uv release for $(uname -s) $(uname -m)"; exit 0 ;;
esac

# A release archive whose uv reports VERSION, as the real one does.
mkdir -p "${WORK_DIR}/archive/uv-${TARGET}"
for tool in uv uvx; do
    printf '#!/usr/bin/env bash\necho "%s %s"\n' "${tool}" "${VERSION}" >"${WORK_DIR}/archive/uv-${TARGET}/${tool}"
    chmod +x "${WORK_DIR}/archive/uv-${TARGET}/${tool}"
done
tar -czf "${WORK_DIR}/uv.tar.gz" -C "${WORK_DIR}/archive" "uv-${TARGET}"
if command -v sha256sum >/dev/null 2>&1; then
    SHA256="$(sha256sum "${WORK_DIR}/uv.tar.gz" | awk '{ print $1 }')"
else
    SHA256="$(shasum -a 256 "${WORK_DIR}/uv.tar.gz" | awk '{ print $1 }')"
fi

# Serves FAKE_BODY for every URL except those on a host in FAKE_DOWN_HOSTS.
mkdir -p "${WORK_DIR}/bin"
cat >"${WORK_DIR}/bin/curl" <<'SH'
#!/usr/bin/env bash
url="" out=""
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift ;;
        https://*) url="$1" ;;
    esac
    shift
done
printf '%s\n' "${url}" >>"${FAKE_CALLS}"
host="${url#https://}"
host="${host%%/*}"
[[ " ${FAKE_DOWN_HOSTS:-} " != *" ${host} "* ]] || { echo "curl: (22) 503 from ${host}" >&2; exit 22; }
cp "${FAKE_BODY}" "${out}"
SH
chmod +x "${WORK_DIR}/bin/curl"

# Only the tools the script needs, so a uv installed on this host stays out of
# its PATH.
mkdir -p "${WORK_DIR}/tools"
for tool in awk basename bash cat chmod cp cut dirname env gzip install mkdir mktemp mv rm \
    sha256sum shasum sleep tar uname; do
    if path="$(command -v "${tool}")"; then
        ln -s "${path}" "${WORK_DIR}/tools/${tool}"
    fi
done

FAILURES=0
fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

# run <repo> <body> <down hosts> [VAR=value...]: runs a copy of the script in
# <repo>, leaving its stdout in out, its stderr in err, and the URLs it
# fetched in calls, all under WORK_DIR.
run() {
    local repo="$1" body="$2" down="$3"
    shift 3
    mkdir -p "${repo}/ci/scripts"
    cp "${SCRIPT}" "${repo}/ci/scripts/ensure-uv.sh"
    : >"${WORK_DIR}/calls"
    env -i HOME="${WORK_DIR}" PATH="${WORK_DIR}/bin:${WORK_DIR}/tools" \
        FAKE_CALLS="${WORK_DIR}/calls" FAKE_BODY="${body}" FAKE_DOWN_HOSTS="${down}" \
        SPACE_UV_INSTALL_ATTEMPTS=2 SPACE_UV_INSTALL_RETRY_DELAY_SECONDS=0 "$@" \
        bash "${repo}/ci/scripts/ensure-uv.sh" >"${WORK_DIR}/out" 2>"${WORK_DIR}/err"
}

# Astral's mirror is down: the same archive comes from GitHub, at its checksum.
repo="${WORK_DIR}/repo-fallback"
if ! run "${repo}" "${WORK_DIR}/uv.tar.gz" releases.astral.sh \
    SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "install with the mirror down exited nonzero: $(cat "${WORK_DIR}/err")"
fi
[ "$(cat "${WORK_DIR}/out")" = "${repo}/ci/bin/uv" ] || fail "printed $(cat "${WORK_DIR}/out"), not ci/bin/uv"
[ "$("${repo}/ci/bin/uv" --version)" = "uv ${VERSION}" ] || fail "ci/bin/uv is not the archive's uv"
[ -x "${repo}/ci/bin/uvx" ] || fail "ci/bin/uvx was not installed"
expected_calls="https://releases.astral.sh/github/uv/releases/download/${VERSION}/uv-${TARGET}.tar.gz
https://github.com/astral-sh/uv/releases/download/${VERSION}/uv-${TARGET}.tar.gz"
[ "$(cat "${WORK_DIR}/calls")" = "${expected_calls}" ] ||
    fail "fetched $(cat "${WORK_DIR}/calls"), not the mirror then GitHub"

# The pinned release in ci/bin is reused without a download.
run "${repo}" "${WORK_DIR}/uv.tar.gz" "" SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}" ||
    fail "a second run exited nonzero"
[ ! -s "${WORK_DIR}/calls" ] || fail "a second run downloaded $(cat "${WORK_DIR}/calls")"

# A ci/bin/uv of another release is replaced by the pinned one.
printf '#!/usr/bin/env bash\necho "uv 0.0.1"\n' >"${repo}/ci/bin/uv"
run "${repo}" "${WORK_DIR}/uv.tar.gz" "" SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}" ||
    fail "replacing another release exited nonzero"
[ "$("${repo}/ci/bin/uv" --version)" = "uv ${VERSION}" ] || fail "ci/bin/uv of another release was kept"

# Both hosts serve an archive that fails the checksum: every attempt tries
# both, and nothing is installed.
printf 'not the release\n' >"${WORK_DIR}/tampered.tar.gz"
repo="${WORK_DIR}/repo-tampered"
if run "${repo}" "${WORK_DIR}/tampered.tar.gz" "" SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "an archive that fails its checksum was accepted"
fi
[ "$(wc -l <"${WORK_DIR}/calls" | tr -d ' ')" = 4 ] || fail "expected 2 attempts of 2 hosts, got $(cat "${WORK_DIR}/calls")"
[ ! -e "${repo}/ci/bin/uv" ] || fail "an archive that fails its checksum was installed"
grep -q "has sha256" "${WORK_DIR}/err" || fail "the checksum mismatch was not reported: $(cat "${WORK_DIR}/err")"

# The default pin is checked too: this archive is not the pinned release.
if run "${WORK_DIR}/repo-default" "${WORK_DIR}/uv.tar.gz" ""; then
    fail "the default pin accepted an archive with another checksum"
fi
[ ! -e "${WORK_DIR}/repo-default/ci/bin/uv" ] || fail "the default pin installed an archive with another checksum"

# A version override without its checksum is refused before any download.
if run "${WORK_DIR}/repo-unpinned" "${WORK_DIR}/uv.tar.gz" "" SPACE_UV_VERSION="${VERSION}"; then
    fail "SPACE_UV_VERSION without SPACE_UV_SHA256 was accepted"
fi
[ ! -s "${WORK_DIR}/calls" ] || fail "SPACE_UV_VERSION without SPACE_UV_SHA256 downloaded $(cat "${WORK_DIR}/calls")"
grep -q SPACE_UV_SHA256 "${WORK_DIR}/err" || fail "the refusal does not name SPACE_UV_SHA256"

if [ "${FAILURES}" -ne 0 ]; then
    echo "${FAILURES} failure(s)" >&2
    exit 1
fi
echo "ensure-uv.sh: all checks passed"
