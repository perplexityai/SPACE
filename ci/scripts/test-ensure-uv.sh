#!/usr/bin/env bash
# Regression test for ci/scripts/ensure-uv.sh against a fake curl: it installs
# the pinned release only at its checksum, takes it from GitHub when Astral's
# mirror fails, installs nothing from an archive that fails the checksum,
# replaces a ci/bin/uv of another release, uses a uv on PATH unless another
# release is asked for or, with CI set, it is not the pinned one, and refuses a
# version override without its checksum.
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

# Serves FAKE_BODY for every URL except those on a host in FAKE_DOWN_HOSTS,
# which answer 503, FAKE_MISSING_HOSTS (404) or FAKE_FORBIDDEN_HOSTS (403), as
# `curl -f` with `-w '%{http_code}'` reports them, or FAKE_BAD_CERT_HOSTS,
# whose certificate does not verify (curl exit 60, no HTTP status).
mkdir -p "${WORK_DIR}/bin"
cat >"${WORK_DIR}/bin/curl" <<'SH'
#!/usr/bin/env bash
url="" out="" format=""
while [ $# -gt 0 ]; do
    case "$1" in
        -o) out="$2"; shift ;;
        -w) format="$2"; shift ;;
        https://*) url="$1" ;;
    esac
    shift
done
printf '%s\n' "${url}" >>"${FAKE_CALLS}"
host="${url#https://}"
host="${host%%/*}"
answer() { [ -z "${format}" ] || printf '%s' "$1"; }
if [[ " ${FAKE_MISSING_HOSTS:-} " == *" ${host} "* ]]; then
    answer 404; echo "curl: (22) 404 from ${host}" >&2; exit 22
fi
if [[ " ${FAKE_FORBIDDEN_HOSTS:-} " == *" ${host} "* ]]; then
    answer 403; echo "curl: (22) 403 from ${host}" >&2; exit 22
fi
if [[ " ${FAKE_BAD_CERT_HOSTS:-} " == *" ${host} "* ]]; then
    answer 000; echo "curl: (60) SSL certificate problem from ${host}" >&2; exit 60
fi
if [[ " ${FAKE_DOWN_HOSTS:-} " == *" ${host} "* ]]; then
    answer 503; echo "curl: (22) 503 from ${host}" >&2; exit 22
fi
cp "${FAKE_BODY}" "${out}"
answer 200
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

# The script with its default pin moved to the fake release, so a run without
# SPACE_UV_VERSION can install the pinned release from the fake archive.
# shellcheck disable=SC2016 # matches the script's own ${SPACE_UV_VERSION:-...}
PINNED="$(sed -n 's/^UV_VERSION="${SPACE_UV_VERSION:-\([^}]*\)}"$/\1/p' "${SCRIPT}")"
[ -n "${PINNED}" ] || { echo "FAIL: no default pin in ${SCRIPT}" >&2; exit 1; }
FAKE_PIN_SCRIPT="${WORK_DIR}/ensure-uv-fake-pin.sh"
sed -E -e "/^UV_VERSION=/s/:-${PINNED}}\"$/:-${VERSION}}\"/" \
    -e "s/^( +uv_sha256=)[0-9a-f]{64}$/\1${SHA256}/" "${SCRIPT}" >"${FAKE_PIN_SCRIPT}"
if ! grep -qF ":-${VERSION}}" "${FAKE_PIN_SCRIPT}" || ! grep -qE "^ +uv_sha256=${SHA256}$" "${FAKE_PIN_SCRIPT}"; then
    echo "FAIL: could not move the default pin of ${SCRIPT}" >&2
    exit 1
fi

# A uv already on PATH, at another release, and one at the default pin.
mkdir -p "${WORK_DIR}/path-uv"
printf '#!/usr/bin/env bash\necho "uv 0.0.2"\n' >"${WORK_DIR}/path-uv/uv"
chmod +x "${WORK_DIR}/path-uv/uv"
mkdir -p "${WORK_DIR}/path-uv-pinned"
printf '#!/usr/bin/env bash\necho "uv %s"\n' "${PINNED}" >"${WORK_DIR}/path-uv-pinned/uv"
chmod +x "${WORK_DIR}/path-uv-pinned/uv"

# run <repo> <body> <down hosts> [VAR=value...]: runs a copy of the script, or
# of SOURCE when set, in <repo>, with EXTRA_PATH before its tools, leaving its
# stdout in out, its stderr in err, and the URLs it fetched in calls, all under
# WORK_DIR.
run() {
    local repo="$1" body="$2" down="$3"
    shift 3
    mkdir -p "${repo}/ci/scripts"
    cp "${SOURCE:-${SCRIPT}}" "${repo}/ci/scripts/ensure-uv.sh"
    : >"${WORK_DIR}/calls"
    env -i HOME="${WORK_DIR}" PATH="${EXTRA_PATH:+${EXTRA_PATH}:}${WORK_DIR}/bin:${WORK_DIR}/tools" \
        FAKE_CALLS="${WORK_DIR}/calls" FAKE_BODY="${body}" FAKE_DOWN_HOSTS="${down}" \
        FAKE_MISSING_HOSTS="${MISSING_HOSTS:-}" FAKE_FORBIDDEN_HOSTS="${FORBIDDEN_HOSTS:-}" \
        FAKE_BAD_CERT_HOSTS="${BAD_CERT_HOSTS:-}" \
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
grep -qxF "Installed uv ${VERSION} from https://github.com/astral-sh/uv/releases/download/${VERSION}/uv-${TARGET}.tar.gz" \
    "${WORK_DIR}/err" || fail "the install did not name the host it came from: $(cat "${WORK_DIR}/err")"

# The pinned release in ci/bin is reused without a download.
run "${repo}" "${WORK_DIR}/uv.tar.gz" "" SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}" ||
    fail "a second run exited nonzero"
[ ! -s "${WORK_DIR}/calls" ] || fail "a second run downloaded $(cat "${WORK_DIR}/calls")"

# A ci/bin/uv of another release is replaced by the pinned one.
printf '#!/usr/bin/env bash\necho "uv 0.0.1"\n' >"${repo}/ci/bin/uv"
run "${repo}" "${WORK_DIR}/uv.tar.gz" "" SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}" ||
    fail "replacing another release exited nonzero"
[ "$("${repo}/ci/bin/uv" --version)" = "uv ${VERSION}" ] || fail "ci/bin/uv of another release was kept"

# Without CI or a version override, a uv on PATH at another release is used
# as it is.
SOURCE="${FAKE_PIN_SCRIPT}" EXTRA_PATH="${WORK_DIR}/path-uv" run "${WORK_DIR}/repo-path" "${WORK_DIR}/uv.tar.gz" "" ||
    fail "a uv on PATH was not accepted"
[ "$(cat "${WORK_DIR}/out")" = "${WORK_DIR}/path-uv/uv" ] || fail "printed $(cat "${WORK_DIR}/out"), not the uv on PATH"
[ ! -s "${WORK_DIR}/calls" ] || fail "a uv on PATH still downloaded $(cat "${WORK_DIR}/calls")"

# With CI set, a uv on PATH at another release is passed over and the pinned
# release is installed.
repo="${WORK_DIR}/repo-ci"
SOURCE="${FAKE_PIN_SCRIPT}" EXTRA_PATH="${WORK_DIR}/path-uv" run "${repo}" "${WORK_DIR}/uv.tar.gz" "" CI=true ||
    fail "CI with a uv on PATH at another release exited nonzero: $(cat "${WORK_DIR}/err")"
[ "$(cat "${WORK_DIR}/out")" = "${repo}/ci/bin/uv" ] ||
    fail "CI printed $(cat "${WORK_DIR}/out"), not the pinned release in ci/bin"
[ -x "${repo}/ci/bin/uv" ] && [ "$("${repo}/ci/bin/uv" --version)" = "uv ${VERSION}" ] ||
    fail "CI did not install the pinned release"

# With CI set, a uv on PATH at the pinned release is used without a download.
EXTRA_PATH="${WORK_DIR}/path-uv-pinned" run "${WORK_DIR}/repo-ci-pinned" "${WORK_DIR}/uv.tar.gz" "" CI=true ||
    fail "CI with the pinned uv on PATH exited nonzero: $(cat "${WORK_DIR}/err")"
[ "$(cat "${WORK_DIR}/out")" = "${WORK_DIR}/path-uv-pinned/uv" ] ||
    fail "CI printed $(cat "${WORK_DIR}/out"), not the pinned uv on PATH"
[ ! -s "${WORK_DIR}/calls" ] || fail "CI with the pinned uv on PATH downloaded $(cat "${WORK_DIR}/calls")"

# An override for another release passes over a uv on PATH at a different
# one and installs the release asked for.
EXTRA_PATH="${WORK_DIR}/path-uv" run "${WORK_DIR}/repo-override" "${WORK_DIR}/uv.tar.gz" "" \
    SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}" || fail "the override exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "${WORK_DIR}/repo-override/ci/bin/uv" ] ||
    fail "the override printed $(cat "${WORK_DIR}/out"), not the release it asked for"

# ...but takes a uv on PATH that is already that release.
EXTRA_PATH="${WORK_DIR}/path-uv" run "${WORK_DIR}/repo-override-path" "${WORK_DIR}/uv.tar.gz" "" \
    SPACE_UV_VERSION=0.0.2 SPACE_UV_SHA256="${SHA256}" || fail "an override matching PATH exited nonzero"
[ "$(cat "${WORK_DIR}/out")" = "${WORK_DIR}/path-uv/uv" ] || fail "an override matching the uv on PATH was not taken from PATH"
[ ! -s "${WORK_DIR}/calls" ] || fail "an override matching the uv on PATH downloaded $(cat "${WORK_DIR}/calls")"

# Both hosts serve the same archive, which fails the checksum: a retry cannot
# change that, so one attempt of both hosts fails, and nothing is installed.
printf 'not the release\n' >"${WORK_DIR}/tampered.tar.gz"
repo="${WORK_DIR}/repo-tampered"
if run "${repo}" "${WORK_DIR}/tampered.tar.gz" "" SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "an archive that fails its checksum was accepted"
fi
[ "$(wc -l <"${WORK_DIR}/calls" | tr -d ' ')" = 2 ] || fail "expected 1 attempt of 2 hosts, got $(cat "${WORK_DIR}/calls")"
[ ! -e "${repo}/ci/bin/uv" ] || fail "an archive that fails its checksum was installed"
grep -q "has sha256" "${WORK_DIR}/err" || fail "the checksum mismatch was not reported: $(cat "${WORK_DIR}/err")"

# Both hosts answer 404: one attempt, then a failure naming the release.
if MISSING_HOSTS="releases.astral.sh github.com" run "${WORK_DIR}/repo-missing" "${WORK_DIR}/uv.tar.gz" "" \
    SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "a release neither host has was installed"
fi
[ "$(wc -l <"${WORK_DIR}/calls" | tr -d ' ')" = 2 ] || fail "a 404 from both hosts was retried: $(cat "${WORK_DIR}/calls")"
grep -q "no host serves uv ${VERSION}" "${WORK_DIR}/err" || fail "the 404s were not reported: $(cat "${WORK_DIR}/err")"

# A certificate that does not verify, or a 403, on both hosts: each host is
# tried once, then the install fails.
both="releases.astral.sh github.com"
if BAD_CERT_HOSTS="${both}" run "${WORK_DIR}/repo-bad-cert" "${WORK_DIR}/uv.tar.gz" "" \
    SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "an install with no verifiable certificate succeeded"
fi
[ "$(wc -l <"${WORK_DIR}/calls" | tr -d ' ')" = 2 ] || fail "a certificate error on both hosts was retried: $(cat "${WORK_DIR}/calls")"
if FORBIDDEN_HOSTS="${both}" run "${WORK_DIR}/repo-forbidden" "${WORK_DIR}/uv.tar.gz" "" \
    SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "an install both hosts refuse succeeded"
fi
[ "$(wc -l <"${WORK_DIR}/calls" | tr -d ' ')" = 2 ] || fail "a 403 from both hosts was retried: $(cat "${WORK_DIR}/calls")"

# A 403 from one host and an outage of the other is retried.
if FORBIDDEN_HOSTS=releases.astral.sh run "${WORK_DIR}/repo-forbidden-down" "${WORK_DIR}/uv.tar.gz" github.com \
    SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "an install with both hosts failing succeeded"
fi
[ "$(wc -l <"${WORK_DIR}/calls" | tr -d ' ')" = 4 ] || fail "a 503 beside a 403 was not retried: $(cat "${WORK_DIR}/calls")"

# A 404 from one host and an outage of the other is still retried.
if MISSING_HOSTS=releases.astral.sh run "${WORK_DIR}/repo-mixed" "${WORK_DIR}/uv.tar.gz" github.com \
    SPACE_UV_VERSION="${VERSION}" SPACE_UV_SHA256="${SHA256}"; then
    fail "an install with both hosts failing succeeded"
fi
[ "$(wc -l <"${WORK_DIR}/calls" | tr -d ' ')" = 4 ] || fail "an outage was not retried: $(cat "${WORK_DIR}/calls")"

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
