#!/usr/bin/env bash
# Ensure uv is available in ci/bin so Buildkite steps and bash helpers
# can run the Python CI helpers directly via `uv run --script`.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOCAL_BIN_DIR="${REPO_ROOT}/ci/bin"
LOCAL_UV_BIN="${LOCAL_BIN_DIR}/uv"

# The release this script installs, as ci/Dockerfile.ci pins it. Each release
# archive is checked against its sha256, so neither download host can hand CI
# another uv. To try another release, set SPACE_UV_VERSION and SPACE_UV_SHA256
# together. Each checksum below is the asset's `digest` in GitHub's release
# API (gh api repos/astral-sh/uv/releases/tags/<version>) and matches the
# archive both hosts serve.
UV_VERSION="${SPACE_UV_VERSION:-0.12.18}"
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)
        uv_target=x86_64-unknown-linux-gnu
        uv_sha256=89eadd7c76fc063887959510d5ba0ab1264dfd5f1143b925ddb73021a40acf16
        ;;
    Linux-aarch64 | Linux-arm64)
        uv_target=aarch64-unknown-linux-gnu
        uv_sha256=afb6291f3f0a6b4521fc67b947822506c41dde5b60d2189dd8f3695b2ac8c9e7
        ;;
    Darwin-arm64)
        uv_target=aarch64-apple-darwin
        uv_sha256=cf40e0c6a202190ccd9e0406dcfdd5b2d6668a9a5c779b17948963df32aafe5b
        ;;
    Darwin-x86_64)
        uv_target=x86_64-apple-darwin
        uv_sha256=2e4108f5395397c8bc5d43bf83d3bdbb2d0e92b90d0efa607756be704905fa33
        ;;
    *)
        uv_target=""
        uv_sha256=""
        ;;
esac
if [ -n "${SPACE_UV_VERSION:-}" ]; then
    uv_sha256="${SPACE_UV_SHA256:-}"
fi

mkdir -p "${LOCAL_BIN_DIR}"

uv_version_of() {
    "$1" --version 2>/dev/null | awk '{ print $2 }'
}

# ci/bin/uv is this script's own install, used only at the pinned release.
if [ -x "${LOCAL_UV_BIN}" ] && [ "$(uv_version_of "${LOCAL_UV_BIN}")" = "${UV_VERSION}" ]; then
    printf '%s\n' "${LOCAL_UV_BIN}"
    exit 0
fi

# A uv on PATH, such as the CI image's, is used only at the pinned release
# when CI is set, so a runner whose host uv is another release still runs the
# pinned one. Elsewhere a uv on PATH is used as it is, unless SPACE_UV_VERSION
# asks for another release.
if path_uv="$(command -v uv)" &&
    { { [ -z "${CI:-}" ] && [ -z "${SPACE_UV_VERSION:-}" ]; } ||
        [ "$(uv_version_of "${path_uv}")" = "${UV_VERSION}" ]; }; then
    printf '%s\n' "${path_uv}"
    exit 0
fi

if [ -z "${uv_target}" ]; then
    echo "Error: no pinned uv release for $(uname -s) $(uname -m); install uv yourself." >&2
    exit 1
fi
if [ -z "${uv_sha256}" ]; then
    echo "Error: SPACE_UV_VERSION=${UV_VERSION} needs SPACE_UV_SHA256, the sha256 of uv-${uv_target}.tar.gz." >&2
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl is required to install uv." >&2
    exit 1
fi

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{ print $1 }'
    else
        shasum -a 256 "$1" | awk '{ print $1 }'
    fi
}

echo "Installing uv ${UV_VERSION} (${uv_target})..." >&2
tmp_dir="$(mktemp -d)"
archive="${tmp_dir}/uv.tar.gz"
trap 'rm -rf "${tmp_dir}"' EXIT

# Astral's release mirror first and GitHub's release download second, which
# is also the order of astral.sh's installer. Each attempt tries both, so an
# outage of either host does not fail the job. Only a failure that could pass
# on a retry is retried: a 408, 429 or 5xx, or a curl error a flaky network
# causes (a DNS or connect failure, a timeout, a body cut short or reset, a
# failed TLS handshake). When neither host failed that way, and none served an
# archive other than the one the others served, the install fails at once: any
# other HTTP error, a certificate that does not verify, or the same wrong
# archive from both.
urls=(
    "https://releases.astral.sh/github/uv/releases/download/${UV_VERSION}/uv-${uv_target}.tar.gz"
    "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-${uv_target}.tar.gz"
)
attempts="${SPACE_UV_INSTALL_ATTEMPTS:-3}"
delay="${SPACE_UV_INSTALL_RETRY_DELAY_SECONDS:-5}"
installed=""
for ((attempt = 1; ; attempt++)); do
    transient=""
    mismatch=""
    mismatches_differ=""
    for url in "${urls[@]}"; do
        rm -f "${archive}"
        status=0
        code="$(curl --proto '=https' --tlsv1.2 -fsSL --connect-timeout 20 --max-time 300 \
            -w '%{http_code}' "${url}" -o "${archive}")" || status=$?
        if [ "${status}" -ne 0 ]; then
            echo "Could not download ${url} (curl exit ${status}, HTTP ${code})." >&2
            # curl -f exits 22 for an HTTP error, whose status then decides.
            case "${status}:${code}" in
                22:408 | 22:429 | 22:5??) transient=1 ;;
                22:*) ;;
                6:* | 7:* | 18:* | 28:* | 35:* | 52:* | 56:*) transient=1 ;;
            esac
            continue
        fi
        actual="$(sha256 "${archive}")"
        if [ "${actual}" != "${uv_sha256}" ]; then
            echo "${url} has sha256 ${actual}, not ${uv_sha256}." >&2
            if [ -z "${mismatch}" ]; then
                mismatch="${actual}"
            elif [ "${actual}" != "${mismatch}" ]; then
                mismatches_differ=1
            fi
            continue
        fi
        tar -xzf "${archive}" -C "${tmp_dir}"
        install -m 0755 "${tmp_dir}/uv-${uv_target}/uv" "${tmp_dir}/uv-${uv_target}/uvx" "${LOCAL_BIN_DIR}/"
        echo "Installed uv ${UV_VERSION} from ${url}" >&2
        installed=1
        break 2
    done
    if [ -z "${transient}" ] && [ -z "${mismatches_differ}" ]; then
        echo "Error: no host serves uv ${UV_VERSION} (uv-${uv_target}.tar.gz) at sha256 ${uv_sha256}." >&2
        exit 1
    fi
    if ((attempt >= attempts)); then
        break
    fi
    echo "uv install failed (attempt ${attempt}/${attempts}); retrying in ${delay}s." >&2
    sleep "${delay}"
    delay=$((delay * 2))
done
if [ -z "${installed}" ]; then
    echo "Error: could not install uv ${UV_VERSION} after ${attempts} attempts." >&2
    exit 1
fi
printf '%s\n' "${LOCAL_UV_BIN}"
