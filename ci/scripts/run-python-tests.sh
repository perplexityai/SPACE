#!/usr/bin/env bash
# Run every pytest/unittest-compatible test that Python discovers in the repo.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=lib/test-deps.sh
source "${SCRIPT_DIR}/lib/test-deps.sh"

cd "${REPO_ROOT}"

if [ -n "${BUILDKITE:-}" ]; then
    export SPACE_CI_REQUIRE_TEST_DEPS=1
fi

uv_bin="$(bash "${SCRIPT_DIR}/ensure-uv.sh")"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/space-uv-cache}"
mkdir -p "${UV_CACHE_DIR}"

if space_test_deps_required; then
    tools=()
    command -v docker >/dev/null 2>&1 || tools+=(docker-cli)
    command -v helm >/dev/null 2>&1 || tools+=(helm)
    if [ "${#tools[@]}" -gt 0 ]; then
        "${uv_bin}" run --script "${SCRIPT_DIR}/install-tools.py" "${tools[@]}"
    fi
    if ! command -v cargo >/dev/null 2>&1; then
        curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
            | sh -s -- -y --profile minimal --default-toolchain none
        export PATH="${HOME}/.cargo/bin:${PATH}"
    fi
fi

if space_test_deps_required; then
    if ! timeout 30 sh -c 'until docker info >/dev/null 2>&1; do sleep 1; done'; then
        echo "Docker daemon is not available for Python integration tests." >&2
        exit 1
    fi
    postgres_container="space-python-tests-${BUILDKITE_BUILD_ID:-$$}"
    postgres_image="$(space_test_dependency_postgres_image)"
    space_pull_docker_image "${postgres_image}"
    docker run \
        --detach \
        --pull=never \
        --name "${postgres_container}" \
        --env POSTGRES_HOST_AUTH_METHOD=trust \
        --publish 127.0.0.1::5432 \
        "${postgres_image}" \
        >/dev/null
    cleanup() {
        docker rm --force "${postgres_container}" >/dev/null 2>&1 || true
    }
    trap cleanup EXIT
    postgres_port="$(docker port "${postgres_container}" 5432/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
    export PRUNE_CLUSTER_ARTIFACTS_TEST_DATABASE_URL="postgres://postgres@127.0.0.1:${postgres_port}/postgres"
    if ! timeout 30 sh -c 'until docker exec "$1" pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done' sh "${postgres_container}"; then
        echo "Postgres test container did not become ready." >&2
        exit 1
    fi
fi

# Never re-lock implicitly: a uv older than the lock's writer, or a host
# uv.toml with its own resolver settings (exclude-newer), would rewrite the
# committed uv.lock. CI uses --locked, which fails when uv.lock no longer
# matches pyproject.toml, so a dependency change cannot pass on a stale lock.
# Local runs use --frozen, the lock as committed, which such a host uv would
# otherwise refuse.
if [ -n "${CI:-}" ]; then
    uv_lock_mode=--locked
else
    uv_lock_mode=--frozen
fi

echo "--- :python: Running every discovered Python test"
"${uv_bin}" run \
    "${uv_lock_mode}" \
    --project "${REPO_ROOT}" \
    --python 3.11 \
    --with boto3 \
    --with cel-python \
    --with pglast==7.10 \
    --with 'psycopg[binary]' \
    --with pytest==8.4.2 \
    --with python-hcl2 \
    --with pyyaml \
    --with requests \
    python -m pytest \
        -c /dev/null \
        -p no:cacheprovider \
        --rootdir "${REPO_ROOT}" \
        -ra \
        --strict-config \
        --strict-markers \
        "${REPO_ROOT}"
