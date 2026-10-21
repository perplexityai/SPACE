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

# Installing missing tools, rustup included, is CI provisioning, so only a run
# that sets SPACE_CI_REQUIRE_TEST_DEPS, as Buildkite does above, gets it. Test
# dependencies are required by default everywhere, but a local run without a
# tool fails on it rather than installing one into the user's home.
if [ "${SPACE_CI_REQUIRE_TEST_DEPS:-0}" != 0 ]; then
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

# Wait up to $1 seconds for the Docker daemon to answer. Polled in the shell
# because stock macOS has no timeout(1).
wait_for_docker() {
    local deadline=$((SECONDS + $1))
    until docker info >/dev/null 2>&1; do
        if [ "${SECONDS}" -ge "${deadline}" ]; then
            return 1
        fi
        sleep 1
    done
}

start_postgres() {
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
    space_wait_for_test_postgres "${postgres_container}"
}

# The Postgres-backed tests need Docker unless the run opts out
# (space_test_deps_required). Opted out, they still run when Docker answers,
# and skip, each saying why, when it does not.
if space_test_deps_required; then
    if ! command -v docker >/dev/null 2>&1; then
        echo "The docker CLI is not installed, and the Python integration tests need" \
            "it for their Postgres; install it$(space_test_deps_opt_out_hint)." >&2
        exit 1
    fi
    if ! wait_for_docker 30; then
        echo "Docker daemon is not available for Python integration tests;" \
            "start it$(space_test_deps_opt_out_hint)." >&2
        exit 1
    fi
    start_postgres
elif command -v docker >/dev/null 2>&1 && wait_for_docker 0; then
    start_postgres
else
    echo "SPACE-TEST-DEP-SKIP: postgres unavailable ($(space_test_deps_opted_out_by)):" \
        "Docker is not reachable, so the Postgres-backed Python tests skip." >&2
fi

# Never re-lock implicitly: a uv older than the lock's writer, or a host
# uv.toml with its own resolver settings (exclude-newer), would rewrite the
# committed uv.lock. CI uses --locked, which fails when uv.lock no longer
# matches pyproject.toml, so a dependency change cannot pass on a stale lock.
# Local runs use --frozen, the lock as committed, which such a host uv would
# otherwise refuse. The tests' own packages are pyproject.toml's `test` group,
# so the lock pins them too.
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
    --group test \
    python -m pytest \
        -c /dev/null \
        -p no:cacheprovider \
        --rootdir "${REPO_ROOT}" \
        -ra \
        --strict-config \
        --strict-markers \
        "${REPO_ROOT}"
