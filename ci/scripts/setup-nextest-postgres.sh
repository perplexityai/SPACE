#!/usr/bin/env bash
# Start one migrated Postgres template for all fixture-backed tests in a
# nextest run. Each test still clones its own isolated database from it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/test-deps.sh
source "${SCRIPT_DIR}/lib/test-deps.sh"

cleanup_when_nextest_exits() {
    local nextest_pid="$1"
    local container_id="$2"

    while kill -0 "${nextest_pid}" 2>/dev/null; do
        sleep 1
    done
    docker rm --force "${container_id}" >/dev/null 2>&1 || true
}

if [ "${1:-}" = "--cleanup-when-nextest-exits" ]; then
    cleanup_when_nextest_exits "$2" "$3"
    exit 0
fi

if [ -z "${NEXTEST_ENV:-}" ] || [ -z "${NEXTEST_RUN_ID:-}" ]; then
    echo "This script must be run as a nextest setup script" >&2
    exit 1
fi

# Migrate the template on the given server and publish it to the tests.
publish_postgres() {
    local admin_database_url="$1"
    local setup_binary="${CARGO_TARGET_DIR:-target}/debug/space-nextest-postgres-setup"
    if [ ! -x "${setup_binary}" ]; then
        echo "Nextest did not build ${setup_binary}" >&2
        exit 1
    fi
    "${setup_binary}" "${admin_database_url}"
    printf 'SPACE_TEST_POSTGRES_ADMIN_URL=%s\n' "${admin_database_url}" >>"${NEXTEST_ENV}"
}

# The caller already runs a server for this nextest run, as GitHub CI's
# unprivileged Rust job does (ci/scripts/ci-test-services.sh): there is no
# Docker daemon to start one, and the caller removes it.
if [ -n "${SPACE_NEXTEST_POSTGRES_ADDRESS:-}" ]; then
    publish_postgres "postgres://postgres@${SPACE_NEXTEST_POSTGRES_ADDRESS}/postgres?sslmode=disable"
    exit 0
fi

nextest_pid="${PPID}"
container_id=""

cleanup_on_error() {
    if [ -n "${container_id}" ]; then
        docker rm --force "${container_id}" >/dev/null 2>&1 || true
    fi
}
trap cleanup_on_error EXIT

space_pull_docker_image "$(space_test_dependency_postgres_image)"
container_id="$(space_start_test_postgres --rm \
    --label "space.nextest.run-id=${NEXTEST_RUN_ID}" \
    --label "space.nextest.cleanup-id=${SPACE_NEXTEST_POSTGRES_CLEANUP_ID:-${NEXTEST_RUN_ID}}" \
    --publish 127.0.0.1::5432)"
space_wait_for_test_postgres "${container_id}"

postgres_port="$(docker inspect \
    --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' \
    "${container_id}")"
postgres_host="${TESTCONTAINERS_HOST_OVERRIDE:-127.0.0.1}"
admin_database_url="postgres://postgres@${postgres_host}:${postgres_port}/postgres?sslmode=disable"

# A published port is served by docker-proxy, a userspace process that copies
# every byte of every test's connection. Where this host can route to the
# container's bridge address, as on Linux and in CI, connect there instead.
# Anywhere it cannot, such as Docker Desktop, the probe fails within a second
# and the published port stays. An explicit host override always wins.
if [ -z "${TESTCONTAINERS_HOST_OVERRIDE:-}" ] && command -v timeout >/dev/null 2>&1; then
    container_ip="$(docker inspect \
        --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
        "${container_id}")"
    if [ -n "${container_ip}" ] &&
        timeout 1 bash -c "exec 3<>/dev/tcp/${container_ip}/5432" 2>/dev/null; then
        admin_database_url="postgres://postgres@${container_ip}:5432/postgres?sslmode=disable"
    fi
fi

publish_postgres "${admin_database_url}"

# Setup scripts finish before tests begin. A detached watcher provides the
# missing teardown hook without holding nextest's stdout/stderr pipes open.
nohup bash "$0" --cleanup-when-nextest-exits "${nextest_pid}" "${container_id}" \
    </dev/null >/dev/null 2>&1 &

trap - EXIT
