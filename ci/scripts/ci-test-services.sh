#!/usr/bin/env bash
# Run the Rust tests' Postgres and Redis beside an unprivileged job container.
#
# Usage: ci/scripts/ci-test-services.sh start <network> <owner>
#        ci/scripts/ci-test-services.sh logs <owner>
#        ci/scripts/ci-test-services.sh stop <owner>
#
# GitHub CI's unprivileged Rust job has no Docker socket, so the nextest setup
# scripts cannot start these servers themselves. The runner starts them on a
# user-defined bridge network that the job container joins, and the setup
# scripts find them through SPACE_NEXTEST_POSTGRES_ADDRESS and
# SPACE_NEXTEST_REDIS_ADDRESS. `start` prints those assignments, one per line,
# for the caller to pass to the job container. They are container addresses
# rather than names, so no test connection waits on Docker's embedded DNS.
#
# Everything `start` creates carries the label space.ci.test-services=<owner>,
# a value unique to the run, and `logs` and `stop` act only on what carries it:
# a container or network that already existed under the same name is never
# read or removed. The containers are not started with --rm, so a server that
# crashes keeps its log for `logs`; `stop` removes them.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/test-deps.sh
source "${SCRIPT_DIR}/lib/test-deps.sh"

POSTGRES_NAME="space-ci-test-postgres"
REDIS_NAME="space-ci-test-redis"
OWNER_LABEL="space.ci.test-services"

container_address() {
    local container="$1"
    local network="$2"
    docker inspect \
        --format "{{(index .NetworkSettings.Networks \"${network}\").IPAddress}}" \
        "${container}"
}

owned_containers() {
    docker ps --all --quiet --filter "label=${OWNER_LABEL}=$1"
}

start() {
    local network="$1"
    local owner="$2"
    local label="${OWNER_LABEL}=${owner}"
    # A failed start removes what it created, and nothing that already existed.
    # The EXIT trap runs after this function returns, so it takes the owner as
    # a literal rather than a local.
    # shellcheck disable=SC2064 # expand the owner now
    trap "stop '${owner}'" EXIT

    # Reuse an existing network of this name without taking ownership of it.
    if ! docker network inspect "${network}" >/dev/null 2>&1; then
        docker network create --label "${label}" "${network}" >/dev/null
    fi
    space_pull_docker_image "$(space_test_dependency_postgres_image)" >&2
    space_pull_docker_image "$(space_test_dependency_redis_image)" >&2
    # For diagnosing a test that timed out acquiring a connection: Postgres
    # already logs a refused connection (too many clients), and this adds
    # every statement that took longer than a second. The log prints on every
    # run, so routine lines stay out of it: logging every connection, or the
    # checkpoint each CREATE and DROP DATABASE forces, costs minutes to print.
    space_start_test_postgres --name "${POSTGRES_NAME}" --label "${label}" --network "${network}" \
        -- -c log_min_duration_statement=1000 -c log_checkpoints=off >/dev/null
    docker run --detach --pull=never --name "${REDIS_NAME}" --label "${label}" \
        --network "${network}" "$(space_test_dependency_redis_image)" >/dev/null

    space_wait_for_test_postgres "${POSTGRES_NAME}"
    local deadline=$((SECONDS + 30))
    until [ "$(docker exec "${REDIS_NAME}" redis-cli ping 2>/dev/null)" = PONG ]; do
        if [ "${SECONDS}" -ge "${deadline}" ]; then
            echo "Redis did not become ready within 30 seconds" >&2
            docker logs "${REDIS_NAME}" >&2 || true
            return 1
        fi
        sleep 0.2
    done
    local postgres_address redis_address
    postgres_address="$(container_address "${POSTGRES_NAME}" "${network}")"
    redis_address="$(container_address "${REDIS_NAME}" "${network}")"
    if [ -z "${postgres_address}" ] || [ -z "${redis_address}" ]; then
        echo "The test services have no address on ${network}" >&2
        return 1
    fi
    trap - EXIT
    echo "SPACE_NEXTEST_POSTGRES_ADDRESS=${postgres_address}:5432"
    echo "SPACE_NEXTEST_REDIS_ADDRESS=${redis_address}:6379"
}

# The servers' own logs. A retried test leaves the job green, so callers print
# these on every run rather than only on failure. Each test's database is
# dropped WITH (FORCE), which logs one FATAL per connection it ends; those are
# counted rather than printed.
logs() {
    local owner="$1"
    local container
    local forced="terminating connection due to administrator command"
    for container in $(owned_containers "${owner}"); do
        echo "--- $(docker inspect --format '{{.Name}}' "${container}" | sed 's#^/##')"
        docker logs "${container}" 2>&1 | grep -vF "${forced}" || true
        echo "(${forced}: $(docker logs "${container}" 2>&1 | grep -cF "${forced}" || true) lines omitted)"
    done
}

stop() {
    local owner="$1"
    local containers=()
    mapfile -t containers < <(owned_containers "${owner}")
    if [ "${#containers[@]}" -gt 0 ]; then
        docker rm --force "${containers[@]}" >/dev/null 2>&1 || true
    fi
    local network
    for network in $(docker network ls --quiet --filter "label=${OWNER_LABEL}=${owner}"); do
        docker network rm "${network}" >/dev/null 2>&1 || true
    done
}

usage() {
    echo "usage: $0 start <network> <owner> | logs <owner> | stop <owner>" >&2
    exit 2
}

case "${1:-}" in
    start)
        [ $# -eq 3 ] || usage
        start "$2" "$3"
        ;;
    logs | stop)
        [ $# -eq 2 ] || usage
        "$1" "$2"
        ;;
    *)
        usage
        ;;
esac
