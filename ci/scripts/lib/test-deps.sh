#!/usr/bin/env bash

# Match pplx_space_db_test_fixtures::require_test_deps(): test dependencies
# are required unless SPACE_TEST_DEPS_OPTIONAL is non-empty and not 0.
# SPACE_CI_REQUIRE_TEST_DEPS keeps the meaning it had before that default:
# non-empty and not 0 requires them regardless, and set to empty or 0 makes
# them optional.
space_test_deps_required() {
    case "${SPACE_CI_REQUIRE_TEST_DEPS:-0}" in
        0) ;;
        *) return 0 ;;
    esac
    case "${SPACE_TEST_DEPS_OPTIONAL:-0}" in
        0) ;;
        *) return 1 ;;
    esac
    [ -z "${SPACE_CI_REQUIRE_TEST_DEPS+set}" ]
}

# The setting that let space_test_deps_required say no, for a skip marker.
space_test_deps_opted_out_by() {
    case "${SPACE_TEST_DEPS_OPTIONAL:-0}" in
        0) echo "SPACE_CI_REQUIRE_TEST_DEPS is empty or 0" ;;
        *) echo "SPACE_TEST_DEPS_OPTIONAL is set" ;;
    esac
}

# The end of a failure message when space_test_deps_required said yes: the
# opt-out, unless SPACE_CI_REQUIRE_TEST_DEPS overrides it.
space_test_deps_opt_out_hint() {
    case "${SPACE_CI_REQUIRE_TEST_DEPS:-0}" in
        0) echo ", or set SPACE_TEST_DEPS_OPTIONAL=1 to skip the tests that need it" ;;
        *) echo " (SPACE_CI_REQUIRE_TEST_DEPS is set, which overrides SPACE_TEST_DEPS_OPTIONAL)" ;;
    esac
}

space_test_dependency_postgres_image() {
    printf '%s\n' "${TESTCONTAINERS_POSTGRES_IMAGE:-public.ecr.aws/docker/library/postgres:16-alpine}"
}

space_test_dependency_redis_image() {
    printf '%s\n' "${TESTCONTAINERS_REDIS_IMAGE:-public.ecr.aws/docker/library/redis:5.0}"
}

# Run a command, ending it after the given number of seconds, so a registry
# transfer that stalls fails over instead of holding the job until its own
# deadline. In the foreground, so Ctrl-C at a terminal still reaches the
# command; the commands bounded here are single processes, which is all
# --foreground bounds. A plain `timeout` without GNU options bounds it from
# its own process group, and without `timeout` (stock macOS) it is unbounded.
space_with_deadline() {
    local seconds="$1"
    shift
    if timeout --foreground --kill-after=10 1 true >/dev/null 2>&1; then
        timeout --foreground --kill-after=10 "${seconds}" "$@"
    elif command -v timeout >/dev/null 2>&1; then
        timeout "${seconds}" "$@"
    else
        "$@"
    fi
}

# Whether an exit status from space_with_deadline means the deadline ended the
# command: 124 after SIGTERM, 137 when it had to be killed.
space_deadline_expired() {
    [ "$1" -eq 124 ] || [ "$1" -eq 137 ]
}

# Registry throttling is transient and a failed pull may still cache completed
# layers. Retry the explicit pull before starting a container so callers never
# trigger a second, implicit registry request through `docker run`. Each
# attempt is bounded by SPACE_DOCKER_PULL_TIMEOUT_SECONDS (default 300).
space_pull_docker_image() {
    local image="$1"
    local max_attempts="${SPACE_DOCKER_PULL_ATTEMPTS:-5}"
    local delay_seconds="${SPACE_DOCKER_PULL_RETRY_DELAY_SECONDS:-2}"
    local attempt=1

    if docker image inspect "${image}" >/dev/null 2>&1; then
        return 0
    fi

    while ! space_with_deadline "${SPACE_DOCKER_PULL_TIMEOUT_SECONDS:-300}" docker pull "${image}"; do
        if [ "${attempt}" -ge "${max_attempts}" ]; then
            echo "Failed to pull ${image} after ${attempt} attempts." >&2
            return 1
        fi

        echo "Pull of ${image} failed (attempt ${attempt}/${max_attempts}); retrying in ${delay_seconds}s." >&2
        sleep "${delay_seconds}"
        attempt=$((attempt + 1))
        delay_seconds=$((delay_seconds * 2))
    done
}

# Start the test Postgres server detached and print its container ID. Callers
# add their own `docker run` options (labels, network, published port, --rm),
# then optionally `--` and extra server settings (`-c name=value`).
#
# The server lives for one nextest run and is deleted with its data, so it
# gives up crash durability: every DROP DATABASE forces a checkpoint, and with
# fsync on each one flushed and fsynced the dirty pages of every concurrently
# running test's database. The testcontainers module this replaces already
# starts Postgres with fsync=off; synchronous_commit and full_page_writes only
# matter for surviving a crash as well. For the same reason the data directory
# is a tmpfs: each test's database is a copy of the template's files, created
# and deleted a file at a time, and that is cheaper in memory than on the
# runner's disk. CI runs 30 tests at once. Most hold two or three connections,
# but a few hold eight or ten, so the default limit of 100 is raised.
space_start_test_postgres() {
    local docker_options=()
    while [ "$#" -gt 0 ] && [ "$1" != -- ]; do
        docker_options+=("$1")
        shift
    done
    [ "$#" -eq 0 ] || shift
    docker run --detach \
        --pull=never \
        "${docker_options[@]}" \
        --env POSTGRES_HOST_AUTH_METHOD=trust \
        --env PGDATA=/var/lib/postgresql/data \
        --tmpfs /var/lib/postgresql/data \
        "$(space_test_dependency_postgres_image)" \
        -c fsync=off \
        -c synchronous_commit=off \
        -c full_page_writes=off \
        -c max_connections=200 \
        "$@"
}

# The image's entrypoint first runs initdb under a temporary server that
# listens only on the Unix socket, then stops it and starts the real one. A
# socket probe can therefore succeed against the temporary server and the
# next one fail while it shuts down. Probe over TCP, which only the real
# server answers, and poll finely: the server is usually up in about a second.
space_wait_for_test_postgres() {
    local container_id="$1"
    local ready_deadline=$((SECONDS + 30))
    until docker exec "${container_id}" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; do
        if [ "${SECONDS}" -ge "${ready_deadline}" ]; then
            echo "Postgres did not become ready within 30 seconds" >&2
            docker logs "${container_id}" >&2 || true
            return 1
        fi
        sleep 0.1
    done
}
