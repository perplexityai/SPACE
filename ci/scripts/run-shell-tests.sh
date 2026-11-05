#!/usr/bin/env bash
# Run every self-contained shell regression test discovered in the repository.
#
# The tests are independent: each works in its own temporary directory. They
# run SPACE_SHELL_TEST_JOBS at a time (default: one per CPU), each with its
# output in a log of its own, printed whole when the test ends so that no two
# tests' output interleaves; each test's name is printed when it starts, so a
# test that hangs is named. Every test runs even after one fails; the run then
# names the failures and exits 1. Interrupted (SIGINT, or SIGTERM from a job
# timeout), the run stops the tests still running and prints what each wrote
# so far.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

test_dirs=(ci/scripts scripts/tests)
if [ -d .buildkite/scripts ]; then
    test_dirs+=(.buildkite/scripts)
fi

mapfile -t tests < <(
    find "${test_dirs[@]}" \
        -maxdepth 1 \
        -type f \
        -name 'test-*.sh' \
        -print \
        | sed 's#^\./##' \
        | sort
)

if [ "${#tests[@]}" -eq 0 ]; then
    echo "No shell regression tests discovered." >&2
    exit 1
fi

jobs="${SPACE_SHELL_TEST_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
if ! [[ "${jobs}" =~ ^[1-9][0-9]*$ ]]; then
    echo "SPACE_SHELL_TEST_JOBS must be a positive integer, not '${jobs}'." >&2
    exit 1
fi

log_dir="$(mktemp -d)"
trap 'rm -rf "${log_dir}"' EXIT

declare -A test_of=() log_of=() started_at=()
failures=()

# Each test runs in a session of its own where setsid exists (Linux), so that
# stopping it stops everything it started.
launcher=()
if command -v setsid >/dev/null 2>&1; then
    launcher=(setsid)
fi

# Stops the tests still running and prints their output so far, before the
# EXIT trap deletes the logs. Background jobs of a non-interactive shell
# ignore SIGINT, so they are stopped explicitly.
interrupted() {
    local signal="$1" pid running grace
    trap - INT TERM
    for pid in "${!test_of[@]}"; do
        kill -TERM -- "-${pid}" 2>/dev/null || kill -TERM "${pid}" 2>/dev/null || true
    done
    # A test that ignores SIGTERM gets SIGKILL after 5 s, so the run still
    # prints every log and exits.
    for ((grace = 0; grace < 25; grace++)); do
        running="$(jobs -rp)"
        [ -n "${running}" ] || break
        sleep 0.2
    done
    for pid in "${!test_of[@]}"; do
        kill -KILL -- "-${pid}" 2>/dev/null || kill -KILL "${pid}" 2>/dev/null || true
    done
    for pid in "${!test_of[@]}"; do
        wait "${pid}" 2>/dev/null || true
        echo "+++ ${test_of[${pid}]} (interrupted by SIG${signal} after $((SECONDS - started_at[${pid}])) s)"
        cat "${log_of[${pid}]}"
    done
    echo "The shell regression tests were interrupted by SIG${signal}." >&2
    if [ "${signal}" = INT ]; then exit 130; fi
    exit 143
}
trap 'interrupted INT' INT
trap 'interrupted TERM' TERM

# Waits for one running test to end, then prints its log and its result. It
# polls Bash's own job table for a test no longer running, rather than use
# `wait -n`, whose `-p` needs Bash 5.1 and which some Bash 5 releases return
# from without naming a test that ended; `wait PID` then reports that test's
# status on every version.
reap() {
    local pid status running
    while true; do
        running=" $(jobs -rp | tr '\n' ' ') "
        for pid in "${!test_of[@]}"; do
            if [[ "${running}" != *" ${pid} "* ]]; then
                break 2
            fi
        done
        sleep 0.2
    done
    status=0
    wait "${pid}" || status=$?
    local test_file="${test_of[${pid}]}"
    unset "test_of[${pid}]"
    echo "+++ ${test_file} ($((SECONDS - started_at[${pid}])) s)"
    cat "${log_of[${pid}]}"
    if [ "${status}" -ne 0 ]; then
        echo "${test_file} failed with status ${status}." >&2
        failures+=("${test_file}")
    fi
}

printf '%s\n' "--- :shell: Running ${#tests[@]} discovered shell regression tests, ${jobs} at a time"
for index in "${!tests[@]}"; do
    while [ "${#test_of[@]}" -ge "${jobs}" ]; do
        reap
    done
    test_file="${tests[${index}]}"
    echo "Started ${test_file}"
    "${launcher[@]}" bash "${test_file}" >"${log_dir}/${index}.log" 2>&1 &
    test_of[$!]="${test_file}"
    log_of[$!]="${log_dir}/${index}.log"
    started_at[$!]="${SECONDS}"
done
while [ "${#test_of[@]}" -gt 0 ]; do
    reap
done

if [ "${#failures[@]}" -ne 0 ]; then
    echo "${#failures[@]} of ${#tests[@]} shell regression tests failed:" >&2
    printf '  %s\n' "${failures[@]}" >&2
    exit 1
fi
echo "All ${#tests[@]} shell regression tests passed."
