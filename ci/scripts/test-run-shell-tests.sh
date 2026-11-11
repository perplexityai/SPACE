#!/usr/bin/env bash
# Regression test for ci/scripts/run-shell-tests.sh, run on stub tests in a
# scratch tree that holds a copy of the runner.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# A scratch tree with the runner and the stub tests named in $@ (pass, fail,
# hang), printed as the tree's path.
scratch() {
    local tree
    tree="$(mktemp -d "${tmp_dir}/tree.XXXXXX")"
    mkdir -p "${tree}/ci/scripts" "${tree}/scripts/tests"
    cp "${repo_root}/ci/scripts/run-shell-tests.sh" "${tree}/ci/scripts/"
    local stub
    for stub in "$@"; do
        case "${stub}" in
            pass) printf '#!/usr/bin/env bash\necho pass-output\n' >"${tree}/scripts/tests/test-pass.sh" ;;
            fail) printf '#!/usr/bin/env bash\necho fail-output\nexit 3\n' >"${tree}/scripts/tests/test-fail.sh" ;;
            hang)
                printf '#!/usr/bin/env bash\necho $$ >"%s/hang.pid"\necho partial-output\nsleep 60\n' \
                    "${tree}" >"${tree}/scripts/tests/test-hang.sh"
                ;;
            stubborn)
                printf '#!/usr/bin/env bash\ntrap "" TERM\necho $$ >"%s/stubborn.pid"\necho stubborn-output\nwhile true; do sleep 1; done\n' \
                    "${tree}" >"${tree}/scripts/tests/test-stubborn.sh"
                ;;
        esac
    done
    printf '%s\n' "${tree}"
}

fail() {
    echo "$1" >&2
    cat "${tmp_dir}/out" >&2
    exit 1
}

# Every test runs and prints its output; a failure is named and fails the run.
tree="$(scratch pass fail)"
status=0
SPACE_SHELL_TEST_JOBS=2 bash "${tree}/ci/scripts/run-shell-tests.sh" >"${tmp_dir}/out" 2>&1 || status=$?
[[ "${status}" -eq 1 ]] || fail "a failing test must fail the run, got ${status}"
grep -q 'pass-output' "${tmp_dir}/out" || fail "the passing test's output is missing"
grep -q 'fail-output' "${tmp_dir}/out" || fail "the failing test's output is missing"
grep -q '^  scripts/tests/test-fail.sh$' "${tmp_dir}/out" || fail "the failing test is not named"

# Passing tests pass the run.
tree="$(scratch pass)"
SPACE_SHELL_TEST_JOBS=2 bash "${tree}/ci/scripts/run-shell-tests.sh" >"${tmp_dir}/out" 2>&1 ||
    fail "passing tests must pass the run"

# A hung test is named when it starts; a SIGTERM, as from a job timeout, stops
# it, prints what it wrote so far and fails the run.
tree="$(scratch pass hang)"
SPACE_SHELL_TEST_JOBS=2 bash "${tree}/ci/scripts/run-shell-tests.sh" >"${tmp_dir}/out" 2>&1 &
runner=$!
for _ in $(seq 50); do
    [[ -s "${tree}/hang.pid" ]] && grep -q 'pass-output' "${tmp_dir}/out" && break
    sleep 0.1
done
kill -TERM "${runner}"
status=0
wait "${runner}" || status=$?
[[ "${status}" -eq 143 ]] || fail "an interrupted run must exit 143, got ${status}"
grep -q '^Started scripts/tests/test-hang.sh$' "${tmp_dir}/out" || fail "the hung test is not named when it starts"
grep -q 'interrupted by SIGTERM' "${tmp_dir}/out" || fail "the hung test's interruption is not reported"
grep -q 'partial-output' "${tmp_dir}/out" || fail "the hung test's output so far is missing"
if kill -0 "$(cat "${tree}/hang.pid")" 2>/dev/null; then
    fail "the hung test is still running"
fi

# A test that ignores SIGTERM is killed after the grace period, so the run
# still reports it and exits.
tree="$(scratch stubborn)"
SPACE_SHELL_TEST_JOBS=2 bash "${tree}/ci/scripts/run-shell-tests.sh" >"${tmp_dir}/out" 2>&1 &
runner=$!
for _ in $(seq 50); do
    [[ -s "${tree}/stubborn.pid" ]] && break
    sleep 0.1
done
kill -TERM "${runner}"
started=${SECONDS}
status=0
wait "${runner}" || status=$?
[[ "${status}" -eq 143 ]] || fail "a run interrupted beside a stubborn test must exit 143, got ${status}"
(( SECONDS - started < 15 )) || fail "the run waited $((SECONDS - started)) s for a test that ignores SIGTERM"
grep -q 'stubborn-output' "${tmp_dir}/out" || fail "the stubborn test's output so far is missing"
if kill -0 "$(cat "${tree}/stubborn.pid")" 2>/dev/null; then
    fail "the stubborn test is still running"
fi

# A job count that is not a positive integer is refused.
if SPACE_SHELL_TEST_JOBS=0 bash "${tree}/ci/scripts/run-shell-tests.sh" >"${tmp_dir}/out" 2>&1; then
    fail "SPACE_SHELL_TEST_JOBS=0 must be refused"
fi

echo "run-shell-tests.sh: all cases pass"
