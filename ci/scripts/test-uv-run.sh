#!/usr/bin/env bash
# Regression test for ci/scripts/lib/uv-run.sh against a fake uv: the command
# runs offline when its environment could be prepared from the cache, online
# otherwise, and exactly once either way, so a failing command is not retried.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=lib/uv-run.sh
source "${repo_root}/ci/scripts/lib/uv-run.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# Logs each call, one bracketed argument after another. An offline
# preparation succeeds only when FAKE_CACHED=1; `run` exits FAKE_STATUS after
# printing what it read on stdin.
cat >"${tmp_dir}/uv" <<'SH'
#!/usr/bin/env bash
{ printf '[%s]' "$@"; echo; } >>"${FAKE_CALLS}"
if [[ "$1" == sync || " $* " == *" python -c  "* ]]; then
    [[ " $* " == *" --offline "* && "${FAKE_CACHED}" == 1 ]]
    exit
fi
cat
exit "${FAKE_STATUS:-0}"
SH
chmod +x "${tmp_dir}/uv"
uv="${tmp_dir}/uv"
export FAKE_CALLS="${tmp_dir}/calls"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

expect_calls() {
    local expected="$1"
    [[ "$(cat "${FAKE_CALLS}")" == "${expected}" ]] ||
        fail "expected calls:"$'\n'"${expected}"$'\n'"got:"$'\n'"$(cat "${FAKE_CALLS}")"
    : >"${FAKE_CALLS}"
}

: >"${FAKE_CALLS}"
log="$(FAKE_CACHED=1 space_uv_run "${uv}" --locked --with pkg==1.0 -- test.py 2>&1 </dev/null)"
[[ "${log}" == "uv: running test.py offline, from uv's cache" ]] || fail "unexpected log: ${log}"
expect_calls "[run][--offline][--quiet][--locked][--with][pkg==1.0][python][-c][]
[run][--offline][--locked][--with][pkg==1.0][test.py]"

log="$(FAKE_CACHED=0 space_uv_run "${uv}" --locked --with pkg==1.0 -- test.py 2>&1 </dev/null)"
[[ "${log}" == *"resolving against the package index" ]] || fail "unexpected log: ${log}"
expect_calls "[run][--offline][--quiet][--locked][--with][pkg==1.0][python][-c][]
[run][--locked][--with][pkg==1.0][test.py]"

# A script's environment is prepared with `uv sync --script`, and the
# preparation leaves the command's stdin to the command.
output="$(echo piped | FAKE_CACHED=1 space_uv_run "${uv}" --script -- check.py --flag 2>/dev/null)"
[[ "${output}" == piped ]] || fail "the command did not read its stdin: ${output}"
expect_calls "[sync][--offline][--quiet][--script][check.py]
[run][--offline][--script][check.py][--flag]"

# A failing command runs once and its status is the call's.
for cached in 1 0; do
    status=0
    FAKE_CACHED="${cached}" FAKE_STATUS=3 space_uv_run "${uv}" --script -- check.py </dev/null 2>/dev/null || status=$?
    [[ "${status}" == 3 ]] || fail "expected status 3 with FAKE_CACHED=${cached}, got ${status}"
    [[ "$(grep -c '^\[run\]' "${FAKE_CALLS}")" == 1 ]] || fail "the failing command ran more than once"
    : >"${FAKE_CALLS}"
done

if space_uv_run "${uv}" --script check.py 2>/dev/null; then
    fail "a call without -- was accepted"
fi

echo "uv-run.sh: all checks passed"
