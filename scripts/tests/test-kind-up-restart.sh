#!/usr/bin/env bash
# up.sh reuses a running Tilt, and restarts it only when it was started with
# other values of the settings Tilt reads once, at start (TILT_SETTINGS in
# up.sh). A restart interrupts whatever else is using the stack — another
# session's smoke run, say — so these cases pin, against the real up.sh:
#
#   1. A changed listed setting restarts Tilt.
#   2. Nothing else does: not a refreshed AWS_SESSION_TOKEN with exchange
#      storage credentials (nothing reads it then), not a knob only up.sh reads
#      (SPACE_KIND_MIN_FREE_GIB), and not a setting set explicitly to its
#      default. With static storage credentials the AWS variables count.
#   3. A run whose settings the bring-up would refuse leaves the running Tilt
#      running: the refusal comes before the stop, not after it.
#
# External commands are PATH stubs. The running Tilt is a real process renamed
# `tilt up`, so up.sh reads its environment from /proc as it would a real
# watcher's, and the stop really signals it — and only it: `pgrep`, `lsof` and
# `curl` report nothing but that process. Each run is ended right after the
# restart decision by the bring-up's port-owner refusal (a kubectl
# port-forward with no --context holding :8080), so nothing past it runs.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ ! -r /proc/self/environ ]]; then
    echo "skip: no /proc/<pid>/environ on this platform; up.sh cannot compare Tilt's settings" >&2
    exit 0
fi

status=0
fail() {
    printf 'FAIL: %s\n' "$1" >&2
    shift
    [ "$#" -eq 0 ] || printf '%s\n' "$@" >&2
    status=1
}

# shellcheck source=../lib/space-smoke-prereqs.sh
source "$repo_root/scripts/lib/space-smoke-prereqs.sh"

work="$(mktemp -d)"
fakes=()
cleanup() {
    local pid
    for pid in ${fakes[@]+"${fakes[@]}"}; do
        kill "$pid" 2>/dev/null || true
    done
    rm -rf "$work"
}
trap cleanup EXIT

# The process holding :8080 that ends each run at the port-owner guard.
bash -c 'exec -a kubectl bash -c "sleep 300; true" _ port-forward svc/space-api 8080:8080' >/dev/null 2>&1 &
api_holder=$!
fakes+=("$api_holder")

bin="$work/bin"
mkdir -p "$bin" "$work/home"
stub() { printf '#!/usr/bin/env bash\n%s\n' "$2" >"$bin/$1"; chmod +x "$bin/$1"; }
tilt_pidfile="$work/fake-tilt.pid"
stub docker 'exit 0'
stub kind 'exit 0'                       # no clusters
stub kubectl 'exit 99'
stub openssl 'exit 0'
stub jq 'exit 0'
stub helm '[[ "$*" == "version --short" ]] && echo v3.15.4; exit 0'
stub aws "echo 'aws-cli/$SPACE_AWS_CLI_VERSION Python/3.12'"
stub pgrep "pid=\$(cat '$tilt_pidfile'); kill -0 \"\$pid\" 2>/dev/null && echo \"\$pid\"; exit 0"
stub curl "case \"\$*\" in *localhost:10350*) kill -0 \"\$(cat '$tilt_pidfile')\" 2>/dev/null ;; *) exit 7 ;; esac"
stub lsof "case \"\$*\" in
  *:10350*) pid=\$(cat '$tilt_pidfile'); kill -0 \"\$pid\" 2>/dev/null && echo \"\$pid\" ;;
  *:8080*) echo $api_holder ;;
esac
exit 0"
stub tilt 'case "$*" in
  *"jsonpath={.status.connection.kubernetes.context}"*) printf kind-space-local ;;
  *"jsonpath={.status.error}"*) ;;
  *) exit 64 ;;
esac'

base_env=(PATH="$bin:$PATH" HOME="$work/home" SPACE_KIND_MIN_FREE_GIB=0)

# up_case NAME EXPECT "TILT_ENV ..." "RUN_ENV ..." — EXPECT is restart, reuse
# or refuse. Starts a fake Tilt with TILT_ENV, runs up.sh with RUN_ENV.
up_case() {
    local name="$1" expect="$2" tilt_env run_env
    read -r -a tilt_env <<<"$3"
    read -r -a run_env <<<"$4"
    env -i "${base_env[@]}" ${tilt_env[@]+"${tilt_env[@]}"} \
        bash -c 'exec -a "tilt up" sleep 300' >/dev/null 2>&1 &
    local tilt_pid=$!
    fakes+=("$tilt_pid")
    printf '%s\n' "$tilt_pid" >"$tilt_pidfile"
    sleep 0.2
    if ! ps -p "$tilt_pid" -o command= | grep -q '^tilt up'; then
        echo "skip: this platform does not let exec -a rename the fake tilt; $name not exercised" >&2
        return 0
    fi
    local out="$work/$name.out" rc=0
    env -i "${base_env[@]}" SPACE_TILT_PIDFILE="$work/tilt.pid" SPACE_TILT_STATE_DIR="$work/state" \
        ${run_env[@]+"${run_env[@]}"} \
        bash "$repo_root/infra/space/kind/scripts/up.sh" >"$out" 2>&1 || rc=$?
    sleep 0.2
    (( rc != 0 )) || fail "$name: the stubbed run must end at the port-owner refusal" "$(cat "$out")"
    case "$expect" in
        restart)
            kill -0 "$tilt_pid" 2>/dev/null \
                && fail "$name: Tilt started with other settings must be restarted" "$(cat "$out")"
            grep -q "Refusing to run" "$out" \
                || fail "$name: the run must carry on to the bring-up after the restart" "$(cat "$out")"
            ;;
        reuse)
            kill -0 "$tilt_pid" 2>/dev/null \
                || fail "$name: Tilt must be reused, not restarted" "$(cat "$out")"
            grep -q "Refusing to run" "$out" \
                || fail "$name: the run must carry on to the bring-up" "$(cat "$out")"
            ;;
        refuse)
            kill -0 "$tilt_pid" 2>/dev/null \
                || fail "$name: a refused run must leave the running Tilt running" "$(cat "$out")"
            grep -q "Refusing to run" "$out" \
                && fail "$name: a refused run must stop before the bring-up" "$(cat "$out")"
            ;;
    esac
    kill "$tilt_pid" 2>/dev/null || true
    wait "$tilt_pid" 2>/dev/null || true
}

# 1: a listed setting changed.
up_case datadog-mode restart "" "SPACE_KIND_DATADOG_MODE=fakeintake"
up_case storage-mode restart "" "SPACE_KIND_STORAGE_CREDENTIALS=static"

# 2: nothing else does.
up_case session-token reuse \
    "AWS_ACCESS_KEY_ID=synthetic-key-id AWS_SECRET_ACCESS_KEY=synthetic-secret AWS_SESSION_TOKEN=first" \
    "AWS_ACCESS_KEY_ID=synthetic-key-id AWS_SECRET_ACCESS_KEY=synthetic-secret AWS_SESSION_TOKEN=second"
up_case up-only-knob reuse "" "SPACE_KIND_MIN_FREE_GIB=1"
up_case explicit-default reuse "" "SPACE_KIND_TELEMETRY_MODE=datadog SPACE_TILT_MAX_PARALLEL_UPDATES=8"
up_case static-session-token restart \
    "SPACE_KIND_STORAGE_CREDENTIALS=static AWS_ACCESS_KEY_ID=synthetic-key-id AWS_SECRET_ACCESS_KEY=synthetic-secret AWS_SESSION_TOKEN=first" \
    "SPACE_KIND_STORAGE_CREDENTIALS=static AWS_ACCESS_KEY_ID=synthetic-key-id AWS_SECRET_ACCESS_KEY=synthetic-secret AWS_SESSION_TOKEN=second"

# 3: a refusal comes before the stop.
up_case invalid-storage-mode refuse "" "SPACE_KIND_STORAGE_CREDENTIALS=bogus"
up_case invalid-parallelism refuse "" "SPACE_TILT_MAX_PARALLEL_UPDATES=many"

if [ "$status" -eq 0 ]; then
    echo "ok: up.sh restarts Tilt only for a changed setting Tilt reads, and refuses invalid settings before stopping it"
fi
exit "$status"
