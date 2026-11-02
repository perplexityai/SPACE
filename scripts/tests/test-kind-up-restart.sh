#!/usr/bin/env bash
# up.sh reuses a running Tilt, and restarts it only when it was started with
# other values of the settings Tilt reads once, at start (TILT_SETTINGS in
# up.sh). A restart interrupts whatever else is using the stack — another
# session's smoke run, say — so these cases pin, against the real up.sh:
#
#   1. A changed listed setting restarts Tilt, the Rust build included.
#   2. Nothing else does: not a refreshed AWS_SESSION_TOKEN with exchange
#      storage credentials (nothing reads it then), not a knob only up.sh reads
#      (SPACE_KIND_MIN_FREE_GIB), and not a setting set explicitly to its
#      default. With static storage credentials the AWS variables count.
#   3. A run whose settings up.sh, the bring-up or the Rust build would refuse
#      (an invalid storage mode, Rust build, parallelism or timeout, or the
#      release build with another profile) leaves the running Tilt running:
#      the refusal comes before the stop, not after it.
#   4. So does a reused cluster whose nodes' vendor copies differ from the host
#      vendor tree, including when they differ only from the tree this run
#      stages (a kernel whose pin changed): check-node-vendor.sh runs, for
#      real, before the stop and after the staging.
#   5. So does a reused cluster running the other substrate (its API
#      Deployment's SPACE_K8S_CONTROL_PLANE), which lib/substrate.sh refuses.
#   6. With no Tilt running, an invalid substrate, or the Direct one without
#      static storage credentials, is refused before the bring-up, which
#      would create the cluster that the Tiltfile then refuses.
#
# External commands are PATH stubs, and up.sh runs from a shadow of the
# checkout whose vendor installer and other cluster checks are stubs too; those
# have their own tests. The running Tilt is a real process renamed
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
# The cluster exists only while $work/cluster does. Its nodes carry every
# mount the rendered config declares, so the mount check passes, and their
# vendor copies differ from the host tree when $work/drift exists, or when
# $work/stale-pin does and this run has restaged the tree ($work/restaged).
vendor_root="$work/home/.space-devbox/space/vendor"
data_root="$work/home/.kind-data"
stub docker "case \"\$*\" in
  'ps -a '*) [[ -e '$work/cluster' ]] && printf 'space-local-worker\\nspace-local-builder\\n' ;;
  inspect*)
    for n in control-plane worker1 worker2 worker3 builder builder2; do
      echo \"/var/lib/containerd $data_root/\$n/containerd\"
    done
    for n in 1 2 3 4; do echo '/opt/space-kind/host-vendor $vendor_root'; echo '/dev/kvm /dev/kvm'; done ;;
  'exec '*' diff '*)
    if [[ -e '$work/drift' || ( -e '$work/stale-pin' && -e '$work/restaged' ) ]]; then
      echo 'Files /opt/space-kind/host-vendor/vmlinux and /space/vendor/vmlinux differ'
      exit 1
    fi ;;
esac
exit 0"
stub kind "[[ \"\$*\" == 'get clusters' && -e '$work/cluster' ]] && echo space-local; exit 0"
# Its API Deployment runs the Kubernetes substrate, or the Direct one when
# $work/direct exists; the read also warns on stderr, as kubectl may on a
# successful read, which is not part of the answer.
stub kubectl "case \"\$*\" in
  *' get nodes '*) [[ -e '$work/cluster' ]] && printf 'space-local-worker\\nspace-local-builder\\n' ;;
  *' get deployments -l app.kubernetes.io/name=space-api '*)
    echo 'Warning: synthetic client warning' >&2
    [[ -e '$work/direct' ]] && echo false || echo true ;;
  *) exit 99 ;;
esac"
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

# A shadow of the checkout: every entry a symlink to the real one, except the
# Kind scripts directory, whose entries are symlinks too but can be replaced.
shadow="$work/checkout"
shadow_dir() {
    local rel="$1" real="$repo_root" here="$shadow" part entry name parts
    IFS=/ read -r -a parts <<<"$rel"
    for part in "${parts[@]}" ""; do
        mkdir -p "$here"
        for entry in "$real"/* "$real"/.[!.]*; do
            [[ -e "$entry" || -L "$entry" ]] || continue
            name="${entry##*/}"
            [[ "$name" == "$part" ]] || ln -s "$entry" "$here/$name"
        done
        [[ -n "$part" ]] || break
        real="$real/$part"
        here="$here/$part"
    done
}
shadow_dir infra/space/kind/scripts
shadow_stub() {
    rm -f "$shadow/infra/space/kind/scripts/$1"
    printf '#!/usr/bin/env bash\n%s\n' "$2" >"$shadow/infra/space/kind/scripts/$1"
}
# The tree is staged unless the pin changed and this run has not restaged it.
shadow_stub install-fc-vendor.sh "if [[ \"\${1:-}\" == --check ]]; then
  [[ ! -e '$work/stale-pin' || -e '$work/restaged' ]]
else
  touch '$work/restaged'
fi"
for check in check-postgres-data.sh check-object-store-transition.sh check-kind-node-image.sh check-node-layout.sh; do
    shadow_stub "$check" 'exit 0'
done

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
        bash "$shadow/infra/space/kind/scripts/up.sh" >"$out" 2>&1 || rc=$?
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
up_case rust-build restart "" "SPACE_KIND_RUST_BUILD=release"

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
up_case invalid-timeout refuse "" "SPACE_BOOTSTRAP_TIMEOUT=30m SPACE_KIND_TELEMETRY_MODE=otel"
up_case invalid-rust-build refuse "" "SPACE_KIND_RUST_BUILD=bogus"
grep -q "SPACE_KIND_RUST_BUILD must be test-hooks or release" "$work/invalid-rust-build.out" \
    || fail "invalid-rust-build: the refusal must be lib/rust-build.sh's" "$(cat "$work/invalid-rust-build.out")"
up_case release-other-profile refuse "" "SPACE_KIND_RUST_BUILD=release SPACE_KIND_RUST_PROFILE=dev-release"
grep -q "SPACE_KIND_RUST_BUILD=release builds the release profile" "$work/release-other-profile.out" \
    || fail "release-other-profile: the refusal must be build-rust-artifacts.sh's" "$(cat "$work/release-other-profile.out")"

# 4: so does a reused cluster whose nodes' vendor copies differ.
vendor_case() {
    local name="$1"
    shift
    rm -f "$work/cluster" "$work/drift" "$work/stale-pin" "$work/restaged"
    touch "$@"
    up_case "$name" refuse "" "SPACE_KIND_DATADOG_MODE=fakeintake"
    grep -q "\[check-node-vendor\] ERROR: kind cluster space-local has SPACE nodes" "$work/$name.out" \
        || fail "$name: the refusal must be check-node-vendor.sh's" "$(cat "$work/$name.out")"
    rm -f "$work/cluster" "$work/drift" "$work/stale-pin" "$work/restaged"
}
vendor_case vendor-copy-differs "$work/cluster" "$work/drift"
vendor_case vendor-pin-changed "$work/cluster" "$work/stale-pin"
# Non-vacuity: the same reused cluster with matching copies is restarted.
touch "$work/cluster"
up_case vendor-copy-matches restart "" "SPACE_KIND_DATADOG_MODE=fakeintake"
rm -f "$work/cluster"

# 5: so does a reused cluster running the other substrate.
touch "$work/cluster" "$work/direct"
up_case other-substrate refuse "" "SPACE_KIND_DATADOG_MODE=fakeintake"
grep -q "kind cluster space-local runs the direct substrate" "$work/other-substrate.out" \
    || fail "other-substrate: the refusal must be lib/substrate.sh's" "$(cat "$work/other-substrate.out")"
rm -f "$work/cluster" "$work/direct"

# 6: with no Tilt running, a substrate the Tiltfile would refuse is refused
# before the bring-up.
no_tilt_case() {
    local name="$1" message="$2" run_env out="$work/$1.out" rc=0
    read -r -a run_env <<<"$3"
    : >"$tilt_pidfile"
    env -i "${base_env[@]}" SPACE_TILT_PIDFILE="$work/tilt.pid" SPACE_TILT_STATE_DIR="$work/state" \
        "${run_env[@]}" bash "$shadow/infra/space/kind/scripts/up.sh" >"$out" 2>&1 || rc=$?
    (( rc != 0 )) || fail "$name: the run must be refused" "$(cat "$out")"
    grep -q "Refusing to run" "$out" \
        && fail "$name: the refusal must come before the bring-up" "$(cat "$out")"
    grep -qF "$message" "$out" || fail "$name: the refusal must say why" "$(cat "$out")"
}
no_tilt_case invalid-substrate "SPACE_KIND_SUBSTRATE must be kubernetes or direct" "SPACE_KIND_SUBSTRATE=Direct"
no_tilt_case direct-exchange "SPACE_KIND_SUBSTRATE=direct needs SPACE_KIND_STORAGE_CREDENTIALS=static" \
    "SPACE_KIND_SUBSTRATE=direct"

if [ "$status" -eq 0 ]; then
    echo "ok: up.sh restarts Tilt only for a changed setting Tilt reads, and refuses invalid settings, the release build with another profile, a reused cluster's drifted vendor copies and a reused cluster on the other substrate before stopping it, and a substrate the Tiltfile would refuse before the bring-up"
fi
exit "$status"
