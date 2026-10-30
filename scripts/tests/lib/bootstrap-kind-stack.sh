#!/usr/bin/env bash
#
# Shared idempotent bootstrap for the local KinD + Tilt SPACE stack, the only
# Kind bring-up. Sourced by `infra/space/kind/scripts/up.sh` (the documented
# command), `infra/space/kind/scripts/prepare-cluster.sh`, `smoke-kind-e2e.sh`,
# and by the harness tests `test-kind-localhost-ports.sh`,
# `test-kind-stale-tilt-recovery.sh`, and `test-kind-tombstone-sweep.sh`.
#
# Contract: after `bootstrap_kind_stack` returns successfully, the kind
# cluster `$KIND_CLUSTER` exists, and — when docker is on PATH and the host
# roots are derivable — its bind mounts match the host paths the cluster
# config renders. That check runs in every mode, SKIP_BOOTSTRAP=1 included,
# via `kind_stack_assert_cluster_mounts`; it stands down rather than failing
# when there is no cluster to compare, no docker to ask, or no derivable root
# to compare against, because none of those yields an honest verdict. Also
# guaranteed: Tilt is running, the api-server is
# reachable at `$API_URL`, the template-seed Job has completed, and
# every worker and builder node-service DaemonSet has rolled out and every
# SPACE node is published through ClusterApi and schedulable, the
# cluster-controller Deployment is on its latest generation, Tilt has no
# build pending or in progress and every workload in the namespace has
# finished rolling, every SPACE container runs the latest build of its image
# (recorded in $SPACE_KIND_RUN_DIR/provenance.json; under SKIP_BOOTSTRAP=1
# checked too, unless Tilt still has an update pending), (when DATA_PLANE_URL is
# supplied) the public data plane is reachable, and no node carries a
# tombstone contradicted by kube. A Tilt build that fails ends the bring-up
# with that resource's log instead of consuming the timeout, as does a
# resource Tilt has settled in error and a failed space-db-migrate Job that
# Tilt has finished replacing; a Tilt bound to a deleted cluster is restarted.
# Tilt is started pinned to kind-$KIND_CLUSTER, and only a watcher identified
# as this bring-up's is ever signalled.
#
# Public entry points: `bootstrap_kind_stack`, `kind_stack_prepare_cluster`,
# `kind_stack_recheck_for_scenario` for each scenario of a suite whose runner
# already bootstrapped, plus `kind_stack_sweep_stale_node_tombstones` and
# `kind_stack_psql` for suites that have to repeat a bring-up step in their own
# setup or cleanup.
#
# Inputs (env vars, all optional with defaults set by the caller):
#   API_URL           — health-check URL (e.g. http://localhost:8080)
#   DATA_PLANE_URL    — optional data-plane health-check URL (e.g. http://localhost:8090)
#   KIND_CLUSTER      — kind cluster name (e.g. space-local)
#   SKIP_BOOTSTRAP=1  — skip everything; caller asserts stack is up. The
#                       cluster's storage credentials mode is still compared
#                       with SPACE_KIND_STORAGE_CREDENTIALS, and a mismatch
#                       refused (`__storage_mode_mismatch`,
#                       `__node_storage_keys_mismatch`), and so is a
#                       cluster-controller whose reconcile-queue GC wiring
#                       differs from this checkout's chart
#                       (`__reconcile_queue_gc_mismatch`)
#   SPACE_KIND_STORAGE_CREDENTIALS — exchange (default) or static
#                       (infra/space/kind/lib/storage-credentials.sh); a live
#                       Tilt running the other mode is restarted in this one
#   SPACE_KIND_RUST_BUILD — test-hooks (default) or release
#                       (infra/space/kind/lib/rust-build.sh); compared and
#                       switched like the storage credentials mode
#   SPACE_KIND_SUBSTRATE — kubernetes (default) or direct
#                       (infra/space/kind/lib/substrate.sh); an existing
#                       cluster running the other one is refused, not
#                       switched, under SKIP_BOOTSTRAP=1 too, and direct
#                       needs SPACE_KIND_STORAGE_CREDENTIALS=static
#   BOOTSTRAP_TIMEOUT — seconds each blocking wait may take: Tilt's start, the
#                       API, the seed Job, each rollout and node publication
#   SPACE_KIND_RUN_DIR — the run's output directory, where the bring-up writes
#                       provenance.json, the image every SPACE container runs
#                       against the latest build of it (default: a new
#                       directory under ~/.space-devbox/kind/runs, named by the
#                       time and cluster, which the bring-up sets this to so
#                       the sourcing script can cite the record)
#   KIND_STACK_PROVENANCE_PASSED_FILE — created once the image provenance
#                       check has passed, so a caller that relies on it
#                       (the typed suite runner) learns it was not skipped
#   SPACE_TILT_DETACH_TIMEOUT — registration wait for a detached start (default
#                       60 seconds, at most 240). A timeout leaves the detached
#                       watcher running; inspect its log and retry once ready.
#   KIND_STACK_DETACH=1 — return as soon as the cluster exists and Tilt is
#                       serving its selected context on :10350; do not wait
#                       for the api-server, seed Job, or application rollouts.
#                       Initial profile/backend setup still happens first, but
#                       this call fails promptly if it is not registered yet.
#                       `kind_stack_start_detached` is the entry point.
#
# Globals expected to be set by the caller before sourcing:
#   REPO_ROOT — absolute path to the repo root
#
# Why an explicit Tilt-liveness check on top of `/healthz`: if a
# previous `tilt up` session was killed but its applied pods are
# still running, `/healthz` would return 200 while the workload is
# pinned to whatever image Tilt last shipped. Subsequent code/chart
# changes won't be applied until Tilt is brought back up. So we
# start Tilt whenever it isn't actively running, even if the
# api-server is reachable, then wait for the cluster-controller
# rollout to settle on the latest generation.

# space_kind_storage_credentials, the requested storage credentials mode. The
# bring-up compares it with the cluster's before its SKIP_BOOTSTRAP=1 return,
# where callers need not set REPO_ROOT, so it is found beside this file.
# shellcheck source=../../../infra/space/kind/lib/storage-credentials.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../../infra/space/kind/lib/storage-credentials.sh"
# shellcheck source=../../../infra/space/kind/lib/rust-build.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../../infra/space/kind/lib/rust-build.sh"
# shellcheck source=../../../infra/space/kind/lib/substrate.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../../infra/space/kind/lib/substrate.sh"
# shellcheck source=../../../infra/space/kind/lib/port-forward-supervisors.sh
source "$(dirname "${BASH_SOURCE[0]}")/../../../infra/space/kind/lib/port-forward-supervisors.sh"

# Assert the suites' prerequisites here, at the entry point every kind suite
# already sources, so callers cannot accidentally omit the prerequisite gate.
kind_stack_assert_smoke_prereqs() {
    local lib="$REPO_ROOT/scripts/lib/space-smoke-prereqs.sh"
    if [[ ! -f "$lib" ]]; then
        echo "ERROR: $lib is missing; cannot check the suites' prerequisites" >&2
        exit 1
    fi
    # shellcheck source=../../lib/space-smoke-prereqs.sh
    . "$lib"
    local missing
    missing="$(space_smoke_prereqs_missing_report)"
    if [[ -n "$missing" ]]; then
        echo "ERROR: the kind bring-up and smoke suites need commands this box does not have:" >&2
        printf '       %s\n' "$missing" >&2
        echo "       Provision them with 'bash infra/space/kind/scripts/install-prereqs.sh'." >&2
        exit 1
    fi
    # Presence is not enough for `aws`, and this gate has to say so for the same
    # reason the installer does: a provisioned image can leave a working-looking
    # `/usr/bin/aws` from a package that cannot configure, and a suite that accepts
    # it runs against an install nothing provisioned. The pin is authoritative
    # everywhere or it is authoritative nowhere.
    if ! space_smoke_aws_cli_matches_pin; then
        echo "ERROR: aws is $(aws --version 2>&1 | head -1), not the pinned" >&2
        echo "       $SPACE_AWS_CLI_VERSION. Provision it with" >&2
        echo "       'bash infra/space/kind/scripts/install-prereqs.sh'." >&2
        exit 1
    fi
    return 0
}

bootstrap_kind_stack__api_reachable() {
    curl -sf -m 2 "$API_URL/healthz" >/dev/null 2>&1
}

# Refuse to run if the API port is owned by a `kubectl port-forward`
# to a non-kind cluster. This catches the easy footgun of a stale
# port-forward to prod/testing leaving `/healthz` reachable on
# `localhost:8080` and the smoke test happily driving the wrong
# cluster. Tilt's own port-forward shows up under the `tilt` process
# and is not flagged.
bootstrap_kind_stack__assert_port_owner() {
    # Fail loud if `lsof` is missing — silently accepting any owner
    # defeats the whole point of this safety guard.
    if ! command -v lsof >/dev/null 2>&1; then
        echo "ERROR: lsof not found on PATH — cannot verify localhost API port owner." >&2
        echo "       Refusing to run a destructive smoke test against an unverified API." >&2
        echo "       Install lsof, or set SKIP_BOOTSTRAP=1 only after manually verifying" >&2
        echo "       that localhost:${API_URL##*:} points at the expected kind cluster." >&2
        exit 1
    fi
    local port="${API_URL##*:}"
    # Strip any path suffix (e.g. `8080/healthz` -> `8080`).
    port="${port%%/*}"
    local pids
    pids=$(lsof -ti ":$port" -nP -sTCP:LISTEN 2>/dev/null || true)
    [[ -z "$pids" ]] && return 0

    local pid
    for pid in $pids; do
        local cmd
        cmd=$(ps -p "$pid" -o command= 2>/dev/null || true)
        [[ -z "$cmd" ]] && continue
        # Only flag kubectl port-forwards. Tilt's port-forward uses
        # tilt's own process, not kubectl, so it won't trip this.
        # Match any kubectl invocation that contains the word `port-forward`
        # anywhere in the argument list (covers `kubectl port-forward ...`,
        # `kubectl -n space --context prod port-forward ...`, etc.).
        if [[ "$cmd" =~ kubectl ]] && [[ "$cmd" =~ port-forward ]]; then
            # The `--context` flag must be explicit on the port-forward
            # process command line. If it is absent, we cannot safely
            # determine which cluster the forward targets — a stale
            # forward started against prod without `--context` looks
            # identical after a context switch. Treat implicit-context
            # forwards as unsafe rather than resolving via current-context
            # at smoke-test time (which changes independently).
            local target_context=""
            if [[ "$cmd" =~ --context[[:space:]]+([^[:space:]]+) ]]; then
                target_context="${BASH_REMATCH[1]}"
            elif [[ "$cmd" =~ --context=([^[:space:]]+) ]]; then
                target_context="${BASH_REMATCH[1]}"
            fi
            if [[ "$target_context" != "kind-${KIND_CLUSTER}" ]]; then
                if [[ -z "$target_context" ]]; then
                    echo "ERROR: localhost:$port is owned by a kubectl port-forward with no explicit" >&2
                    echo "       --context flag. Cannot safely determine its target cluster." >&2
                else
                    echo "ERROR: localhost:$port is owned by a kubectl port-forward to '$target_context'," >&2
                    echo "       not the expected kind-${KIND_CLUSTER}." >&2
                fi
                echo "       Refusing to run the smoke test against the wrong cluster." >&2
                echo "       Offending process: PID $pid: $cmd" >&2
                echo "       Kill it (\`kill $pid\`) and retry." >&2
                exit 1
            fi
        fi
    done
}

bootstrap_kind_stack__tilt_running() {
    # A blunt presence gate is fine here — the port and context checks that
    # follow decide — and only here: anything that signals a process goes
    # through `__tilt_pids`.
    pgrep -f "tilt up" >/dev/null 2>&1 \
        && curl -sf -m 2 http://localhost:10350/ >/dev/null 2>&1 || return 1
    # Query the serving Tilt instance, not the caller's kubeconfig or process
    # environment: a watcher for another cluster can still own this port.
    local context
    context=$(timeout 5s tilt get cluster default --port 10350 \
        -o 'jsonpath={.status.connection.kubernetes.context}' 2>/dev/null) || return 1
    [[ "$context" == "kind-${KIND_CLUSTER}" ]]
}

bootstrap_kind_stack__tilt_pid_is_expected() {
    local pid="$1" command
    command="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
    [[ "$command" =~ ^[[:space:]]*([^[:space:]]*/)?tilt[[:space:]]+up([[:space:]]|$) ]]
}

# Every process whose command line IS `tilt up`, oldest first. Never a bare
# `pgrep -f "tilt up"`: that matches any process whose arguments merely contain
# the phrase — a shell running a script that mentions it, an editor, or an
# agent whose prompt does — and the compatibility `pkill -f "tilt up"` this
# library used to carry killed exactly such a process on 2026-09-22: the
# automation driving the bring-up, whose task text named `tilt up`. Presence
# checks and kills below all go through this filter.
bootstrap_kind_stack__tilt_pids() {
    local pid
    for pid in $(pgrep -f "tilt up" 2>/dev/null || true); do
        bootstrap_kind_stack__tilt_pid_is_expected "$pid" || continue
        printf '%s %s\n' "$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')" "$pid"
    done | sort -rn | awk '{ print $2 }'
}

# The `tilt up` this bring-up tracks: the pidfile's, when it names a live
# `tilt up`. Prints nothing otherwise. Never "the oldest `tilt up` on the
# box": that is whatever another project happens to run, and the stops below
# would signal it.
bootstrap_kind_stack__tilt_pid() {
    local state_dir pidfile pid
    state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
    pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
    if [[ -s "$pidfile" ]]; then
        read -r pid <"$pidfile"
        if [[ "$pid" =~ ^[0-9]+$ ]] && bootstrap_kind_stack__tilt_pid_is_expected "$pid"; then
            printf '%s\n' "$pid"
            return 0
        fi
    fi
    return 0
}

# Epoch seconds at which a process started; fails when unknown.
bootstrap_kind_stack__process_start_epoch() {
    local started
    started="$(ps -o lstart= -p "$1" 2>/dev/null)" || return 1
    [[ -n "$started" ]] || return 1
    date -d "$started" +%s 2>/dev/null
}

# Epoch seconds at which the current cluster's control-plane container was
# created; fails when docker cannot say (no docker, no such container).
bootstrap_kind_stack__cluster_created_epoch() {
    command -v docker >/dev/null 2>&1 || return 1
    local created
    created="$(docker inspect -f '{{.Created}}' "${KIND_CLUSTER}-control-plane" 2>/dev/null)" || return 1
    [[ -n "$created" ]] || return 1
    date -d "$created" +%s 2>/dev/null
}

# A responsive Tilt whose context matches can still be bound to a cluster that
# no longer exists. `kind delete` + `kind create` gives the new apiserver a new
# host port, and client-go loaded the old one when Tilt started; the context
# name is the same either way, so `__tilt_running` is satisfied while every
# apply Tilt attempts fails. Seen on a pool devbox on 2026-09-22: a `tilt up`
# from four days earlier held :10350, the bring-up read "tilt is running", and
# the api-server wait ran out its full 1800 s. Two independent signals mark that
# Tilt stale: it reports a connection error itself, or it started before the
# current control-plane container was created — a client cannot be bound to a
# server that did not exist yet. When neither can be determined the answer is
# "not stale": reusing a live watcher stays the default.
bootstrap_kind_stack__tilt_stale_for_cluster() {
    local pid tilt_started cluster_created error
    error="$(timeout 5s tilt get cluster default --port 10350 -o 'jsonpath={.status.error}' 2>/dev/null)" || error=""
    if [[ -n "$error" ]]; then
        echo "[bootstrap] tilt reports a cluster connection error: $error"
        return 0
    fi
    pid="$(bootstrap_kind_stack__served_tilt_pid)"
    [[ -n "$pid" ]] || return 1
    tilt_started="$(bootstrap_kind_stack__process_start_epoch "$pid")" || return 1
    cluster_created="$(bootstrap_kind_stack__cluster_created_epoch)" || return 1
    if (( tilt_started < cluster_created )); then
        echo "[bootstrap] tilt (pid $pid, started at epoch $tilt_started) predates the current kind" \
            "cluster (control-plane created at epoch $cluster_created): it is bound to the previous apiserver"
        return 0
    fi
    return 1
}

# The `tilt up` serving :10350 — the watcher whose context `__tilt_running`
# just read — or, when the listener cannot be resolved (a proxy holds the port,
# or another user's process lsof cannot see), the pidfile's verified watcher.
# Prints nothing when neither resolves.
bootstrap_kind_stack__served_tilt_pid() {
    bootstrap_kind_stack__tilt_listener_pid || bootstrap_kind_stack__tilt_pid
}

# The directory a `tilt up` runs in (the pid given, or the serving one), which
# is the checkout it watches and builds. Fails when that is unknown (no such
# process, or no /proc to ask). A checkout deleted since Tilt started reads back
# with a " (deleted)" suffix, so it never matches a live path.
bootstrap_kind_stack__tilt_cwd() {
    local pid="${1:-}"
    [[ -n "$pid" ]] || pid="$(bootstrap_kind_stack__served_tilt_pid)"
    [[ -n "$pid" ]] || return 1
    readlink "/proc/$pid/cwd" 2>/dev/null
}

# A live Tilt serving this cluster from another checkout rebuilds that
# checkout's files, never this one's, so reusing it validates someone else's
# code while every edit here goes undeployed. Seen in the 2026-09-23 reuse
# bench: `kind` from a fresh worktree found the previous run's watcher, returned
# 0 in 8 s, said "Tilt is watching this checkout", and deployed none of its
# edits. When the watcher's directory cannot be read the answer is "no": reusing
# a live watcher stays the default.
bootstrap_kind_stack__tilt_watches_other_checkout() {
    local cwd here
    cwd="$(bootstrap_kind_stack__tilt_cwd)" || return 1
    [[ -n "$cwd" ]] || return 1
    here="$(cd "$REPO_ROOT" && pwd -P)" || return 1
    [[ "$cwd" != "$here" ]] || return 1
    echo "[bootstrap] tilt (pid $(bootstrap_kind_stack__served_tilt_pid)) is watching $cwd, not this checkout ($here)"
}

# A responsive :10350 that `__tilt_running` rejected — no context, or a context
# that is not ours — is usually another watcher and must not be touched. One
# shape is ours and dead: a `tilt up` the devbox's own setup step started that
# never connected to any cluster (the cluster did not exist yet, or was deleted
# under it before its first connection), so its connection context is EMPTY
# and `status.error` is terminal. `__tilt_stale_for_cluster` would call that
# stale, but it is consulted only after `__tilt_running` succeeds, so the
# bring-up took the "absent" branch, whose `__kill_stale_tilt` refuses any
# responsive server and exits 1 — a permanent wedge until someone kills the
# process by hand (seen on a pool devbox, 2026-09-23). The terminal error is
# required, not just the empty context: a watcher that is still initializing
# also has no context yet and no error, and restarting it would be the very
# thing this library exists not to do. A context query that fails is not an
# observed empty context, and keeps the refusal. Ownership is decided before
# anything is signalled, and for the process that holds :10350, since that is
# the watcher whose status was just read; with several watchers on the box the
# pidfile's or the oldest `tilt up` can be a healthy one on another port. The
# listener is ours when the pidfile names it or it was started from this
# checkout. A watcher with someone else's context, one still connecting, or one
# we cannot tie to this repo, keeps the refusal. Prints the listener's pid for
# `__stop_stale_tilt`; the verdict goes to stderr.
bootstrap_kind_stack__tilt_orphaned() {
    curl -sf -m 2 http://localhost:10350/ >/dev/null 2>&1 || return 1
    local context error
    context="$(timeout 5s tilt get cluster default --port 10350 \
        -o 'jsonpath={.status.connection.kubernetes.context}' 2>/dev/null)" || return 1
    error="$(timeout 5s tilt get cluster default --port 10350 -o 'jsonpath={.status.error}' 2>/dev/null)" || error=""
    [[ -n "$error" ]] || return 1
    if [[ -n "$context" && "$context" != "kind-${KIND_CLUSTER}" ]]; then
        return 1
    fi
    local pid
    pid="$(bootstrap_kind_stack__tilt_listener_pid)" || return 1
    local state_dir pidfile named=""
    state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
    pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
    [[ -s "$pidfile" ]] && read -r named <"$pidfile"
    local here
    here="$(cd "$REPO_ROOT" && pwd -P)" || return 1
    if [[ "$named" != "$pid" && "$(bootstrap_kind_stack__tilt_cwd "$pid" || true)" != "$here" ]]; then
        return 1
    fi
    echo "[bootstrap] tilt (pid $pid) is ours but never connected to a cluster" \
        "(context '${context:-<none>}'${error:+, error: $error}); restarting it" >&2
    printf '%s\n' "$pid"
}

# The `tilt up` listening on :10350. Fails when the listener is not a
# `tilt up` or cannot be resolved; `__assert_port_owner` has already required
# lsof by the time the bring-up asks.
bootstrap_kind_stack__tilt_listener_pid() {
    local pid
    for pid in $(lsof -ti :10350 -nP -sTCP:LISTEN 2>/dev/null || true); do
        bootstrap_kind_stack__tilt_pid_is_expected "$pid" || continue
        printf '%s\n' "$pid"
        return 0
    done
    return 1
}

# Stop the `tilt up` this bring-up verified as ours and not reusable (the pid
# given, or the tracked one), then wait for :10350 to free. Unlike
# `__kill_stale_tilt` this may take down a responsive server: the caller has
# established that it serves kind-$KIND_CLUSTER and is either bound to a
# cluster that no longer exists, watching another checkout, running the other
# storage credentials mode, or started with other settings (up.sh), or that it
# is ours and never connected to any cluster (`__tilt_orphaned`).
#
# Only an identified process is signalled: the pid given (the :10350 listener,
# resolved by the caller) or the pidfile's verified watcher. When neither
# resolves and :10350 still answers, nothing is signalled and the bring-up
# refuses.
bootstrap_kind_stack__stop_stale_tilt() {
    local pid="${1:-}" state_dir pidfile
    [[ -n "$pid" ]] || pid="$(bootstrap_kind_stack__tilt_pid)"
    state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
    pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
    if [[ -z "$pid" ]]; then
        if curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
            echo "ERROR: the watcher on :10350 has to be restarted, but its process cannot be identified:" >&2
            echo "       it is not a \`tilt up\` lsof can see, and the pidfile does not name one." >&2
            echo "       Stop the watcher on :10350, then rerun." >&2
            return 1
        fi
        rm -f "$pidfile"
        return 0
    fi
    echo "[bootstrap] stopping stale tilt (pid $pid) before starting fresh"
    kill "$pid" 2>/dev/null || true
    rm -f "$pidfile"
    local deadline=$(( $(date +%s) + 20 ))
    while [[ $(date +%s) -lt $deadline ]]; do
        if ! curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
    sleep 1
    if curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
        echo "ERROR: stale tilt still holds :10350; stop it manually and retry" >&2
        return 1
    fi
    return 0
}

# Every Tilt resource and its update status, one `name status` per line.
bootstrap_kind_stack__tilt_update_statuses() {
    timeout 10s tilt get uiresources --port 10350 \
        -o 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.updateStatus}{"\n"}{end}' 2>/dev/null
}

# Resources whose last update failed, one name per line.
bootstrap_kind_stack__tilt_failed_resources() {
    bootstrap_kind_stack__tilt_update_statuses | awk '$2 == "error" { print $1 }'
}

# Resources Tilt is still updating, space-separated on one line.
bootstrap_kind_stack__tilt_busy_resources() {
    bootstrap_kind_stack__tilt_update_statuses \
        | awk '$2 == "pending" || $2 == "in_progress" { print $1 }' | tr '\n' ' '
}

# Tilt does not retry a failed build on its own — only a watched-file change or
# `tilt trigger` does — so once one appears during a cold bring-up nothing the
# waits below are waiting for can arrive. The honest outcome is that failure
# now, with the resource's log, rather than a timeout half an hour later that
# names the api-server. Two consecutive sightings, not one: a single poll can
# catch a resource between an error and the retrigger a file change causes.
# Callers poll this at $BOOTSTRAP_TILT_FAILCHECK_INTERVAL seconds (default 15).
BOOTSTRAP_KIND_STACK__FAILED_SEEN=""
BOOTSTRAP_KIND_STACK__FAILED_CHECKED_AT=0
bootstrap_kind_stack__die_on_failed_builds() {
    local now interval="${BOOTSTRAP_TILT_FAILCHECK_INTERVAL:-15}"
    now=$(date +%s)
    (( now - BOOTSTRAP_KIND_STACK__FAILED_CHECKED_AT >= interval )) || return 0
    BOOTSTRAP_KIND_STACK__FAILED_CHECKED_AT=$now
    local failed
    failed="$(bootstrap_kind_stack__tilt_failed_resources)"
    if [[ -z "$failed" ]]; then
        BOOTSTRAP_KIND_STACK__FAILED_SEEN=""
        return 0
    fi
    if [[ "$BOOTSTRAP_KIND_STACK__FAILED_SEEN" != "$failed" ]]; then
        BOOTSTRAP_KIND_STACK__FAILED_SEEN="$failed"
        return 0
    fi
    echo "ERROR: Tilt reports failed builds; nothing else this bring-up waits for can complete:" >&2
    bootstrap_kind_stack__report_failed_builds "$failed"
}

# Print each failed resource's log tail and the remedy, then exit 1.
bootstrap_kind_stack__report_failed_builds() {
    local res
    while IFS= read -r res; do
        [[ -n "$res" ]] || continue
        echo "----- tilt logs $res (tail)" >&2
        timeout 15s tilt logs "$res" --port 10350 2>/dev/null | tail -n 40 >&2 || true
    done <<<"$1"
    echo "       Fix the cause, then 'tilt trigger <resource>' (Tilt does not retry on its own)" >&2
    echo "       or rerun this bring-up. Full log: ${SPACE_TILT_LOG:-$HOME/.space-devbox/kind/tilt-${KIND_CLUSTER}.log}" >&2
    exit 1
}

# The waits in `__bring_up` cover the api-server, the seed Job, the node stacks
# and the cluster-controller — not every image. On three cold runs the
# data-plane image finished about 90 s after the bring-up returned and rolled a
# new ReplicaSet under the first typed scenarios, which then saw their connects
# refused. Wait until Tilt has nothing pending or in progress, then for every
# workload rollout in the namespace, so "bring-up returned" means "stack
# settled" and no caller needs its own ReplicaSet-age heuristic.
#
# Quiet is not success. On a warm stack a change whose build fails leaves the
# old pods healthy, so every rollout below passes against the previous
# Deployments; a resource still in error once nothing is pending is that
# failure, and ends the bring-up with its log. Two sightings a poll apart, as
# in `__die_on_failed_builds`, so a file change that is about to retrigger it
# is not mistaken for a settled error.
bootstrap_kind_stack__wait_for_tilt_quiet() {
    local deadline=$(( $(date +%s) + BOOTSTRAP_TIMEOUT ))
    local busy failed settled_failed="" now last_log_at=0
    echo "[bootstrap] waiting for Tilt to finish every pending build"
    while :; do
        busy="$(bootstrap_kind_stack__tilt_busy_resources)"
        if [[ -z "${busy// /}" ]]; then
            failed="$(bootstrap_kind_stack__tilt_failed_resources)"
            if [[ -z "$failed" ]]; then
                # A Job failure is not an update status: wait for the
                # migration check below to confirm it rather than settle.
                bootstrap_kind_stack__failed_migration_uid >/dev/null || break
                busy="(settling: space-db-migrate failed)"
            elif [[ "$failed" == "$settled_failed" ]]; then
                echo "ERROR: Tilt finished updating with failed resources; the stack runs their previous build:" >&2
                bootstrap_kind_stack__report_failed_builds "$failed"
            else
                settled_failed="$failed"
                busy="(settling: $(tr '\n' ' ' <<<"$failed"))"
            fi
        else
            settled_failed=""
            bootstrap_kind_stack__die_on_failed_builds
        fi
        bootstrap_kind_stack__die_on_failed_migration
        now=$(date +%s)
        if (( now >= deadline )); then
            echo "ERROR: Tilt still building after ${BOOTSTRAP_TIMEOUT}s: $busy" >&2
            exit 1
        fi
        if (( now - last_log_at >= 30 )); then
            echo "  ...tilt still updating: ${busy}(timeout in $(( deadline - now ))s)"
            last_log_at=$now
        fi
        sleep 5
    done
    echo "[bootstrap] waiting for every workload rollout in namespace space"
    local workload
    while IFS= read -r workload; do
        [[ -n "$workload" ]] || continue
        if ! kubectl --context "kind-${KIND_CLUSTER}" -n space rollout status "$workload" --timeout="${BOOTSTRAP_TIMEOUT}s" >/dev/null; then
            echo "ERROR: $workload did not finish rolling within ${BOOTSTRAP_TIMEOUT}s" >&2
            kubectl --context "kind-${KIND_CLUSTER}" -n space get "$workload" -o wide >&2 || true
            exit 1
        fi
    done < <(kubectl --context "kind-${KIND_CLUSTER}" -n space get deploy,statefulset,daemonset -o name 2>/dev/null)
}

# Settled is not the same as current: a Tilt stopped between a build and its
# rollout, or a workload pinned to an earlier image, leaves pods on a build
# that is no longer the latest, and every wait above passes. Compare every
# SPACE container's image with the last build of it (image-provenance.py),
# refuse a mismatch naming the workload, and keep the record in the run's
# output directory for a report to cite.
bootstrap_kind_stack__check_provenance() {
    if [[ -z "${SPACE_KIND_RUN_DIR:-}" ]]; then
        # mktemp, so two runs starting in the same second keep two records.
        local runs="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}/runs"
        mkdir -p "$runs"
        SPACE_KIND_RUN_DIR="$(mktemp -d "$runs/$(date -u +%Y%m%dT%H%M%SZ)-${KIND_CLUSTER}.XXXXXX")"
    fi
    echo "[bootstrap] checking that every SPACE pod runs the latest build of its image"
    if ! python3 "$REPO_ROOT/infra/space/kind/scripts/image-provenance.py" check \
            --cluster "$KIND_CLUSTER" --output "$SPACE_KIND_RUN_DIR/provenance.json"; then
        exit 1
    fi
    if [[ -n "${KIND_STACK_PROVENANCE_PASSED_FILE:-}" ]]; then
        : >"$KIND_STACK_PROVENANCE_PASSED_FILE"
    fi
}

# Under SKIP_BOOTSTRAP=1, the mode a live stack is used in and so where a
# workload left on an earlier build after an edit is most likely met, the same
# check runs once Tilt has nothing pending or in progress. While Tilt is still
# updating, a mismatch may be the rollout it is about to make, so the check is
# skipped with a warning rather than refused. No cluster, nothing to check:
# tested first, as the mount check does, so a box without one writes nothing.
bootstrap_kind_stack__check_live_provenance() {
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0
    # A Tilt that does not answer has nothing pending either: a stopped Tilt
    # is how a workload is left on an earlier build, so the check runs.
    local busy
    busy="$(bootstrap_kind_stack__tilt_busy_resources)" || busy=""
    busy="${busy% }"
    if [[ -n "${busy// /}" ]]; then
        echo "[bootstrap] WARNING: Tilt is still updating ${busy}; not checking which build each" \
            "SPACE pod runs. Rerun once \`tilt get uiresources\` shows nothing pending." >&2
        return 0
    fi
    bootstrap_kind_stack__check_provenance
}

# The space-db-migrate Job makes one attempt (backoffLimit 0) and gates the
# api-server, so once it has failed nothing the API wait waits for can arrive.
#
# Only the current Job's failure is final. A Failed Job stays in the cluster
# until Tilt replaces it, and after a user fixes `db/` and reruns, Tilt may
# still be rebuilding the image or replacing the Job: that old Job's logs are
# not this run's verdict. So a Job's failure counts once Tilt's resource of
# the same name is idle (neither pending nor in progress), and only when the
# same Job is seen failed that way on two checks a poll apart, which also
# covers a file change Tilt has not registered yet. Callers poll
# `__die_on_failed_migration` (and `__die_on_failed_job` for the seed and
# bucket-init Jobs) at $BOOTSTRAP_TILT_FAILCHECK_INTERVAL seconds (default 15).
#
# The uid of Job $1 when it has failed and Tilt is not updating its resource;
# fails otherwise.
bootstrap_kind_stack__failed_job_uid() {
    local name="$1" job uid conditions status
    job="$(kubectl --context "kind-${KIND_CLUSTER}" -n space get job "$name" \
        -o jsonpath='{.metadata.uid}{" "}{range .status.conditions[?(@.status=="True")]}{.type}{" "}{end}' 2>/dev/null)" \
        || return 1
    read -r uid conditions <<<"$job"
    [[ -n "$uid" && " $conditions " == *" Failed "* ]] || return 1
    status="$(bootstrap_kind_stack__tilt_update_statuses | awk -v r="$name" '$1 == r { print $2 }')"
    [[ -n "$status" && "$status" != pending && "$status" != in_progress ]] || return 1
    printf '%s\n' "$uid"
}

bootstrap_kind_stack__failed_migration_uid() {
    bootstrap_kind_stack__failed_job_uid space-db-migrate
}

declare -gA BOOTSTRAP_KIND_STACK__JOB_FAILED_SEEN=()
declare -gA BOOTSTRAP_KIND_STACK__JOB_CHECKED_AT=()
# End the bring-up once Job $1 has failed as described above, with its logs;
# $2 says what that failure blocks.
bootstrap_kind_stack__die_on_failed_job() {
    local name="$1" consequence="${2:-}" now interval="${BOOTSTRAP_TILT_FAILCHECK_INTERVAL:-15}" uid
    now=$(date +%s)
    (( now - ${BOOTSTRAP_KIND_STACK__JOB_CHECKED_AT[$name]:-0} >= interval )) || return 0
    BOOTSTRAP_KIND_STACK__JOB_CHECKED_AT[$name]=$now
    if ! uid="$(bootstrap_kind_stack__failed_job_uid "$name")"; then
        BOOTSTRAP_KIND_STACK__JOB_FAILED_SEEN[$name]=""
        return 0
    fi
    if [[ "${BOOTSTRAP_KIND_STACK__JOB_FAILED_SEEN[$name]:-}" != "$uid" ]]; then
        BOOTSTRAP_KIND_STACK__JOB_FAILED_SEEN[$name]="$uid"
        return 0
    fi
    echo "ERROR: the $name Job failed${consequence:+, so $consequence}. Its logs:" >&2
    kubectl --context "kind-${KIND_CLUSTER}" -n space logs "job/$name" \
        --all-containers --prefix --tail=50 >&2 || true
    echo "       Fix the cause, then 'tilt trigger $name' to run it again." >&2
    exit 1
}

bootstrap_kind_stack__die_on_failed_migration() {
    bootstrap_kind_stack__die_on_failed_job space-db-migrate "the api-server cannot start"
}

# Wait for Job $1 to complete, up to $2 seconds, failing on its failure (see
# `__die_on_failed_job`), a failed build or a failed migration instead of the
# timeout. A plain `kubectl wait` diagnosed none of them until it expired.
bootstrap_kind_stack__wait_for_job() {
    local name="$1" timeout="$2" deadline conditions now last_log_at
    deadline=$(( $(date +%s) + timeout ))
    last_log_at=$(date +%s)
    while :; do
        conditions="$(kubectl --context "kind-${KIND_CLUSTER}" -n space get job "$name" \
            -o jsonpath='{.metadata.uid}{" "}{range .status.conditions[?(@.status=="True")]}{.type}{" "}{end}' 2>/dev/null)" \
            || conditions=""
        [[ " $conditions " == *" Complete "* ]] && return 0
        bootstrap_kind_stack__die_on_failed_job "$name"
        bootstrap_kind_stack__die_on_failed_builds
        bootstrap_kind_stack__die_on_failed_migration
        now=$(date +%s)
        if (( now >= deadline )); then
            echo "ERROR: the $name Job did not complete within ${timeout}s" >&2
            kubectl --context "kind-${KIND_CLUSTER}" get jobs -n space >&2 || true
            kubectl --context "kind-${KIND_CLUSTER}" logs "job/$name" -n space --tail=50 >&2 || true
            exit 1
        fi
        if (( now - last_log_at >= 30 )); then
            echo "  ...waiting for the $name Job (timeout in $(( deadline - now ))s)"
            last_log_at=$now
        fi
        sleep 2
    done
}

# Every SPACE node published through ClusterApi and schedulable.
bootstrap_kind_stack__wait_node_publication() {
    SPACE_KIND_CLUSTER="$KIND_CLUSTER" SPACE_KIND_API_URL="$API_URL" \
        bash "$REPO_ROOT/infra/space/kind/scripts/wait-node-publication.sh" "$BOOTSTRAP_TIMEOUT" || exit 1
}

bootstrap_kind_stack__wait_for_api() {
    local deadline=$(( $(date +%s) + BOOTSTRAP_TIMEOUT ))
    local last_log_at=0
    while [[ $(date +%s) -lt $deadline ]]; do
        if bootstrap_kind_stack__api_reachable; then
            return 0
        fi
        bootstrap_kind_stack__die_on_failed_builds
        bootstrap_kind_stack__die_on_failed_migration
        local now
        now=$(date +%s)
        if (( now - last_log_at >= 15 )); then
            echo "  ...waiting for $API_URL/healthz (timeout in $(( deadline - now ))s)"
            last_log_at=$now
        fi
        sleep 2
    done
    return 1
}

bootstrap_kind_stack__start_tilt() {
    local state_dir log pidfile tilt_pid
    local startup_timeout="${BOOTSTRAP_TIMEOUT:-1800}"
    if [[ "${KIND_STACK_DETACH:-0}" == 1 ]]; then
        startup_timeout="${SPACE_TILT_DETACH_TIMEOUT:-60}"
    fi
    [[ "$startup_timeout" =~ ^[1-9][0-9]*$ ]] || {
        echo "ERROR: Tilt startup timeout must be a positive integer number of seconds" >&2
        return 2
    }
    if [[ "${KIND_STACK_DETACH:-0}" == 1 && "$startup_timeout" -gt 240 ]]; then
        echo "ERROR: SPACE_TILT_DETACH_TIMEOUT must not exceed 240 seconds" >&2
        return 2
    fi
    state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
    log="${SPACE_TILT_LOG:-$state_dir/tilt-${KIND_CLUSTER}.log}"
    pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
    mkdir -p "$state_dir"
    echo "[bootstrap] starting tilt in background; logs → $log"
    echo "[bootstrap] tilt UI: http://localhost:10350 (per-resource build progress)"
    tilt_pid=$(
        cd "$REPO_ROOT" || exit
        # The smokes drive the full production-shaped harness on the
        # multi-worker cluster: KubernetesSubstrate + shim + cluster-controller.
        # The Tiltfile defaults to exactly this, but both knobs stay explicit so
        # a caller's environment cannot flip the bench under a running suite.
        #
        # `setsid`: Tilt must outlive whoever started it. `nohup` alone only
        # ignores SIGHUP; a caller killed by process group -- the devbox setup
        # agent SIGKILLs a step's whole group at its 5-minute deadline, and
        # `devbox exec` tears its group down on exit -- takes Tilt with it,
        # leaving half-built images and a cluster nobody is reconciling
        # (measured on a cold devbox: `tilt up` died mid-compile at exactly
        # 300s). A new session puts Tilt in its own process group.
        #
        # `--context`: the Tiltfile's allow_k8s_contexts only permits a
        # context, it does not select one, so a plain `tilt up` would apply
        # the stack to whatever context the kubeconfig currently names.
        TILT_BROWSER_DISABLED=1 SPACE_KIND_CLUSTER="$KIND_CLUSTER" \
            setsid nohup tilt up --context "kind-${KIND_CLUSTER}" >"$log" 2>&1 </dev/null &
        pid=$!
        printf '%s\n' "$pid" >"$pidfile"
        printf '%s\n' "$pid"
    )

    # Cluster registration follows Tiltfile evaluation. The selected profile
    # reconciles receivers before returning producer manifests, including image
    # loading and bounded Helm waits on a cold start. This is initialization,
    # not merely a daemon-listen handshake. Blocking starts use the bootstrap
    # budget; detached starts have a separate short bound so the caller is not
    # held past its setup-step deadline. Never claim readiness without the
    # selected context, and never kill the detached watcher on a timeout.
    local tilt_deadline=$(( $(date +%s) + startup_timeout ))
    local error="" evaluation_error now last_log_at=0
    while [[ $(date +%s) -lt $tilt_deadline ]]; do
        if ! kill -0 "$tilt_pid" 2>/dev/null; then
            error="Tilt exited before registering context kind-${KIND_CLUSTER}"
            break
        fi
        if bootstrap_kind_stack__tilt_running; then
            return 0
        fi
        # A failed evaluation leaves the Tilt server alive but cannot register
        # its cluster. Do not consume the entire budget on that terminal result.
        evaluation_error="$(timeout 5s tilt get tiltfile '(Tiltfile)' --port 10350 \
            -o 'jsonpath={.status.terminated.error}' 2>/dev/null)" || evaluation_error=""
        if [[ -n "$evaluation_error" ]]; then
            error="Tiltfile initialization failed: $evaluation_error"
            break
        fi
        now=$(date +%s)
        if (( now - last_log_at >= 30 )); then
            echo "[bootstrap] waiting for Tiltfile initialization and context kind-${KIND_CLUSTER}" \
                "(timeout in $(( tilt_deadline - now ))s)"
            last_log_at=$now
        fi
        sleep 2
    done
    echo "ERROR: ${error:-tilt did not report context kind-${KIND_CLUSTER} on :10350 within ${startup_timeout}s}" >&2
    tail -n 60 "$log" >&2 || true
    echo "  inspect: $log" >&2
    if [[ "${KIND_STACK_DETACH:-0}" == 1 ]] && kill -0 "$tilt_pid" 2>/dev/null; then
        echo "  detached Tilt remains running; inspect initialization and retry when ready" >&2
    fi
    return 1
}

bootstrap_kind_stack__kill_stale_tilt() {
    # The caller already failed the context-aware liveness check. A responsive
    # server may belong to another cluster or still be initializing; do not kill
    # it or mistake it for the watcher this bring-up is meant to start.
    if curl -sf -m 2 http://localhost:10350/ >/dev/null 2>&1; then
        echo "ERROR: Tilt on :10350 could not be verified as kind-${KIND_CLUSTER}." >&2
        echo "       Inspect 'tilt get cluster default --port 10350 -o yaml'." >&2
        echo "       Wait for it to initialize, or stop the other watcher and retry." >&2
        return 1
    fi
    # Only a watcher positively identified as this bring-up's is signalled:
    # the one the pidfile names, or a `tilt up` started from this checkout
    # (sessions from before the pidfile existed) that does not name another
    # context. Any other `tilt up` on the box — another project's, or one on
    # another port — is left running. Candidates are only processes whose
    # command line is `tilt up` itself (see __tilt_pids), never `pkill -f`.
    local stale_pids
    stale_pids="$(bootstrap_kind_stack__tilt_pids)"
    [[ -n "$stale_pids" ]] || return 0
    local state_dir pidfile named="" here tilt_pid command ours=()
    state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
    pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
    [[ -s "$pidfile" ]] && read -r named <"$pidfile"
    here="$(cd "$REPO_ROOT" && pwd -P)" || here=""
    for tilt_pid in $stale_pids; do
        command="$(ps -p "$tilt_pid" -o command= 2>/dev/null)" || continue
        if [[ "$tilt_pid" == "$named" ]]; then
            ours+=("$tilt_pid")
        elif [[ "$command" =~ --context[[:space:]=]([^[:space:]]+) \
                && "${BASH_REMATCH[1]}" != "kind-${KIND_CLUSTER}" ]]; then
            echo "[bootstrap] leaving 'tilt up' (pid $tilt_pid) running: it serves ${BASH_REMATCH[1]}"
        elif [[ -n "$here" && "$(bootstrap_kind_stack__tilt_cwd "$tilt_pid" || true)" == "$here" ]]; then
            ours+=("$tilt_pid")
        else
            echo "[bootstrap] leaving 'tilt up' (pid $tilt_pid) running: not started from this checkout"
        fi
    done
    rm -f "$pidfile"
    (( ${#ours[@]} > 0 )) || return 0
    echo "[bootstrap] stale 'tilt up' detected (pid ${ours[*]}) — stopping it before starting fresh"
    kill "${ours[@]}" 2>/dev/null || true
    local kill_deadline=$(( $(date +%s) + 15 )) alive
    while [[ $(date +%s) -lt $kill_deadline ]]; do
        alive=""
        for tilt_pid in "${ours[@]}"; do
            bootstrap_kind_stack__tilt_pid_is_expected "$tilt_pid" && alive=1
        done
        if [[ -z "$alive" ]] && ! curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    if curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
        echo "ERROR: failed to free port 10350; kill manually and retry" >&2
        return 1
    fi
    return 0
}

# Stop the `kubectl port-forward` supervisors (and their kubectl children)
# that a previous bring-up of THIS kind context left behind. The retired
# scripted bring-up left them running under `setsid` on :3000, :8333 and the
# telemetry ports, where they keep Tilt's own forwards from binding. Called
# just before a fresh Tilt starts, so no watcher of ours is running; a
# supervisor whose parent is a `tilt up` is a live Tilt's serve_cmd (the
# Tiltfile's observability forwards, perhaps another port's watcher) and is
# left alone. A supervisor is recognised by its whole command line
# (lib/port-forward-supervisors.sh), and only its own kubectl children go with
# it. Never `pkill -f '<label>|--context <ctx>.*port-forward'`: that unanchored
# alternation matched any process whose arguments merely mention a context and
# "port-forward" — on 2026-09-22 an automation runner whose task text did was
# SIGTERMed by the bring-up it had just started — and the label alone still
# matches a search whose last argument it is. A forward nobody's supervisor
# owns, or one to another context, is never touched (the suites' port-owner
# guard exists for that footgun). The scripted bring-up's pidfile, shared by
# every cluster on the host, is removed once nothing it lists is a live
# supervisor of any context, so teardown never meets a pid that has since been
# reused.
kind_stack_reap_stale_port_forwards() {
    local kctx="kind-${KIND_CLUSTER}" pid parent pidfile live="" reaped=() attempt
    for pid in $(pgrep -f "space-kind-pf:${kctx}" 2>/dev/null || true); do
        space_kind_pf_is_supervisor "$pid" "$kctx" || continue
        parent="$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')"
        [[ -n "$parent" ]] && bootstrap_kind_stack__tilt_pid_is_expected "$parent" && continue
        echo "[bootstrap] stopping stale kind port-forward supervisor (pid $pid): $(ps -o command= -p "$pid" 2>/dev/null | cut -c1-80)"
        space_kind_pf_stop_supervisor "$pid" "$kctx"
        reaped+=("$pid")
    done
    # Let the stopped loops exit before the pidfile is judged.
    for ((attempt = 0; attempt < 20 && ${#reaped[@]} > 0; attempt++)); do
        kill -0 "${reaped[@]}" 2>/dev/null || break
        sleep 0.25
    done
    pidfile="$(space_kind_pf_pidfile)"
    [[ -s "$pidfile" ]] || return 0
    while IFS= read -r pid; do
        space_kind_pf_is_supervisor "$pid" && live=1
    done <"$pidfile"
    [[ -n "$live" ]] || rm -f "$pidfile"
}

# Postgres in the kind cluster. Every piece of control-plane state a client can
# reach goes through the real interfaces instead, so this exists for exactly the
# state that has no interface: `node_k8s_tombstones` is written by the drain
# reconciler and read by placement, and nothing anywhere deletes a row.
# Resolve the credentials once, both keys in one read. Callers invoke this
# before a command substitution so the cache lands in their shell: resolving
# lazily inside `$(kind_stack_psql …)` would discard it with the subshell and
# re-read the secret on every query.
kind_stack_psql_resolve_creds() {
    [[ -n "${KIND_STACK_PG_USER:-}" && -n "${KIND_STACK_PG_DB:-}" ]] && return 0
    local creds
    creds=$(kubectl --context "kind-${KIND_CLUSTER}" -n "${NAMESPACE:-space}" \
        get secret space-postgres -o go-template='{{index .data "POSTGRES_USER" | base64decode}} {{index .data "POSTGRES_DB" | base64decode}}') \
        || return 1
    read -r KIND_STACK_PG_USER KIND_STACK_PG_DB <<<"$creds"
    [[ -n "$KIND_STACK_PG_USER" && -n "$KIND_STACK_PG_DB" ]]
}

# Over loopback TLS rather than the pod's Unix socket: the server refuses
# unencrypted TCP and the socket cannot encrypt, so sessions from this helper
# show up encrypted in pg_stat_ssl like every other client's.
kind_stack_psql() {
    kind_stack_psql_resolve_creds || return 1
    kubectl --context "kind-${KIND_CLUSTER}" -n "${NAMESPACE:-space}" \
        exec -i sts/space-postgres -- \
        env PGHOST=127.0.0.1 PGSSLMODE=require \
        psql -U "$KIND_STACK_PG_USER" -d "$KIND_STACK_PG_DB" -tA -v ON_ERROR_STOP=1 "$@"
}

# Wait for `space-api` to report `/readyz=200`, up to a caller-chosen bound.
# 0 when it did, 1 on timeout — the caller decides whether that is fatal.
#
# `/readyz`, not `/healthz`, and that is the whole point of having it: `/readyz`
# gates the Service endpoint on the sandbox-pod reflector's initial LIST, so a
# 200 comes from a replica whose caches are warm, while `/healthz` answers from
# one whose caches are cold. Callers use it after a rollout, where the Deployment
# going Available says nothing about the reflector.
#
# Here rather than in each suite because two of them had the same loop with the
# same reasoning in duplicated comments, and the copies had already diverged on
# the bound and on whether a timeout was fatal.
kind_stack_wait_api_ready() { # timeout-secs [url]
    local timeout="$1" url="${2:-${API_URL:?API_URL must be set}/readyz}"
    local deadline=$((SECONDS + timeout))
    while ((SECONDS <= deadline)); do
        if [ "$(curl -sS -o /dev/null -w '%{http_code}' -m 5 "$url" 2>/dev/null)" = "200" ]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# Delete tombstones whose kube Node is back — and only those.
#
# A drain on this bench runs to termination (`drained_node_terminator` is on and
# the terminator is `kube-delete`), which deletes the kube Node and writes a
# `node_k8s_tombstones` row. Restarting that worker's kubelet — how the retired
# `smoke-kind-node-drain.sh` got its second worker back, and how a developer
# does — resurrects only the kube half. What is left is a cluster that
# contradicts itself:
# kube advertises a Ready, schedulable, space-enabled worker whose SPACE row
# placement is guaranteed to refuse, so a later `POST /sandboxes` that lands
# there fails with `409 … is tombstoned` — in this suite or in whichever suite
# runs next on a shared devbox. The gate that produces the 409 is deliberate
# (without it an inventory upsert resurrects a terminated node's row), so the
# asymmetric cleanup is what gets fixed.
#
# Restricting the delete to nodes kube currently has is what keeps this a
# harness repair rather than a hole in that gate: a tombstone for a node that is
# genuinely gone is left alone, and it gets swept on a later run once kube has
# the node back. That makes a crashed run whose trap never fired self-healing.
kind_stack_sweep_stale_node_tombstones() {
    # Both reads are deliberately fail-closed: an unreadable table is not an
    # empty one, and a kube API that cannot be listed is not a cluster with no
    # nodes. Losing either distinction is how a stale tombstone survives a sweep
    # that reported success, and the next create fails with the 409 this exists
    # to prevent. A stack this cannot query is one the suite is about to fail
    # against anyway, so the loud version costs nothing and says why.
    #
    # The credential read is hoisted out of the command substitutions below so
    # its cache survives, and the output goes through a file for the same reason.
    if ! kind_stack_psql_resolve_creds; then
        echo "ERROR: could not read the kind cluster's Postgres credentials." >&2
        echo "       Refusing to report a clean tombstone sweep it never performed." >&2
        return 1
    fi

    local rows_file
    rows_file="$(mktemp)"
    if ! kind_stack_psql -c \
        "SELECT node_id || ' ' || k8s_node_name FROM node_k8s_tombstones;" >"$rows_file"; then
        rm -f "$rows_file"
        echo "ERROR: could not read node_k8s_tombstones (kubectl or psql failed)." >&2
        echo "       Refusing to report a clean sweep: a leftover tombstone makes the" >&2
        echo "       next sandbox create fail with 409 … is tombstoned." >&2
        return 1
    fi
    local rows
    rows="$(cat "$rows_file")"
    rm -f "$rows_file"
    [[ -n "$rows" ]] || return 0

    # One list call rather than one `get node` per row, so a sweep costs the same
    # whatever the residue is.
    local kube_nodes
    if ! kube_nodes=$(kubectl --context "kind-${KIND_CLUSTER}" get nodes -o name); then
        echo "ERROR: could not list kube nodes; a tombstone cannot be judged stale" >&2
        echo "       without them, and reporting a clean sweep here would be a guess." >&2
        return 1
    fi

    local ids=() names=() node_id node_name
    while read -r node_id node_name; do
        # A malformed id would abort the DELETE at the uuid cast; skip it here so
        # one bad row cannot block every good one.
        [[ "$node_id" =~ ^[0-9a-fA-F-]{36}$ ]] || continue
        [[ -n "$node_name" ]] || continue
        printf '%s\n' "$kube_nodes" | grep -qxF "node/$node_name" || continue
        ids+=("$node_id")
        names+=("$node_name")
    done <<<"$rows"
    if ((${#ids[@]} == 0)); then
        return 0
    fi

    local id_list
    id_list=$(IFS=,; echo "${ids[*]}")
    echo "[bootstrap] clearing node tombstones contradicted by kube: ${names[*]}"
    # Bound through psql's `-v` rather than interpolated into the statement, per
    # the convention the rest of the harness follows — and fed on stdin, because
    # psql does not interpolate `:'var'` in a `-c` string. That combination fails
    # with `syntax error at or near ":"`, which is why the statement is a heredoc.
    kind_stack_psql -v ids="$id_list" >/dev/null <<'SQL'
DELETE FROM node_k8s_tombstones WHERE node_id = ANY(string_to_array(:'ids', ',')::uuid[]);
SQL
}

# Render the cluster config and compare it against a cluster that already
# exists. Outside `__bring_up` for the same reason the tombstone sweep is: a
# cluster whose bind mounts predate the rendered config is residue of a
# previous run, and SKIP_BOOTSTRAP=1 — the mode every devbox run uses once the
# stack is up — is where that residue is met. Inside the bring-up this check
# never ran in that mode, and the smoke failed wherever the stale mount first
# bit instead of naming it.
kind_stack_assert_cluster_mounts() {
    # No cluster, nothing to compare against. This test comes first so the
    # rest of the function — which resolves host roots and writes a file —
    # never runs on a box that has no cluster at all.
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0

    # shellcheck source=../../../infra/space/kind/scripts/lib/kind-host-paths.sh
    source "$REPO_ROOT/infra/space/kind/scripts/lib/kind-host-paths.sh"

    # Underivable roots mean the expected mounts are unknown, so there is
    # nothing to compare and no honest verdict to give. Skip loudly rather
    # than fail: before this check existed, SKIP_BOOTSTRAP=1 read $HOME
    # nowhere, and turning an unevaluable comparison into a hard error would
    # break every kind suite on a box that never needed one. Creating a
    # cluster, below, still requires them.
    if [[ -z "${SPACE_VENDOR_ROOT:-}" || -z "${SPACE_KIND_DATA_ROOT:-}" ]]; then
        echo "[bootstrap] host roots not derivable (no HOME and no explicit" \
            "SPACE_VENDOR_ROOT/SPACE_KIND_DATA_ROOT); skipping the live-cluster" \
            "mount check" >&2
        return 0
    fi

    # Rendered to a temp file, not to $SPACE_KIND_CONFIG: the comparison only
    # needs the expected mount set, and writing the real config would make
    # every suite depend on a writable ~/.space-devbox/kind — which a
    # root-owned ~/.space-devbox, a documented devbox shape, does not provide.
    local expected
    expected="$(mktemp "${TMPDIR:-/tmp}/kind-cluster-expected.XXXXXX")" || return 0
    kind_render_cluster_config \
        "$REPO_ROOT/infra/space/kind/kind-cluster.yaml.tmpl" "$expected"
    kind_assert_live_cluster_mounts "$KIND_CLUSTER" "$expected"
    rm -f "$expected"
    # The vendor mount is right; each SPACE node keeps the copy of it it made
    # on its first boot, which a host tree changed since never reaches.
    bash "$REPO_ROOT/infra/space/kind/scripts/check-node-vendor.sh" \
        "$KIND_CLUSTER" || exit 1
}

# Refuse a cluster whose Postgres data directory an earlier space-postgres
# chart initialized (PostgreSQL 16), before anything reads or applies it.
# Beside the mount check, and for the same reason: under SKIP_BOOTSTRAP=1 such
# a bench still runs the old server, which has no TLS, and the first
# sslmode=require `kind_stack_psql` would fail without naming the cause.
kind_stack_assert_postgres_data() {
    # No cluster, nothing stale. Tested first, as the mount check does, so a
    # box with no cluster never resolves the script's path.
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0
    bash "$REPO_ROOT/infra/space/kind/scripts/check-postgres-data.sh" "$KIND_CLUSTER" || exit 1
}

# Detached application bring-up: cluster created, Tilt started in its own
# session and its selected context verified, then return. Initial profile
# reconciliation still completes before that context is registered. Registration
# has a short independent bound; timeout is a failed readiness check, not a
# request to stop the detached process. Everything
# `bootstrap_kind_stack` additionally guarantees (api-server,
# seed Job, rollouts) is left for Tilt to finish in the background; a later
# `bootstrap_kind_stack` (any kind smoke) finds Tilt running and waits then.
#
# The mount check runs here too: this path reuses an existing cluster as
# readily as `bootstrap_kind_stack` does, and a detached start against one
# whose mounts predate the rendered config would hand Tilt a cluster that
# cannot run a sandbox.
kind_stack_start_detached() {
    kind_stack_assert_smoke_prereqs
    kind_stack_assert_cluster_mounts
    kind_stack_assert_postgres_data
    KIND_STACK_DETACH=1 bootstrap_kind_stack__bring_up
}

# For each scenario the typed suite runner (`python -m space_smoke run --all`)
# launches after its own `bootstrap_kind_stack` passed against the same live
# stack. It repeats what can change between two scenarios: another cluster
# taking the API port, another bring-up switching the storage credentials
# mode, and node tombstones a scenario left. It leaves out the prerequisite,
# mount, node vendor copy, Postgres data and substrate checks, which only
# provisioning the box or creating the cluster changes, and the image provenance and Rust
# build checks, which only a Tilt build changes; scenarios restart and
# reconfigure workloads but build no image. The runner uses this only when its own bootstrap's
# provenance check passed (KIND_STACK_PROVENANCE_PASSED_FILE), not when a busy
# Tilt skipped it.
# Each check added to `bootstrap_kind_stack` belongs here or in that list.
kind_stack_recheck_for_scenario() {
    bootstrap_kind_stack__assert_port_owner
    bootstrap_kind_stack__refuse_storage_mode_mismatch
    kind_stack_sweep_stale_node_tombstones
}

bootstrap_kind_stack() {
    kind_stack_assert_smoke_prereqs
    kind_stack_assert_cluster_mounts
    kind_stack_assert_postgres_data
    bootstrap_kind_stack__bring_up
    # Outside the bring-up, so it also runs under SKIP_BOOTSTRAP=1 — the mode
    # every devbox run uses once the stack is up, and therefore the mode in
    # which a previous run's residue is actually encountered.
    kind_stack_sweep_stale_node_tombstones
}

# In GitHub CI (SPACE_CI_IMAGE_MIRROR set), take the third-party images the
# infra charts run from the repository's GHCR mirror (ci/mirrored-images.txt)
# and load them into the infra nodes that run them, so their kubelets make no
# anonymous registry pull. The images are read from the charts' values; an
# image neither the mirror nor its source gives is left to the kubelet, as
# before. Never fails the bring-up.
bootstrap_kind_stack__load_mirrored_chart_images() {
    [[ -n "${SPACE_CI_IMAGE_MIRROR:-}" ]] || return 0
    local chart image images=() present=() nodes="" attempt
    # Each chart's top-level `image:` block, plus the object store's
    # `bucketInitImage:` (its bucket-init Job runs the aws CLI image).
    # metrics-server is pinned to the infra node for the same reason.
    for chart in space-postgres space-redis-local space-object-store space-metrics-server; do
        while IFS= read -r image; do
            [[ -n "$image" ]] && images+=("$image")
        done < <(awk '
            function emit() { if (in_image && repository != "" && tag != "") print repository ":" tag }
            /^(image|bucketInitImage):/ { emit(); in_image = 1; repository = ""; tag = ""; next }
            /^[^ ]/ { emit(); in_image = 0 }
            in_image && $1 == "repository:" { repository = $2 }
            in_image && $1 == "tag:" { gsub(/"/, "", $2); tag = $2 }
            END { emit() }' \
            "$REPO_ROOT/infra/space/kind/charts/$chart/values.yaml")
    done
    bash "$REPO_ROOT/ci/scripts/mirror-images.sh" pull "${images[@]}"
    for image in "${images[@]}"; do
        docker image inspect "$image" >/dev/null 2>&1 && present+=("$image")
    done
    # kind labels each node as its kubelet registers.
    for ((attempt = 1; attempt <= 30 && ${#present[@]} > 0; attempt++)); do
        nodes="$(kubectl --context "kind-$KIND_CLUSTER" get nodes -l space.pplx.net/node-pool=infra \
            -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
        [[ -z "$nodes" ]] || break
        sleep 2
    done
    if [[ ${#present[@]} -eq 0 || -z "$nodes" ]]; then
        echo "::warning::Not loading the charts' images into kind; the kubelets pull them"
        return 0
    fi
    if ! kind load docker-image --name "$KIND_CLUSTER" --nodes "${nodes// /,}" "${present[@]}"; then
        echo "::warning::Could not load the charts' images into kind; the kubelets pull them"
    fi
}

# Prepare the same cluster as the full bootstrap, without starting Tilt or
# requiring Rust artifacts. CI can do this while its producer compiles.
kind_stack_prepare_cluster() {
    # As the Tiltfile refuses it, before a cluster is created for it.
    kind_stack_assert_substrate_request
    kind_stack_assert_smoke_prereqs
    kind_stack_assert_cluster_mounts
    kind_stack_assert_postgres_data
    bootstrap_kind_stack__assert_port_owner
    bootstrap_kind_stack__prepare_cluster
}

bootstrap_kind_stack__prepare_cluster() {
    # `kind create` reads static YAML, so the host paths it bind-mounts are
    # resolved and stamped in here — including staging the vendor tree, because
    # `kind_prepare_host_mounts` refuses a missing one and this bring-up is
    # documented as self-bootstrapping.
    # shellcheck source=../../../infra/space/kind/scripts/lib/kind-host-paths.sh
    source "$REPO_ROOT/infra/space/kind/scripts/lib/kind-host-paths.sh"
    # Process-private, not the shared $SPACE_KIND_CONFIG: the vendor
    # staging below can take a minute, and `kind create` reads this file
    # afterwards. Rendering into the shared per-user path leaves a window in
    # which another bring-up on the same box replaces it, and this cluster is
    # then created against the *other* run's host roots — silently, because
    # every mount it declares exists. Two bootstraps racing on one devbox is
    # the normal contended case, not a hypothetical.
    #
    # Nothing publishes from here: `infra/space/kind/scripts/render-cluster-config.sh`
    # owns the documented path, for the manual render-and-create flow, and a
    # second writer to it is what created the window in the first place.
    # In $TMPDIR and reaped explicitly below, not by an EXIT trap: the smoke
    # scripts that source this library each install their own `trap cleanup
    # EXIT`, which deletes sandboxes and clusters, and a trap set here would
    # replace it. A preflight refusal calls `kind_host_paths__die`, which
    # exits, so on that path this file does survive — a few KB in $TMPDIR,
    # where it cannot be mistaken for the published config and gets reaped
    # with the rest of the directory. `kind_stack_assert_cluster_mounts` above
    # has the same shape for the same reason.
    local kind_stack_config
    kind_stack_config="$(mktemp "${TMPDIR:-/tmp}/kind-cluster-bringup.XXXXXX")"
    kind_render_cluster_config \
        "$REPO_ROOT/infra/space/kind/kind-cluster.yaml.tmpl" "$kind_stack_config"
    kind_stack_stage_vendor

    if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
        # Creates the containerd-storage dirs (a clean box has none until the
        # first bring-up) and refuses a missing vendor tree or /dev/kvm, which
        # `kind create` would otherwise accept and Docker would supply as an
        # empty directory.
        # The config this run just rendered, not $SPACE_KIND_CONFIG — the
        # preflight has to check the mounts of the file `kind create` is about
        # to read, or it verifies one config and builds another.
        kind_prepare_host_mounts "$kind_stack_config"
        # The SPACE Kind node image the config names for sandbox workers and
        # builders: local, else GHCR, else a local build. Tested like the
        # create below, so the rendered config never leaks.
        if ! bash "$REPO_ROOT/infra/space/kind/scripts/kind-node-image.sh" ensure >/dev/null; then
            rm -f "$kind_stack_config"
            return 1
        fi
        # The add-on images each node image preloads, into the empty
        # containerd directories the config mounts over them. Never fails.
        bash "$REPO_ROOT/infra/space/kind/scripts/seed-node-containerd.sh" "$kind_stack_config"
        echo "[bootstrap] kind cluster '$KIND_CLUSTER' not found; creating from $kind_stack_config"
        # Tested in the `if`, not followed by `$?`: a caller running under
        # `set -e` would exit on the failure before the cleanup and the
        # rendered config would leak.
        if kind create cluster --name "$KIND_CLUSTER" \
            --config "$kind_stack_config"; then
            rm -f "$kind_stack_config"
            bootstrap_kind_stack__load_mirrored_chart_images
        else
            local kind_stack_create_rc=$?
            rm -f "$kind_stack_config"
            return "$kind_stack_create_rc"
        fi
    else
        rm -f "$kind_stack_config"
        echo "[bootstrap] kind cluster '$KIND_CLUSTER' already exists"
        # Mount drift was already checked by kind_stack_assert_cluster_mounts,
        # which runs in every mode rather than only this one.
        kind_stack_assert_existing_cluster
    fi
}

# Stage the Firecracker vendor tree the cluster bind-mounts, unless it already
# holds this Firecracker release and the pinned guest kernel
# (`install-fc-vendor.sh --check`). Before `kind create`, which refuses a
# missing tree, and before the existing-cluster refusals below, which compare
# each node's copy with the tree this run stages.
kind_stack_stage_vendor() {
    # shellcheck source=../../../infra/space/kind/scripts/lib/kind-host-paths.sh
    source "$REPO_ROOT/infra/space/kind/scripts/lib/kind-host-paths.sh"
    if ! SPACE_VENDOR_ROOT="$SPACE_VENDOR_ROOT" \
        bash "$REPO_ROOT/infra/space/kind/scripts/install-fc-vendor.sh" --check; then
        echo "[bootstrap] staging Firecracker vendor tree → $SPACE_VENDOR_ROOT"
        SPACE_VENDOR_ROOT="$SPACE_VENDOR_ROOT" \
            bash "$REPO_ROOT/infra/space/kind/scripts/install-fc-vendor.sh"
    fi
}

# The refusals an existing cluster can earn before Tilt (re)starts on it, once
# the vendor tree is staged (`kind_stack_stage_vendor`). Exits on a refusal;
# nothing to check when there is no cluster.
kind_stack_assert_existing_cluster() {
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0
    # Before Tilt (re)starts and applies the versitygw charts: a cluster
    # still on SeaweedFS has to be recreated, not half-switched.
    bash "$REPO_ROOT/infra/space/kind/scripts/check-object-store-transition.sh" \
        "$KIND_CLUSTER" || exit 1
    # SPACE nodes get their node layer from their image, fixed at create.
    bash "$REPO_ROOT/infra/space/kind/scripts/check-kind-node-image.sh" \
        "$KIND_CLUSTER" || exit 1
    # Each of those nodes copied the host vendor tree on its first boot and
    # keeps that copy, so a host tree changed since (kind_stack_stage_vendor
    # replaces a kernel whose pin changed) would never reach it. The mount
    # check makes the same comparison, but before this run stages the tree.
    bash "$REPO_ROOT/infra/space/kind/scripts/check-node-vendor.sh" \
        "$KIND_CLUSTER" || exit 1
    # A cluster still running one node-stack pod per node has to be
    # recreated before the per-service node DaemonSets land beside it.
    bash "$REPO_ROOT/infra/space/kind/scripts/check-node-layout.sh" \
        "$KIND_CLUSTER" || exit 1
    # Its database holds one substrate's node and sandbox rows.
    space_kind_check_substrate "$KIND_CLUSTER" || exit 1
}

# Refuses a SPACE_KIND_SUBSTRATE the Tiltfile would refuse: an invalid one,
# or `direct` without static storage credentials, which the Tiltfile reaches
# only once the cluster exists. Reads nothing from the cluster.
kind_stack_assert_substrate_request() {
    local substrate storage
    substrate="$(space_kind_substrate)" || exit 1
    [[ "$substrate" == direct ]] || return 0
    storage="$(space_kind_storage_credentials)" || exit 1
    [[ "$storage" == static ]] && return 0
    echo "ERROR: SPACE_KIND_SUBSTRATE=direct needs SPACE_KIND_STORAGE_CREDENTIALS=static; see \"Direct substrate lane\" in infra/space/kind/README.md" >&2
    exit 1
}

# Prints the storage credentials mode the cluster runs when it differs from
# the requested SPACE_KIND_STORAGE_CREDENTIALS, and nothing when they match or
# there is nothing to compare yet. The authority's Deployment records the mode
# its chart was rendered with; one without the label predates the modes,
# which ran static keys. Fails on an invalid request or an unreadable cluster.
bootstrap_kind_stack__storage_mode_mismatch() {
    local requested deployed
    requested="$(space_kind_storage_credentials)" || return 1
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0
    if ! deployed="$(kubectl --context "kind-${KIND_CLUSTER}" -n space get deployments \
            -l app.kubernetes.io/name=space-credmgr-authority \
            -o jsonpath='{range .items[*]}{.metadata.labels.space\.pplx\.net/kind-storage-credentials}{"|"}{end}' 2>&1)"; then
        echo "ERROR: cannot read the storage credentials mode of kind cluster $KIND_CLUSTER: $deployed" >&2
        return 1
    fi
    [[ -n "$deployed" ]] || return 0
    deployed="${deployed%%|*}"
    deployed="${deployed:-static}"
    [[ "$deployed" == "$requested" ]] || printf '%s\n' "$deployed"
}

# Prints the Rust build the cluster's SPACE Rust containers run when it
# differs from the requested SPACE_KIND_RUST_BUILD ("mixed" when they run no
# single build), and nothing when it matches or there is nothing to compare
# yet. The build is read from the image each running container uses, checked
# against the build its pod was rendered for
# (infra/space/kind/scripts/deployed-rust-build.py). With a timeout ($1), every
# Rust Deployment and DaemonSet must first finish rolling out. Fails on an
# invalid request or an unreadable cluster.
bootstrap_kind_stack__rust_build_mismatch() {
    local requested deployed status=0
    local -a wait_args=()
    requested="$(space_kind_rust_build)" || return 1
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0
    [[ -z "${1:-}" ]] || wait_args=(--wait "$1")
    deployed="$(python3 "$REPO_ROOT/infra/space/kind/scripts/deployed-rust-build.py" \
        --cluster "$KIND_CLUSTER" ${wait_args[@]+"${wait_args[@]}"})" || status=$?
    case "$status" in
        0) [[ "$deployed" == "$requested" ]] || printf '%s\n' "$deployed" ;;
        1) printf 'mixed\n' ;;
        3) ;;
        *)
            echo "ERROR: cannot read the Rust build of kind cluster $KIND_CLUSTER" >&2
            return 1
            ;;
    esac
}

# The node-service DaemonSets that read the object store, each with the
# container that does: the sandbox workers' and builders' volume-manager and
# the builders' build-manager. Each holds the store's static keys in static
# mode only.
BOOTSTRAP_KIND_STACK__STORAGE_DAEMONSETS=(
    space-volume-manager/volume-manager
    space-builder-volume-manager/volume-manager
    space-builder-build-manager/build-manager
)

# jq definitions over a DaemonSet (the input), the namespace's pods and the
# envFrom sources read so far, for the function below. `pod_specs` are the
# DaemonSet's template's and its pods'; `names` are a container's env names,
# each envFrom source resolved to its keys (null when it cannot be read).
BOOTSTRAP_KIND_STACK__STORAGE_KEYS_JQ='
    def pod_specs:
        {where: "template", spec: .spec.template.spec},
        ($pods[0].items[]
            | select(any(.metadata.ownerReferences[]?; .kind == "DaemonSet" and .name == $name))
            | {where: "pod \(.metadata.name)", spec: .spec});
    def containers: (.spec.initContainers // [])[], (.spec.containers // [])[], (.spec.ephemeralContainers // [])[];
    def source_ref:
        if .secretRef then "secret/\(.secretRef.name)"
        elif .configMapRef then "configmap/\(.configMapRef.name)"
        else empty end;
    def names: [.env[]?.name] + [.envFrom[]? | (.prefix // "") as $prefix | source_ref as $ref
        | ($sources[0][$ref] // [])[] | $prefix + .];
    def static_keys: [names[] | select(startswith("SPACE_OBJECT_STORE_STATIC_"))] | unique;
    def disagreements:
        pod_specs | .where as $where
        | if $requested == "exchange" then
            containers | .name as $cname
            | (static_keys | select(length > 0)
                | "\($name) \($where), container \($cname) holds static keys (\(join(", ")))"),
              (.envFrom[]? | source_ref | select($sources[0][.] == null)
                | "\($name) \($where), container \($cname) takes env from \(.), which cannot be read")
          else
            [containers | select(.name == $container)][0] as $c
            | if $c == null then "\($name) \($where) has no \($container) container"
              else (["SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID", "SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY"]
                  - ($c | static_keys)) | select(length > 0)
                | "\($name) \($where), container \($container) lacks static keys (\(join(", ")))"
              end
          end;
'

# Prints one "<daemonset> <where> ..." line for each disagreement with the
# requested mode, for the DaemonSet's pod template and for every pod it owns:
# in exchange mode, any container (init and ephemeral ones included) holding
# a SPACE_OBJECT_STORE_STATIC_* name, by `env` or through an `envFrom` Secret
# or ConfigMap (resolved to its keys; one that cannot be read is refused); in
# static mode, the consuming container lacking either key. Nothing when they
# agree or a DaemonSet does not exist yet. The label above records how the
# authority's chart was rendered; a failed apply, a stale Tilt resource or a
# rollout still under way can leave a template or a pod on the other side of
# it. Fails on an invalid request or an unreadable cluster.
bootstrap_kind_stack__node_storage_keys_mismatch() {
    local requested work
    requested="$(space_kind_storage_credentials)" || return 1
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0
    # Files, not arguments: a namespace's pod list outgrows one argument.
    work="$(mktemp -d "${TMPDIR:-/tmp}/kind-storage-keys.XXXXXX")" || return 1
    bootstrap_kind_stack__node_storage_keys_in "$requested" "$work"
    local status=$?
    rm -rf "$work"
    return "$status"
}

bootstrap_kind_stack__node_storage_keys_in() {
    local requested=$1 work=$2 entry daemonset container ref found
    local kube=(kubectl --context "kind-${KIND_CLUSTER}" -n space)
    if ! "${kube[@]}" get pods -o json >"$work/pods.json" 2>"$work/err"; then
        echo "ERROR: cannot list the pods of kind cluster $KIND_CLUSTER: $(cat "$work/err")" >&2
        return 1
    fi
    for entry in "${BOOTSTRAP_KIND_STACK__STORAGE_DAEMONSETS[@]}"; do
        daemonset="${entry%/*}" container="${entry#*/}"
        if ! "${kube[@]}" get daemonset "$daemonset" --ignore-not-found -o json >"$work/ds.json" 2>"$work/err"; then
            echo "ERROR: cannot read DaemonSet $daemonset of kind cluster $KIND_CLUSTER: $(cat "$work/err")" >&2
            return 1
        fi
        [[ -s "$work/ds.json" ]] || continue
        echo '{}' >"$work/sources.json"
        if ! found="$(bootstrap_kind_stack__storage_keys_jq "$work" "$daemonset" "$container" "$requested" \
                'pod_specs | containers | .envFrom[]? | source_ref')"; then
            return 1
        fi
        while read -r ref; do
            [[ -n "$ref" ]] || continue
            if ! "${kube[@]}" get "$ref" --ignore-not-found -o json >"$work/source.json" 2>"$work/err"; then
                echo "ERROR: cannot read $ref of kind cluster $KIND_CLUSTER: $(cat "$work/err")" >&2
                return 1
            fi
            jq -c --arg ref "$ref" --slurpfile doc "$work/source.json" \
                '.[$ref] = ($doc[0] // null | if . == null then null else (.data // {}) + (.binaryData // {}) | keys end)' \
                "$work/sources.json" >"$work/sources.next" && mv "$work/sources.next" "$work/sources.json" || return 1
        done < <(sort -u <<<"$found")
        bootstrap_kind_stack__storage_keys_jq "$work" "$daemonset" "$container" "$requested" disagreements || return 1
    done
}

# Runs `expression` after the definitions above, over the DaemonSet, pods and
# envFrom sources read into `work`.
bootstrap_kind_stack__storage_keys_jq() {
    local work=$1 daemonset=$2 container=$3 requested=$4 expression=$5
    jq -r --arg name "$daemonset" --arg container "$container" --arg requested "$requested" \
        --slurpfile pods "$work/pods.json" --slurpfile sources "$work/sources.json" \
        "$BOOTSTRAP_KIND_STACK__STORAGE_KEYS_JQ $expression" "$work/ds.json"
}

# A bring-up without SKIP_BOOTSTRAP=1 runs Tilt from this checkout, which
# re-applies each chart and waits for its rollout: the remedy for a chart
# rendered from another checkout or an older commit, and for a rollout still
# under way, so it is offered first where it has not just run. Neither a
# running nor a restarted Tilt removes a field added to a DaemonSet out of band
# (apply keeps fields it never owned), so for that the remedy recreates each
# one named from its render.
bootstrap_kind_stack__node_reapply_hint() {
    local mismatches=$1 offer_bring_up=$2 daemonset
    if [[ "$offer_bring_up" == 1 ]]; then
        echo "       Run one bring-up without SKIP_BOOTSTRAP=1: it runs Tilt from this checkout," >&2
        echo "       which re-applies these charts and waits for their rollouts. If one still" >&2
        echo "       disagrees afterwards, a field was added to it out of band:" >&2
    fi
    while read -r daemonset; do
        echo "       Recreate $daemonset from its render: kubectl --context kind-${KIND_CLUSTER} -n space" >&2
        echo "       delete daemonset $daemonset, then tilt trigger $daemonset." >&2
    done < <(awk '{ print $1 }' <<<"$mismatches" | sort -u)
}

# Without a bring-up nothing restarts Tilt in the requested mode, so a cluster
# running the other one is refused.
bootstrap_kind_stack__refuse_storage_mode_mismatch() {
    local deployed_mode node_keys
    deployed_mode="$(bootstrap_kind_stack__storage_mode_mismatch)" || exit 1
    if [[ -n "$deployed_mode" ]]; then
        echo "ERROR: kind cluster $KIND_CLUSTER runs SPACE_KIND_STORAGE_CREDENTIALS=$deployed_mode," >&2
        echo "       and this run asks for ${SPACE_KIND_STORAGE_CREDENTIALS:-exchange} (the default is exchange)." >&2
        echo "       Set SPACE_KIND_STORAGE_CREDENTIALS=$deployed_mode, or switch the cluster by" >&2
        echo "       running once without SKIP_BOOTSTRAP=1, which restarts Tilt in the requested" >&2
        echo "       mode; both modes share one object store, so nothing is recreated." >&2
        exit 1
    fi
    node_keys="$(bootstrap_kind_stack__node_storage_keys_mismatch)" || exit 1
    if [[ -n "$node_keys" ]]; then
        echo "ERROR: kind cluster $KIND_CLUSTER runs SPACE_KIND_STORAGE_CREDENTIALS=${SPACE_KIND_STORAGE_CREDENTIALS:-exchange}," >&2
        echo "       but in its node services:" >&2
        sed 's/^/         /' <<<"$node_keys" >&2
        bootstrap_kind_stack__node_reapply_hint "$node_keys" 1
        exit 1
    fi
}

# Prints each part of the reconcile-queue GC's Kind wiring that the applied
# cluster-controller Deployment lacks against this checkout's render of its
# chart: the GC switch, AWS_ENDPOINT_URL_EC2 and the pod's loopback alias for
# the public EC2 name (`reconcilers.reconcileQueueGc` and `ec2.fakeEndpoint`
# in charts/space-cluster-controller-local), plus an available
# space-kind-fake-ec2 when the render names it. Nothing when they agree or
# there is no Deployment yet. A bench brought up before the fake existed runs
# the GC off with nothing to answer it, and no suite would notice. Fails on
# an unreadable cluster or render.
bootstrap_kind_stack__reconcile_queue_gc_mismatch() {
    kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER" || return 0
    local ctx="kind-${KIND_CLUSTER}" applied rendered manifest available errors
    if ! applied="$(kubectl --context "$ctx" -n space get deployment space-cluster-controller \
            --ignore-not-found -o json 2>&1)"; then
        echo "ERROR: cannot read the cluster-controller of kind cluster $KIND_CLUSTER: $applied" >&2
        return 1
    fi
    [[ -n "$applied" ]] || return 0
    # helm warns about the charts' symlinked libraries on stderr, so only
    # stdout is the manifest.
    errors="$(mktemp)"
    if ! manifest="$(helm template space "$REPO_ROOT/infra/space/kind/charts/space-cluster-controller-local" \
            --namespace space --show-only templates/deployment.yaml 2>"$errors")" \
        || ! rendered="$(kubectl --context "$ctx" create --dry-run=client -o json -f - \
            <<<"$manifest" 2>"$errors")"; then
        echo "ERROR: cannot render this checkout's cluster-controller chart: $(<"$errors")" >&2
        rm -f "$errors"
        return 1
    fi
    rm -f "$errors"
    local diff endpoint
    if ! diff="$(jq -nr --argjson want "$rendered" --argjson have "$applied" '
            def env($name): [.spec.template.spec.containers[]
                | select(.name == "cluster-controller") | .env[]?
                | select(.name == $name) | .value] | first // "";
            def wiring: {
                SPACE_RECONCILE_QUEUE_GC_ENABLED: env("SPACE_RECONCILE_QUEUE_GC_ENABLED"),
                AWS_ENDPOINT_URL_EC2: env("AWS_ENDPOINT_URL_EC2"),
                hostAliases: ([.spec.template.spec.hostAliases[]? | .ip as $ip
                    | .hostnames[] | "\(.)->\($ip)"] | sort | join(","))
            };
            ($want | wiring) as $w | ($have | wiring) as $h
            | $w | keys_unsorted[] | select($w[.] != $h[.])
            | "\(.): applied \($h[.] | @json), this checkout renders \($w[.] | @json)"' 2>&1)" \
        || ! endpoint="$(jq -r '[.spec.template.spec.containers[].env[]?
            | select(.name == "AWS_ENDPOINT_URL_EC2") | .value] | first // ""' <<<"$rendered" 2>&1)"; then
        echo "ERROR: cannot compare the cluster-controller's reconcile-queue GC wiring: ${endpoint:-$diff}" >&2
        return 1
    fi
    [[ -z "$diff" ]] || printf '%s\n' "$diff"
    [[ -n "$endpoint" ]] || return 0
    if ! available="$(kubectl --context "$ctx" -n space get deployment space-kind-fake-ec2 \
            --ignore-not-found -o jsonpath='{.status.availableReplicas}' 2>&1)"; then
        echo "ERROR: cannot read the fake EC2 endpoint of kind cluster $KIND_CLUSTER: $available" >&2
        return 1
    fi
    [[ "${available:-0}" -ge 1 ]] || echo "space-kind-fake-ec2: no available replica"
}

bootstrap_kind_stack__bring_up() {
    # Always run the port-owner guard, even under SKIP_BOOTSTRAP — the
    # whole point is that the smoke test is destructive against
    # whatever cluster `$API_URL` points at.
    bootstrap_kind_stack__assert_port_owner

    # Before the SKIP_BOOTSTRAP return: suites pick scenarios and expectations
    # by the requested mode, so running them against a cluster in the other
    # one would test what was not asked for.
    local deployed_mode gc_wiring deployed_build
    deployed_build="$(bootstrap_kind_stack__rust_build_mismatch)" || exit 1
    # Unlike the modes above, a cluster's substrate is never switched: its
    # database holds that substrate's node and sandbox rows. Before
    # prepare_cluster, so a request the Tiltfile refuses creates no cluster.
    kind_stack_assert_substrate_request
    space_kind_check_substrate "$KIND_CLUSTER" || exit 1
    if [[ "${SKIP_BOOTSTRAP:-0}" == "1" ]]; then
        if [[ -n "$deployed_build" ]]; then
            echo "ERROR: kind cluster $KIND_CLUSTER runs the $deployed_build Rust build (read from its Rust" >&2
            echo "       containers' images), and this run asks for SPACE_KIND_RUST_BUILD=${SPACE_KIND_RUST_BUILD:-test-hooks}" >&2
            echo "       (the default is test-hooks). Set SPACE_KIND_RUST_BUILD to the cluster's build, or" >&2
            echo "       switch the cluster by running once without SKIP_BOOTSTRAP=1, which restarts Tilt" >&2
            echo "       in the requested build." >&2
            exit 1
        fi
        bootstrap_kind_stack__refuse_storage_mode_mismatch
        gc_wiring="$(bootstrap_kind_stack__reconcile_queue_gc_mismatch)" || exit 1
        if [[ -n "$gc_wiring" ]]; then
            echo "ERROR: kind cluster $KIND_CLUSTER does not run this checkout's reconcile-queue GC wiring:" >&2
            sed 's/^/       /' <<<"$gc_wiring" >&2
            echo "       Bring it up from this checkout by running once without SKIP_BOOTSTRAP=1," >&2
            echo "       which starts Tilt here (or restarts one watching another checkout) to apply" >&2
            echo "       the cluster-controller chart and deploy space-kind-fake-ec2." >&2
            exit 1
        fi
        echo "[bootstrap] SKIP_BOOTSTRAP=1; assuming kind+tilt are already running"
        bootstrap_kind_stack__check_live_provenance
        return
    fi
    deployed_mode="$(bootstrap_kind_stack__storage_mode_mismatch)" || exit 1

    # curl is listed because this lib polls `/healthz` itself, not only because
    # the bring-up commands need it.
    for bin in curl kind kubectl tilt; do
        if ! command -v "$bin" >/dev/null 2>&1; then
            echo "ERROR: required bring-up command not found: $bin (or set SKIP_BOOTSTRAP=1)" >&2
            exit 1
        fi
    done

    bootstrap_kind_stack__prepare_cluster

    # The two-axis decision: api-server reachable? tilt running?
    #
    # - tilt up + api up    → nothing to do; latest chart is already applied.
    # - tilt up + api down  → bootstrap is still warming; just wait — unless
    #   that Tilt predates the cluster it claims to serve (see
    #   `__tilt_stale_for_cluster`), in which case it is restarted like a
    #   dead one; waiting on it never ends.
    # - tilt up from another checkout, either way → restarted here (see
    #   `__tilt_watches_other_checkout`); the cluster and its pods stay up.
    # - tilt down + api up  → previous tilt session was killed; the
    #   running pods reflect that session's state, NOT the current
    #   files on disk. Start tilt so file changes (Rust code, chart
    #   values) get applied. Then wait for /healthz to re-stabilize
    #   in case the api-server rolled.
    # - tilt down + api down → cold start.
    local tilt_state=absent orphan_pid=""
    if bootstrap_kind_stack__tilt_running; then
        tilt_state=live
        if bootstrap_kind_stack__tilt_stale_for_cluster; then
            tilt_state=stale
        elif bootstrap_kind_stack__tilt_watches_other_checkout; then
            tilt_state=elsewhere
        elif [[ -n "$deployed_mode" || -n "$deployed_build" ]]; then
            # Tilt reads the modes once, from the environment it started with.
            tilt_state=mode
        fi
    elif orphan_pid="$(bootstrap_kind_stack__tilt_orphaned)"; then
        # Ours, responsive, never connected: restart it rather than refuse the
        # unverified server (see `__tilt_orphaned`).
        tilt_state=orphaned
    fi
    if [[ "$tilt_state" == "live" ]]; then
        if [[ "${KIND_STACK_DETACH:-0}" == "1" ]]; then
            echo "[bootstrap] tilt is already running; KIND_STACK_DETACH=1, not waiting for the stack"
            return 0
        fi
        if bootstrap_kind_stack__api_reachable; then
            echo "[bootstrap] tilt is running and api-server is reachable — stack live"
        else
            echo "[bootstrap] tilt is running but api-server not reachable yet; waiting"
            if ! bootstrap_kind_stack__wait_for_api; then
                echo "ERROR: api-server did not become reachable within ${BOOTSTRAP_TIMEOUT}s" >&2
                exit 1
            fi
        fi
    else
        # The watcher just verified is the one serving :10350; stop that one,
        # not whichever `tilt up` the pidfile or process age would pick.
        local served_pid=""
        if [[ "$tilt_state" == stale || "$tilt_state" == elsewhere || "$tilt_state" == mode ]]; then
            served_pid="$(bootstrap_kind_stack__served_tilt_pid)"
        fi
        if [[ "$tilt_state" == "stale" ]]; then
            echo "[bootstrap] tilt is running but bound to a cluster that no longer exists; restarting it"
            bootstrap_kind_stack__stop_stale_tilt "$served_pid" || exit 1
        elif [[ "$tilt_state" == "elsewhere" ]]; then
            echo "[bootstrap] restarting tilt from this checkout so its code is what gets built and deployed"
            bootstrap_kind_stack__stop_stale_tilt "$served_pid" || exit 1
        elif [[ "$tilt_state" == "mode" ]]; then
            echo "[bootstrap] restarting tilt to switch to storage credentials ${SPACE_KIND_STORAGE_CREDENTIALS:-exchange} and the ${SPACE_KIND_RUST_BUILD:-test-hooks} Rust build"
            bootstrap_kind_stack__stop_stale_tilt "$served_pid" || exit 1
        elif [[ "$tilt_state" == "orphaned" ]]; then
            bootstrap_kind_stack__stop_stale_tilt "$orphan_pid" || exit 1
        else
            if bootstrap_kind_stack__api_reachable; then
                echo "[bootstrap] api-server reachable but tilt is NOT running; starting tilt so code/chart changes get applied"
            else
                echo "[bootstrap] api-server not reachable and tilt is NOT running; cold start"
            fi
            bootstrap_kind_stack__kill_stale_tilt || exit 1
        fi
        kind_stack_reap_stale_port_forwards
        bootstrap_kind_stack__start_tilt || exit 1
        if [[ "${KIND_STACK_DETACH:-0}" == "1" ]]; then
            echo "[bootstrap] tilt is up and building; KIND_STACK_DETACH=1, returning without waiting"
            echo "         (first-ever build cooks the full Rust workspace; expect 10-20 min)"
            echo "         follow build progress at http://localhost:10350"
            return 0
        fi
        echo "[bootstrap] waiting up to ${BOOTSTRAP_TIMEOUT}s for $API_URL/healthz"
        echo "         (first-ever build cooks the full Rust workspace; expect 10-20 min)"
        if ! bootstrap_kind_stack__wait_for_api; then
            echo "ERROR: api-server did not become reachable within ${BOOTSTRAP_TIMEOUT}s" >&2
            exit 1
        fi
    fi

    # The seed-publish Job uploads the locally built seed to the object store and
    # registers the release through the ClusterApi; CreateSandbox validates
    # the template tag against `template_release_tags`, so the smoke tests
    # cannot create sandboxes until it has completed.
    # Two waits, not one, because they are gated on different things and their
    # natural scales differ by an order of magnitude. The Job *appearing* waits
    # on Tilt, which builds images serially -- the same dependency the healthz
    # wait above budgets $BOOTSTRAP_TIMEOUT for -- while the Job *completing*
    # takes seconds once applied (23s observed on a cold box).
    #
    # Conflating them gave both arms 300s and reported either as "did not
    # complete within 300s". That diagnosis was wrong in the case it actually
    # fired: `kubectl wait` against an absent Job returns NotFound immediately
    # rather than consuming its timeout, so the 300s in the message was the
    # existence loop's budget and the Job had not failed at all -- it had not
    # been created yet, and completed fine 23s after bring-up gave up on it.
    local seed_exists_timeout="${SEED_PUBLISH_EXISTS_TIMEOUT:-$BOOTSTRAP_TIMEOUT}"
    local seed_complete_timeout="${SEED_PUBLISH_COMPLETE_TIMEOUT:-$BOOTSTRAP_TIMEOUT}"
    echo "[bootstrap] waiting up to ${seed_exists_timeout}s for the space-seed-publish Job to be applied"
    local seed_deadline=$(( $(date +%s) + seed_exists_timeout ))
    local seed_job_applied=0
    while [[ $(date +%s) -lt $seed_deadline ]]; do
        if kubectl --context "kind-${KIND_CLUSTER}" get job/space-seed-publish \
                -n space >/dev/null 2>&1; then
            seed_job_applied=1
            break
        fi
        bootstrap_kind_stack__die_on_failed_builds
        bootstrap_kind_stack__die_on_failed_migration
        sleep 3
    done
    # Never fall through: an expired existence loop is its own diagnosis, and
    # letting it reach the completion wait is what produced the wrong one.
    if [[ $seed_job_applied -eq 0 ]]; then
        echo "ERROR: space-seed-publish Job was never applied within ${seed_exists_timeout}s;" >&2
        echo "       Tilt has not gotten to it -- check 'tilt get uiresources' for space-seed-publish" >&2
        kubectl --context "kind-${KIND_CLUSTER}" get jobs -n space >&2 || true
        exit 1
    fi
    echo "[bootstrap] waiting up to ${seed_complete_timeout}s for the space-seed-publish Job to complete"
    bootstrap_kind_stack__wait_for_job space-seed-publish "$seed_complete_timeout"

    echo "[bootstrap] waiting for the worker and builder node-service DaemonSets to roll"
    # shellcheck source=../../../infra/space/kind/lib/node-services.sh
    . "$REPO_ROOT/infra/space/kind/lib/node-services.sh"
    local node_daemonset
    while read -r node_daemonset; do
        if ! kubectl --context "kind-${KIND_CLUSTER}" rollout status \
                "daemonset/$node_daemonset" -n space --timeout="${BOOTSTRAP_TIMEOUT}s" >/dev/null; then
            echo "ERROR: $node_daemonset did not become Ready within ${BOOTSTRAP_TIMEOUT}s" >&2
            kubectl --context "kind-${KIND_CLUSTER}" get \
                "daemonset/$node_daemonset" -n space -o wide >&2 || true
            kubectl --context "kind-${KIND_CLUSTER}" get pods -n space -o wide \
                -l app.kubernetes.io/part-of=space-node >&2 || true
            exit 1
        fi
    done < <(space_kind_node_daemonsets)

    # Ready pods are not a registered node: each node's node-ctl publishes it
    # through ClusterApi, and only then does the API schedule onto it.
    echo "[bootstrap] waiting for every SPACE node to be published and ready"
    bootstrap_kind_stack__wait_node_publication

    # Cluster-controller rollout: any chart-value or image change Tilt
    # picked up needs to roll before the smoke tests touch reconciler-
    # driven state. The deployment uses Recreate strategy, so this is a
    # genuine "old pod gone, new pod available".
    echo "[bootstrap] waiting for cluster-controller deployment to roll"
    if ! kubectl --context "kind-${KIND_CLUSTER}" rollout status \
            deploy/space-cluster-controller -n space --timeout="${BOOTSTRAP_TIMEOUT}s" >/dev/null; then
        echo "ERROR: space-cluster-controller did not become Available within ${BOOTSTRAP_TIMEOUT}s" >&2
        kubectl --context "kind-${KIND_CLUSTER}" get deploy/space-cluster-controller -n space >&2 || true
        exit 1
    fi

    bootstrap_kind_stack__wait_for_tilt_quiet

    # The waits above can be satisfied by the previous revision: a warm rerun
    # that changes the seed finds the old space-seed-publish Job complete, and
    # Tilt applies its replacement only later. Once Tilt has settled, the Jobs
    # in the cluster are the ones this revision applied, so check them again,
    # and the node publication that depends on them.
    echo "[bootstrap] verifying the Jobs Tilt settled on and node publication"
    local settled_job
    for settled_job in space-object-store-bucket-init space-seed-publish; do
        bootstrap_kind_stack__wait_for_job "$settled_job" "${SEED_PUBLISH_COMPLETE_TIMEOUT:-$BOOTSTRAP_TIMEOUT}"
    done
    bootstrap_kind_stack__wait_node_publication

    deployed_mode="$(bootstrap_kind_stack__storage_mode_mismatch)" || exit 1
    if [[ -n "$deployed_mode" ]]; then
        echo "ERROR: the bring-up finished with the cluster still in storage credentials mode $deployed_mode" >&2
        exit 1
    fi
    # Tilt replaces each Rust workload's pods when its image or its rendered
    # build changes; wait for every replacement before reading what runs.
    deployed_build="$(bootstrap_kind_stack__rust_build_mismatch 600)" || exit 1
    if [[ -n "$deployed_build" ]]; then
        echo "ERROR: the bring-up finished with SPACE Rust containers on the $deployed_build Rust build," >&2
        echo "       not ${SPACE_KIND_RUST_BUILD:-test-hooks}; per pod:" >&2
        python3 "$REPO_ROOT/infra/space/kind/scripts/deployed-rust-build.py" \
            --cluster "$KIND_CLUSTER" --pods >/dev/null || true
        exit 1
    fi
    node_keys="$(bootstrap_kind_stack__node_storage_keys_mismatch)" || exit 1
    if [[ -n "$node_keys" ]]; then
        echo "ERROR: the bring-up finished in storage credentials mode ${SPACE_KIND_STORAGE_CREDENTIALS:-exchange}" >&2
        echo "       with node services that disagree:" >&2
        sed 's/^/         /' <<<"$node_keys" >&2
        bootstrap_kind_stack__node_reapply_hint "$node_keys" 0
        exit 1
    fi

    bootstrap_kind_stack__check_provenance

    if [[ -n "${DATA_PLANE_URL:-}" ]]; then
        echo "[bootstrap] waiting up to ${BOOTSTRAP_TIMEOUT}s for $DATA_PLANE_URL/healthz"
        local data_plane_deadline=$(( $(date +%s) + BOOTSTRAP_TIMEOUT ))
        while [[ $(date +%s) -lt $data_plane_deadline ]]; do
            if curl -sf -m 2 "$DATA_PLANE_URL/healthz" >/dev/null 2>&1; then
                return 0
            fi
            sleep 2
        done
        echo "ERROR: data plane did not become reachable within ${BOOTSTRAP_TIMEOUT}s" >&2
        exit 1
    fi
}
