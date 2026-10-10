#!/usr/bin/env bash
#
# Shared idempotent bootstrap for the local KinD + Tilt SPACE stack.
# Sourced by `smoke-kind-e2e.sh`, and by the harness tests
# `test-kind-port-forward-parity.sh`, `test-kind-stale-tilt-recovery.sh`, and
# `test-kind-tombstone-sweep.sh`.
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
# finished rolling, (when DATA_PLANE_URL is supplied) the public data plane
# is reachable, and no node carries a tombstone contradicted by kube. A Tilt
# build that fails ends the bring-up with that resource's log instead of
# consuming the timeout; a Tilt bound to a deleted cluster is restarted.
#
# Public entry points: `bootstrap_kind_stack`, plus
# `kind_stack_sweep_stale_node_tombstones`, `kind_stack_psql`, and
# `kind_stack_ensure_redis` for suites that have to repeat a bring-up step in
# their own setup or cleanup.
#
# Inputs (env vars, all optional with defaults set by the caller):
#   API_URL           — health-check URL (e.g. http://localhost:8080)
#   DATA_PLANE_URL    — optional data-plane health-check URL (e.g. http://localhost:8090)
#   KIND_CLUSTER      — kind cluster name (e.g. space-local)
#   SKIP_BOOTSTRAP=1  — skip everything; caller asserts stack is up
#   BOOTSTRAP_TIMEOUT — seconds to wait for api-server `/healthz`
#   KIND_STACK_DETACH=1 — return as soon as the cluster exists and Tilt is
#                       serving :10350; do not wait for the api-server, the
#                       seed Job, or any rollout. For callers that must return
#                       quickly (devbox setup steps are SIGKILLed at 5 min)
#                       and leave the build running. `kind_stack_start_detached`
#                       is the entry point for that mode.
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
        echo "ERROR: the kind smoke suites need commands this box does not have:" >&2
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

# The `tilt up` this bring-up tracks: the pidfile when it names a live
# `tilt up`, otherwise the oldest `tilt up` on the box (sessions started before
# the pidfile existed). Prints nothing when there is none.
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
    bootstrap_kind_stack__tilt_pids | head -n 1
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
    pid="$(bootstrap_kind_stack__tilt_pid)"
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

# The directory a `tilt up` runs in (the pid given, or the tracked one), which
# is the checkout it watches and builds. Fails when that is unknown (no such
# process, or no /proc to ask). A checkout deleted since Tilt started reads back
# with a " (deleted)" suffix, so it never matches a live path.
bootstrap_kind_stack__tilt_cwd() {
    local pid="${1:-}"
    [[ -n "$pid" ]] || pid="$(bootstrap_kind_stack__tilt_pid)"
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
    echo "[bootstrap] tilt (pid $(bootstrap_kind_stack__tilt_pid)) is watching $cwd, not this checkout ($here)"
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
# cluster that no longer exists or watching another checkout, or that it is
# ours and never connected to any cluster (`__tilt_orphaned`).
bootstrap_kind_stack__stop_stale_tilt() {
    local pid="${1:-}" state_dir pidfile
    [[ -n "$pid" ]] || pid="$(bootstrap_kind_stack__tilt_pid)"
    state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
    pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
    if [[ -n "$pid" ]]; then
        echo "[bootstrap] stopping stale tilt (pid $pid) before starting fresh"
        kill "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
    local deadline=$(( $(date +%s) + 20 ))
    while [[ $(date +%s) -lt $deadline ]]; do
        if ! curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    [[ -z "$pid" ]] || kill -9 "$pid" 2>/dev/null || true
    sleep 1
    if curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
        echo "ERROR: stale tilt still holds :10350; stop it manually and retry" >&2
        return 1
    fi
    return 0
}

# Resources whose last update failed, one name per line.
bootstrap_kind_stack__tilt_failed_resources() {
    timeout 10s tilt get uiresources --port 10350 \
        -o 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.updateStatus}{"\n"}{end}' 2>/dev/null \
        | awk '$2 == "error" { print $1 }'
}

# Resources Tilt is still updating, space-separated on one line.
bootstrap_kind_stack__tilt_busy_resources() {
    timeout 10s tilt get uiresources --port 10350 \
        -o 'jsonpath={range .items[*]}{.metadata.name}{" "}{.status.updateStatus}{"\n"}{end}' 2>/dev/null \
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
    local res
    while IFS= read -r res; do
        [[ -n "$res" ]] || continue
        echo "----- tilt logs $res (tail)" >&2
        timeout 15s tilt logs "$res" --port 10350 2>/dev/null | tail -n 40 >&2 || true
    done <<<"$failed"
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
bootstrap_kind_stack__wait_for_tilt_quiet() {
    local deadline=$(( $(date +%s) + BOOTSTRAP_TIMEOUT ))
    local busy now last_log_at=0
    echo "[bootstrap] waiting for Tilt to finish every pending build"
    while :; do
        busy="$(bootstrap_kind_stack__tilt_busy_resources)"
        [[ -n "${busy// /}" ]] || break
        bootstrap_kind_stack__die_on_failed_builds
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
        if ! kubectl --context "kind-${KIND_CLUSTER}" -n space rollout status "$workload" --timeout=300s >/dev/null; then
            echo "ERROR: $workload did not finish rolling within 300s" >&2
            kubectl --context "kind-${KIND_CLUSTER}" -n space get "$workload" -o wide >&2 || true
            exit 1
        fi
    done < <(kubectl --context "kind-${KIND_CLUSTER}" -n space get deploy,statefulset,daemonset -o name 2>/dev/null)
}

bootstrap_kind_stack__wait_for_api() {
    local deadline=$(( $(date +%s) + BOOTSTRAP_TIMEOUT ))
    local last_log_at=0
    while [[ $(date +%s) -lt $deadline ]]; do
        if bootstrap_kind_stack__api_reachable; then
            return 0
        fi
        bootstrap_kind_stack__die_on_failed_builds
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
    local state_dir log pidfile
    state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
    log="${SPACE_TILT_LOG:-$state_dir/tilt-${KIND_CLUSTER}.log}"
    pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
    mkdir -p "$state_dir"
    echo "[bootstrap] starting tilt in background; logs → $log"
    echo "[bootstrap] tilt UI: http://localhost:10350 (per-resource build progress)"
    (
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
        TILT_BROWSER_DISABLED=1 SPACE_KIND_CLUSTER="$KIND_CLUSTER" \
            setsid nohup tilt up >"$log" 2>&1 </dev/null &
        echo $! >"$pidfile"
    )

    local tilt_deadline=$(( $(date +%s) + 60 ))
    while [[ $(date +%s) -lt $tilt_deadline ]]; do
        if bootstrap_kind_stack__tilt_running; then
            return 0
        fi
        sleep 2
    done
    echo "ERROR: tilt did not report context kind-${KIND_CLUSTER} on :10350 within 60s" >&2
    echo "  inspect: $log" >&2
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
    local stale_pids
    stale_pids="$(bootstrap_kind_stack__tilt_pids)"
    if [[ -n "$stale_pids" ]]; then
        echo "[bootstrap] stale 'tilt up' detected — killing before starting fresh"
        local state_dir pidfile tilt_pid
        state_dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}"
        pidfile="${SPACE_TILT_PIDFILE:-$state_dir/tilt-${KIND_CLUSTER}.pid}"
        if [[ -s "$pidfile" ]]; then
            read -r tilt_pid <"$pidfile"
            if [[ "$tilt_pid" =~ ^[0-9]+$ ]] \
                    && bootstrap_kind_stack__tilt_pid_is_expected "$tilt_pid"; then
                kill "$tilt_pid" 2>/dev/null || true
            fi
            rm -f "$pidfile"
        fi
        # Sessions started before the pidfile existed: only processes whose
        # command line is `tilt up` itself (see __tilt_pids), never `pkill -f`.
        for tilt_pid in $stale_pids; do
            kill "$tilt_pid" 2>/dev/null || true
        done
        local kill_deadline=$(( $(date +%s) + 15 ))
        while [[ $(date +%s) -lt $kill_deadline ]]; do
            if ! curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1 \
                    && [[ -z "$(bootstrap_kind_stack__tilt_pids)" ]]; then
                return 0
            fi
            sleep 1
        done
        if curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1; then
            echo "ERROR: failed to free port 10350; kill manually and retry" >&2
            return 1
        fi
    fi
    return 0
}

# Stop stale `kubectl port-forward` supervisors (and their kubectl children)
# that a previous bring-up of THIS kind context left behind and no pidfile
# names. bootstrap.sh starts each supervisor as
# `setsid bash port-forward.sh <ctx> <ns> <target> <mapping> space-kind-pf:<ctx>`
# — the label is deliberately its LAST argument — and the loop's kubectl child
# carries `--context <ctx> … port-forward`. Both are matched on the process's
# own command line, anchored: the label at the end, `kubectl` as the command.
# Never `pkill -f '<label>|--context <ctx>.*port-forward'`: that unanchored
# alternation matched any process whose arguments merely mention a context and
# "port-forward" — on 2026-09-22 an automation runner whose task text did was
# SIGTERMed by the bring-up it had just started. A forward to another context
# is never touched (the suites' port-owner guard exists for that footgun).
kind_stack_reap_stale_port_forwards() {
    local kctx="kind-${KIND_CLUSTER}" pid command
    for pid in $(pgrep -f "space-kind-pf:${kctx}|--context ${kctx}" 2>/dev/null || true); do
        command="$(ps -o command= -p "$pid" 2>/dev/null)" || continue
        if [[ "$command" =~ [[:space:]]space-kind-pf:${kctx}[[:space:]]*$ ]] \
            || [[ "$command" =~ ^[[:space:]]*([^[:space:]]*/)?kubectl[[:space:]].*--context[[:space:]=]${kctx}([[:space:]]|$).*port-forward ]]; then
            echo "[bootstrap] stopping stale kind port-forward (pid $pid): ${command:0:80}"
            kill "$pid" 2>/dev/null || true
        fi
    done
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

kind_stack_psql() {
    kind_stack_psql_resolve_creds || return 1
    kubectl --context "kind-${KIND_CLUSTER}" -n "${NAMESPACE:-space}" \
        exec -i sts/space-postgres -- \
        psql -U "$KIND_STACK_PG_USER" -d "$KIND_STACK_PG_DB" -tA -v ON_ERROR_STOP=1 "$@"
}

# Redis, the store behind the two API rate limiters. Part of the harness
# (`bootstrap.sh` / the Tiltfile), so this renders the chart only when it is
# missing — which keeps the normal path from re-applying it while still letting a
# suite run against a stack that predates the chart. It always scales to 1 and
# waits, so it also recovers a prior run that died with Redis scaled to zero.
#
# Here rather than in each suite because it is bring-up, not assertion. Two
# suites had the same twelve lines when it was extracted; one of them
# (`smoke-kind-node-selector-extras.sh`) dropped its Redis setup later in the same
# change, once the token bucket turned out not to affect either of its phases — so
# `smoke-kind-rate-limiters.sh` is the only caller today.
kind_stack_ensure_redis() {
    local deploy="${1:-space-redis}"
    # Declared here rather than in the bring-up loop above, because a suite
    # running with SKIP_BOOTSTRAP=1 reaches this function and never that loop.
    if ! command -v helm >/dev/null 2>&1; then
        echo "ERROR: helm is required to ensure the Redis chart is applied" >&2
        return 1
    fi
    # Targeting flags last, per the repository's kubectl convention. None of
    # these is an `exec`, where the flags would have to precede `--`.
    local target=(-n "${NAMESPACE:-space}" --context "kind-${KIND_CLUSTER}")
    if ! kubectl get "deploy/$deploy" "${target[@]}" >/dev/null 2>&1; then
        helm template space "$REPO_ROOT/infra/space/kind/charts/space-redis-local" \
            --namespace "${NAMESPACE:-space}" \
            | kubectl apply -f - "${target[@]}" >/dev/null
    fi
    kubectl scale "deploy/$deploy" --replicas=1 "${target[@]}" >/dev/null 2>&1 || true
    kubectl rollout status "deploy/$deploy" --timeout=120s "${target[@]}" >/dev/null
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
    # break every kind suite on a box that never needed one.
    # `bootstrap.sh`, which actually creates clusters, still requires them.
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
}

# Non-blocking bring-up: cluster created, Tilt started in its own session,
# return. Everything `bootstrap_kind_stack` additionally guarantees (api-server,
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
    KIND_STACK_DETACH=1 bootstrap_kind_stack__bring_up
}

bootstrap_kind_stack() {
    kind_stack_assert_smoke_prereqs
    kind_stack_assert_cluster_mounts
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
    for chart in space-postgres space-redis-local space-object-store; do
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

bootstrap_kind_stack__bring_up() {
    # Always run the port-owner guard, even under SKIP_BOOTSTRAP — the
    # whole point is that the smoke test is destructive against
    # whatever cluster `$API_URL` points at.
    bootstrap_kind_stack__assert_port_owner

    if [[ "${SKIP_BOOTSTRAP:-0}" == "1" ]]; then
        echo "[bootstrap] SKIP_BOOTSTRAP=1; assuming kind+tilt are already running"
        return
    fi

    # curl is listed because this lib polls `/healthz` itself, not only because
    # the bring-up commands need it.
    for bin in curl kind kubectl tilt; do
        if ! command -v "$bin" >/dev/null 2>&1; then
            echo "ERROR: required bring-up command not found: $bin (or set SKIP_BOOTSTRAP=1)" >&2
            exit 1
        fi
    done

    # `kind create` reads static YAML, so the host paths it bind-mounts are
    # resolved and stamped in here the same way
    # `infra/space/kind/scripts/bootstrap.sh` does it — including staging the
    # vendor tree, because `kind_prepare_host_mounts` refuses a missing one and
    # this suite is documented as self-bootstrapping.
    # shellcheck source=../../../infra/space/kind/scripts/lib/kind-host-paths.sh
    source "$REPO_ROOT/infra/space/kind/scripts/lib/kind-host-paths.sh"
    # Process-private, not the shared $SPACE_KIND_CONFIG, and for the same
    # reason `bootstrap.sh` renders to `$SPACE_KIND_CONFIG.$$`: the vendor
    # staging below can take a minute, and `kind create` reads this file
    # afterwards. Rendering into the shared per-user path leaves a window in
    # which another bring-up on the same box replaces it, and this cluster is
    # then created against the *other* run's host roots — silently, because
    # every mount it declares exists. Two bootstraps racing on one devbox is
    # the normal contended case, not a hypothetical.
    #
    # Nothing publishes from here: `bootstrap.sh` and
    # `infra/space/kind/scripts/render-cluster-config.sh` own the documented
    # path, and a test library adding itself as a second writer to it is what
    # created the window in the first place.
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
    if [[ ! -x "$SPACE_VENDOR_ROOT/$KIND_HOST_PATHS_VENDOR_PROBE" ]]; then
        echo "[bootstrap] staging Firecracker vendor tree → $SPACE_VENDOR_ROOT"
        SPACE_VENDOR_ROOT="$SPACE_VENDOR_ROOT" \
            bash "$REPO_ROOT/infra/space/kind/scripts/install-fc-vendor.sh"
    fi

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
        # Before Tilt (re)starts and applies the versitygw charts: a cluster
        # still on SeaweedFS has to be recreated, not half-switched.
        bash "$REPO_ROOT/infra/space/kind/scripts/check-object-store-transition.sh" \
            "$KIND_CLUSTER" || exit 1
        # SPACE nodes get their node layer from their image, fixed at create.
        bash "$REPO_ROOT/infra/space/kind/scripts/check-kind-node-image.sh" \
            "$KIND_CLUSTER" || exit 1
        # A cluster still running one node-stack pod per node has to be
        # recreated before the per-service node DaemonSets land beside it.
        bash "$REPO_ROOT/infra/space/kind/scripts/check-node-layout.sh" \
            "$KIND_CLUSTER" || exit 1
    fi

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
        if [[ "$tilt_state" == "stale" ]]; then
            echo "[bootstrap] tilt is running but bound to a cluster that no longer exists; restarting it"
            bootstrap_kind_stack__stop_stale_tilt || exit 1
        elif [[ "$tilt_state" == "elsewhere" ]]; then
            echo "[bootstrap] restarting tilt from this checkout so its code is what gets built and deployed"
            bootstrap_kind_stack__stop_stale_tilt || exit 1
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
    local seed_complete_timeout="${SEED_PUBLISH_COMPLETE_TIMEOUT:-300}"
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
    if ! kubectl --context "kind-${KIND_CLUSTER}" wait --for=condition=complete \
            job/space-seed-publish -n space --timeout="${seed_complete_timeout}s" >/dev/null 2>&1; then
        echo "ERROR: space-seed-publish Job was applied but did not complete within ${seed_complete_timeout}s" >&2
        kubectl --context "kind-${KIND_CLUSTER}" get jobs -n space >&2 || true
        kubectl --context "kind-${KIND_CLUSTER}" logs job/space-seed-publish -n space --tail=50 >&2 || true
        exit 1
    fi

    echo "[bootstrap] waiting for the worker and builder node-service DaemonSets to roll"
    # shellcheck source=../../../infra/space/kind/lib/node-services.sh
    . "$REPO_ROOT/infra/space/kind/lib/node-services.sh"
    local node_daemonset
    while read -r node_daemonset; do
        if ! kubectl --context "kind-${KIND_CLUSTER}" rollout status \
                "daemonset/$node_daemonset" -n space --timeout=600s >/dev/null; then
            echo "ERROR: $node_daemonset did not become Ready within 600s" >&2
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
    if ! SPACE_KIND_CLUSTER="$KIND_CLUSTER" SPACE_KIND_API_URL="$API_URL" \
            bash "$REPO_ROOT/infra/space/kind/scripts/wait-node-publication.sh" 600; then
        exit 1
    fi

    # Cluster-controller rollout: any chart-value or image change Tilt
    # picked up needs to roll before the smoke tests touch reconciler-
    # driven state. The deployment uses Recreate strategy, so this is a
    # genuine "old pod gone, new pod available".
    echo "[bootstrap] waiting for cluster-controller deployment to roll"
    if ! kubectl --context "kind-${KIND_CLUSTER}" rollout status \
            deploy/space-cluster-controller -n space --timeout=300s >/dev/null; then
        echo "ERROR: space-cluster-controller did not become Available within 300s" >&2
        kubectl --context "kind-${KIND_CLUSTER}" get deploy/space-cluster-controller -n space >&2 || true
        exit 1
    fi

    bootstrap_kind_stack__wait_for_tilt_quiet

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
