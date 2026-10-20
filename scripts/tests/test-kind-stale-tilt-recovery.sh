#!/usr/bin/env bash
# The kind bring-up reuses a running `tilt up` when it serves this cluster's
# context. That check cannot tell a live watcher from one bound to a cluster
# that has since been deleted and recreated: the context name is identical, only
# the apiserver's host port changed, and client-go loaded the old one at
# startup. A pool devbox hit exactly this on 2026-09-22 — a four-day-old Tilt
# held :10350, the bring-up read "tilt is running", and the api-server wait ran
# its full 1800 s for nothing. These cases pin, against the real bring-up code
# with only the external boundaries stubbed:
#
#   1. A Tilt that predates the current control-plane container is stopped and
#      a fresh one started; a Tilt younger than the cluster is reused untouched.
#   2. A Tilt that itself reports a cluster connection error is restarted.
#      A Tilt that never connected at all (empty context, terminal error —
#      the devbox setup step's watcher when the cluster was not there yet)
#      fails the context check, so the old code took the "absent" branch and
#      refused the responsive server forever; when the process holding :10350
#      is ours (the pidfile names it, or it was started from this checkout) it,
#      and not some other `tilt up` on the box, is restarted; another watcher,
#      one serving a different context, or one whose context cannot be read,
#      keeps the refusal.
#   3. When the cluster's creation time cannot be read, the watcher is reused —
#      the reuse path stays the default; the check never kills on a guess.
#   4. A failed Tilt build ends the api-server wait with the resource named,
#      long before BOOTSTRAP_TIMEOUT; one sighting is not enough, two are.
#   5. Teardown stops a `tilt up` the pidfile does not name when :10350 reports
#      this cluster's context, and leaves one serving another context alone.
#   6. A process whose arguments merely contain "tilt up" — a shell running a
#      script that mentions it, or an agent whose prompt does — is not Tilt:
#      neither the stale-watcher kill nor teardown may signal it. The library's
#      old `pkill -f "tilt up"` killed the automation driving a bring-up on
#      2026-09-22, and the bring-up then ran on with nobody watching.
#   7. The stale port-forward reap stops a supervisor carrying the
#      `space-kind-pf:<ctx>` label as its last argument and a `kubectl …
#      --context <ctx> … port-forward` child, and nothing else — not a process
#      whose arguments mention the context and "port-forward" in passing (the
#      second way the same automation was killed that day), and not a forward
#      to another context.
#   8. A live Tilt watching another checkout — or a checkout deleted since it
#      started — is restarted from this one; one whose directory cannot be read
#      is reused. Reusing the former made `kind` from a fresh worktree pass in
#      8 s against the previous run's code (2026-09-23 reuse bench).
#   9. Initial Tiltfile evaluation may install observability receivers before
#      registering the cluster. It gets the bounded bootstrap budget, while a
#      dead process or failed evaluation stops early and a wrong context never
#      satisfies readiness.
#
# Hermetic by construction: every case that reaches a real `kill` runs with a
# `pgrep` that reports ONLY the fake processes the case started. The first
# version of this file let the real `pgrep -f` see the box, and case 5 — running
# the real teardown against a `tilt` stub that answered "our context" — killed
# the live Tilt watcher of the devbox it ran on while printing `ok:`.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

status=0
fail() {
    printf 'FAIL: %s\n' "$1" >&2
    shift
    [ "$#" -eq 0 ] || printf '%s\n' "$@" >&2
    status=1
}

# Write `$1/pgrep` that prints only the pids given after it, whatever the
# pattern. The library and teardown filter these through `ps -o command=`, which
# stays real, so the anchored matching under test is still exercised — only the
# candidate set is confined to processes this test owns.
write_pgrep_stub() {
    local dir="$1"
    shift
    {
        printf '#!/usr/bin/env bash\n'
        printf 'for p in %s; do kill -0 "$p" 2>/dev/null && printf "%%s\\n" "$p"; done\n' "$*"
        printf 'exit 0\n'
    } >"$dir/pgrep"
    chmod +x "$dir/pgrep"
}

# One bring-up under stubs. Arguments are NAME=VALUE knobs read by the stubs;
# the body runs in a subshell so no stub leaks between cases. Writes marker
# files into $work: `stopped`, `started`, `killed`.
run_bring_up() {
    local work="$1"
    shift
    (
        for knob in "$@"; do export "$knob"; done
        mkdir -p "$work/bin"
        # Layout-check subprocesses must not discover this machine's real
        # clusters. The parent shell's kind() below models the fixture cluster;
        # child guards see no cluster, and unknown cluster calls fail closed.
        printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/kind"
        printf '#!/usr/bin/env bash\nexit 99\n' >"$work/bin/kubectl"
        printf '#!/usr/bin/env bash\nexit 99\n' >"$work/bin/docker"
        chmod +x "$work/bin/kind" "$work/bin/kubectl" "$work/bin/docker"
        cat >"$work/bin/tilt" <<'TILT'
#!/usr/bin/env bash
case "$*" in
    *'jsonpath={.status.connection.kubernetes.context}'*)
        [[ -z "${TEST_TILT_CONTEXT_FAILS:-}" ]] || exit 1
        printf '%s' "${TEST_TILT_CONTEXT-kind-space-local}" ;;
    *'jsonpath={.status.error}'*) printf '%s' "${TEST_TILT_ERROR:-}" ;;
    *'get uiresources'*) printf '%b' "${TEST_UIRESOURCES:-}" ;;
    *'logs '*) echo "fake build log for $2" ;;
    *) exit 64 ;;
esac
TILT
        chmod +x "$work/bin/tilt"
        # The harness wraps its tilt queries in coreutils `timeout`, which a
        # macOS developer machine lacks; the stubs above return instantly, so
        # a pass-through stands in for it here and nowhere else.
        if ! command -v timeout >/dev/null 2>&1; then
            printf '#!/usr/bin/env bash\nshift\nexec "$@"\n' >"$work/bin/timeout"
            chmod +x "$work/bin/timeout"
        fi
        export PATH="$work/bin:$PATH"
        REPO_ROOT="$repo_root"
        KIND_CLUSTER=space-local
        API_URL=http://localhost:8080
        SKIP_BOOTSTRAP=0
        BOOTSTRAP_TIMEOUT="${TEST_BOOTSTRAP_TIMEOUT:-600}"
        BOOTSTRAP_TILT_FAILCHECK_INTERVAL=1
        # Its own knob rather than SPACE_TILT_PIDFILE, so a value inherited
        # from the box running this test can never name a real pidfile.
        SPACE_TILT_PIDFILE="${TEST_TILT_PIDFILE:-$work/no-such-pidfile}"
        unset SPACE_KIND_STORAGE_CREDENTIALS
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"

        pgrep() { return 0; }
        curl() { return 0; }
        lsof() { return 0; }
        # The cluster runs the requested storage credentials mode: the
        # authority's label and the worker volume-manager's env agree with the
        # default. Every other cluster read fails.
        kubectl() {
            case "$*" in
                *" get deployments -l app.kubernetes.io/name=space-credmgr-authority "*) printf 'exchange|' ;;
                *" get daemonset space-volume-manager "*) printf 'space-volume-manager|' ;;
                *) return 1 ;;
            esac
        }
        kill() { touch "$work/signaled"; }
        pkill() { touch "$work/signaled"; }
        kind() {
            [[ "$*" == 'get clusters' ]] || return 1
            printf '%s\n' "$KIND_CLUSTER"
        }
        bootstrap_kind_stack__api_reachable() { return "${TEST_API_REACHABLE:-1}"; }
        bootstrap_kind_stack__tilt_pid() { printf '%s\n' "${TEST_TILT_PID:-4242}"; }
        # The watcher on :10350 is the tracked one unless a case says otherwise.
        bootstrap_kind_stack__tilt_listener_pid() {
            printf '%s\n' "${TEST_TILT_LISTENER_PID:-${TEST_TILT_PID:-4242}}"
        }
        bootstrap_kind_stack__process_start_epoch() { printf '%s\n' "${TEST_TILT_EPOCH:-2000}"; }
        # Defaults to this checkout, so every other case sees its own watcher.
        bootstrap_kind_stack__tilt_cwd() {
            [[ "${TEST_TILT_CWD-}" != unknown ]] || return 1
            printf '%s\n' "${TEST_TILT_CWD:-$(cd "$REPO_ROOT" && pwd -P)}"
        }
        bootstrap_kind_stack__cluster_created_epoch() {
            [[ -n "${TEST_CLUSTER_EPOCH:-}" ]] || return 1
            printf '%s\n' "$TEST_CLUSTER_EPOCH"
        }
        # Records the pid it was handed; empty means "the tracked one".
        bootstrap_kind_stack__stop_stale_tilt() { printf '%s\n' "${1-}" >"$work/stopped"; }
        bootstrap_kind_stack__start_tilt() { touch "$work/started"; }
        bootstrap_kind_stack__kill_stale_tilt() { touch "$work/killed"; }
        bootstrap_kind_stack__bring_up
    ) >"$work/result" 2>&1
}

# Case 1a: Tilt older than the cluster → stopped and restarted (detached mode).
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_EPOCH=1000 TEST_CLUSTER_EPOCH=2000; then
    [[ -e "$work/stopped" && -e "$work/started" ]] \
        || fail "a Tilt older than the cluster must be stopped and restarted" "$(cat "$work/result")"
    [[ ! -e "$work/killed" ]] \
        || fail "the stale path must not go through the unverified-server kill" "$(cat "$work/result")"
    grep -q "predates the current kind cluster" "$work/result" \
        || fail "the stale verdict must say why" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed after restarting a stale Tilt" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 1b: Tilt younger than the cluster → reused, nothing signaled.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000; then
    [[ ! -e "$work/stopped" && ! -e "$work/started" && ! -e "$work/killed" && ! -e "$work/signaled" ]] \
        || fail "a Tilt younger than the cluster must be reused untouched" "$(cat "$work/result")"
    grep -q "tilt is already running" "$work/result" \
        || fail "the reuse path must report the running watcher" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed on a live Tilt" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 2: Tilt reports a connection error → restarted regardless of age.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_TILT_ERROR="connection refused to 127.0.0.1:36263"; then
    [[ -e "$work/stopped" && -e "$work/started" ]] \
        || fail "a Tilt reporting a cluster connection error must be restarted" "$(cat "$work/result")"
    grep -q "connection error" "$work/result" \
        || fail "the error-driven verdict must quote Tilt's error" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed after restarting an erroring Tilt" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 2b: a Tilt that never connected to any cluster — empty context, terminal
# status.error — fails `__tilt_running`, so before this fix the bring-up took
# the "absent" branch and `__kill_stale_tilt` refused the responsive server
# (exit 1, forever). Ours by pidfile alone (it runs from another checkout) →
# restarted like a stale one, with the stop handed the listener it verified.
work="$(mktemp -d)"
printf '4242\n' >"$work/orphan.pid"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT="" \
        TEST_TILT_ERROR="context kind-space-local does not exist" \
        TEST_TILT_PIDFILE="$work/orphan.pid" TEST_TILT_CWD=/somewhere/else; then
    [[ -e "$work/stopped" && -e "$work/started" ]] \
        || fail "a never-connected Tilt the pidfile names must be stopped and restarted" "$(cat "$work/result")"
    [[ "$(cat "$work/stopped")" == 4242 ]] \
        || fail "the orphan stop must be handed the verified listener's pid" "$(cat "$work/result")"
    [[ ! -e "$work/killed" ]] \
        || fail "the orphan path must not go through the unverified-server refusal" "$(cat "$work/result")"
    grep -q "never connected to a cluster" "$work/result" \
        || fail "the orphan verdict must say why" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed after restarting an owned never-connected Tilt" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 2c: the same shape, ours by checkout alone (no pidfile, started from
# this checkout) → restarted.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT="" \
        TEST_TILT_ERROR="context kind-space-local does not exist"; then
    [[ -e "$work/stopped" && -e "$work/started" && ! -e "$work/killed" ]] \
        || fail "a never-connected Tilt started from this checkout must be restarted" "$(cat "$work/result")"
    [[ "$(cat "$work/stopped")" == 4242 ]] \
        || fail "the orphan stop must be handed the verified listener's pid" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed after restarting a never-connected Tilt from this checkout" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 2d: the same never-connected shape but NOT ours (no pidfile, cwd not
# this checkout) keeps the refusal: another watcher is never signalled.
# (`__kill_stale_tilt` is stubbed here, so the refusal path is recognised by
# its marker rather than by the exit code it produces on a real box.)
work="$(mktemp -d)"
run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT="" \
        TEST_TILT_ERROR="context kind-space-local does not exist" TEST_TILT_CWD=/somewhere/else || true
[[ ! -e "$work/stopped" && ! -e "$work/signaled" ]] \
    || fail "an unowned never-connected Tilt must not be stopped" "$(cat "$work/result")"
[[ -e "$work/killed" ]] \
    || fail "an unowned responsive server must still reach the refusal path" "$(cat "$work/result")"
grep -q "never connected" "$work/result" \
    && fail "the orphan verdict must not fire for a Tilt that is not ours" "$(cat "$work/result")"
rm -rf "$work"

# Case 2e: a never-connected Tilt with someone else's context is not ours by
# definition, pidfile or not.
work="$(mktemp -d)"
printf '4242\n' >"$work/orphan.pid"
run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT="kind-other" \
        TEST_TILT_ERROR="connection refused" TEST_TILT_PIDFILE="$work/orphan.pid" || true
[[ ! -e "$work/stopped" && ! -e "$work/signaled" ]] \
    || fail "a Tilt serving another context must not be stopped" "$(cat "$work/result")"
[[ -e "$work/killed" ]] \
    || fail "a Tilt serving another context must reach the refusal path" "$(cat "$work/result")"
rm -rf "$work"

# Case 2f: empty context but NO error — a watcher still initializing, or one
# whose error the stub cannot answer — is not an orphan, pidfile or not. The
# first cut of the orphan check accepted the empty context alone and, run
# from test-kind-port-forward-parity.sh on a devbox with a live stack, walked
# the real pidfile into `__stop_stale_tilt`, which deleted it (bench
# 2026-09-23). Nothing is stopped and the refusal path is kept.
work="$(mktemp -d)"
printf '4242\n' >"$work/orphan.pid"
run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT="" TEST_TILT_ERROR="" \
        TEST_TILT_PIDFILE="$work/orphan.pid" || true
[[ ! -e "$work/stopped" && ! -e "$work/signaled" ]] \
    || fail "an empty context without a terminal error must not be treated as an orphan" "$(cat "$work/result")"
[[ -e "$work/killed" ]] \
    || fail "a still-initializing watcher must reach the refusal path, not a restart" "$(cat "$work/result")"
[[ -s "$work/orphan.pid" ]] \
    || fail "the refusal path must leave the pidfile alone" "$(cat "$work/result")"
rm -rf "$work"

# Case 2g: several watchers. The pidfile names the tracked `tilt up` (4242), a
# healthy one on another port, while a different `tilt up` (5151) started
# elsewhere holds :10350 and reported the terminal error. Ownership is decided
# for the listener, whose status was read: not ours, so nothing is stopped.
# Deciding it for the tracked pid instead stopped the healthy watcher and left
# :10350 held.
work="$(mktemp -d)"
printf '4242\n' >"$work/orphan.pid"
run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT="" \
        TEST_TILT_ERROR="context kind-space-local does not exist" TEST_TILT_PIDFILE="$work/orphan.pid" \
        TEST_TILT_LISTENER_PID=5151 TEST_TILT_CWD=/somewhere/else || true
[[ ! -e "$work/stopped" && ! -e "$work/signaled" ]] \
    || fail "a pidfile naming another watcher must not make an unowned listener ours" "$(cat "$work/result")"
[[ -e "$work/killed" ]] \
    || fail "an unowned listener must reach the refusal path" "$(cat "$work/result")"
rm -rf "$work"

# Case 2h: the converse. No pidfile, so the tracked pid falls back to the
# oldest `tilt up` (4242), a healthy watcher on another port; the listener
# (5151) is the never-connected one, started from this checkout. The stop is
# handed 5151, not left to resolve the tracked pid on its own.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT="" \
        TEST_TILT_ERROR="context kind-space-local does not exist" TEST_TILT_LISTENER_PID=5151; then
    [[ "$(cat "$work/stopped" 2>/dev/null)" == 5151 ]] \
        || fail "the orphan stop must target the listener, not the tracked watcher" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed after restarting the never-connected listener" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 2i: a context query that fails is not an observed empty context. The
# refusal stays, even with a terminal error and a pidfile naming the listener.
work="$(mktemp -d)"
printf '4242\n' >"$work/orphan.pid"
run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_CONTEXT_FAILS=1 \
        TEST_TILT_ERROR="context kind-space-local does not exist" TEST_TILT_PIDFILE="$work/orphan.pid" || true
[[ ! -e "$work/stopped" && ! -e "$work/signaled" ]] \
    || fail "a Tilt whose context cannot be read must not be stopped" "$(cat "$work/result")"
[[ -e "$work/killed" ]] \
    || fail "a Tilt whose context cannot be read must reach the refusal path" "$(cat "$work/result")"
rm -rf "$work"

# Case 3: cluster creation time unknown → reuse (never kill on a guess).
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_EPOCH=1000; then
    [[ ! -e "$work/stopped" && ! -e "$work/started" ]] \
        || fail "an undeterminable cluster age must not restart the watcher" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed when the cluster age is unknown" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 8a: a live Tilt watching another checkout → stopped and restarted here.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_TILT_CWD=/home/dev/.devbox/worktrees/space-worktree-8cf65586; then
    [[ -e "$work/stopped" && -e "$work/started" ]] \
        || fail "a Tilt watching another checkout must be restarted from this one" "$(cat "$work/result")"
    grep -q "is watching /home/dev/.devbox/worktrees/space-worktree-8cf65586, not this checkout" "$work/result" \
        || fail "the restart must name the checkout the old watcher served" "$(cat "$work/result")"
    [[ ! -e "$work/killed" ]] \
        || fail "the other-checkout path must not go through the unverified-server kill" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed after moving Tilt to this checkout" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 8b: this checkout deleted and recreated since Tilt started (same path,
# " (deleted)" cwd) → restarted; the old watcher holds the dead directory.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_TILT_CWD="$(cd "$repo_root" && pwd -P) (deleted)"; then
    [[ -e "$work/stopped" && -e "$work/started" ]] \
        || fail "a Tilt watching a deleted checkout must be restarted" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed after replacing a watcher of a deleted checkout" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 8c: the watcher's directory cannot be read → reused, nothing signaled.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_TILT_CWD=unknown; then
    [[ ! -e "$work/stopped" && ! -e "$work/started" && ! -e "$work/signaled" ]] \
        || fail "an unreadable watcher directory must not restart the watcher" "$(cat "$work/result")"
else
    fail "detached bring-up must succeed when the watcher's directory is unknown" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 4: a failed build fails the api-server wait fast, naming the resource.
# BOOTSTRAP_TIMEOUT is 600 s; the wait must give up within a few seconds and
# only after seeing the failure twice (the check runs every 1 s here).
work="$(mktemp -d)"
started_at=$SECONDS
if run_bring_up "$work" KIND_STACK_DETACH=0 TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_API_REACHABLE=1 TEST_BOOTSTRAP_TIMEOUT=600 \
        TEST_UIRESOURCES='(Tiltfile) ok\nspace-api error\nspace-postgres ok\n'; then
    fail "a failed Tilt build must fail the bring-up" "$(cat "$work/result")"
else
    elapsed=$(( SECONDS - started_at ))
    (( elapsed < 60 )) \
        || fail "the failed build must end the wait well before BOOTSTRAP_TIMEOUT (took ${elapsed}s)"
    grep -q "Tilt reports failed builds" "$work/result" && grep -q "tilt logs space-api" "$work/result" \
        || fail "the failure must name the failed resource and show its log" "$(cat "$work/result")"
    grep -q "fake build log for space-api" "$work/result" \
        || fail "the failed resource's log tail must be printed" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 4b: a resource that is merely pending never trips the fail-fast; the
# wait runs to its (short) timeout and reports the api-server, as before.
work="$(mktemp -d)"
if run_bring_up "$work" KIND_STACK_DETACH=0 TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_API_REACHABLE=1 TEST_BOOTSTRAP_TIMEOUT=3 \
        TEST_UIRESOURCES='(Tiltfile) ok\nspace-api in_progress\nspace-postgres pending\n'; then
    fail "an unreachable api-server must still time out" "$(cat "$work/result")"
else
    grep -q "api-server did not become reachable" "$work/result" \
        || fail "a pending build must not be reported as a failed one" "$(cat "$work/result")"
    grep -q "Tilt reports failed builds" "$work/result" \
        && fail "pending/in_progress must not trip the failed-build check" "$(cat "$work/result")"
fi
rm -rf "$work"

# Initial startup, against the real start/liveness helpers. Only the process,
# clock and CLI boundaries are stubbed: no real Tilt, cluster or signal.
run_startup() {
    local work="$1"
    shift
    (
        for knob in "$@"; do export "$knob"; done
        REPO_ROOT="$repo_root"
        KIND_CLUSTER=space-local
        BOOTSTRAP_TIMEOUT="${TEST_STARTUP_TIMEOUT:-120}"
        SPACE_TILT_STATE_DIR="$work/state"
        SPACE_TILT_LOG="$work/tilt.log"
        SPACE_TILT_PIDFILE="$work/tilt.pid"
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"
        clock=0
        # The detached command produces only this fixture's log/marker.
        setsid() { echo "synthetic startup log"; touch "$work/spawned"; }
        pgrep() { return 0; }
        curl() { return 0; }
        timeout() { shift; "$@"; }
        date() {
            [[ "$*" == +%s ]] || return 99
            # Let the background stub write its log before a failure tails it.
            local attempt
            for ((attempt = 0; attempt < 100; attempt++)); do
                [[ -e "$work/spawned" ]] && break
                command sleep 0.01
            done
            printf '%s\n' "$clock"
        }
        sleep() { clock=$((clock + 20)); }
        kill() {
            [[ "$1" == -0 ]] || { touch "$work/unexpected-signal"; return 99; }
            [[ "${TEST_PROCESS_DEAD:-0}" == 0 ]]
        }
        tilt() {
            case "$*" in
                *'jsonpath={.status.connection.kubernetes.context}'*)
                    if (( clock >= ${TEST_CONTEXT_AFTER:-80} )); then
                        printf '%s' "${TEST_CONTEXT:-kind-space-local}"
                    fi ;;
                *'jsonpath={.status.terminated.error}'*)
                    printf '%s' "${TEST_EVALUATION_ERROR:-}" ;;
                *) return 99 ;;
            esac
        }
        result=0
        bootstrap_kind_stack__start_tilt || result=$?
        printf '%s\n' "$clock" >"$work/elapsed"
        exit "$result"
    ) >"$work/result" 2>&1
}

for detached in 0 1; do
    work="$(mktemp -d)"
    if run_startup "$work" TEST_CONTEXT_AFTER=80 TEST_STARTUP_TIMEOUT=120 \
        KIND_STACK_DETACH="$detached" SPACE_TILT_DETACH_TIMEOUT=60; then
        [[ "$detached" == 0 && "$(cat "$work/elapsed")" == 80 ]] \
            || fail "only a blocking start may wait past the detached budget" "$(cat "$work/result")"
    else
        [[ "$detached" == 1 && "$(cat "$work/elapsed")" == 60 ]] \
            || fail "detached registration must stop at its short bound" "$(cat "$work/result")"
        grep -q "detached Tilt remains running" "$work/result" \
            || fail "timeout must explain that the watcher is still running" "$(cat "$work/result")"
    fi
    [[ ! -e "$work/unexpected-signal" ]] || fail "startup must not signal a process"
    rm -rf "$work"
done

for reason in dead evaluation; do
    work="$(mktemp -d)"
    knobs=(TEST_CONTEXT_AFTER=200 TEST_STARTUP_TIMEOUT=120)
    if [[ "$reason" == dead ]]; then
        knobs+=(TEST_PROCESS_DEAD=1)
    else
        knobs+=(TEST_EVALUATION_ERROR=synthetic-evaluation-failure)
    fi
    if run_startup "$work" "${knobs[@]}"; then
        fail "$reason startup must fail rather than wait for readiness"
    fi
    [[ "$(cat "$work/elapsed")" == 0 ]] \
        || fail "$reason startup must fail before consuming the budget" "$(cat "$work/result")"
    grep -q "synthetic startup log" "$work/result" \
        || fail "$reason startup must include its diagnostic log tail" "$(cat "$work/result")"
    [[ ! -e "$work/unexpected-signal" ]] || fail "startup must not signal a process"
    rm -rf "$work"
done

work="$(mktemp -d)"
if run_startup "$work" TEST_CONTEXT_AFTER=0 TEST_CONTEXT=kind-another-bench TEST_STARTUP_TIMEOUT=100; then
    fail "a responsive Tilt with another context must never satisfy startup"
fi
[[ "$(cat "$work/elapsed")" == 100 ]] \
    || fail "wrong-context startup must stop at the configured budget" "$(cat "$work/result")"
grep -q "within 100s" "$work/result" \
    || fail "a timed-out startup must report its actual bound" "$(cat "$work/result")"
rm -rf "$work"

for invalid in 0 -1 abc; do
    work="$(mktemp -d)"
    if run_startup "$work" TEST_CONTEXT_AFTER=0 TEST_STARTUP_TIMEOUT="$invalid"; then
        fail "invalid startup budget $invalid must be refused"
    fi
    [[ ! -e "$work/spawned" ]] || fail "invalid startup budget must fail before starting Tilt"
    rm -rf "$work"
done

work="$(mktemp -d)"
if run_startup "$work" KIND_STACK_DETACH=1 SPACE_TILT_DETACH_TIMEOUT=241; then
    fail "detached startup cannot exceed the setup-step deadline"
fi
[[ ! -e "$work/spawned" ]] || fail "oversized detached budget must fail before starting Tilt"
rm -rf "$work"

# Case 5: teardown stops a pidfile-less `tilt up` serving this cluster's
# context, and leaves one serving another context alone. A real process stands
# in for Tilt: `exec -a "tilt up"` gives it the command line the harness's
# ownership regex requires, so `pgrep -f`, `ps -o command=` and `kill` all run
# unstubbed; only kind, kubectl, curl and tilt are replaced on PATH.
teardown_case() {
    local served_context="$1" expect_dead="$2"
    local work
    work="$(mktemp -d)"
    mkdir -p "$work/bin"
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/kind"
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/kubectl"
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/curl"
    printf '#!/usr/bin/env bash\nprintf %%s "%s"\n' "$served_context" >"$work/bin/tilt"
    if ! command -v timeout >/dev/null 2>&1; then
        printf '#!/usr/bin/env bash\nshift\nexec "$@"\n' >"$work/bin/timeout"
    fi
    chmod +x "$work/bin/"*
    bash -c 'exec -a "tilt up" sleep 300' >/dev/null 2>&1 &
    local fake_pid=$!
    sleep 0.2
    if ! ps -p "$fake_pid" -o command= | grep -q '^tilt up'; then
        echo "skip: this platform does not let exec -a rename the fake tilt; case 5 not exercised" >&2
        kill "$fake_pid" 2>/dev/null || true
        rm -rf "$work"
        return 0
    fi
    write_pgrep_stub "$work/bin" "$fake_pid"
    PATH="$work/bin:$PATH" SPACE_TILT_PIDFILE="$work/no-such-pidfile" \
        SPACE_PORT_FORWARD_PIDFILE="$work/no-such-pf" \
        bash "$repo_root/infra/space/kind/scripts/teardown.sh" >"$work/result" 2>&1 \
        || fail "teardown must succeed with a fake Tilt on the box" "$(cat "$work/result")"
    sleep 0.5
    if [[ "$expect_dead" == 1 ]]; then
        if kill -0 "$fake_pid" 2>/dev/null; then
            fail "teardown must stop a pidfile-less tilt serving $served_context" "$(cat "$work/result")"
        fi
        grep -q "not in the pidfile" "$work/result" \
            || fail "teardown must say it stopped the pidfile-less watcher" "$(cat "$work/result")"
    else
        kill -0 "$fake_pid" 2>/dev/null \
            || fail "teardown must leave a tilt serving $served_context alone" "$(cat "$work/result")"
    fi
    kill "$fake_pid" 2>/dev/null || true
    wait "$fake_pid" 2>/dev/null || true
    rm -rf "$work"
}
# Belt and braces: the real-process cases below confine `pgrep`, so a live
# watcher on this box is never a candidate — but if one is serving :10350 and
# the operator did not opt in, skip them rather than trust that alone.
if command -v curl >/dev/null 2>&1 && curl -sf -m 1 http://localhost:10350/ >/dev/null 2>&1 \
        && [[ "${SPACE_TEST_ALLOW_LIVE_TILT:-0}" != "1" ]]; then
    echo "skip: a Tilt watcher is serving :10350 on this box; the real-process cases (5-7) are" \
        "skipped (set SPACE_TEST_ALLOW_LIVE_TILT=1 to run them anyway)" >&2
    real_process_cases=0
else
    real_process_cases=1
fi

if (( real_process_cases )); then
teardown_case kind-space-local 1
teardown_case kind-production 0

# Case 6: a bystander whose command line contains "tilt up" survives both the
# stale-watcher kill in the library and teardown. Real processes again.
bystander_case() {
    local work
    work="$(mktemp -d)"
    mkdir -p "$work/bin"
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/kind"
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/kubectl"
    printf '#!/usr/bin/env bash\nexit 1\n' >"$work/bin/curl"   # :10350 does not answer
    printf '#!/usr/bin/env bash\nprintf %%s kind-space-local\n' >"$work/bin/tilt"
    if ! command -v timeout >/dev/null 2>&1; then
        printf '#!/usr/bin/env bash\nshift\nexec "$@"\n' >"$work/bin/timeout"
    fi
    chmod +x "$work/bin/"*
    bash -c 'exec -a "note: run tilt up later" sleep 300' >/dev/null 2>&1 &
    local bystander=$!
    sleep 0.2
    if ! ps -p "$bystander" -o command= | grep -q 'tilt up'; then
        echo "skip: this platform does not let exec -a rename the bystander; case 6 not exercised" >&2
        kill "$bystander" 2>/dev/null || true
        rm -rf "$work"
        return 0
    fi
    write_pgrep_stub "$work/bin" "$bystander"
    # The library's stale-watcher kill, with no Tilt on the box at all: the
    # bystander is the only `pgrep -f "tilt up"` match and must be left alone.
    (
        export PATH="$work/bin:$PATH"
        REPO_ROOT="$repo_root"
        KIND_CLUSTER=space-local
        SPACE_TILT_PIDFILE="$work/no-such-pidfile"
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"
        bootstrap_kind_stack__kill_stale_tilt
        bootstrap_kind_stack__stop_stale_tilt
    ) >"$work/result" 2>&1 || fail "the stale-watcher kill must succeed with only a bystander present" "$(cat "$work/result")"
    kill -0 "$bystander" 2>/dev/null \
        || fail "the stale-watcher kill signaled a process that merely mentions 'tilt up'" "$(cat "$work/result")"
    grep -q "stale 'tilt up' detected" "$work/result" \
        && fail "a bystander must not be reported as a stale Tilt" "$(cat "$work/result")"
    # Teardown with :10350 answering for our context (curl stub now succeeds).
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/curl"
    PATH="$work/bin:$PATH" SPACE_TILT_PIDFILE="$work/no-such-pidfile" \
        SPACE_PORT_FORWARD_PIDFILE="$work/no-such-pf" \
        bash "$repo_root/infra/space/kind/scripts/teardown.sh" >"$work/result" 2>&1 \
        || fail "teardown must succeed with a bystander present" "$(cat "$work/result")"
    sleep 0.3
    kill -0 "$bystander" 2>/dev/null \
        || fail "teardown signaled a process that merely mentions 'tilt up'" "$(cat "$work/result")"
    kill "$bystander" 2>/dev/null || true
    wait "$bystander" 2>/dev/null || true
    rm -rf "$work"
}
bystander_case

# Case 7: the port-forward reap. Real processes: a labelled supervisor (label
# last), a fake kubectl child (argv-0 renamed to kubectl), a bystander whose
# arguments mention the context and the word in passing, and a forward to
# another context. Only the first two may die.
port_forward_reap_case() {
    local work
    work="$(mktemp -d)"
    # Every fake detaches from this script's stdout/stderr: a `sleep` child
    # that inherits the pipe would keep the test runner's reader open after the
    # script exits. `; true` stops bash from exec'ing its single command, which
    # would lose the renamed argv[0] the reap's anchored match looks for.
    bash -c 'sleep 60; true' _ svc/space-api 8080:8080 api space kind-space-local space-kind-pf:kind-space-local \
        >/dev/null 2>&1 &
    local supervisor=$!
    bash -c 'exec -a kubectl bash -c "sleep 60; true" _ --context kind-space-local -n space port-forward svc/space-api 8080:8080' \
        >/dev/null 2>&1 &
    local child=$!
    bash -c 'sleep 60; true' _ "note: kubectl --context kind-space-local get rs; later run test-kind-port-forward-parity.sh" \
        >/dev/null 2>&1 &
    local bystander=$!
    bash -c 'exec -a kubectl bash -c "sleep 60; true" _ --context kind-production -n space port-forward svc/space-api 8080:8080' \
        >/dev/null 2>&1 &
    local foreign=$!
    sleep 0.3
    if ! ps -p "$child" -o command= | grep -q '^kubectl'; then
        echo "skip: this platform does not let exec -a rename the fake kubectl; case 7 not exercised" >&2
        kill "$supervisor" "$child" "$bystander" "$foreign" 2>/dev/null || true
        rm -rf "$work"
        return 0
    fi
    mkdir -p "$work/bin"
    write_pgrep_stub "$work/bin" "$supervisor" "$child" "$bystander" "$foreign"
    (
        export PATH="$work/bin:$PATH"
        REPO_ROOT="$repo_root"
        KIND_CLUSTER=space-local
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"
        kind_stack_reap_stale_port_forwards
    ) >"$work/result" 2>&1 || fail "the port-forward reap must not fail" "$(cat "$work/result")"
    sleep 0.5
    # The reap signals the wrapper shells; their sleep children are reaped here
    # so nothing outlives the case.
    pkill -P "$supervisor" 2>/dev/null || true
    pkill -P "$child" 2>/dev/null || true
    kill -0 "$supervisor" 2>/dev/null \
        && fail "the reap must stop the labelled supervisor" "$(cat "$work/result")"
    kill -0 "$child" 2>/dev/null \
        && fail "the reap must stop the kubectl port-forward child for this context" "$(cat "$work/result")"
    kill -0 "$bystander" 2>/dev/null \
        || fail "the reap signaled a process that merely mentions the context and port-forward" "$(cat "$work/result")"
    kill -0 "$foreign" 2>/dev/null \
        || fail "the reap signaled a port-forward to another context" "$(cat "$work/result")"
    local p
    for p in "$supervisor" "$child" "$bystander" "$foreign"; do
        pkill -P "$p" 2>/dev/null || true
        kill "$p" 2>/dev/null || true
    done
    wait "$supervisor" "$child" "$bystander" "$foreign" 2>/dev/null || true
    rm -rf "$work"
}
port_forward_reap_case
fi

if [ "$status" -eq 0 ]; then
    echo "ok: a Tilt bound to a deleted cluster or another checkout is restarted, a live one is reused, a failed build" \
        "fails the bring-up fast, teardown stops the watcher it would otherwise orphan, and neither" \
        "the Tilt nor the port-forward reap signals a process that merely mentions its pattern"
fi
exit "$status"
