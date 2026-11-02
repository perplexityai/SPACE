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
#      `space-kind-pf:<ctx>` label as its last argument and its own `kubectl …
#      --context <ctx> … port-forward` child, and nothing else — not a live
#      Tilt's own supervisor, not a forward nobody's supervisor owns, not a
#      process whose arguments mention the context and "port-forward" in
#      passing (the second way the same automation was killed that day), and
#      not a forward to another context.
#   8. A live Tilt watching another checkout — or a checkout deleted since it
#      started — is restarted from this one; one whose directory cannot be read
#      is reused. Reusing the former made `kind` from a fresh worktree pass in
#      8 s against the previous run's code (2026-09-23 reuse bench).
#   9. Initial Tiltfile evaluation may install observability receivers before
#      registering the cluster. It gets the bounded bootstrap budget, while a
#      dead process or failed evaluation stops early and a wrong context never
#      satisfies readiness. Tilt is started with `--context kind-<cluster>`:
#      allow_k8s_contexts only permits a context, so a plain `tilt up` applied
#      the stack to whatever context the kubeconfig named.
#  10. With :10350 silent, the stale-watcher kill stops only a `tilt up` the
#      pidfile names or one started from this checkout; another checkout's
#      watcher, or one naming another context, keeps running.
#  11. A warm bring-up whose Tilt has settled with a resource in error fails
#      with that resource's log: the old pods still pass every rollout, and
#      the quiet wait used to return 0 over them.
#  12. A Failed space-db-migrate Job is final only once Tilt's space-db-migrate
#      resource is idle: while Tilt is still replacing it, the old Job's
#      failure is not this run's verdict.
#  13. A watcher that has to be restarted but cannot be identified (the
#      :10350 listener is not a `tilt up` lsof can see, and no pidfile names
#      one) is refused; an older unrelated `tilt up` is never signalled in its
#      place.
#  14. "Up" means the current revision: once Tilt settles, the seed and
#      bucket-init Jobs it applied are checked again, so an old completed seed
#      Job with a pending replacement is not success; and a migration that
#      fails during the seed wait is diagnosed in seconds, not at its timeout.
#  15. Teardown stops only the :10350 listener, never another `tilt up` on the
#      host, and signals a pid from the scripted bring-up's port-forward
#      pidfile only while it is still a supervisor of this context.
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

# Write `$1/lsof` that reports the pid given (if any) as the :10350 listener
# and nothing else, appending each call to `$1/lsof.calls`. Every fixture that
# runs teardown or the library's real-process paths puts it first on PATH: the
# real lsof would find a developer's live Tilt on :10350, which passes the
# `tilt up` check and would be signalled.
write_lsof_stub() {
    local dir="$1" listener="${2:-}"
    printf '#!/usr/bin/env bash
printf "%%s\\n" "$*" >>"%s/lsof.calls"
case "$*" in *:10350*) [ -z "%s" ] || echo %s ;; esac
exit 0
' \
        "$dir" "$listener" "$listener" >"$dir/lsof"
    chmod +x "$dir/lsof"
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
        REPO_ROOT="${TEST_REPO_ROOT:-$repo_root}"
        KIND_CLUSTER=space-local
        API_URL=http://localhost:8080
        SKIP_BOOTSTRAP=0
        BOOTSTRAP_TIMEOUT="${TEST_BOOTSTRAP_TIMEOUT:-600}"
        BOOTSTRAP_TILT_FAILCHECK_INTERVAL=1
        # Its own knob rather than SPACE_TILT_PIDFILE, so a value inherited
        # from the box running this test can never name a real pidfile.
        SPACE_TILT_PIDFILE="${TEST_TILT_PIDFILE:-$work/no-such-pidfile}"
        unset SPACE_KIND_STORAGE_CREDENTIALS SPACE_KIND_RUST_BUILD SPACE_KIND_SUBSTRATE
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"

        pgrep() { [[ -z "${TEST_PGREP_PIDS:-}" ]] || printf '%s\n' $TEST_PGREP_PIDS; return 0; }
        curl() { return 0; }
        lsof() { return 0; }
        # The cluster runs the requested storage credentials mode, Rust build
        # and substrate: the authority's label and the node services' env agree
        # with the default, each storage DaemonSet's template holding no static
        # key and running no pod, the build deployed-rust-build.py reads is the
        # default, and so is the API's SPACE_K8S_CONTROL_PLANE. Every other
        # cluster read fails.
        python3() {
            case "$*" in
                *deployed-rust-build.py*) echo test-hooks ;;
                *) command python3 "$@" ;;
            esac
        }
        kubectl() {
            case "$*" in
                *" get deployments -l app.kubernetes.io/name=space-credmgr-authority "*) printf 'exchange|' ;;
                *" get deployments -l app.kubernetes.io/name=space-api "*) echo true ;;
                *" get pods -o json") printf '{"items": []}' ;;
                *" get daemonset "*" --ignore-not-found -o json")
                    printf '{"spec": {"template": {"spec": {"containers": [{"name": "service", "env": [{"name": "SPACE_OBJECT_STORE_BUCKET"}]}]}}}}' ;;
                *" get job space-db-migrate "*)
                    [[ -n "${TEST_MIGRATE_JOB:-}" ]] || return 1
                    printf '%s' "$TEST_MIGRATE_JOB" ;;
                *" logs job/space-db-migrate "*) echo "fake migration log" ;;
                *" logs job/"*) echo "fake job log" ;;
                *" get deploy,statefulset,daemonset "*) ;;
                *" get job/space-seed-publish "*) [[ -n "${TEST_SEED_JOBS:-}" ]] ;;
                # The seed Job's state on successive reads, `|`-separated; the
                # last one repeats.
                *" get job space-seed-publish "*)
                    [[ -n "${TEST_SEED_JOBS:-}" ]] || return 1
                    local reads states
                    reads=$(( $(cat "$work/seed-reads" 2>/dev/null || echo 0) + 1 ))
                    echo "$reads" >"$work/seed-reads"
                    IFS='|' read -r -a states <<<"$TEST_SEED_JOBS"
                    (( reads <= ${#states[@]} )) || reads=${#states[@]}
                    printf '%s' "${states[reads - 1]}" ;;
                *" get job space-object-store-bucket-init "*) printf 'uid-bucket Complete ' ;;
                # The previous code's blocking wait: as long as asked, then
                # its verdict.
                *" wait --for=condition=complete "*)
                    [[ -z "${TEST_SEED_WAIT_SECS:-}" ]] || { sleep "$TEST_SEED_WAIT_SECS"; return 1; } ;;
                *" rollout status "*) ;;
                *" get jobs "*) echo "fake job list" ;;
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
        # TEST_REAL_STOP=1 keeps the real tracked-pid lookup and stop, so a
        # case sees which process they would signal (`kill` is still a stub).
        if [[ "${TEST_REAL_STOP:-0}" != 1 ]]; then
            bootstrap_kind_stack__tilt_pid() { printf '%s\n' "${TEST_TILT_PID:-4242}"; }
        fi
        # The watcher on :10350 is the tracked one unless a case says otherwise;
        # `none` is a listener lsof cannot tie to a `tilt up`.
        bootstrap_kind_stack__tilt_listener_pid() {
            [[ "${TEST_TILT_LISTENER_PID:-}" != none ]] || return 1
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
        if [[ "${TEST_REAL_STOP:-0}" != 1 ]]; then
            bootstrap_kind_stack__stop_stale_tilt() { printf '%s\n' "${1-}" >"$work/stopped"; }
        fi
        bootstrap_kind_stack__start_tilt() { touch "$work/started"; }
        bootstrap_kind_stack__kill_stale_tilt() { touch "$work/killed"; }
        # Which build each pod runs is image-provenance.py's to check, with
        # its own tests; here it would ask the stubbed cluster and write a run
        # record under the real $HOME.
        bootstrap_kind_stack__check_provenance() { touch "$work/provenance-checked"; }
        "${TEST_ENTRY:-bootstrap_kind_stack__bring_up}"
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
# from a port-forward harness test on a devbox with a live stack, walked
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

# Case 11: Tilt has nothing pending or in progress, but a resource's last
# update failed. On a warm stack the old pods keep every rollout green, so the
# quiet wait must fail on the error itself, with the resource's log.
work="$(mktemp -d)"
started_at=$SECONDS
if run_bring_up "$work" TEST_ENTRY=bootstrap_kind_stack__wait_for_tilt_quiet TEST_BOOTSTRAP_TIMEOUT=60 \
        TEST_UIRESOURCES='(Tiltfile) ok\nspace-ui error\nspace-api ok\n'; then
    fail "a settled Tilt with a failed resource must fail the bring-up" "$(cat "$work/result")"
else
    (( SECONDS - started_at < 30 )) \
        || fail "the settled failure must end the wait well before BOOTSTRAP_TIMEOUT" "$(cat "$work/result")"
    grep -q "fake build log for space-ui" "$work/result" \
        || fail "the settled failure must print the failed resource's log tail" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 11b: settled with nothing in error → the quiet wait returns.
work="$(mktemp -d)"
run_bring_up "$work" TEST_ENTRY=bootstrap_kind_stack__wait_for_tilt_quiet TEST_BOOTSTRAP_TIMEOUT=60 \
        TEST_UIRESOURCES='(Tiltfile) ok\nspace-ui ok\n' \
    || fail "a settled Tilt with every resource ok must pass the quiet wait" "$(cat "$work/result")"
rm -rf "$work"

# Case 12: the space-db-migrate Job in the cluster is Failed, but Tilt is
# still updating space-db-migrate (a rerun after fixing db/). The API wait must
# not end on that old Job; with a 3 s budget it times out on the api-server.
work="$(mktemp -d)"
if run_bring_up "$work" TEST_ENTRY=bootstrap_kind_stack__wait_for_api TEST_BOOTSTRAP_TIMEOUT=3 \
        TEST_MIGRATE_JOB='uid-old Failed ' TEST_UIRESOURCES='space-db-migrate in_progress\nspace-api pending\n'; then
    fail "an unreachable api-server must still time out" "$(cat "$work/result")"
fi
grep -q "space-db-migrate Job failed" "$work/result" \
    && fail "a Failed Job Tilt is still replacing must not end the bring-up" "$(cat "$work/result")"
rm -rf "$work"

# Case 12b: the same Failed Job with space-db-migrate idle is the current one:
# the wait ends on it, with its logs, long before the budget.
work="$(mktemp -d)"
started_at=$SECONDS
run_bring_up "$work" TEST_ENTRY=bootstrap_kind_stack__wait_for_api TEST_BOOTSTRAP_TIMEOUT=600 \
        TEST_MIGRATE_JOB='uid-new Failed ' TEST_UIRESOURCES='space-db-migrate ok\nspace-api pending\n' || true
grep -q "space-db-migrate Job failed" "$work/result" && grep -q "fake migration log" "$work/result" \
    || fail "a Failed Job Tilt has finished applying must end the bring-up with its logs" "$(cat "$work/result")"
(( SECONDS - started_at < 60 )) \
    || fail "the current Job's failure must end the wait well before BOOTSTRAP_TIMEOUT" "$(cat "$work/result")"
rm -rf "$work"

# Case 13: Tilt on :10350 reports a connection error, so it has to be
# restarted, but the listener cannot be tied to a `tilt up` and no pidfile
# names one. The only `tilt up` on the box is an older, unrelated watcher on
# another port (a real process, so the real lookups see it). The bring-up must
# refuse and signal nothing; it used to fall back to that oldest watcher.
work="$(mktemp -d)"
bash -c 'exec -a "tilt up --port 10351" sleep 300' >/dev/null 2>&1 &
unrelated=$!
sleep 0.2
if ps -p "$unrelated" -o command= | grep -q '^tilt up'; then
    if run_bring_up "$work" KIND_STACK_DETACH=1 TEST_REAL_STOP=1 TEST_TILT_LISTENER_PID=none \
            TEST_PGREP_PIDS="$unrelated" TEST_TILT_ERROR="connection refused to 127.0.0.1:36263"; then
        fail "an unidentifiable watcher that must be restarted must be refused" "$(cat "$work/result")"
    fi
    [[ ! -e "$work/signaled" && ! -e "$work/started" ]] \
        || fail "the refusal must signal no process and start no Tilt" "$(cat "$work/result")"
    grep -q "Stop the watcher on :10350, then rerun" "$work/result" \
        || fail "the refusal must say what to do" "$(cat "$work/result")"
else
    echo "skip: this platform does not let exec -a rename the fake tilt; case 13 not exercised" >&2
fi
kill "$unrelated" 2>/dev/null || true
wait "$unrelated" 2>/dev/null || true
rm -rf "$work"

# Cases 14 run the whole bring-up, so node publication — a script that asks
# the API — is replaced in a shadow of the checkout; everything else in it is
# the real tree.
shadow_root="$(mktemp -d)"
shadow_checkout() {
    local target="$1" rel="$2" real="$repo_root" here="$1" part entry name
    local parts
    IFS=/ read -r -a parts <<<"$rel"
    for part in "${parts[@]}"; do
        mkdir -p "$here"
        for entry in "$real"/* "$real"/.[!.]*; do
            [[ -e "$entry" || -L "$entry" ]] || continue
            name="${entry##*/}"
            [[ "$name" == "$part" ]] || ln -s "$entry" "$here/$name"
        done
        real="$real/$part"
        here="$here/$part"
    done
}
shadow_checkout "$shadow_root" infra/space/kind/scripts/wait-node-publication.sh
printf '#!/usr/bin/env bash\necho "[stub] every SPACE node is published"\n' \
    >"$shadow_root/infra/space/kind/scripts/wait-node-publication.sh"

# Case 14a: a warm rerun whose seed changed. The seed wait finds the previous
# revision's completed Job; once Tilt settles, the Job in the cluster is its
# pending replacement. That is not "up".
work="$(mktemp -d)"
if run_bring_up "$work" TEST_REPO_ROOT="$shadow_root" TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_API_REACHABLE=0 SEED_PUBLISH_COMPLETE_TIMEOUT=4 \
        TEST_SEED_JOBS='uid-old Complete |uid-new ' TEST_UIRESOURCES='space-seed-publish ok\nspace-api ok\n'; then
    fail "a pending replacement seed Job must not be reported as up" "$(cat "$work/result")"
else
    grep -q "space-seed-publish Job did not complete" "$work/result" \
        || fail "the bring-up must name the seed Job it is still waiting for" "$(cat "$work/result")"
fi
rm -rf "$work"

# Case 14a': the same rerun with the replacement complete is up.
work="$(mktemp -d)"
run_bring_up "$work" TEST_REPO_ROOT="$shadow_root" TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_API_REACHABLE=0 SEED_PUBLISH_COMPLETE_TIMEOUT=4 \
        TEST_SEED_JOBS='uid-old Complete |uid-new Complete ' TEST_UIRESOURCES='space-seed-publish ok\n' \
    || fail "a settled stack with its current Jobs complete must be up" "$(cat "$work/result")"
[[ -e "$work/provenance-checked" ]] \
    || fail "the bring-up must still check which build each pod runs" "$(cat "$work/result")"
rm -rf "$work"

# Case 14b: the seed Job is pending and the migration fails meanwhile. The
# seed wait must end on the migration within seconds; a blocking wait reported
# the seed at its timeout instead.
work="$(mktemp -d)"
started_at=$SECONDS
run_bring_up "$work" TEST_REPO_ROOT="$shadow_root" TEST_TILT_EPOCH=3000 TEST_CLUSTER_EPOCH=2000 \
        TEST_API_REACHABLE=0 SEED_PUBLISH_COMPLETE_TIMEOUT=20 TEST_SEED_WAIT_SECS=20 \
        TEST_SEED_JOBS='uid-seed ' TEST_MIGRATE_JOB='uid-migrate Failed ' \
        TEST_UIRESOURCES='space-db-migrate ok\nspace-seed-publish ok\n' || true
grep -q "space-db-migrate Job failed" "$work/result" && grep -q "fake migration log" "$work/result" \
    || fail "a migration failing during the seed wait must be diagnosed as such" "$(cat "$work/result")"
(( SECONDS - started_at < 15 )) \
    || fail "the migration failure must end the seed wait in seconds, not at its timeout" "$(cat "$work/result")"
rm -rf "$work"
rm -rf "$shadow_root"

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
        setsid() { printf '%s\n' "$*" >"$work/argv"; echo "synthetic startup log"; touch "$work/spawned"; }
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
    [[ "$(cat "$work/argv")" == "nohup tilt up --context kind-space-local" ]] \
        || fail "Tilt must be started pinned to kind-space-local, not the kubeconfig's context" "$(cat "$work/argv")"
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
    bash -c 'exec -a "tilt up" sleep 300' >/dev/null 2>&1 &
    local fake_pid=$!
    # Another project's watcher on another port: never teardown's to stop.
    bash -c 'exec -a "tilt up --port 10351" sleep 300' >/dev/null 2>&1 &
    local other_pid=$!
    chmod +x "$work/bin/"*
    # lsof reports the first fake as the :10350 listener.
    write_lsof_stub "$work/bin" "$fake_pid"
    sleep 0.2
    if ! ps -p "$fake_pid" -o command= | grep -q '^tilt up'; then
        echo "skip: this platform does not let exec -a rename the fake tilt; case 5 not exercised" >&2
        kill "$fake_pid" "$other_pid" 2>/dev/null || true
        rm -rf "$work"
        return 0
    fi
    write_pgrep_stub "$work/bin" "$fake_pid" "$other_pid"
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
    kill -0 "$other_pid" 2>/dev/null \
        || fail "teardown stopped another project's tilt up (not the :10350 listener)" "$(cat "$work/result")"
    kill "$fake_pid" "$other_pid" 2>/dev/null || true
    wait "$fake_pid" "$other_pid" 2>/dev/null || true
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
    # No :10350 listener this fixture owns, so none at all.
    write_lsof_stub "$work/bin"
    [[ "$(PATH="$work/bin:$PATH" command -v lsof)" == "$work/bin/lsof" ]] \
        || fail "the bystander case must shadow the real lsof"
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
    # Teardown did look for the :10350 listener, and asked the stub.
    grep -q ':10350' "$work/bin/lsof.calls" 2>/dev/null \
        || fail "teardown's listener lookup must reach the stub lsof, not the real one" "$(cat "$work/result")"
    kill "$bystander" 2>/dev/null || true
    wait "$bystander" 2>/dev/null || true
    rm -rf "$work"
}
bystander_case

# Case 7: the port-forward reap. Real processes: an orphaned labelled
# supervisor (label last) with its own fake kubectl child (argv-0 renamed to
# kubectl); the same pair under a fake `tilt up` parent, as a live Tilt's
# serve_cmd runs; a kubectl forward to this context that no supervisor owns; a
# bystander whose arguments mention the context and the word in passing; and a
# forward to another context. Only the orphaned pair may die.
port_forward_reap_case() {
    local work
    work="$(mktemp -d)"
    # A supervisor that starts one kubectl-named child and waits on it. Every
    # fake detaches from this script's stdout/stderr: a `sleep` child that
    # inherits the pipe would keep the test runner's reader open after the
    # script exits. `; true` stops bash from exec'ing its single command, which
    # would lose the renamed argv[0] the reap's anchored match looks for.
    # Named and called exactly as a real supervisor is; it records its child
    # where PF_CHILD_FILE says.
    cat >"$work/port-forward.sh" <<'SUP'
(exec -a kubectl bash -c "sleep 60; true" _ --context "$1" -n "$2" port-forward "$3" "$4") &
printf '%s\n' "$!" >"$PF_CHILD_FILE"
wait
SUP
    PF_CHILD_FILE="$work/child.pid" bash "$work/port-forward.sh" kind-space-local space svc/space-ui 3000:3000 \
        space-kind-pf:kind-space-local >/dev/null 2>&1 &
    local supervisor=$!
    # Another cluster's supervisor, listed in the same shared pidfile.
    PF_CHILD_FILE="$work/other-child.pid" bash "$work/port-forward.sh" kind-other-bench space svc/space-ui 3001:3000 \
        space-kind-pf:kind-other-bench >/dev/null 2>&1 &
    local other_supervisor=$!
    cat >"$work/tilt-parent.sh" <<'TILTP'
PF_CHILD_FILE="$1/tilt-child.pid" bash "$1/port-forward.sh" kind-space-local space-logs svc/loki 13101:3100 \
    space-kind-pf:kind-space-local &
printf '%s\n' "$!" >"$1/tilt-supervisor.pid"
wait
TILTP
    bash -c 'exec -a "tilt up" bash "$1/tilt-parent.sh" "$1"' _ "$work" >/dev/null 2>&1 &
    local tilt_parent=$!
    bash -c 'exec -a kubectl bash -c "sleep 60; true" _ --context kind-space-local -n space port-forward svc/space-api 8081:8080' \
        >/dev/null 2>&1 &
    local unowned=$!
    bash -c 'sleep 60; true' _ "note: kubectl --context kind-space-local get rs; later run the port-forward reap" \
        >/dev/null 2>&1 &
    local bystander=$!
    bash -c 'exec -a kubectl bash -c "sleep 60; true" _ --context kind-production -n space port-forward svc/space-api 8080:8080' \
        >/dev/null 2>&1 &
    local foreign=$!
    # A search whose last argument is the label: not a supervisor.
    bash -c 'exec -a rg bash -c "sleep 60; true" _ --count space-kind-pf:kind-space-local' >/dev/null 2>&1 &
    local search=$!
    sleep 0.5
    local child tilt_supervisor tilt_child
    child="$(cat "$work/child.pid" 2>/dev/null || true)"
    tilt_supervisor="$(cat "$work/tilt-supervisor.pid" 2>/dev/null || true)"
    tilt_child="$(cat "$work/tilt-child.pid" 2>/dev/null || true)"
    local other_child
    other_child="$(cat "$work/other-child.pid" 2>/dev/null || true)"
    local all=("$supervisor" "$other_supervisor" "$tilt_parent" "$unowned" "$bystander" "$foreign" "$search" \
        $child $other_child $tilt_supervisor $tilt_child)
    if [[ -z "$child" || -z "$tilt_child" ]] || ! ps -p "$child" -o command= | grep -q '^kubectl' \
            || ! ps -p "$tilt_parent" -o command= | grep -q '^tilt up'; then
        echo "skip: this platform does not let exec -a rename the fakes; case 7 not exercised" >&2
        kill "${all[@]}" 2>/dev/null || true
        rm -rf "$work"
        return 0
    fi
    mkdir -p "$work/bin"
    write_pgrep_stub "$work/bin" "${all[@]}"
    write_lsof_stub "$work/bin"
    # The scripted bring-up's pidfile, listing the orphaned supervisor.
    printf '%s\n' "$supervisor" >"$work/pf.pids"
    (
        export PATH="$work/bin:$PATH"
        REPO_ROOT="$repo_root"
        KIND_CLUSTER=space-local
        SPACE_PORT_FORWARD_PIDFILE="$work/pf.pids"
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"
        kind_stack_reap_stale_port_forwards
    ) >"$work/result" 2>&1 || fail "the port-forward reap must not fail" "$(cat "$work/result")"
    sleep 0.5
    kill -0 "$supervisor" 2>/dev/null \
        && fail "the reap must stop the orphaned labelled supervisor" "$(cat "$work/result")"
    kill -0 "$child" 2>/dev/null \
        && fail "the reap must stop the orphaned supervisor's kubectl child" "$(cat "$work/result")"
    kill -0 "$tilt_supervisor" 2>/dev/null && kill -0 "$tilt_child" 2>/dev/null \
        || fail "the reap signaled a supervisor a live Tilt runs" "$(cat "$work/result")"
    kill -0 "$unowned" 2>/dev/null \
        || fail "the reap signaled a kubectl forward no supervisor owns" "$(cat "$work/result")"
    kill -0 "$bystander" 2>/dev/null \
        || fail "the reap signaled a process that merely mentions the context and port-forward" "$(cat "$work/result")"
    kill -0 "$foreign" 2>/dev/null \
        || fail "the reap signaled a port-forward to another context" "$(cat "$work/result")"
    kill -0 "$search" 2>/dev/null \
        || fail "the reap signaled a command whose last argument is merely the label" "$(cat "$work/result")"
    [[ ! -e "$work/pf.pids" ]] \
        || fail "the reap must remove the scripted bring-up's pidfile once its supervisors are gone" "$(cat "$work/result")"
    kill -0 "$other_supervisor" 2>/dev/null \
        || fail "the reap signaled another cluster's supervisor" "$(cat "$work/result")"
    # The shared pidfile stays while it lists another cluster's live supervisor.
    printf '%s\n' "$other_supervisor" >"$work/pf.pids"
    (
        export PATH="$work/bin:$PATH"
        REPO_ROOT="$repo_root"
        KIND_CLUSTER=space-local
        SPACE_PORT_FORWARD_PIDFILE="$work/pf.pids"
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"
        kind_stack_reap_stale_port_forwards
    ) >"$work/result" 2>&1 || fail "the port-forward reap must not fail" "$(cat "$work/result")"
    [[ -s "$work/pf.pids" ]] \
        || fail "the reap removed a shared pidfile that still lists another cluster's supervisor" "$(cat "$work/result")"
    local p
    for p in "${all[@]}"; do
        pkill -P "$p" 2>/dev/null || true
        kill "$p" 2>/dev/null || true
    done
    wait "$supervisor" "$other_supervisor" "$tilt_parent" "$unowned" "$bystander" "$foreign" "$search" 2>/dev/null || true
    rm -rf "$work"
}
port_forward_reap_case

# Case 15b: teardown and the scripted bring-up's pidfile. One recorded pid is
# still a supervisor of this context (with its kubectl child) and is stopped;
# the other has been reused by an unrelated process that leads its own process
# group, which the old group kill took down. It must survive.
teardown_pidfile_case() {
    local work
    work="$(mktemp -d)"
    mkdir -p "$work/bin"
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/kind"
    printf '#!/usr/bin/env bash\nexit 0\n' >"$work/bin/kubectl"
    printf '#!/usr/bin/env bash\nexit 1\n' >"$work/bin/curl"   # no Tilt on :10350
    chmod +x "$work/bin/"*
    write_pgrep_stub "$work/bin"
    write_lsof_stub "$work/bin"
    cat >"$work/port-forward.sh" <<'SUP'
(exec -a kubectl bash -c "sleep 60; true" _ --context "$1" -n "$2" port-forward "$3" "$4") &
printf '%s\n' "$!" >"$PF_CHILD_FILE"
wait
SUP
    PF_CHILD_FILE="$work/child.pid" bash "$work/port-forward.sh" kind-space-local space svc/space-ui 3000:3000 \
        space-kind-pf:kind-space-local >/dev/null 2>&1 &
    local supervisor=$!
    setsid bash -c 'exec -a unrelated-service sleep 300' >/dev/null 2>&1 &
    local reused=$!
    sleep 0.3
    local child
    child="$(cat "$work/child.pid" 2>/dev/null || true)"
    if [[ -z "$child" || "$(ps -o pgid= -p "$reused" | tr -d ' ')" != "$reused" ]]; then
        echo "skip: could not stage the pidfile fixtures; case 15b not exercised" >&2
        kill "$supervisor" "$reused" $child 2>/dev/null || true
        rm -rf "$work"
        return 0
    fi
    printf '%s\n%s\n' "$supervisor" "$reused" >"$work/pf.pids"
    PATH="$work/bin:$PATH" SPACE_TILT_PIDFILE="$work/no-such-pidfile" \
        SPACE_PORT_FORWARD_PIDFILE="$work/pf.pids" \
        bash "$repo_root/infra/space/kind/scripts/teardown.sh" >"$work/result" 2>&1 \
        || fail "teardown must succeed with a stale port-forward pidfile" "$(cat "$work/result")"
    sleep 0.5
    kill -0 "$reused" 2>/dev/null \
        || fail "teardown signaled a reused pid from the port-forward pidfile" "$(cat "$work/result")"
    kill -0 "$supervisor" 2>/dev/null \
        && fail "teardown must stop a recorded pid that is still a supervisor" "$(cat "$work/result")"
    kill -0 "$child" 2>/dev/null \
        && fail "teardown must stop that supervisor's kubectl child" "$(cat "$work/result")"
    kill "$supervisor" "$reused" "$child" 2>/dev/null || true
    wait "$supervisor" 2>/dev/null || true
    rm -rf "$work"
}
teardown_pidfile_case

# Case 10: :10350 does not answer, so the bring-up clears stale watchers before
# starting its own. Real processes again: a `tilt up` started from another
# checkout, one from this checkout naming another context, one from this
# checkout, and one the pidfile names (started elsewhere). Only the last two
# may be signalled.
stale_kill_case() {
    local work
    work="$(mktemp -d)"
    mkdir -p "$work/bin" "$work/other-checkout"
    printf '#!/usr/bin/env bash\nexit 1\n' >"$work/bin/curl"   # :10350 does not answer
    chmod +x "$work/bin/curl"
    (cd "$work/other-checkout" && exec -a "tilt up" sleep 300) >/dev/null 2>&1 &
    local other=$!
    (cd "$repo_root" && exec -a "tilt up --context kind-other-bench --port 10351" sleep 300) >/dev/null 2>&1 &
    local other_context=$!
    (cd "$repo_root" && exec -a "tilt up" sleep 300) >/dev/null 2>&1 &
    local here=$!
    (cd "$work/other-checkout" && exec -a "tilt up" sleep 300) >/dev/null 2>&1 &
    local named=$!
    printf '%s\n' "$named" >"$work/tilt.pid"
    sleep 0.2
    if ! ps -p "$here" -o command= | grep -q '^tilt up'; then
        echo "skip: this platform does not let exec -a rename the fake tilt; case 10 not exercised" >&2
        kill "$other" "$other_context" "$here" "$named" 2>/dev/null || true
        rm -rf "$work"
        return 0
    fi
    write_pgrep_stub "$work/bin" "$other" "$other_context" "$here" "$named"
    write_lsof_stub "$work/bin"
    (
        export PATH="$work/bin:$PATH"
        REPO_ROOT="$repo_root"
        KIND_CLUSTER=space-local
        SPACE_TILT_PIDFILE="$work/tilt.pid"
        source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"
        bootstrap_kind_stack__kill_stale_tilt
    ) >"$work/result" 2>&1 || fail "the stale-watcher kill must succeed" "$(cat "$work/result")"
    sleep 0.3
    kill -0 "$other" 2>/dev/null \
        || fail "the stale-watcher kill stopped another checkout's tilt up" "$(cat "$work/result")"
    kill -0 "$other_context" 2>/dev/null \
        || fail "the stale-watcher kill stopped a tilt up serving another context" "$(cat "$work/result")"
    kill -0 "$here" 2>/dev/null \
        && fail "the stale-watcher kill must stop a tilt up started from this checkout" "$(cat "$work/result")"
    kill -0 "$named" 2>/dev/null \
        && fail "the stale-watcher kill must stop the tilt up the pidfile names" "$(cat "$work/result")"
    grep -q "leaving 'tilt up' (pid $other) running" "$work/result" \
        || fail "the stale-watcher kill must say which watchers it left running" "$(cat "$work/result")"
    kill "$other" "$other_context" "$here" "$named" 2>/dev/null || true
    wait "$other" "$other_context" "$here" "$named" 2>/dev/null || true
    rm -rf "$work"
}
stale_kill_case
fi

if [ "$status" -eq 0 ]; then
    echo "ok: a Tilt bound to a deleted cluster or another checkout is restarted, a live one is reused, Tilt is" \
        "pinned to the kind context, a failed build or a settled error fails the bring-up fast, only the current" \
        "migration Job's failure is final, teardown stops the watcher it would otherwise orphan, and neither" \
        "the Tilt nor the port-forward reap signals a process that is not this bring-up's"
fi
exit "$status"
