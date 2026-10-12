#!/usr/bin/env bash
# A kind smoke suite reaches the cluster through localhost port-forwards, and the
# stack can be brought up two supported ways: `infra/space/kind/scripts/bootstrap.sh`
# or Tilt. Both have their own forward list, so an endpoint added to a suite —
# or to one list only — produces a suite that passes under one bring-up and fails
# under the other, blaming whatever it was asserting.
#
# That is not hypothetical: the since-retired node-drain suite's object-store
# assertion queried localhost:8333, which Tilt forwarded and the scripted
# bootstrap did not.
#
# So: every localhost port a suite talks to must be forwarded by both, or be one
# of the cluster's host port mappings (the API and data plane entries in
# kind-cluster.yaml.tmpl), which both bring-ups create the cluster from. A
# mapped port must not also be forwarded: the forward could not bind it. The
# reverse is deliberately not required — a bring-up may expose extras (the UI on
# 3000) that no suite drives.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
bootstrap="${repo_root}/infra/space/kind/scripts/bootstrap.sh"
tiltfile="${repo_root}/Tiltfile"
cluster_template="${repo_root}/infra/space/kind/kind-cluster.yaml.tmpl"

status=0
fail() {
    printf 'FAIL: %s\n' "$1" >&2
    shift
    [ "$#" -eq 0 ] || printf '%s\n' "$@" >&2
    status=1
}

# Match a pattern against a string without a pipeline. `printf … | grep -q` is a
# race under `set -o pipefail`: grep -q exits at the first match, printf then dies
# of SIGPIPE, and pipefail reports the pipeline as failed — so a property that
# holds intermittently reads as broken. Here-strings have no such stage.
has_match() { # has_match <pattern> <string> [extra grep flags]
    local pattern="$1" subject="$2"
    shift 2
    grep -q "$@" -e "$pattern" <<<"$subject"
}

# Ports the suites default to. Written as full URLs in their env-knob defaults,
# which is what makes them discoverable rather than a list to maintain here.
suite_ports="$(
    grep -hoE 'https?://(localhost|127\.0\.0\.1):[0-9]+' "${repo_root}"/scripts/tests/smoke-kind-*.sh \
        | sed -E 's/.*:([0-9]+)$/\1/' | sort -u
)"
[ -n "$suite_ports" ] || fail "found no localhost endpoints in the kind smoke suites"

scripted_ports="$(
    grep -oE '^[[:space:]]*start_pf +svc/[A-Za-z0-9.-]+ +[0-9]+:[0-9]+' "$bootstrap" \
        | awk '{print $3}' | cut -d: -f1 | sort -u
)"
[ -n "$scripted_ports" ] || fail "found no start_pf forwards in ${bootstrap#"${repo_root}"/}"

tilt_ports="$(
    grep -oE '"[0-9]+:[0-9]+"' "$tiltfile" | tr -d '"' | cut -d: -f1 | sort -u
)"
[ -n "$tilt_ports" ] || fail "found no port_forwards in Tiltfile"

mapped_ports="$(
    grep -oE '^[[:space:]]*hostPort: *[0-9]+' "$cluster_template" | awk '{print $2}' | sort -u
)"
[ -n "$mapped_ports" ] || fail "found no host port mappings in ${cluster_template#"${repo_root}"/}"

while IFS= read -r port; do
    [ -n "$port" ] || continue
    for forwarded in "bootstrap.sh:$scripted_ports" "the Tiltfile:$tilt_ports"; do
        has_match "$port" "${forwarded#*:}" -x \
            && fail "localhost:$port is a host port mapping of the cluster, but ${forwarded%%:*} also" \
                "      forwards it; the forward cannot bind a port Docker already holds."
    done
done <<<"$mapped_ports"

while IFS= read -r port; do
    [ -n "$port" ] || continue
    has_match "$port" "$mapped_ports" -x && continue
    has_match "$port" "$scripted_ports" -x \
        || fail "the suites use localhost:$port but ${bootstrap#"${repo_root}"/} does not forward it." \
            "      That suite fails under the scripted bring-up for a reason that has" \
            "      nothing to do with what it asserts."
    has_match "$port" "$tilt_ports" -x \
        || fail "the suites use localhost:$port but the Tiltfile does not forward it." \
            "      That suite fails under the Tilt bring-up for a reason that has" \
            "      nothing to do with what it asserts."
done <<<"$suite_ports"

# A stale self-restarting forward from an earlier run holds its port, and the
# pattern-based cleanups reap by Service name. A forward whose Service the
# pattern misses therefore survives and blocks the next bring-up's forward — the
# suite then fails for want of a port, not for what it asserts. So every target
# has to be matched by every pattern that claims to reap these forwards. The
# patterns stay narrow rather than becoming `pkill -f kubectl`: a shared devbox
# runs other workspaces' port-forwards too.
pf_targets="$(grep -oE '^[[:space:]]*start_pf +svc/[A-Za-z0-9.-]+' "$bootstrap" \
    | awk '{print $2}' | sort -u)"
# Every pattern any script hands to `pkill -f` for this purpose, whichever way it
# is quoted — including the one bootstrap.sh prints for a human to paste, because
# a hint that no longer matches is its own trap.
reap_patterns="$(
    grep -hoE 'pkill -f \\?["'"'"'][^"'"'"']+' \
        "$bootstrap" \
        | sed -E -e 's/^pkill -f \\?["'"'"']//' -e 's/\\$//' | sort -u
)"
[ -n "$reap_patterns" ] || fail "found no port-forward cleanup patterns to check targets against"

# Each forward is two processes, and they look nothing alike: the kubectl child
# carries `--context <ctx> … port-forward`, while the supervisor's command line
# is port-forward.sh and its arguments, ending in the label. A pattern matching
# only the child kills it and leaves the loop to respawn one, so both shapes are
# checked.
kctx="kind-space-local"
# The patterns reference the context through a variable; substitute the default
# so they can be matched against a representative command line.
expand_ctx() {
    printf '%s' "$1" \
        | sed -e 's/kind-\${SPACE_KIND_CLUSTER:-space-local}/'"$kctx"'/g' \
              -e 's/\${kctx}/'"$kctx"'/g' \
              -e 's/\$KCTX/'"$kctx"'/g'
}
while IFS= read -r target; do
    [ -n "$target" ] || continue
    child="kubectl --context $kctx -n space port-forward --address=127.0.0.1 $target 1:1"
    supervisor="bash $repo_root/infra/space/kind/scripts/port-forward.sh $kctx space $target 1:1 space-kind-pf:$kctx"
    while IFS= read -r pattern; do
        [ -n "$pattern" ] || continue
        expanded="$(expand_ctx "$pattern")"
        for shape in "child:$child" "supervisor:$supervisor"; do
            has_match "$expanded" "${shape#*:}" -E \
                || fail "the cleanup pattern '$pattern' does not match the ${shape%%:*} process" \
                    "      of the forward for $target, so it survives a reap and holds the port."
        done
    done <<<"$reap_patterns"
done <<<"$pf_targets"
# And the patterns must not reach beyond this cluster. A `kubectl port-forward`
# aimed at prod or testing is the footgun the suites' port-owner guard exists
# for; reaping it from a kind bring-up would be worse than leaving it.
foreign="kubectl --context example-eks-cluster -n space port-forward svc/space-api 8080:8080"
while IFS= read -r pattern; do
    [ -n "$pattern" ] || continue
    expanded="$(expand_ctx "$pattern")"
    has_match "$expanded" "$foreign" -E \
        && fail "the cleanup pattern '$pattern' also matches a port-forward to another" \
            "      cluster ($foreign)."
done <<<"$reap_patterns"

# Exercise detached bring-up with a live Tilt endpoint. Host prerequisites and
# external CLI boundaries are replaced: the production liveness/bring-up/cleanup code runs.
if ! (
    work="$(mktemp -d)"
    trap 'rm -rf "$work"' EXIT
    mkdir -p "$work/bin"
    cat >"$work/bin/tilt" <<'TILT'
#!/usr/bin/env bash
[[ "$*" == 'get cluster default --port 10350 -o jsonpath={.status.connection.kubernetes.context}' ]] || exit 64
printf '%s' "${TEST_TILT_CONTEXT:-}"
exit "${TEST_TILT_EXIT:-0}"
TILT
    chmod +x "$work/bin/tilt"
    export PATH="$work/bin:$PATH"
    REPO_ROOT="$repo_root"
    KIND_CLUSTER=space-local
    API_URL=http://localhost:8080
    KIND_STACK_DETACH=1
    SKIP_BOOTSTRAP=0
    source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"

    pgrep() { return 0; }
    curl() { return 0; }
    lsof() { return 0; }
    kind() {
        [[ "$*" == 'get clusters' ]] || return 1
        printf '%s\n' "$KIND_CLUSTER"
    }
    kubectl() { return 1; }
    kill() { touch "$work/signaled"; }
    pkill() { touch "$work/signaled"; }

    export TEST_TILT_CONTEXT=kind-space-local
    (bootstrap_kind_stack__bring_up) >"$work/result" 2>&1 || exit 1
    for TEST_TILT_CONTEXT in kind-space-local-other kind-production ""; do
        export TEST_TILT_CONTEXT
        if (bootstrap_kind_stack__bring_up) >"$work/result" 2>&1; then
            echo "FAIL: detached bring-up accepted an unverified Tilt context" >&2
            exit 1
        fi
    done
    export TEST_TILT_CONTEXT=kind-space-local TEST_TILT_EXIT=1
    if (bootstrap_kind_stack__bring_up) >"$work/result" 2>&1; then
        echo "FAIL: detached bring-up accepted a failed Tilt context query" >&2
        exit 1
    fi
    [[ ! -e "$work/signaled" ]] || exit 1
); then
    fail "Tilt bring-up must verify its serving cluster and preserve other watchers"
fi


if [ "$status" -eq 0 ]; then
    echo "ok: every localhost port the kind suites use ($(printf '%s' "$suite_ports" | tr '\n' ' ')) is" \
        "mapped onto the cluster's host ports or forwarded by both bring-up paths, and every" \
        "forward is reaped by the cleanup patterns"
fi
exit "$status"
