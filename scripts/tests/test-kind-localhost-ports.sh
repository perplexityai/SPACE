#!/usr/bin/env bash
# A kind smoke suite reaches the cluster on localhost, through either the
# cluster's host port mappings (the API and data plane entries in
# kind-cluster.yaml.tmpl) or a port forward the Tiltfile holds. An endpoint a
# suite adds without either fails that suite for a reason that has nothing to
# do with what it asserts: the since-retired node-drain suite's object-store
# assertion queried localhost:8333 under a bring-up that did not forward it.
#
# So: every localhost port a suite talks to must be mapped or forwarded by the
# Tiltfile, and a mapped port must not also be forwarded: the forward could
# not bind it. The reverse is deliberately not required — the bring-up may
# expose extras (the UI on 3000) that no suite drives.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
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
    has_match "$port" "$tilt_ports" -x \
        && fail "localhost:$port is a host port mapping of the cluster, but the Tiltfile also" \
            "      forwards it; the forward cannot bind a port Docker already holds."
done <<<"$mapped_ports"

while IFS= read -r port; do
    [ -n "$port" ] || continue
    has_match "$port" "$mapped_ports" -x && continue
    has_match "$port" "$tilt_ports" -x \
        || fail "the suites use localhost:$port but the Tiltfile does not forward it." \
            "      That suite fails for a reason that has nothing to do with what it asserts."
done <<<"$suite_ports"

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
    unset SPACE_KIND_STORAGE_CREDENTIALS SPACE_KIND_RUST_BUILD SPACE_KIND_SUBSTRATE
    source "$repo_root/scripts/tests/lib/bootstrap-kind-stack.sh"

    pgrep() { return 0; }
    curl() { return 0; }
    lsof() { return 0; }
    kind() {
        [[ "$*" == 'get clusters' ]] || return 1
        printf '%s\n' "$KIND_CLUSTER"
    }
    # The cluster runs the requested storage credentials mode, Rust build and
    # substrate: the authority's label, the worker volume-manager's env, the
    # build deployed-rust-build.py reads and the API's SPACE_K8S_CONTROL_PLANE
    # agree with the defaults. Every other cluster read fails.
    python3() {
        case "$*" in
            *deployed-rust-build.py*) echo test-hooks ;;
            *) command python3 "$@" ;;
        esac
    }
    kubectl() {
        case "$*" in
            *" get deployments -l app.kubernetes.io/name=space-credmgr-authority "*) printf 'exchange|' ;;
            *" get daemonset space-volume-manager "*) printf 'space-volume-manager|' ;;
            *" get deployments -l app.kubernetes.io/name=space-api "*) echo true ;;
            *) return 1 ;;
        esac
    }
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
        "mapped onto the cluster's host ports or forwarded by the Tiltfile, and the Tilt" \
        "bring-up verifies the cluster its Tilt serves"
fi
exit "$status"
