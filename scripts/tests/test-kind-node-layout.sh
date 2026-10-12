#!/usr/bin/env bash
# Exercise check-node-layout.sh at the kind, kubectl and Docker boundaries;
# never touch a real cluster or the host daemon.
#   1. No cluster: nothing to refuse.
#   2. A cluster still on the node-stack layout is refused with the recreate
#      commands, whatever its port mappings.
#   3. A cluster publishing every host port mapping kind-cluster.yaml.tmpl
#      declares passes; a mapping on another address, or one missing, is
#      refused, naming the missing mapping and the recreate commands.
#   4. A cluster whose node containers cannot be listed is refused, not passed.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/infra/space/kind/scripts/check-node-layout.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin"
cat >"$work/bin/kind" <<'EOF'
#!/usr/bin/env bash
[[ "$1 $2" == "get clusters" ]] || { echo "unexpected kind call: $*" >&2; exit 99; }
printf '%s\n' ${STUB_CLUSTERS:-}
EOF
cat >"$work/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
printf '%s' "${STUB_LEGACY:-}"
EOF
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "$1" in
    ps)
        [[ -z "${STUB_PS_FAILS:-}" ]] || { echo "Cannot connect to the Docker daemon" >&2; exit 1; }
        printf '%s\n' space-local-control-plane space-local-worker3 ;;
    inspect)
        printf '%b' "${STUB_PUBLISHED:-}" ;;
    *) echo "unexpected Docker operation: $*" >&2; exit 99 ;;
esac
EOF
chmod +x "$work/bin/"*
export PATH="$work/bin:$PATH"

status=0
fail() {
    printf 'FAIL: %s\n' "$1" >&2
    shift
    [ "$#" -eq 0 ] || printf '%s\n' "$@" >&2
    status=1
}
check() { # expected-status description
    local want="$1" what="$2" got=0
    bash "$script" >"$work/output" 2>&1 || got=$?
    [[ "$got" == "$want" ]] || fail "$what: exit $got, want $want" "$(cat "$work/output")"
}

api_port='6443/tcp 127.0.0.1 41234\n'
entry='30080/tcp 127.0.0.1 8080\n30090/tcp 127.0.0.1 8090\n'

# Case 1.
STUB_CLUSTERS="" check 0 "no cluster"

# Case 2.
export STUB_CLUSTERS=space-local
STUB_LEGACY="daemonset.apps/space-node-stack" STUB_PUBLISHED="$api_port$entry" \
    check 1 "node-stack layout"
grep -q 'single node-stack pod' "$work/output" || fail "node-stack refusal names the layout" "$(cat "$work/output")"
grep -q 'bash infra/space/kind/scripts/teardown.sh' "$work/output" \
    || fail "node-stack refusal prints the recreate commands" "$(cat "$work/output")"

# Case 3.
STUB_PUBLISHED="$api_port$entry" check 0 "every declared mapping published"
STUB_PUBLISHED="$api_port"'30080/tcp 0.0.0.0 8080\n30090/tcp 127.0.0.1 8090\n' \
    check 1 "a mapping on another address"
grep -q '30080/tcp 127.0.0.1 8080' "$work/output" \
    || fail "the refusal names the mapping it wanted" "$(cat "$work/output")"
STUB_PUBLISHED="$api_port" check 1 "no entry mappings"
grep -q '30090/tcp 127.0.0.1 8090' "$work/output" \
    || fail "the refusal names every missing mapping" "$(cat "$work/output")"
grep -q 'bash infra/space/kind/scripts/bootstrap.sh' "$work/output" \
    || fail "the port refusal prints the recreate commands" "$(cat "$work/output")"

# Case 4.
STUB_PS_FAILS=1 check 1 "node containers cannot be listed"

if (( status == 0 )); then
    echo "check-node-layout tests passed"
fi
exit "$status"
