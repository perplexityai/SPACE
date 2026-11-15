#!/usr/bin/env bash
# Exercise space-kind-node.sh's fresh_instance against a temporary root. The
# script sources the node image's libraries and works on the node's own
# paths, so the test takes that one function from it and runs it in a shell of
# its own, every path it touches under the temporary directory and
# `systemctl` stubbed.
#   1. A boot that finds the marker removes it, everything under the node's
#      run directory and the node artifact store, keeps the rest of the vendor
#      tree, and leaves the done file.
#   2. A boot whose wipe fails has removed the marker all the same, so the
#      next boot doesn't retry it, and leaves no done file.
#   3. Outside boot it removes the marker and nothing else, and fails.
#   4. Without the marker it changes nothing.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/infra/space/kind/node/space-kind-node.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

function_text="$(sed -n '/^fresh_instance() {$/,/^}$/p' "$script")"
if [[ -z $function_text ]]; then
    echo "FAIL: $script has no fresh_instance() to run" >&2
    exit 1
fi

mkdir -p "$work/bin"
cat >"$work/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == is-system-running ]] || { echo "unexpected systemctl call: $*" >&2; exit 99; }
printf '%s\n' "${STUB_SYSTEM:-starting}"
EOF
chmod +x "$work/bin/systemctl"
export PATH="$work/bin:$PATH"

status=0
fail() {
    printf 'FAIL: %s\n' "$1" >&2
    shift
    [ "$#" -eq 0 ] || printf '%s\n' "$@" >&2
    status=1
}

# A stopped node's disk under $work/<name>, with the marker unless told not to.
plant() { # name [no-marker]
    local root="$work/$1"
    mkdir -p "$root/etc/space" "$root/space/run/sandbox-manager" \
        "$root/space/vendor/artifacts/artifact-1" "$root/space/vendor/kernels"
    : >"$root/space/run/sandbox-manager/state.db"
    : >"$root/space/vendor/artifacts/artifact-1/disk.img"
    : >"$root/space/vendor/kernels/vmlinux"
    [[ ${2:-} == no-marker ]] || : >"$root/etc/space/kind-fresh-instance"
}

# Run fresh_instance on $work/<name>'s paths, with `prelude` run first, in a
# shell of its own so errexit holds; its output goes to $work/output.
run_fresh() { # name [prelude]
    local root="$work/$1"
    cat >"$work/run.sh" <<EOF
set -euo pipefail
log() { printf '%s\n' "\$*"; }
die() { printf 'ERROR: %s\n' "\$*" >&2; exit 1; }
FRESH_INSTANCE_MARKER="$root/etc/space/kind-fresh-instance"
CACHE_STATE_DIR="$root/run/space-kind-node"
FRESH_INSTANCE_DONE="\$CACHE_STATE_DIR/fresh-instance.done"
NODE_RUN_DIR="$root/space/run"
VENDOR_DIR="$root/space/vendor"
$function_text
${2:-}
fresh_instance
EOF
    bash "$work/run.sh" >"$work/output" 2>&1
}

# Case 1.
plant fresh
run_fresh fresh || fail "a boot with the marker fails" "$(cat "$work/output")"
root="$work/fresh"
[[ ! -e $root/etc/space/kind-fresh-instance ]] || fail "the marker is left"
[[ -d $root/space/run && -z "$(ls -A "$root/space/run")" ]] || fail "the run directory is not emptied"
[[ ! -e $root/space/vendor/artifacts ]] || fail "the artifact store is left"
[[ -e $root/space/vendor/kernels/vmlinux ]] || fail "the rest of the vendor tree is removed"
[[ -e $root/run/space-kind-node/fresh-instance.done ]] || fail "no done file"

# Case 2.
plant cut-short
if run_fresh cut-short 'find() { echo "find: cannot delete" >&2; return 1; }'; then
    fail "a boot whose wipe fails succeeds" "$(cat "$work/output")"
fi
root="$work/cut-short"
[[ ! -e $root/etc/space/kind-fresh-instance ]] || fail "a failed wipe leaves the marker for the next boot"
[[ ! -e $root/run/space-kind-node/fresh-instance.done ]] || fail "a failed wipe leaves a done file"

# Case 3.
plant live
if STUB_SYSTEM=running run_fresh live; then
    fail "fresh-instance runs on a live node" "$(cat "$work/output")"
fi
grep -q 'runs only while the node boots (the system is running)' "$work/output" \
    || fail "the refusal names the system state" "$(cat "$work/output")"
root="$work/live"
[[ ! -e $root/etc/space/kind-fresh-instance ]] || fail "outside boot the marker is left"
[[ -e $root/space/run/sandbox-manager/state.db ]] || fail "outside boot the run directory is touched"
[[ -e $root/space/vendor/artifacts/artifact-1/disk.img ]] || fail "outside boot the artifact store is touched"
[[ ! -e $root/run/space-kind-node/fresh-instance.done ]] || fail "outside boot a done file is left"

# Case 4.
plant ordinary no-marker
run_fresh ordinary || fail "a boot without the marker fails" "$(cat "$work/output")"
root="$work/ordinary"
[[ -e $root/space/run/sandbox-manager/state.db ]] || fail "without the marker the run directory is touched"
[[ -e $root/space/vendor/artifacts/artifact-1/disk.img ]] || fail "without the marker the artifact store is touched"
[[ ! -e $root/run/space-kind-node/fresh-instance.done ]] || fail "without the marker a done file is left"

exit "$status"
