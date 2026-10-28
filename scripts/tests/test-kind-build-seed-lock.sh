#!/usr/bin/env bash
# build-seed.sh is run by two Tilt resources at once (seed-artifacts and the
# space-seed-publish image build), and that image build then reads the seed
# directory. Its lock must keep every reader and writer of the directory
# apart. These cases run the real script, copied with the files it sources
# into a scratch checkout so the recipe can change under it, with only docker
# stubbed:
#
#   1. A build that waited on the lock builds with the recipe as it is once it
#      runs: settings read before the wait would be stamped with the new
#      recipe's hash and pass as current from then on.
#   2. A build that starts while `--read-lock` runs a command waits for that
#      command, so the image build never reads a directory being rewritten.
#   3. Without flock it refuses to run: two unguarded privileged builds would
#      write the same rootfs.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d)"
pids=()
cleanup() {
    local pid
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    rm -rf "$work"
}
trap cleanup EXIT
status=0

fail() {
    echo "FAIL: $1" >&2
    [[ -z "${2:-}" ]] || sed 's/^/    /' "$2" >&2
    status=1
}

# Up to ten seconds for a condition another process makes true.
wait_for() {
    local _
    for _ in $(seq 1 100); do
        eval "$1" && return 0
        sleep 0.1
    done
    return 1
}

repo="$work/repo"
for path in \
    Cargo.toml \
    ci/mirrored-images.txt \
    ci/scripts/mirror-images.sh \
    ci/scripts/lib/test-deps.sh \
    infra/space/kind/lib/build-cache.sh \
    infra/space/kind/lib/shared-build-inputs.sh \
    infra/space/kind/scripts/build-seed.sh \
    infra/space/kind/scripts/lib/kind-host-paths.sh \
    templates/firecracker/recipes/seed.toml \
    templates/firecracker/seed/Dockerfile \
    templates/firecracker/seed/rootfs-builder.sh; do
    mkdir -p "$repo/$(dirname "$path")"
    cp "$repo_root/$path" "$repo/$path"
done
script="$repo/infra/space/kind/scripts/build-seed.sh"
recipe="$repo/templates/firecracker/recipes/seed.toml"

export HOME="$work/home"
export SPACE_TILT_STATE_DIR="$work/state"
export SPACE_VENDOR_ROOT="$work/vendor"
export SPACE_SEED_GUEST_BIN_DIR="$work/guest-bin"
unset SPACE_SEED_REBUILD SPACE_KIND_BUILD_CACHE_REPOSITORY SPACE_KIND_BUILD_CACHE_WRITE SPACE_CI_IMAGE_MIRROR
mkdir -p "$SPACE_VENDOR_ROOT/firecracker/default/kernel" "$SPACE_SEED_GUEST_BIN_DIR" \
    "$HOME/.space-devbox/kind/ssh"
printf 'kernel\n' >"$SPACE_VENDOR_ROOT/firecracker/default/kernel/vmlinux.bin"
printf 'spaced\n' >"$SPACE_SEED_GUEST_BIN_DIR/spaced"
printf 'spaced-svc\n' >"$SPACE_SEED_GUEST_BIN_DIR/spaced-svc"
printf 'key\n' >"$HOME/.space-devbox/kind/ssh/sandbox_ed25519"
printf 'pubkey\n' >"$HOME/.space-devbox/kind/ssh/sandbox_ed25519.pub"

# The stub docker: image builds succeed, except the cloud-image export, which
# the script tolerates. The rootfs builder run writes the outputs it names and
# records the recipe date it was given, one line per run, in $STUB_RUNS.
stubs="$work/stubs"
mkdir -p "$stubs"
cat >"$stubs/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  build)
    for arg in "$@"; do [[ "$arg" != type=local,dest=* ]] || exit 1; done
    ;;
  run)
    out="" date=""
    while (( $# )); do
      case "$1" in
        -v) [[ "$2" != *:/output ]] || out="${2%:/output}"; shift 2 ;;
        -e) [[ "$2" != SPACE_SEED_UBUNTU_DATE=* ]] || date="${2#*=}"; shift 2 ;;
        *) shift ;;
      esac
    done
    [[ -n "$out" ]] || { echo "stub docker: no /output mount" >&2; exit 1; }
    printf '%s %s\n' "$out" "$date" >>"$STUB_RUNS"
    printf 'rootfs %s\n' "$date" >"$out/rootfs.ext4"
    printf '{"schema_version": 2, "execution": {}, "name": "seed"}\n' >"$out/metadata.json"
    ;;
esac
SH
chmod +x "$stubs/docker"
export STUB_RUNS="$work/runs"
: >"$STUB_RUNS"
base_path="$stubs:$PATH"

# Case 1: the recipe changes while a build waits on the lock.
out="$work/out1"
mkdir -p "$out"
exec 9>>"$out/.build.lock"
flock -x 9
# 9>&-: the build must not inherit the lock this shell holds.
SPACE_SEED_OUTPUT_DIR="$out" PATH="$base_path" bash "$script" >"$work/case1.log" 2>&1 9>&- &
pids+=("$!")
waiter=$!
if ! wait_for 'grep -q "another seed build holds" "$work/case1.log"'; then
    fail "case 1: the build did not wait on a held lock" "$work/case1.log"
fi
sed -i 's/^date = .*/date = "release-20991231"/' "$recipe"
exec 9>&-
rc=0
wait "$waiter" || rc=$?
built="$(awk -v out="$out" '$1 == out { print $2 }' "$STUB_RUNS")"
if (( rc != 0 )); then
    fail "case 1: the build that waited failed (rc $rc)" "$work/case1.log"
elif [[ "$built" != release-20991231 ]]; then
    fail "case 1: the build that waited built recipe date '${built:-none}', not the release-20991231 saved before it ran" "$work/case1.log"
fi

# Case 2: a build starts while `--read-lock` runs the image build.
out="$work/out2"
mkdir -p "$out"
SPACE_SEED_OUTPUT_DIR="$out" PATH="$base_path" bash "$script" --read-lock -- \
    bash -c ': >"$1/reading"; while [[ ! -e "$1/done" ]]; do sleep 0.1; done' _ "$work" \
    >"$work/case2-reader.log" 2>&1 &
pids+=("$!")
reader=$!
if ! wait_for '[[ -e "$work/reading" ]]'; then
    fail "case 2: --read-lock did not run its command" "$work/case2-reader.log"
else
    SPACE_SEED_OUTPUT_DIR="$out" PATH="$base_path" bash "$script" >"$work/case2-writer.log" 2>&1 &
    pids+=("$!")
    writer=$!
    if ! wait_for 'grep -q "another seed build holds" "$work/case2-writer.log"'; then
        fail "case 2: a build did not wait for the --read-lock command" "$work/case2-writer.log"
    fi
    if grep -q "^$out " "$STUB_RUNS" || [[ -e "$out/rootfs.ext4" ]]; then
        fail "case 2: a build wrote the seed while the --read-lock command was reading it" "$work/case2-writer.log"
    fi
    : >"$work/done"
    rc=0
    wait "$reader" || rc=$?
    (( rc == 0 )) || fail "case 2: the --read-lock command failed (rc $rc)" "$work/case2-reader.log"
    rc=0
    wait "$writer" || rc=$?
    if (( rc != 0 )) || [[ ! -f "$out/manifest.json" ]]; then
        fail "case 2: the build that waited did not complete (rc $rc)" "$work/case2-writer.log"
    fi
fi

# Case 3: no flock on PATH. Every other command stays, so only the lock is
# missing: the stubs first, then each PATH directory's entries but flock.
tools="$work/tools"
mkdir -p "$tools"
IFS=: read -ra path_dirs <<<"$PATH"
for dir in "${path_dirs[@]}"; do
    [[ -d "$dir" ]] || continue
    for entry in "$dir"/*; do
        name="${entry##*/}"
        [[ "$name" != flock && -x "$entry" && ! -e "$tools/$name" ]] || continue
        ln -s "$entry" "$tools/$name"
    done
done
out="$work/out3"
rc=0
SPACE_SEED_OUTPUT_DIR="$out" PATH="$stubs:$tools" bash "$script" >"$work/case3.log" 2>&1 || rc=$?
if (( rc == 0 )) || ! grep -q "flock" "$work/case3.log"; then
    fail "case 3: without flock the build must refuse to run, naming flock (rc $rc)" "$work/case3.log"
fi
if grep -q "^$out " "$STUB_RUNS"; then
    fail "case 3: without flock the rootfs builder still ran" "$work/case3.log"
fi

if (( status == 0 )); then
    echo "ok: build-seed.sh reads its inputs only once it holds its lock, a build waits for a --read-lock reader, and it refuses to run without flock"
fi
exit "$status"
