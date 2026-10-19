#!/usr/bin/env bash
# seed-node-containerd.sh and the copy it runs in each node image
# (lib/seed-containerd-dir.sh): absent and empty containerd directories are
# seeded from their node's image, one that holds state is never touched, and a
# copy that fails, is killed, or overlaps another leaves the target as it was
# or whole. Hermetic: docker is a stub whose `run` executes the copy script on
# this host against a fake preload, and no container runs.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/infra/space/kind/scripts/seed-node-containerd.sh"
copy_script="$repo_root/infra/space/kind/scripts/lib/seed-containerd-dir.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

FAILURES=0
fail() {
  echo "FAIL: $*" >&2
  FAILURES=$((FAILURES + 1))
}

# A node image's preload: a few files, as the copy sees them.
preload="$work/preload"
mkdir -p "$preload/io.containerd.content.v1.content/blobs/sha256" "$preload/io.containerd.metadata.v1.bolt"
for blob in one two three four; do echo "$blob" >"$preload/io.containerd.content.v1.content/blobs/sha256/$blob"; done
echo "preloaded" >"$preload/io.containerd.metadata.v1.bolt/meta.db"
same_as_preload() { diff -r "$preload" "$1" >/dev/null 2>&1; }
staging_in() { find "$1" -maxdepth 1 -name '.containerd.seed.*' -type d | wc -l | tr -d ' '; }

# A cp that, as FAKE_CP says, fails after one file, is killed after one file,
# or waits for $FAKE_CP_RELEASE (announcing $FAKE_CP_STARTED) before copying.
mkdir -p "$work/cp-bin"
cat >"$work/cp-bin/cp" <<'EOF'
#!/usr/bin/env bash
source_dir="${2%/.}"
target="${3%/}"
case "${FAKE_CP:-}" in
  fail | die)
    /bin/cp "$source_dir/io.containerd.metadata.v1.bolt/meta.db" "$target/"
    [[ "$FAKE_CP" == fail ]] && exit 1
    kill -KILL "$PPID"
    exit 1
    ;;
  wait)
    touch "$FAKE_CP_STARTED"
    while [[ ! -e "$FAKE_CP_RELEASE" ]]; do sleep 0.1; done
    ;;
esac
exec /bin/cp "$@"
EOF
chmod +x "$work/cp-bin/cp"

# copy <parent> [env...]: the copy script, as a node image runs it.
copy() {
  local parent="$1"
  shift
  env SEED_SOURCE="$preload" SEED_PARENT="$parent" SEED_TARGET=containerd "$@" \
    PATH="$work/cp-bin:$PATH" sh "$copy_script"
}

# ── the copy ─────────────────────────────────────────────────────────────────
node="$work/node-a"
mkdir -p "$node/containerd"
copy "$node" 2>"$work/err" || fail "a copy into an empty directory failed: $(cat "$work/err")"
same_as_preload "$node/containerd" || fail "a copy did not give the target the whole preload"
[[ "$(staging_in "$node")" == 0 ]] || fail "a copy left its staging directory"

status=0
copy "$node" 2>/dev/null || status=$?
[[ "$status" == 3 ]] || fail "a copy onto a seeded directory did not report it holds state (status $status)"

# A copy that fails leaves the target empty, and removes only its own staging.
node="$work/node-b"
mkdir -p "$node/containerd"
echo "another file" >"$node/unrelated"
status=0
copy "$node" FAKE_CP=fail 2>/dev/null || status=$?
[[ "$status" == 1 ]] || fail "a failed copy did not fail (status $status)"
[[ -z "$(ls -A "$node/containerd")" ]] || fail "a failed copy left files in the target"
[[ "$(staging_in "$node")" == 0 ]] || fail "a failed copy left its staging directory"
[[ -e "$node/unrelated" ]] || fail "a failed copy removed something besides its own staging"

# A copy that is killed partway leaves the target empty; the next copy removes
# what it left and seeds the target whole.
node="$work/node-c"
mkdir -p "$node/containerd"
copy "$node" FAKE_CP=die 2>/dev/null || true
[[ -z "$(ls -A "$node/containerd")" ]] || fail "a killed copy left a partial target"
[[ "$(staging_in "$node")" == 1 ]] || fail "a killed copy left no staging directory to clean up"
copy "$node" 2>"$work/err" || fail "the copy after a killed one failed: $(cat "$work/err")"
same_as_preload "$node/containerd" || fail "the copy after a killed one did not seed the target whole"
[[ "$(staging_in "$node")" == 0 ]] || fail "the dead copy's staging directory was not removed"
grep -q "left by a seed that did not finish" "$work/err" || fail "removing a dead copy's staging was not logged"

# Two copies at once, taking turns under the lock: the second finds the
# target seeded and leaves it.
node="$work/node-d"
mkdir -p "$node/containerd"
copy "$node" FAKE_CP=wait FAKE_CP_STARTED="$work/d-started" FAKE_CP_RELEASE="$work/d-release" \
  2>/dev/null &
first=$!
while [[ ! -e "$work/d-started" ]]; do sleep 0.1; done
copy "$node" 2>/dev/null &
second=$!
sleep 1
kill -0 "$second" 2>/dev/null || fail "an overlapping copy did not wait for the lock"
[[ -z "$(ls -A "$node/containerd")" ]] || fail "the target filled before either copy finished"
touch "$work/d-release"
status=0
wait "$first" || status=$?
[[ "$status" == 0 ]] || fail "the first of two copies failed (status $status)"
status=0
wait "$second" || status=$?
[[ "$status" == 3 ]] || fail "the second of two copies did not leave the seeded target (status $status)"
same_as_preload "$node/containerd" || fail "two copies at once left the target incomplete"
[[ "$(staging_in "$node")" == 0 ]] || fail "two copies at once left a staging directory"

# Without flock, two copies at once both run: the first to finish renames its
# copy onto the target, and the other finds it filled and removes only its own.
node="$work/node-e"
mkdir -p "$node/containerd"
copy "$node" SEED_FLOCK=no-such-flock FAKE_CP=wait FAKE_CP_STARTED="$work/e-started" \
  FAKE_CP_RELEASE="$work/e-release" 2>/dev/null &
first=$!
while [[ ! -e "$work/e-started" ]]; do sleep 0.1; done
copy "$node" SEED_FLOCK=no-such-flock 2>/dev/null || fail "an unlocked copy beside another failed"
same_as_preload "$node/containerd" || fail "an unlocked copy did not seed the target whole"
echo "booted" >"$node/containerd/io.containerd.metadata.v1.bolt/meta.db"
touch "$work/e-release"
status=0
wait "$first" || status=$?
[[ "$status" == 3 ]] || fail "the slower unlocked copy did not leave the filled target (status $status)"
[[ "$(cat "$node/containerd/io.containerd.metadata.v1.bolt/meta.db")" == booted ]] \
  || fail "the slower unlocked copy changed state the other run's node had written"
[[ "$(staging_in "$node")" == 0 ]] || fail "the slower unlocked copy left its staging directory"

# ── seed-node-containerd.sh ──────────────────────────────────────────────────
base="registry.example/node:v1.0.0@sha256:$(printf 'a%.0s' {1..64})"
space="registry.example/space-node:0123456789abcdef"

mkdir -p "$work/bin"
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
# Local images are FAKE_LOCAL, pullable ones FAKE_PULLABLE; `run` executes the
# copy script with the mounted directory as its parent and FAKE_PRELOAD as the
# image's preload, unless FAKE_RUN_FAILS.
printf '%s\n' "$*" >>"$FAKE_CALLS"
case "$1 $2" in
  "image inspect") [[ " $FAKE_LOCAL " == *" $3 "* ]] ;;
  "pull --quiet") [[ " ${FAKE_PULLABLE:-} " == *" $3 "* ]] ;;
  "run --rm")
    [[ -z "${FAKE_RUN_FAILS:-}" ]] || exit 1
    parent="" target="" copy_script="${*: -1}"
    while (($#)); do
      case "$1" in
        --volume) parent="${2%:/node}" ;;
        --env) target="${2#SEED_TARGET=}" ;;
      esac
      shift
    done
    SEED_SOURCE="$FAKE_PRELOAD" SEED_PARENT="$parent" SEED_TARGET="$target" sh -c "$copy_script"
    ;;
  *) echo "unexpected docker call: $*" >&2; exit 99 ;;
esac
EOF
chmod +x "$work/bin/docker"

data="$work/data"
cat >"$work/kind-cluster.yaml" <<EOF
kind: Cluster
nodes:
  - role: control-plane
    image: $base
    extraMounts:
      - hostPath: $data/control-plane/containerd   # absent
        containerPath: /var/lib/containerd
  - role: worker
    image: $space
    extraMounts:
      - hostPath: $data/worker1/containerd
        containerPath: /var/lib/containerd
      - hostPath: $work/vendor
        containerPath: /space/vendor
        readOnly: true
  - role: worker
    image: $space
    extraMounts:
      - hostPath: $data/worker2/containerd
        containerPath: /var/lib/containerd
EOF
printf 'kind: Cluster\nnodes:\n  - role: control-plane\n    image: %s\n' "$base" >"$work/no-mounts.yaml"

# reset: worker1 empty, worker2 holding a previous cluster's state, the
# control plane's directory absent.
reset() {
  rm -rf "$data" "$work/vendor"
  mkdir -p "$data/worker1/containerd" "$data/worker2/containerd" "$work/vendor"
  echo "previous cluster" >"$data/worker2/containerd/meta.db"
}
run_seed() {
  local config="$work/kind-cluster.yaml"
  if [[ "$1" == --config ]]; then
    config="$2"
    shift 2
  fi
  : >"$work/calls"
  env HOME="$work/home" PATH="$work/bin:$PATH" FAKE_CALLS="$work/calls" FAKE_PRELOAD="$preload" "$@" \
    bash "$script" "$config" >"$work/out" 2>"$work/err"
}
runs_for() { grep -c -- "--volume $(dirname "$1"):/node --env SEED_TARGET=$(basename "$1")" "$work/calls" || true; }

reset
run_seed FAKE_LOCAL="$base $space" || fail "seeding exited nonzero: $(cat "$work/err")"
grep -q -- "--volume $data/control-plane:/node .* $base -c" "$work/calls" \
  || fail "the control plane was not seeded from its own image"
same_as_preload "$data/control-plane/containerd" || fail "an absent directory was not seeded"
grep -q -- "--volume $data/worker1:/node .* $space -c" "$work/calls" \
  || fail "a worker was not seeded from its own image"
same_as_preload "$data/worker1/containerd" || fail "an empty directory was not seeded"
[[ "$(runs_for "$data/worker2/containerd")" == 0 ]] || fail "a directory holding state was seeded"
[[ "$(cat "$data/worker2/containerd/meta.db")" == "previous cluster" ]] || fail "a directory holding state was changed"
grep -q -- "--volume $work:/node" "$work/calls" && fail "a mount other than containerd's was seeded"
grep -q "seeded $data/worker1/containerd from $space" "$work/err" || fail "a seeded directory was not logged"
grep -q "$data/worker2/containerd already holds containerd state; left as it is" "$work/err" \
  || fail "a directory left as it is was not logged"
[[ -s "$work/out" ]] && fail "seeding wrote to stdout"

reset
run_seed FAKE_LOCAL="$base $space" FAKE_RUN_FAILS=1 || fail "a failed copy failed the bring-up"
grep -q "WARNING: could not seed $data/worker1/containerd from $space (status 1); it is as it was" \
  "$work/err" || fail "a failed copy was not reported: $(cat "$work/err")"
[[ -z "$(ls -A "$data/worker1/containerd")" ]] || fail "a failed copy changed an empty directory"

reset
run_seed FAKE_LOCAL="$space" FAKE_PULLABLE="$base" || fail "seeding with an absent image exited nonzero"
grep -qxF "pull --quiet $base" "$work/calls" || fail "an absent image was not pulled"
same_as_preload "$data/control-plane/containerd" || fail "the pulled image did not seed its node"

reset
run_seed FAKE_LOCAL="$space" FAKE_PULLABLE= || fail "an image that cannot be pulled failed the bring-up"
[[ "$(runs_for "$data/control-plane/containerd")" == 0 ]] || fail "a node whose image is missing was seeded"
grep -q "WARNING: $base is not local and could not be pulled" "$work/err" \
  || fail "a missing image was not reported"
same_as_preload "$data/worker1/containerd" || fail "one missing image stopped the other nodes' seeding"

reset
run_seed FAKE_LOCAL="$base $space" SPACE_KIND_SEED_CONTAINERD=0 || fail "a disabled seed exited nonzero"
[[ -s "$work/calls" ]] && fail "a disabled seed called docker"

run_seed --config "$work/no-mounts.yaml" FAKE_LOCAL="$base" || fail "a config without containerd mounts failed"
grep -q "WARNING: $work/no-mounts.yaml mounts no directory over /var/lib/containerd" "$work/err" \
  || fail "a config without containerd mounts was not reported"

if ((FAILURES != 0)); then
  exit 1
fi
echo "ok: empty and absent containerd directories are seeded whole from their node's image, others are left alone, and a failed, killed or overlapping copy leaves the target as it was or whole"
