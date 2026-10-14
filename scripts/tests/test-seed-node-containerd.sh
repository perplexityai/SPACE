#!/usr/bin/env bash
# seed-node-containerd.sh against a fake docker: it seeds each node's absent or
# empty containerd directory from that node's image, never touches one that
# holds state, and leaves the directory to the kubelets when it cannot seed.
# Hermetic: docker is a stub, and no container runs.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo_root/infra/space/kind/scripts/seed-node-containerd.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

FAILURES=0
fail() {
  echo "FAIL: $*" >&2
  FAILURES=$((FAILURES + 1))
}

base="registry.example/node:v1.0.0@sha256:$(printf 'a%.0s' {1..64})"
space="registry.example/space-node:0123456789abcdef"

mkdir -p "$work/bin"
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
# Local images are FAKE_LOCAL, pullable ones FAKE_PULLABLE; a `run` copies by
# creating a file in the directory mounted at /seed, unless FAKE_RUN_FAILS.
printf '%s\n' "$*" >>"$FAKE_CALLS"
case "$1 $2" in
  "image inspect") [[ " $FAKE_LOCAL " == *" $3 "* ]] ;;
  "pull --quiet") [[ " ${FAKE_PULLABLE:-} " == *" $3 "* ]] ;;
  "run --rm")
    [[ -z "${FAKE_RUN_FAILS:-}" ]] || exit 1
    while (($#)); do
      if [[ "$1" == --volume ]]; then
        touch "${2%:/seed}/io.containerd.metadata.v1.bolt"
      fi
      shift
    done
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

# reset: worker1 empty, worker2 holding a previous cluster's state, the
# control plane's directory absent.
reset() {
  rm -rf "$data" "$work/vendor"
  mkdir -p "$data/worker1/containerd" "$data/worker2/containerd" "$work/vendor"
  echo "previous cluster" >"$data/worker2/containerd/io.containerd.metadata.v1.bolt"
}
run_seed() {
  : >"$work/calls"
  env PATH="$work/bin:$PATH" FAKE_CALLS="$work/calls" "$@" \
    bash "$script" "$work/kind-cluster.yaml" >"$work/out" 2>"$work/err"
}
runs_for() { grep -c -- "--volume $1:/seed" "$work/calls" || true; }

reset
run_seed FAKE_LOCAL="$base $space" || fail "seeding exited nonzero: $(cat "$work/err")"
[[ "$(runs_for "$data/control-plane/containerd")" == 1 ]] || fail "an absent directory was not seeded"
grep -q -- "--volume $data/control-plane/containerd:/seed .* $base -c" "$work/calls" \
  || fail "the control plane was not seeded from its own image"
[[ "$(runs_for "$data/worker1/containerd")" == 1 ]] || fail "an empty directory was not seeded"
grep -q -- "--volume $data/worker1/containerd:/seed .* $space -c" "$work/calls" \
  || fail "a worker was not seeded from its own image"
[[ "$(runs_for "$data/worker2/containerd")" == 0 ]] || fail "a directory holding state was seeded"
[[ "$(cat "$data/worker2/containerd/io.containerd.metadata.v1.bolt")" == "previous cluster" ]] \
  || fail "a directory holding state was changed"
grep -q -- "--volume $work/vendor:/seed" "$work/calls" && fail "a mount other than containerd's was seeded"
grep -q "seeded $data/worker1/containerd from $space" "$work/err" || fail "a seeded directory was not logged"
grep -q "$data/worker2/containerd already holds containerd state; left as it is" "$work/err" \
  || fail "a directory left as it is was not logged"
[[ -s "$work/out" ]] && fail "seeding wrote to stdout"

reset
run_seed FAKE_LOCAL="$base $space" FAKE_RUN_FAILS=1 || fail "a failed copy failed the bring-up"
grep -q "WARNING: could not seed $data/worker1/containerd from $space; it stays empty and the kubelet pulls" \
  "$work/err" || fail "a failed copy was not reported: $(cat "$work/err")"

reset
run_seed FAKE_LOCAL="$space" FAKE_PULLABLE="$base" || fail "seeding with an absent image exited nonzero"
grep -qxF "pull --quiet $base" "$work/calls" || fail "an absent image was not pulled"
[[ "$(runs_for "$data/control-plane/containerd")" == 1 ]] || fail "the pulled image did not seed its node"

reset
run_seed FAKE_LOCAL="$space" FAKE_PULLABLE= || fail "an image that cannot be pulled failed the bring-up"
[[ "$(runs_for "$data/control-plane/containerd")" == 0 ]] || fail "a node whose image is missing was seeded"
grep -q "WARNING: $base is not local and could not be pulled" "$work/err" \
  || fail "a missing image was not reported"
[[ "$(runs_for "$data/worker1/containerd")" == 1 ]] || fail "one missing image stopped the other nodes' seeding"

reset
run_seed FAKE_LOCAL="$base $space" SPACE_KIND_SEED_CONTAINERD=0 || fail "a disabled seed exited nonzero"
[[ -s "$work/calls" ]] && fail "a disabled seed called docker"

if ((FAILURES != 0)); then
  exit 1
fi
echo "ok: empty and absent containerd directories are seeded from their node's image, others are left alone, and a failed seed leaves them to the kubelets"
