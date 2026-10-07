#!/usr/bin/env bash
# prepare-kind-node.sh — give each Kind SPACE node the node-level layer a
# production node image provides, before any SPACE pod starts there.
#
# A production worker or builder boots with its btrfs /space/cache, cgroup
# subtree, loop-device headroom, device permissions and /space layout already
# in place (infra/space/packer). A kind node is a stock kindest/node container,
# so this script installs systemd units on every sandbox-worker and builder
# node (not the control-plane or infra node) that set those up on every node
# start, ordered before kubelet — including after `docker restart`, which
# discards a container's /dev, mounts and cgroups. The units and the script
# they run live in infra/space/kind/node/; see space-kind-node.sh there.
#
# It also installs btrfs-progs on those nodes, as packer's 01-partitions.sh does
# for the node image, so the node formats its own cache image instead of a pod
# doing it.
#
# Idempotent: re-running reinstalls the files, re-runs every unit (each step
# is a no-op when its result is already in place) and verifies the cache. Safe
# against a live cluster, including one created before these units existed:
# the cache unit reuses the loop device a running node-stack pod already has.
#
# Usage:
#   bash infra/space/kind/scripts/prepare-kind-node.sh [node...]
#
# With no arguments, prepares every node labelled
# space.pplx.net/node-pool=sandbox or =builder.
#
# Env knobs:
#   SPACE_KIND_CLUSTER        kind cluster name (default: space-local)
#   SPACE_KIND_CACHE_SIZE_MB  size of a newly created /space/cache image, in MiB
#                             (default: 40960). An existing image keeps its
#                             size; delete /var/lib/space-cache.img on the node
#                             (with its node-stack pod stopped) to resize.
#   SPACE_LOOP_DEVICE_TARGET  loop-device nodes to stage on each node (default:
#                             the library's floor of 64); the node-stack chart's
#                             loopDeviceTarget passes the same value to
#                             node-manager.
set -euo pipefail

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
KCTX="kind-$KIND_CLUSTER"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
NODE_SRC="$REPO_ROOT/infra/space/kind/node"
NODE_LIB=/usr/local/lib/space/kind-node
# Must sit comfortably above the 16 GiB logical ceiling of the encrypted
# suspend-snapshot volume: the backing file is sparse at the source and small
# in the object store, but a cross-node encrypted restore's pull materializes
# the full logical size on the target worker, so a 16 GiB cache can never
# receive one — the pull ENOSPCs and the node ends up ssd-placement-blocked.
# The image is sparse; unwritten capacity costs no host disk.
CACHE_SIZE_MB="${SPACE_KIND_CACHE_SIZE_MB:-40960}"
LOOP_TARGET="${SPACE_LOOP_DEVICE_TARGET:-}"
# Started in this order; space-cache needs the loop nodes.
UNITS=(space-loop-nodes space-kind-devices space-cgroup space-layout space-cache)

log() { printf '[prepare-kind-node] %s\n' "$*"; }
die() { printf '[prepare-kind-node] ERROR: %s\n' "$*" >&2; exit 1; }

[[ "$CACHE_SIZE_MB" =~ ^[1-9][0-9]*$ ]] || die "SPACE_KIND_CACHE_SIZE_MB must be a positive count of MiB"
[[ -z "$LOOP_TARGET" || "$LOOP_TARGET" =~ ^[0-9]+$ ]] || die "SPACE_LOOP_DEVICE_TARGET must be a count"

# Pool labels come from the cluster config and are applied by each kubelet as
# it registers, so right after `kind create` a node may not show one yet.
list_space_nodes() {
  local attempt nodes
  for ((attempt = 1; attempt <= 60; attempt++)); do
    nodes="$(kubectl --context "$KCTX" get nodes \
      -l 'space.pplx.net/node-pool in (sandbox,builder)' \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"
    if [[ -n "$nodes" ]]; then
      printf '%s\n' "$nodes"
      return 0
    fi
    sleep 2
  done
  return 1
}

# Installed like the packer recipe's package step, once per node container. The
# package stays in the container's filesystem across `docker restart`.
install_btrfs_progs() {
  local node=$1 attempt output
  if docker exec "$node" sh -c 'command -v mkfs.btrfs' >/dev/null 2>&1; then
    return 0
  fi
  log "$node: installing btrfs-progs"
  for attempt in 1 2 3; do
    if output="$(docker exec -e DEBIAN_FRONTEND=noninteractive "$node" sh -c \
      'apt-get update -qq && apt-get install -y -qq --no-install-recommends btrfs-progs' 2>&1)"; then
      return 0
    fi
    printf '%s\n' "$output" | tail -n 5 >&2
    log "$node: btrfs-progs install attempt $attempt failed; retrying"
    sleep $((attempt * 5))
  done
  die "$node: could not install btrfs-progs (the node needs Debian package mirror access once)"
}

# `docker cp` then rename, so a unit starting concurrently never runs a
# half-written script.
install_file() {
  local node=$1 src=$2 dst=$3 mode=$4
  docker cp "$src" "$node:$dst.tmp"
  docker exec "$node" sh -c "chown root:root '$dst.tmp' && chmod $mode '$dst.tmp' && mv -f '$dst.tmp' '$dst'"
}

prepare_node() {
  local node=$1 unit env
  log "$node: installing the SPACE node units"
  install_btrfs_progs "$node"
  docker exec "$node" mkdir -p "$NODE_LIB" /etc/space
  install_file "$node" "$NODE_SRC/space-kind-node.sh" "$NODE_LIB/space-kind-node.sh" 0755
  install_file "$node" "$REPO_ROOT/scripts/lib/space-loop-devices.sh" "$NODE_LIB/space-loop-devices.sh" 0644
  install_file "$node" "$REPO_ROOT/infra/space/identity/node-identities.sh" "$NODE_LIB/node-identities.sh" 0644
  for unit in "${UNITS[@]}"; do
    install_file "$node" "$NODE_SRC/systemd/$unit.service" "/etc/systemd/system/$unit.service" 0644
  done
  env="$(mktemp)"
  {
    echo "# Written by infra/space/kind/scripts/prepare-kind-node.sh."
    echo "SPACE_CACHE_SIZE_MB=$CACHE_SIZE_MB"
    [[ -z "$LOOP_TARGET" ]] || echo "SPACE_LOOP_DEVICE_TARGET=$LOOP_TARGET"
  } >"$env"
  install_file "$node" "$env" /etc/space/kind-node.env 0644
  rm -f "$env"

  docker exec "$node" systemctl daemon-reload
  docker exec "$node" systemctl enable --quiet "${UNITS[@]/%/.service}"
  # restart, not start: an already-active oneshot would otherwise keep the
  # result of the previous script. Every step is idempotent, and none stops
  # anything, so this is safe under running pods.
  for unit in "${UNITS[@]}"; do
    if ! docker exec "$node" systemctl restart "$unit.service"; then
      docker exec "$node" journalctl -u "$unit.service" -n 40 --no-pager >&2 || true
      die "$node: $unit.service failed"
    fi
  done

  local cache
  cache="$(docker exec "$node" findmnt -n -o FSTYPE,PROPAGATION --mountpoint /space/cache || true)"
  read -r -a cache <<<"$cache"
  [[ "${cache[0]:-}" == btrfs && "${cache[1]:-}" == shared ]] \
    || die "$node: /space/cache is not a shared btrfs mount after space-cache.service (got: ${cache[*]:-nothing})"
  log "$node: /space/cache is btrfs, shared"
}

command -v docker >/dev/null || die "docker not on PATH"
command -v kubectl >/dev/null || die "kubectl not on PATH"

if (( $# > 0 )); then
  nodes="$(printf '%s\n' "$@")"
else
  nodes="$(list_space_nodes)" \
    || die "no node labelled space.pplx.net/node-pool=sandbox or =builder in $KCTX"
fi

# Nodes in parallel: the first run on each installs a package, and Tilt holds
# its other serial resources (and the Rust build) until this one is done.
pids=()
for node in $nodes; do
  prepare_node "$node" &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || failed=1
done
(( failed == 0 )) || die "some nodes were not prepared (see above)"
log "prepared: $(echo "$nodes" | tr '\n' ' ')"
