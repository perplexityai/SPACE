#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# space-kind-node.sh — the Kind node's stand-in for the SPACE node image.
#
# A production worker or builder gets its btrfs /space/cache, cgroup subtree,
# loop-device headroom, device permissions and /space layout from the node
# image (infra/space/packer) before any SPACE pod starts. The Kind node image
# for sandbox workers and builders (Dockerfile beside this script) bakes in this
# script and enables the units that run it, ordered before kubelet:
#
#   space-loop-nodes.service    loop-device node headroom in the node's /dev
#   space-kind-devices.service  Kind-only /dev/kvm and vhost permission widening
#   space-cgroup.service        /sys/fs/cgroup/space   (packer 07-sysctl.sh)
#   space-layout.service        /space/run skeleton    (packer 07-sysctl.sh)
#   space-cache.service         btrfs /space/cache     (packer 01-partitions.sh,
#                                                       11-nvme-cache.sh)
#
# A container's /dev, mounts and cgroups do not survive `docker restart`, which
# is why these are boot units and not a one-time setup. Each step is
# idempotent: after a restart the cache image and the directories are already
# in place, and re-running a unit by hand on a live node is safe.
#
# Defaults are baked like the rest of the layer; /etc/space/kind-node.env, when
# present, overrides SPACE_CACHE_SIZE_MB and SPACE_LOOP_DEVICE_TARGET.
#
# Usage: space-kind-node.sh loop-nodes|devices|cgroup|layout|cache
set -euo pipefail

# Baked together into the Kind node image; the source= paths below are their
# repository copies.
LIB_DIR=/usr/local/lib/space/kind-node
# shellcheck source=../../identity/node-identities.sh
. "$LIB_DIR/node-identities.sh"
# shellcheck source=../../../../scripts/lib/space-loop-devices.sh
. "$LIB_DIR/space-loop-devices.sh"

STEP="${1:-}"
CACHE_DIR=/space/cache
# On the node's /var, which kind keeps on a volume, so the cache outlives node
# restarts as well as pod restarts, as the instance-store disk does on EKS.
CACHE_IMAGE=/var/lib/space-cache.img
CACHE_SIZE_MB="${SPACE_CACHE_SIZE_MB:-40960}"

log() { printf '%s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# ── Loop devices ─────────────────────────────────────────────────────────────
# Docker gives the node a /dev tmpfs holding the eight loop nodes the host had
# at creation, and every pod on the node copies the node's /dev when it starts.
# The node's cache attach below and each pod's first attaches need more; the
# library explains why, and what production gets from max_loop=10000 instead.
loop_nodes() {
  space_loop_stage_pool log
}

# ── Device permissions (Kind only) ───────────────────────────────────────────
# Production grants the jailer's per-sandbox uids /dev/kvm through the `kvm`
# group on the node image. Kind has no such group membership: the sandbox uids
# are created on the fly and own no numeric group that holds /dev/kvm, so
# Firecracker fails with "KVM object: Permission denied". Widen the modes on
# the node, where pods copy them from. Do NOT replicate this on a real worker.
devices() {
  local dev
  for dev in /dev/kvm /dev/vhost-vsock /dev/vhost-net; do
    [[ -e $dev ]] || continue
    chmod 0666 "$dev"
    log "chmod 0666 $dev (Kind-only kvm-group workaround)"
  done
}

# ── cgroup v2 parent ─────────────────────────────────────────────────────────
# node-manager writes /sys/fs/cgroup/space/{cpu,memory}.max at startup and
# creates per-sandbox children with these controllers delegated. Same commands
# as packer's space-cgroup.service.
cgroup() {
  echo "+io" >/sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
  mkdir -p /sys/fs/cgroup/space
  echo "+cpu +memory +io +pids" >/sys/fs/cgroup/space/cgroup.subtree_control
  log "/sys/fs/cgroup/space delegates: $(cat /sys/fs/cgroup/space/cgroup.subtree_control)"
}

# ── /space/run skeleton ──────────────────────────────────────────────────────
# The host state directories packer 07-sysctl.sh bakes into the node image, on
# the hostPath the node-stack pod mounts. The directories only SPACE services'
# own init containers create in production stay with the pod entrypoint.
layout() {
  local sbxclients_gid gate gate_path
  sbxclients_gid="$(space_expected_gid_for_group space-sbxclients)"
  install -d -m 0755 -o 0 -g 0 /space /space/etc /space/run /space/run/node-manager \
    /space/run/firecracker /space/run/firecracker/sandboxes
  install -d -m 0750 -o 0 -g "$sbxclients_gid" \
    /space/run/dependency-drain /space/run/dependency-drain/sandbox-manager
  # Lock files a running sandbox-manager may hold: create or re-own them, never
  # truncate or replace them.
  for gate in capability-v1.lock admission-v1.lock work-v1.lock; do
    gate_path="/space/run/dependency-drain/sandbox-manager/$gate"
    if [[ -L "$gate_path" || ( -e "$gate_path" && ! -f "$gate_path" ) ]]; then
      die "dependency-drain gate is not a regular file: $gate_path"
    fi
    touch "$gate_path"
    chown "0:$sbxclients_gid" "$gate_path"
    chmod 0660 "$gate_path"
  done
  install -d -m 0750 -o "$SPACE_SBXMGR_UID" -g "$sbxclients_gid" \
    /space/run/sandbox-manager /space/run/credential-manager
  install -d -m 0755 -o "$SPACE_SBXMGR_UID" -g "$SPACE_SBXMGR_GID" /space/run/sandbox-host-control
  install -d -m 0750 -o "$SPACE_TMPLMGR_UID" -g "$(space_expected_gid_for_group space-tmplclients)" \
    /space/run/template-manager
  log "/space/run skeleton in place"
}

# ── btrfs /space/cache ───────────────────────────────────────────────────────
# sandbox-manager clones template rootfs into instance dirs with btrfs subvolume
# snapshots, so the cache must be btrfs. Production formats instance-store NVMe
# (or a loopback image on nodes without one); here it is a sparse loopback image
# on the node, formatted once and reused on every later start.
cache_is_mounted() {
  findmnt -n -t btrfs --mountpoint "$CACHE_DIR" >/dev/null 2>&1
}

cache_image_is_btrfs() {
  [[ -s $CACHE_IMAGE ]] && [[ "$(blkid -p -s TYPE -o value "$CACHE_IMAGE" 2>/dev/null)" == btrfs ]]
}

format_cache_image() {
  # A populated image that is not btrfs (a format interrupted between truncate
  # and mkfs, or damage) would fail to mount on every start until someone
  # deleted it; reformat it like an empty one.
  if [[ -s $CACHE_IMAGE ]]; then
    log "backing image $CACHE_IMAGE is not a btrfs filesystem; reformatting it"
  else
    log "creating btrfs backing image $CACHE_IMAGE (${CACHE_SIZE_MB}M, sparse)"
  fi
  # Sparse (truncate, not fallocate): the virtual size gives disk admission
  # production-like headroom while real blocks are only consumed as sandboxes
  # write.
  truncate -s 0 "$CACHE_IMAGE"
  truncate -s "${CACHE_SIZE_MB}M" "$CACHE_IMAGE"
  mkfs.btrfs -f -L space-cache "$CACHE_IMAGE" >/dev/null
}

mount_cache_image() {
  local loopdev reused=0
  # A device already bound to this image is reused rather than a second one
  # attached: one a node-stack pod from before this unit existed attached and
  # still has mounted (btrfs refuses a second device with the same fsid while
  # that mount lives), or one a crash left attached and unmounted, which would
  # otherwise never be reclaimed.
  loopdev="$(losetup -l -n -O NAME -j "$CACHE_IMAGE" 2>/dev/null | head -n 1 | tr -d '[:space:]')"
  if [[ -n $loopdev && -b $loopdev ]]; then
    reused=1
    log "reusing $loopdev, already attached to $CACHE_IMAGE"
  else
    # The library's retrying attach: allocation is two ioctls, and attaches
    # racing on other nodes can be handed the same free index.
    loopdev="$(space_loop_attach "$CACHE_IMAGE")" || loopdev=""
    if [[ -z $loopdev || ! -b $loopdev ]]; then
      space_loop_exhaustion_report "$CACHE_IMAGE" | while IFS= read -r line; do log "$line" >&2; done
      die "no loop device available for $CACHE_IMAGE"
    fi
  fi
  if ! mount -o nosuid "$loopdev" "$CACHE_DIR"; then
    (( reused )) || space_loop_detach "$loopdev" || true
    die "mount $loopdev $CACHE_DIR failed (image $CACHE_IMAGE; if this repeats, delete the image on the node)"
  fi
  # `losetup -d` on a mounted device does not detach it: it arms
  # LO_FLAGS_AUTOCLEAR, so the kernel releases the device once the filesystem
  # is unmounted everywhere (node stop, `docker restart`). Without it every node
  # restart would leak one device from the kernel-global pool.
  space_loop_detach "$loopdev" \
    || log "WARNING: could not set autoclear on $loopdev; it will leak when the node stops"
  log "mounted btrfs loop $loopdev on $CACHE_DIR (image $CACHE_IMAGE, autoclear)"
}

# The cache directories packer bakes and 11-nvme-cache.sh recreates after it
# mounts the instance store: setgid dirs the space-cache-rw services (sbxmgr,
# tmplmgr, volmgr) write, root-owned jails. The two shared parents are also
# sticky, so those services cannot rename away the root-owned kernel and asset
# roots staged beneath them.
cache_skeleton() {
  local cache_rw_gid
  cache_rw_gid="$(space_expected_gid_for_group space-cache-rw)"
  install -d -m 3775 -o 0 -g "$cache_rw_gid" "$CACHE_DIR" "$CACHE_DIR/firecracker"
  install -d -m 2775 -o 0 -g "$cache_rw_gid" \
    "$CACHE_DIR/firecracker/templates" \
    "$CACHE_DIR/firecracker/sandboxes" \
    "$CACHE_DIR/firecracker/builds" \
    "$CACHE_DIR/firecracker/.volmgr-staging"
  install -d -m 0755 -o 0 -g 0 "$CACHE_DIR/firecracker/jails"
}

cache() {
  install -d -m 0755 -o 0 -g 0 /space "$CACHE_DIR"
  if cache_is_mounted; then
    log "$CACHE_DIR is already btrfs"
  else
    if findmnt -n --mountpoint "$CACHE_DIR" >/dev/null 2>&1; then
      die "$CACHE_DIR is mounted but not btrfs: $(findmnt -n -o SOURCE,FSTYPE --mountpoint "$CACHE_DIR")"
    fi
    command -v mkfs.btrfs >/dev/null \
      || die "mkfs.btrfs missing; this node is not running the SPACE Kind node image (infra/space/kind/node/Dockerfile)"
    space_loop__losetup_bin >/dev/null || die "losetup missing (util-linux)"
    cache_image_is_btrfs || format_cache_image
    mount_cache_image
  fi
  # Shared, so a pod that receives the cache HostToContainer also receives a
  # later remount. kind's entrypoint makes `/` rshared, which new mounts
  # inherit; this holds for a mount whatever made it.
  mount --make-shared "$CACHE_DIR"
  local img_mb
  img_mb=$(( $(stat -c %s "$CACHE_IMAGE") / 1048576 ))
  if (( img_mb != CACHE_SIZE_MB )); then
    log "WARNING: the cache size is ${CACHE_SIZE_MB}M (SPACE_CACHE_SIZE_MB) but the reused image is ${img_mb}M; see \"The node layer\" in infra/space/kind/README.md to resize"
  fi
  cache_skeleton
}

case "$STEP" in
  loop-nodes) loop_nodes ;;
  devices) devices ;;
  cgroup) cgroup ;;
  layout) layout ;;
  cache) cache ;;
  *) die "usage: $0 loop-nodes|devices|cgroup|layout|cache" ;;
esac
