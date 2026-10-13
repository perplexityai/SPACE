#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# space-kind-node.sh — the Kind node's stand-in for the SPACE node image.
#
# A production worker or builder gets its btrfs /space/cache, cgroup subtree,
# loop-device headroom, device permissions, SPACE accounts and /space layout
# from the node image (infra/space/packer) before any SPACE pod starts. The Kind
# node image for sandbox workers and builders (Dockerfile beside this script)
# bakes in this script and enables the units that run it, ordered before
# kubelet:
#
#   space-loop-nodes.service       loop-device node headroom in the node's /dev
#   space-kind-devices.service     Kind-only /dev/kvm and vhost permission widening
#   space-cgroup.service           /sys/fs/cgroup/space   (packer 07-sysctl.sh)
#   space-layout.service           SPACE accounts, /space skeleton, Firecracker
#                                  links, sandbox.toml    (packer 04-firecracker.sh,
#                                                          07-sysctl.sh)
#   space-cache.service            btrfs /space/cache     (packer 01-partitions.sh,
#                                                          11-nvme-cache.sh)
#   space-kind-vsock-jails.service Kind-only vhost-vsock nodes in Firecracker jails
#
# The node also runs space-runtime-supervisor.service, which node-manager's
# init container installs and upgrades (scripts/k8s/node-manager-init.sh), as
# on a production node.
#
# A container's /dev, mounts and cgroups do not survive `docker restart`, which
# is why these are boot units and not a one-time setup. Each step is
# idempotent: after a restart the cache image and the directories are already
# in place, and re-running a unit by hand on a live node is safe.
#
# Defaults are baked like the rest of the layer; /etc/space/kind-node.env, when
# present, overrides SPACE_CACHE_SIZE_MB and SPACE_LOOP_DEVICE_TARGET.
#
# Usage: space-kind-node.sh loop-nodes|devices|cgroup|layout|cache|vsock-jails
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

# ── SPACE accounts ───────────────────────────────────────────────────────────
# The groups and service users packer 07-sysctl.sh creates on the node image,
# with the same numbers the worker image uses: the host runtime-supervisor and
# the sidecars it launches run here, and the node's files carry these owners.
ensure_identities() {
  local spec group gid actual _alias user uid primary
  for spec in $SPACE_GROUP_SPECS; do
    group="${spec%%:*}" gid="${spec##*:}"
    if actual="$(getent group "$group" | cut -d: -f3)" && [[ -n $actual ]]; then
      [[ $actual == "$gid" ]] || groupmod -g "$gid" "$group"
    else
      groupadd -g "$gid" "$group"
    fi
  done
  while IFS=: read -r _alias user uid primary; do
    [[ -n $user ]] || continue
    if id "$user" >/dev/null 2>&1; then
      [[ "$(id -u "$user")" == "$uid" ]] || usermod -u "$uid" "$user"
      usermod -g "$primary" "$user"
    else
      useradd --system --no-create-home --uid "$uid" --gid "$primary" \
        --shell /usr/sbin/nologin "$user"
    fi
  done <<<"$SPACE_USER_SPECS"
}

# ── /space skeleton ──────────────────────────────────────────────────────────
# The host state directories, Firecracker links and per-VM defaults packer
# bakes into the node image, on the hostPaths the node-service pods mount. The
# directories only a service's own init container creates in production
# (volume-manager's, node-ctl's, runtime-manager's, build-manager's) are left
# to those charts.
layout() {
  local sbxclients_gid gate gate_path
  ensure_identities
  sbxclients_gid="$(space_expected_gid_for_group space-sbxclients)"
  install -d -m 0755 -o 0 -g 0 /space /space/etc /space/bin /space/run /space/run/node-manager \
    /space/run/firecracker /space/run/firecracker/sandboxes /var/run/netns
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
  # The canonical binary paths the runtime inventory and the jailer launch
  # use, onto the vendor tree kind-cluster.yaml.tmpl bind-mounts (packer
  # 04-firecracker.sh). node-manager's init container stages the SPACE host
  # binaries beside them.
  ln -sfn ../vendor/firecracker/default/bin/firecracker /space/bin/firecracker
  ln -sfn ../vendor/firecracker/default/bin/jailer /space/bin/jailer
  # Per-VM defaults sandbox-manager reads; packer writes its sizing knobs here,
  # and Kind sets none of them.
  printf '%s\n' "# Generated by the Kind node layer (infra/space/kind/node/space-kind-node.sh)" \
    >/space/etc/sandbox.toml
  chmod 0644 /space/etc/sandbox.toml
  log "SPACE accounts and /space skeleton in place"
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
  # attached: one an earlier attach still has mounted (btrfs refuses a second
  # device with the same fsid while that mount lives), or one a crash left
  # attached and unmounted, which would otherwise never be reclaimed.
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

# ── vhost-vsock nodes in Firecracker jails (Kind only) ───────────────────────
# The jailer mknods /dev/kvm, /dev/net/tun, /dev/urandom and /dev/userfaultfd
# into each jail, but not /dev/vhost-vsock (a known Firecracker jailer
# limitation). Without it Firecracker silently disables the vsock device, the
# host bridge cannot reach the guest, and readiness expires at 30 s. A
# production node image provides the device to its jails; here this
# long-running unit races the jailer instead: as soon as a jail's dev/ appears
# it mknods the node into it, polling at 10 ms because Firecracker opens the
# device very shortly after exec. The jails live in the node's mount
# namespace, under the host runtime-supervisor, so they appear here directly.
# Do NOT replicate this on a real worker.
vsock_jails() {
  local src=/dev/vhost-vsock major minor jail_dev
  if [[ ! -c $src ]]; then
    log "no /dev/vhost-vsock on this node; nothing to provide"
    exec sleep infinity
  fi
  major=$((16#$(stat -c '%t' "$src")))
  minor=$((16#$(stat -c '%T' "$src")))
  log "providing vhost-vsock (c $major $minor) to Firecracker jails"
  while true; do
    for jail_dev in "$CACHE_DIR"/firecracker/jails/firecracker/*/root/dev; do
      [[ -d $jail_dev && ! -e $jail_dev/vhost-vsock ]] || continue
      # 0666 so the sandbox uid Firecracker switches to can open it.
      if mknod "$jail_dev/vhost-vsock" c "$major" "$minor" 2>/dev/null; then
        chmod 0666 "$jail_dev/vhost-vsock" 2>/dev/null || true
        log "mknod $jail_dev/vhost-vsock"
      fi
    done
    sleep 0.01
  done
}

case "$STEP" in
  loop-nodes) loop_nodes ;;
  devices) devices ;;
  cgroup) cgroup ;;
  layout) layout ;;
  cache) cache ;;
  vsock-jails) vsock_jails ;;
  *) die "usage: $0 loop-nodes|devices|cgroup|layout|cache|vsock-jails" ;;
esac
