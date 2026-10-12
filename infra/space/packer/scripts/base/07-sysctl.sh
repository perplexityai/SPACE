#!/usr/bin/env bash
# Configure kernel parameters and jailer prerequisites for SPACE node.
# Runs as root during Packer build.
#
# Required env vars:
#   HUGEPAGES — number of 2MB hugepages, or "auto" to compute at boot from actual RAM.
#               The AWS worker and builder recipes pass 0: hugepages are set by
#               deployment and runtime policy, not baked into the image.
set -euxo pipefail

: "${HUGEPAGES:?must be set}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPACE_NODE_IDENTITIES_FILE="${SPACE_NODE_IDENTITIES_FILE:-$SCRIPT_DIR/../../../identity/node-identities.sh}"
if [[ ! -f "$SPACE_NODE_IDENTITIES_FILE" && -f /tmp/space-node-identities.sh ]]; then
  SPACE_NODE_IDENTITIES_FILE=/tmp/space-node-identities.sh
fi
. "$SPACE_NODE_IDENTITIES_FILE"

# ── Chrony: allow clock step at any time ─────────────────────────────────────
# Default "makestep 1 3" only steps during first 3 updates after boot.
# On UTM (VM suspend/resume), the clock can drift by hours. With "-1" chrony
# will step immediately whenever the offset exceeds 1 second.
if [[ -f /etc/chrony/chrony.conf ]]; then
  sed -i 's/^makestep .*/makestep 1 -1/' /etc/chrony/chrony.conf
fi

# ── Sysctl ────────────────────────────────────────────────────────────────────
if [[ "$HUGEPAGES" == "auto" ]]; then
  # Write 0 for now; a boot-time service will compute the real value
  tee /etc/sysctl.d/99-space.conf > /dev/null <<EOF
vm.nr_hugepages     = 0
vm.swappiness       = 10
vm.vfs_cache_pressure = 50
EOF

  # Hugepages auto-sizing script (not inline ExecStart= to avoid systemd $ expansion).
  install -d /usr/local/bin
  RESERVED_MB="${HUGEPAGES_RESERVED_MB:-32768}"
  tee /usr/local/bin/space-hugepages-auto.sh > /dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail

RESERVED_MB=${RESERVED_MB}
ram_mb="\$(awk '/MemTotal/ { print int(\$2 / 1024) }' /proc/meminfo)"
hugepages="\$(( (ram_mb - RESERVED_MB) / 2 ))"

if [[ "\$hugepages" -lt 0 ]]; then
  hugepages=0
fi

echo "Auto-sizing hugepages: \${ram_mb}MB RAM - \${RESERVED_MB}MB reserved = \${hugepages} hugepages (2MB each)"
sysctl -w "vm.nr_hugepages=\${hugepages}"
EOF
  chmod 0755 /usr/local/bin/space-hugepages-auto.sh

  tee /etc/systemd/system/space-hugepages.service > /dev/null <<'UNIT'
[Unit]
Description=SPACE hugepages auto-sizing
DefaultDependencies=no
After=sysinit.target
Before=space-cgroup.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/bin/space-hugepages-auto.sh

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable space-hugepages.service
else
  tee /etc/sysctl.d/99-space.conf > /dev/null <<EOF
vm.nr_hugepages     = ${HUGEPAGES}
vm.swappiness       = 10
vm.vfs_cache_pressure = 50
EOF
fi

sysctl --system

# ── Host kernel args ─────────────────────────────────────────────────────────
# quiet + loglevel=1: reduce host console logging (FC docs recommend for performance)
if [ -f /etc/default/grub ]; then
  sed -i 's/^GRUB_CMDLINE_LINUX_DEFAULT=.*/GRUB_CMDLINE_LINUX_DEFAULT="quiet loglevel=1"/' /etc/default/grub
  update-grub 2>/dev/null || true
fi

# ── Cgroup v2 setup ───────────────────────────────────────────────────────────
# Cgroups don't persist across reboots. Create a oneshot service to set up the
# parent cgroup with delegated controllers on every boot.
tee /etc/systemd/system/space-cgroup.service > /dev/null <<'UNIT'
[Unit]
Description=SPACE cgroup v2 parent setup
DefaultDependencies=no
After=sysinit.target
Before=space-nodemgr.service space-sbxmgr.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c 'echo "+io" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null; mkdir -p /sys/fs/cgroup/space && echo "+cpu +memory +io +pids" > /sys/fs/cgroup/space/cgroup.subtree_control'

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable space-cgroup.service

# Also run now during image build
mkdir -p /sys/fs/cgroup/space
echo "+cpu +memory +io +pids" > /sys/fs/cgroup/space/cgroup.subtree_control 2>/dev/null || true

# Remount with favordynmods to avoid Linux 6.1+ cgroup performance regression
mount -o remount,favordynmods /sys/fs/cgroup 2>/dev/null || true
if ! grep -q favordynmods /etc/fstab; then
  echo "cgroup2 /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec,relatime,favordynmods 0 0" >> /etc/fstab
fi

# ── SPACE groups ─────────────────────────────────────────────────────────────
# GIDs start at 950. Operators provision membership for each enabled feature.
for spec in $SPACE_GROUP_SPECS; do
  group="${spec%%:*}"
  gid="${spec##*:}"
  if getent group "$group" >/dev/null; then
    actual_gid="$(getent group "$group" | cut -d: -f3)"
    if [[ "$actual_gid" != "$gid" ]]; then
      groupmod -g "$gid" "$group"
    fi
  else
    groupadd -g "$gid" "$group"
  fi
done

ensure_space_user() {
  local user="$1"
  local uid="$2"
  local group="$3"

  if id "$user" >/dev/null 2>&1; then
    local actual_uid
    actual_uid="$(id -u "$user")"
    if [[ "$actual_uid" != "$uid" ]]; then
      usermod -u "$uid" "$user"
    fi
    usermod -g "$group" "$user"
  else
    useradd --system --no-create-home --uid "$uid" \
      --gid "$group" \
      --shell /usr/sbin/nologin "$user"
  fi
}

# Host service accounts. Runtime access groups are assigned by systemd or
# Kubernetes, not persisted as broad /etc/group memberships.
while IFS=: read -r _ user uid group; do
  [[ -n "$user" ]] || continue
  ensure_space_user "$user" "$uid" "$group"
done <<< "$SPACE_USER_SPECS"

# ── UID/GID pool for sandboxes ───────────────────────────────────────────────
# Reserve UIDs 10000-10999 for per-sandbox Firecracker processes.
echo "root:10000:1000" >> /etc/subuid
echo "root:10000:1000" >> /etc/subgid

# ── /dev/kvm permissions ────────────────────────────────────────────────────
# Only root (node-manager, jailer) needs /dev/kvm. Jailer creates its own
# /dev/kvm inside the chroot. No unprivileged process needs host /dev/kvm.
tee /etc/udev/rules.d/99-space-kvm.rules > /dev/null <<'EOF'
KERNEL=="kvm", GROUP="kvm", MODE="0660"
EOF

# ── Directory skeleton ────────────────────────────────────────────────────────
mkdir -p \
  /space/etc/deployed \
  /space/run/node-manager \
  /space/run/dependency-drain/sandbox-manager \
  /space/run/sandbox-manager \
  /space/run/sandbox-host-control \
  /space/run/credential-manager \
  /space/run/template-manager \
  /space/run/firecracker/sandboxes \
  /space/cache/firecracker/templates \
  /space/cache/firecracker/sandboxes \
  /space/cache/firecracker/jails \
  /space/cache/firecracker/builds

# Host state directories used by the containerized worker services.
# The daemons themselves should run in containers on EKS, but the mutable state
# still lives on the host under /space.
chown root:root /space/run /space/run/firecracker/sandboxes
chown root:root /space/run/node-manager
chown root:space-sbxclients /space/run/dependency-drain /space/run/dependency-drain/sandbox-manager
chown space-sbxmgr:space-sbxmgr /space/run/sandbox-host-control
chown space-sbxmgr:space-sbxclients /space/run/sandbox-manager /space/run/credential-manager
chown space-tmplmgr:space-tmplclients /space/run/template-manager
chown root:space-cache-rw \
  /space/cache \
  /space/cache/firecracker \
  /space/cache/firecracker/templates \
  /space/cache/firecracker/sandboxes \
  /space/cache/firecracker/builds
chown root:root /space/cache/firecracker/jails
chmod 0755 /space/run /space/run/node-manager /space/run/sandbox-host-control /space/run/firecracker/sandboxes /space/cache/firecracker/jails
chmod 0750 /space/run/dependency-drain /space/run/dependency-drain/sandbox-manager
chmod 0750 /space/run/sandbox-manager /space/run/credential-manager /space/run/template-manager
for gate in capability-v1.lock admission-v1.lock work-v1.lock; do
  install -m 0660 -o root -g space-sbxclients /dev/null "/space/run/dependency-drain/sandbox-manager/$gate"
done
chmod 0775 \
  /space/cache/firecracker/templates \
  /space/cache/firecracker/sandboxes \
  /space/cache/firecracker/builds
# The two shared cache parents are also sticky: the cache services can still
# create entries there, but cannot rename away the root-owned kernel and asset
# roots beneath them.
chmod 1775 /space/cache /space/cache/firecracker

# ── Runtime manager ────────────────────────────────────────────────────────
# Node-manager creates a separate managed store at startup; do not relocate
# execution artifacts or grant runtime-manager write access to them here.
install -d -m 0750 -o "$SPACE_RTMGR_USER" -g space-rtmgrclients /space/run/runtime-manager

# ── Sandbox config ──────────────────────────────────────────────────────────
# Per-VM config. SANDBOX_RAM_GB sets memory cap.
# SANDBOX_HUGEPAGES enables hugepage-backed memory for cold boot only.
{
  echo "# Generated by Packer (07-sysctl.sh)"
  if [[ -n "${SANDBOX_RAM_GB:-}" ]]; then
    MAX_MEM_MIB=$(( SANDBOX_RAM_GB * 1024 ))
    echo "max_memory_mib = ${MAX_MEM_MIB}"
    echo "sandbox.toml: max_memory_mib = ${MAX_MEM_MIB}" >&2
  fi
  if [[ "${SANDBOX_HUGEPAGES:-}" == "true" ]]; then
    echo "hugepages = true"
    echo "sandbox.toml: hugepages = true" >&2
    echo "WARNING: hugepages=true cannot restore Firecracker snapshots; snapshot boot modes will be rejected" >&2
  fi
} > /space/etc/sandbox.toml

# ── Sandbox SSH key ──────────────────────────────────────────────────────────
# Private key is root-only. space-ssh and builder read it via sudo.
# Pubkey is in /root/.ssh (canonical) and copied to /space/etc/ssh/ for seed builds.
# This key is per-image: every bake mints a new one, so a seed rootfs that
# trusts it goes stale on the next AMI roll. On EKS the space-build-manager
# chart shadows both paths with the cluster's secret-delivered key
# (sandboxSsh.enabled; docs/space/aws/sandbox-ssh-key.md), and this bake-time
# key then serves direct-deploy and UTM nodes only.
mkdir -p /root/.ssh
ssh-keygen -t ed25519 -f /root/.ssh/sandbox_ed25519 -N "" -C "sandbox"
chmod 600 /root/.ssh/sandbox_ed25519
chmod 644 /root/.ssh/sandbox_ed25519.pub
mkdir -p /space/etc/ssh
cp /root/.ssh/sandbox_ed25519.pub /space/etc/ssh/sandbox_ed25519.pub
# Private key stays in /root/.ssh/ only — readable by root.
# deploy — local operator role. Uses scoped sudo for deploys and direct CLI
# access for sandbox/template operations.
# dev — local observer role. Debug access without sudo or direct state writes.

# ── Deploy user ───────────────────────────────────────────────────────────────
# Created by cloud-init for operator deployments using scoped sudo.
# Also in SPACE socket client groups so CLI tools work without sudo. Human
# operators should not get direct write access to /space/cache or /space/run.
SPACE_OPERATOR_GROUP_CSV="${SPACE_OPERATOR_GROUPS// /,}"
usermod -aG "$SPACE_OPERATOR_GROUP_CSV" deploy
mkdir -p /home/deploy/.ssh
chmod 700 /home/deploy/.ssh
echo 'export PATH="/space/bin:$PATH"' >> /home/deploy/.profile
chown -R deploy:deploy /home/deploy

# ── Dev user ─────────────────────────────────────────────────────────────────
# Created by cloud-init. Has read-only/debug RPC access only.
# UTM images bake an SSH key; other images require --dev-user enable.
usermod -aG "$SPACE_OPERATOR_GROUP_CSV" dev
mkdir -p /home/dev/.ssh
chmod 700 /home/dev/.ssh
echo 'export PATH="/space/bin:$PATH"' >> /home/dev/.profile
chown -R dev:dev /home/dev

# ── PATH for root ────────────────────────────────────────────────────────────
echo 'export PATH="/space/bin:$PATH"' >> /root/.profile

# ── Network namespace directory ───────────────────────────────────────────────
mkdir -p /var/run/netns

# ── Hugepages/TUN boot-time prep ─────────────────────────────────────────────
mkdir -p /mnt/hugepages
tee /etc/modules-load.d/space-tun.conf > /dev/null <<'EOF'
tun
EOF
tee /etc/systemd/system/space-hugetlbfs.service > /dev/null <<'UNIT'
[Unit]
Description=SPACE hugepages mount setup
DefaultDependencies=no
After=local-fs.target systemd-modules-load.service
Before=space-cgroup.service kubelet.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/bash -c 'mkdir -p /mnt/hugepages; mountpoint -q /mnt/hugepages || mount -t hugetlbfs none /mnt/hugepages'

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable space-hugetlbfs.service

# ── Apply TUN module state during image build ────────────────────────────────
modprobe tun 2>/dev/null || true

# ── Loop devices (max_loop=10000) ────────────────────────────────────────────
# SPACE nodes back filesystem images (Firecracker rootfs, the btrfs
# /space/cache loopback) with loop devices, about 2 per sandbox, for up to
# ~1000 sandboxes per node. What max_loop does in the kernel's loop driver
# (drivers/block/loop.c): loop_init pre-creates max_loop devices when the
# module loads, and opening a /dev/loopN node at or above max_loop is refused.
# It is not a cap on /dev/loop-control, which is what losetup -f and mount -o
# loop allocate through: that path adds devices without checking max_loop. So
# this pre-creates /dev/loop0 to /dev/loop9999 at boot rather than bounding
# allocation. The drop-in applies whenever the module loads (autoload on first
# mount, or modules-load.d at boot); /sys/module/loop/parameters/max_loop is
# read-only at runtime, so the value can only be set at module load time.
tee /etc/modprobe.d/space-loop.conf > /dev/null <<'EOF'
options loop max_loop=10000
EOF
tee /etc/modules-load.d/space-loop.conf > /dev/null <<'EOF'
loop
EOF
# Not reloaded here: 01-partitions mounts /space/cache from a loop-backed btrfs
# image during the build, so the module is in use and cannot be unloaded. The
# drop-in takes effect on the next boot of an instance launched from this AMI.

# ── Apply hugepages mount state during image build ───────────────────────────
mountpoint -q /mnt/hugepages || mount -t hugetlbfs none /mnt/hugepages 2>/dev/null || true

# ── Disable KSM (Kernel Same-page Merging) ───────────────────────────────────
# KSM is controlled through sysfs, not a sysctl, and is off by default at boot;
# this only makes sure of it during the bake.
echo 0 > /sys/kernel/mm/ksm/run 2>/dev/null || true

echo "=== Jailer prerequisites configured ==="
echo "  Cgroup:     /sys/fs/cgroup/space/"
echo "  UID pool:   10000-10999"
echo "  Netns dir:  /var/run/netns/"
echo "  TUN module: loaded + persistent"
echo "  Loop:       max_loop=10000 (bounded cap)"
echo "  KSM:        disabled"
