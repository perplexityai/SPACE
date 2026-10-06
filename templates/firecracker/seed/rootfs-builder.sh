#!/usr/bin/env bash
# rootfs-builder.sh — Build a minimal "seed" Firecracker rootfs inside a
# containerized Linux builder environment.
#
# The seed is the smallest bootable Ubuntu image with SSH and networking.
# All application packages (Python, Node, etc.) are installed later by
# space-builder using recipes such as recipes/code-runtime.toml.
#
# Base image: Ubuntu 26.04 Minimal Cloud (cloud-images.ubuntu.com)
#
# Produces /output/rootfs.ext4 and /output/metadata.json.
#
# Usage: rootfs-builder.sh --id <id> [--force]
#
# The seed template chain:
#   seed (this script) → code-runtime (recipe)
set -euo pipefail

extract_verified_squashfs() (
  set -euo pipefail
  local base_url="$1" name="$2" cache_dir="$3" build_dir="$4" keyring="$5" signer="$6"
  local verify_dir checksum cache_tmp=""
  verify_dir="$(mktemp -d)"
  trap 'rm -rf "$verify_dir"; [[ -z "$cache_tmp" ]] || rm -f "$cache_tmp"' EXIT

  # Authenticate this release even when reusing a shared cache entry.
  for file in SHA256SUMS SHA256SUMS.gpg; do
    curl -fSL --retry 3 --retry-delay 5 -o "${verify_dir}/${file}" "${base_url}/${file}"
  done
  gpgv --status-fd 3 --homedir "$verify_dir" --keyring "$keyring" \
    "${verify_dir}/SHA256SUMS.gpg" "${verify_dir}/SHA256SUMS" 3>"${verify_dir}/status"
  if ! awk -v signer="$signer" '
    $1 == "[GNUPG:]" && $2 == "VALIDSIG" && $3 == signer { valid = 1 }
    END { exit !valid }
  ' "${verify_dir}/status"; then
    echo "ERROR: checksum signature does not match the pinned signer" >&2
    exit 1
  fi
  checksum="$(awk -v name="$name" '$2 == name || $2 == "*" name { print $1 }' "${verify_dir}/SHA256SUMS")"
  if [[ ! "$checksum" =~ ^[a-fA-F0-9]{64}$ ]]; then
    echo "ERROR: expected exactly one SHA-256 for ${name}" >&2
    exit 1
  fi

  if [[ -f "${cache_dir}/${name}" ]]; then
    echo "Using cached squashfs: ${cache_dir}/${name}"
    cp "${cache_dir}/${name}" "${verify_dir}/image.squashfs"
    if ! printf '%s  %s\n' "$checksum" "${verify_dir}/image.squashfs" | sha256sum --check --strict -; then
      echo "Cached squashfs checksum mismatch; downloading a replacement" >&2
      rm -f "${verify_dir}/image.squashfs"
    fi
  fi
  if [[ ! -f "${verify_dir}/image.squashfs" ]]; then
    echo "Downloading: ${name}"
    curl -fSL --retry 3 --retry-delay 5 -o "${verify_dir}/image.squashfs" "${base_url}/${name}"
    printf '%s  %s\n' "$checksum" "${verify_dir}/image.squashfs" | sha256sum --check --strict -
    if [[ -d "$cache_dir" ]]; then
      cache_tmp="$(mktemp "${cache_dir}/.${name}.XXXXXX")"
      cp -p "${verify_dir}/image.squashfs" "$cache_tmp"
      mv -f "$cache_tmp" "${cache_dir}/${name}"
    fi
  fi

  echo "=== Extracting squashfs ==="
  rm -rf "$build_dir"
  mkdir -p "$build_dir"
  unsquashfs -d "${build_dir}/squashfs-root" "${verify_dir}/image.squashfs"
)

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return
fi

ID=""
FORCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id)    ID="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$ID" ]]; then
  echo "Usage: rootfs-builder.sh --id <id> [--force]" >&2
  exit 1
fi

ARCH=$(uname -m)
OUTPUT="/output"
BUILD="/tmp/fc-rootfs-build"
CHROOT="${BUILD}/squashfs-root"
: "${SPACE_SEED_SOURCE_TYPE:?SPACE_SEED_SOURCE_TYPE is required}"
: "${SPACE_SEED_UBUNTU_RELEASE:?SPACE_SEED_UBUNTU_RELEASE is required}"
: "${SPACE_SEED_UBUNTU_VERSION:?SPACE_SEED_UBUNTU_VERSION is required}"
: "${SPACE_SEED_UBUNTU_DATE:?SPACE_SEED_UBUNTU_DATE is required}"
SOURCE_TYPE="${SPACE_SEED_SOURCE_TYPE}"
SPACED_VERSION="${SPACE_SEED_SPACED_VERSION:-}"
if [[ -z "$SPACED_VERSION" && -f /space/Cargo.toml ]]; then
  SPACED_VERSION="$(awk '
    /^\[workspace\.package\]/ { in_workspace_package = 1; next }
    /^\[/ { in_workspace_package = 0 }
    in_workspace_package && /^version[[:space:]]*=/ {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' /space/Cargo.toml)"
fi
if [[ -z "$SPACED_VERSION" ]]; then
  echo "ERROR: could not determine spaced version (set SPACE_SEED_SPACED_VERSION or provide /space/Cargo.toml)" >&2
  exit 1
fi

spaced_compatibility_family() {
  local version="${1%%+*}"
  version="${version%%-*}"
  local major minor patch extra
  IFS=. read -r major minor patch extra <<< "$version"
  if [[ -z "$major" || -z "$minor" || -z "$patch" || -n "${extra:-}" ]]; then
    echo "ERROR: invalid SemVer version: $1" >&2
    return 1
  fi
  if [[ "$major" == "0" ]]; then
    if [[ "$minor" == "0" ]]; then
      printf '0.0.%s\n' "$patch"
    else
      printf '0.%s\n' "$minor"
    fi
  else
    printf '%s\n' "$major"
  fi
}
SPACED_COMPATIBILITY_FAMILY="$(spaced_compatibility_family "$SPACED_VERSION")"
RUNTIME="${SPACE_SEED_RUNTIME:-firecracker}"
: "${SPACE_SEED_RUNTIME_VERSION:?SPACE_SEED_RUNTIME_VERSION is required for seed execution metadata}"
: "${SPACE_SEED_STATE_FORMAT_VERSION:?SPACE_SEED_STATE_FORMAT_VERSION is required for seed execution metadata}"
RUNTIME_VERSION="${SPACE_SEED_RUNTIME_VERSION}"
STATE_FORMAT_VERSION="${SPACE_SEED_STATE_FORMAT_VERSION}"
KERNEL_VERSION="${SPACE_SEED_KERNEL_VERSION:-}"
KERNEL_DIGEST="${SPACE_SEED_KERNEL_DIGEST:-}"
KERNEL_COMPONENT=""
if [[ -n "$KERNEL_VERSION" && -z "$KERNEL_DIGEST" ]] ||
  [[ -z "$KERNEL_VERSION" && -n "$KERNEL_DIGEST" ]]; then
  echo "ERROR: seed execution metadata requires both SPACE_SEED_KERNEL_VERSION and SPACE_SEED_KERNEL_DIGEST" >&2
  exit 1
fi
if [[ -n "$KERNEL_VERSION" ]]; then
  KERNEL_COMPONENT=$(cat <<EOF
{
    "kind": "kernel",
    "role": "boot",
    "version": "${KERNEL_VERSION}",
    "digest": "${KERNEL_DIGEST}",
    "format": "vmlinux"
  }
EOF
)
fi
UBUNTU_APT_MIRROR="${SPACE_UBUNTU_APT_MIRROR:-https://archive.ubuntu.com/ubuntu}"

# ── Download Ubuntu Minimal Cloud squashfs ───────────────────────────────────
echo "=== Downloading Ubuntu Minimal Cloud squashfs ==="

if [[ "$SOURCE_TYPE" != "ubuntu-minimal-cloud-squashfs" ]]; then
  echo "ERROR: unsupported seed source type: ${SOURCE_TYPE}" >&2
  exit 1
fi

UBUNTU_RELEASE="${SPACE_SEED_UBUNTU_RELEASE}"
UBUNTU_VERSION="${SPACE_SEED_UBUNTU_VERSION}"
UBUNTU_DATE="${SPACE_SEED_UBUNTU_DATE}"
BASE_URL="https://cloud-images.ubuntu.com/minimal/releases/${UBUNTU_RELEASE}/${UBUNTU_DATE}"
# Ubuntu uses "arm64" not "aarch64" in file names
case "$ARCH" in
  aarch64) IMG_ARCH="arm64" ;;
  x86_64) IMG_ARCH="amd64" ;;
  *) echo "ERROR: unsupported seed architecture: ${ARCH}" >&2; exit 1 ;;
esac
SQUASHFS_NAME="ubuntu-${UBUNTU_VERSION}-minimal-cloudimg-${IMG_ARCH}.squashfs"
SQUASHFS_URL="${BASE_URL}/${SQUASHFS_NAME}"

echo "Image: ${SQUASHFS_NAME}"
echo "URL:   ${SQUASHFS_URL}"

extract_verified_squashfs "$BASE_URL" "$SQUASHFS_NAME" /cache "$BUILD" \
  /usr/share/keyrings/ubuntu-cloudimage-keyring.gpg D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81
cd "$BUILD"

# ── Unmount helper ────────────────────────────────────────────────────────────
safe_unmount() {
  for mnt in dev/pts dev/shm tmp dev sys proc; do
    if mountpoint -q "${CHROOT}/${mnt}" 2>/dev/null; then
      umount "${CHROOT}/${mnt}" 2>/dev/null || umount -l "${CHROOT}/${mnt}" 2>/dev/null || true
    fi
  done
}
trap safe_unmount EXIT

# ── Mount virtual filesystems for chroot ──────────────────────────────────────
echo "=== Setting up chroot ==="
mkdir -p "${CHROOT}"/{proc,sys,dev,dev/shm,dev/pts,tmp}

echo "  mounting /proc..."
mount --bind /proc "${CHROOT}/proc"
echo "  mounting /sys..."
mount --bind /sys "${CHROOT}/sys"
echo "  mounting /dev..."
mount -t devtmpfs devtmpfs "${CHROOT}/dev" 2>/dev/null \
  || mount --bind /dev "${CHROOT}/dev"
echo "  mounting /dev/shm..."
mount -t tmpfs tmpfs "${CHROOT}/dev/shm" 2>/dev/null || true
echo "  mounting /dev/pts..."
mount -t devpts devpts "${CHROOT}/dev/pts" -o gid=5,mode=620 2>/dev/null || true
echo "  mounting /tmp..."
mount -t tmpfs tmpfs "${CHROOT}/tmp"
chmod 1777 "${CHROOT}/tmp"
echo "  mounts done."

# resolv.conf may be a dangling symlink to systemd-resolved; replace it
rm -f "${CHROOT}/etc/resolv.conf"
printf 'nameserver 8.8.8.8\nnameserver 1.1.1.1\n' > "${CHROOT}/etc/resolv.conf"

mkdir -p "${CHROOT}/usr/local/sbin"
cat > "${CHROOT}/usr/local/sbin/space-configure-apt-mirror" <<'APTMIRROR'
#!/usr/bin/env bash
set -euo pipefail

mirror="${SPACE_UBUNTU_APT_MIRROR:-https://archive.ubuntu.com/ubuntu}"
mkdir -p /etc/apt/apt.conf.d
cat > /etc/apt/apt.conf.d/99space-apt-mirror <<'APTCONF'
Acquire::GzipIndexes "true";
APTCONF

mapfile -t files < <(find /etc/apt -type f \( -name '*.sources' -o -name 'sources.list' \) 2>/dev/null || true)
if ((${#files[@]} == 0)); then
  exit 0
fi

sed -i -E "s#https?://(archive|security)\.ubuntu\.com/ubuntu/?#${mirror}#g" "${files[@]}"
APTMIRROR
chmod 755 "${CHROOT}/usr/local/sbin/space-configure-apt-mirror"

# ── Copy builder SSH public key into chroot (if available) ───────────────────
BUILDER_PUBKEY="/space/etc/ssh/sandbox_ed25519.pub"
if [[ -f "$BUILDER_PUBKEY" ]]; then
  echo "Injecting sandbox SSH public key into rootfs..."
  cp "$BUILDER_PUBKEY" "${CHROOT}/tmp/builder_pubkey"
else
  echo "NOTE: ${BUILDER_PUBKEY} not found — sandbox SSH key will not be injected."
  echo "      Generated at packer build time in /root/.ssh/sandbox_ed25519"
fi

# ── Copy guest binaries (spaced daemon + CLI) ───────────────────────────────
# Mounted into the build container at /guest-bin/ by the outer launcher.
for bin in spaced-svc spaced; do
  if [[ -f "/guest-bin/$bin" ]]; then
    echo "Injecting guest binary: $bin"
    cp "/guest-bin/$bin" "${CHROOT}/usr/local/bin/$bin"
    chmod 755 "${CHROOT}/usr/local/bin/$bin"
  fi
done

# Release binaries resolve their deployment revision at runtime so the same
# compiled artifact can be reused across commits. Carry the bundle/image
# identity into the guest rootfs when this builder is running from a deployed
# SPACE installation.
for git_sha_file in /space/etc/build-git-sha /etc/space/build-git-sha; do
  if [[ -f "$git_sha_file" ]]; then
    mkdir -p "${CHROOT}/etc/space"
    cp "$git_sha_file" "${CHROOT}/etc/space/build-git-sha"
    break
  fi
done

# ── Chroot: configure seed ───────────────────────────────────────────────────
echo "=== Configuring seed in chroot ==="
chroot "${CHROOT}" /bin/bash -c "
set -euo pipefail

mkdir -p /var/cache/apt/archives/partial /var/log/apt
SPACE_UBUNTU_APT_MIRROR='${UBUNTU_APT_MIRROR}' /usr/local/sbin/space-configure-apt-mirror
apt-get update

# Strip cloud packages not needed in Firecracker guests
DEBIAN_FRONTEND=noninteractive apt-get purge -y \
  cloud-init cloud-guest-utils cloud-init-base \
  chrony pollinate apparmor apport apport-core-dump-handler \
  ubuntu-pro-client ubuntu-advantage-tools \
  ubuntu-release-upgrader-core update-manager-core update-notifier-common \
  unattended-upgrades \
  2>/dev/null || true
rm -rf /etc/cloud /var/lib/cloud

# Reinstall packages that the cloud image may not include, plus procps. Avoid a
# full distro upgrade here; it makes seed builds depend on transient mirror
# state and has previously hung on package fetches.
#
# Keep iproute2 explicitly installed: autoremove would otherwise remove tools
# such as ss that workload readiness checks need.
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  -o Dpkg::Options::=--force-confdef \
  -o Dpkg::Options::=--force-confold \
  openssh-server sudo curl ca-certificates procps iproute2 \
  || dpkg --configure -a
apt-get autoremove -y 2>/dev/null || true

# user account (UID 2000) — rename existing 'ubuntu' user if present,
# otherwise create 'user' fresh. UID 2000 avoids conflation with host
# 'space' user (UID 1000).
if id -u ubuntu 2>/dev/null; then
  usermod -l user -d /home/user -m -u 2000 ubuntu
  groupmod -n user -g 2000 ubuntu
elif ! id -u user 2>/dev/null; then
  useradd -m -u 2000 -s /bin/bash user
fi
echo 'user ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/user
chmod 440 /etc/sudoers.d/user
mkdir -p /home/user/.ssh /home/user/workspace
chown -R user:user /home/user
chmod 700 /home/user/.ssh

# Builder SSH key — injected from /tmp/builder_pubkey if present
if [ -f /tmp/builder_pubkey ]; then
  cat /tmp/builder_pubkey >> /home/user/.ssh/authorized_keys
  chmod 600 /home/user/.ssh/authorized_keys
  chown user:user /home/user/.ssh/authorized_keys
  mkdir -p /root/.ssh
  cat /tmp/builder_pubkey >> /root/.ssh/authorized_keys
  chmod 600 /root/.ssh/authorized_keys
  rm /tmp/builder_pubkey
fi

# SSH config
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config
sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sed -i 's/^#\?AcceptEnv.*/AcceptEnv */' /etc/ssh/sshd_config
grep -q '^AcceptEnv ' /etc/ssh/sshd_config || echo 'AcceptEnv *' >> /etc/ssh/sshd_config
ssh-keygen -A
usermod --password '!' user

# Static network (Firecracker guest — fixed link-local, same for every sandbox.
# Per-sandbox identity comes from namespace NAT, not guest IP. Kernel boot args
# set this via ip= before userspace; keep the networkd file only as a manual
# debugging fallback if networkd is unmasked/enabled.
# Note: 169.254.x.x is link-local scope, so the boot args must set a source on
# the default route, otherwise the kernel uses 0.0.0.0 for non-link-local
# destinations.
mkdir -p /etc/systemd/network
cat > /etc/systemd/network/10-eth.network <<'NETEOF'
[Match]
Name=eth0

[Network]
Address=169.254.0.21/30
DNS=8.8.8.8 1.1.1.1

[Route]
Gateway=169.254.0.22
PreferredSource=169.254.0.21
NETEOF
systemctl disable systemd-networkd 2>/dev/null || true
# Keep SSH available without making every cold boot wait for sshd startup.
# The first space-ssh connection socket-activates ssh.service.
systemctl disable ssh.service sshd.service 2>/dev/null || true
mkdir -p /etc/systemd/system/ssh.socket.d
cat > /etc/systemd/system/ssh.socket.d/00-space-fast-boot.conf <<'SSHSOCKET'
[Unit]
DefaultDependencies=no
SSHSOCKET
systemctl enable ssh.socket 2>/dev/null || true

# Permit SSH logins without pulling multi-user.target/basic.target into the
# snapshot-optimized boot path. The stock systemd-user-sessions.service has
# DefaultDependencies=yes, so requiring it directly from space.target widens the
# boot transaction substantially.
mkdir -p /etc/tmpfiles.d
cat > /etc/tmpfiles.d/systemd-nologin.conf <<'NOLOGIN'
# SPACE boots to space.target instead of multi-user.target. The vendor tmpfiles
# rule creates /run/nologin for the normal boot path and expects
# systemd-user-sessions.service to remove it later; space.target intentionally
# skips that service, so do not create /run/nologin in sandbox guests.
NOLOGIN

cat > /etc/systemd/system/space-user-sessions.service <<'USERSESSIONS'
[Unit]
Description=Permit SPACE Sandbox User Sessions
DefaultDependencies=no
After=local-fs.target
Before=space.target ssh.socket ssh.service shutdown.target
Conflicts=shutdown.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/rm -f /run/nologin /etc/nologin
USERSESSIONS

# Boot to the smallest target that makes a SPACE sandbox useful. This is also
# the snapshot-capture readiness point, so keep local-fs in the dependency
# chain: warm starts are more important than the earliest possible cold control
# plane response.
cat > /etc/systemd/system/space.target <<'SPACETARGET'
[Unit]
Description=SPACE Sandbox Target
DefaultDependencies=no
Requires=local-fs.target spaced.service space-user-sessions.service
Wants=ssh.socket
After=local-fs.target spaced.service space-user-sessions.service
Conflicts=shutdown.target
Before=shutdown.target
AllowIsolate=yes

[Install]
Alias=default.target
SPACETARGET
systemctl set-default space.target 2>/dev/null || true

# spaced guest daemon — runs as root, listens on /run/spaced.sock for
# CLI and agent processes (user UID 2000), and vsock for host communication.
if [ -f /usr/local/bin/spaced-svc ]; then
  cat > /etc/systemd/system/spaced.service <<'SVCEOF'
[Unit]
Description=SPACE Guest Daemon
DefaultDependencies=no
After=local-fs.target
Before=space.target basic.target shutdown.target
Conflicts=shutdown.target
# Never stop restarting: the default start limit (5 starts per 10 s) would
# leave a crash-looping spaced failed for good, and the sandbox unreachable.
# RestartSec bounds the rate instead; NRestarts and the journal record it.
StartLimitIntervalSec=0

[Service]
Type=notify
NotifyAccess=main
# READY=1 comes once the bridge listener is bound, well under a second after
# start; a start that hangs is killed and restarted rather than left waiting
# for the 90 s default.
TimeoutStartSec=30
# Prefer workloads over spaced when the guest runs out of memory: losing spaced
# makes the sandbox unreachable and, through KillMode, ends every workload
# anyway. Not -1000: a spaced that is itself the problem must stay killable.
# Spawned workloads reset their own score to 0 before exec, so they do not
# inherit this protection.
OOMScoreAdjust=-900
# A workload or worker killed by the OOM killer must not stop the service
# (the default for delegated units, set explicitly).
OOMPolicy=continue
# Hand spaced its own cgroup subtree so it can split itself off from the user
# workloads it spawns: spaced-svc moves into a supervisor/ leaf with a
# dominating cpu.weight and a memory.min reservation, while workloads land in a
# capped workloads/ leaf. Delegate=yes is what makes systemd enable the
# cpu/memory/pids controllers for this subtree and stop managing it, so the
# daemon can write cgroup.subtree_control and create the leaves. Without it a
# runaway workload shares spaced's budget and starves the host health pings.
Delegate=yes
# No unit-wide task cap. systemd's default (15% of the kernel's task limit)
# would cover spaced and every workload together, so a workload that creates
# a few thousand threads would leave spaced unable to start a thread or
# process. spaced caps its workloads/ leaf below the kernel's task limit
# instead, keeping a reserve for itself and the system.
TasksMax=infinity
# New port exposures are limited to 128 and forwarding is bounded to 256 active
# connections (two stream FDs each). Leave headroom for grandfathered exposure
# listeners, RPC transports, process pipes, filesystem watches, and transient
# accepts.
LimitNOFILE=4096
# Systemd watchdog deadlines are captured in Firecracker snapshots. After
# enough wall-clock time passes, restoring a snapshot can immediately trip the
# stale deadline and restart spaced-svc. Host-side watchdogs own guest health.
WatchdogSec=0
ExecStartPre=-/bin/mknod -m 666 /dev/vsock c 10 241
# Transparent Huge Pages only where memory asks for them (madvise): khugepaged
# then scans just those regions instead of wasting CPU across FC guests, and
# spaced's upload buffers can be physically contiguous for large disk requests.
# Neither faults (defrag) nor khugepaged may compact memory for them. Guests
# boot with THP off (transparent_hugepage=never), so one from an older seed,
# without this step, keeps it off whatever spaced it runs; and the minimal boot
# target does not run systemd-tmpfiles-setup before spaced. So this turns THP
# on together with both compaction settings, before spaced starts. A kernel
# without THP has nothing to set; a control that exists but cannot be set fails
# the start. Inline, so that guests assembled from this seed's units need no
# helper. Set at boot, never from the seed build: its chroot shares the
# builder kernel's /sys.
ExecStartPre=/bin/sh -c 'test -d /sys/kernel/mm/transparent_hugepage || exit 0; echo madvise > /sys/kernel/mm/transparent_hugepage/enabled && echo never > /sys/kernel/mm/transparent_hugepage/defrag && echo 0 > /sys/kernel/mm/transparent_hugepage/khugepaged/defrag'
ExecStart=/usr/local/bin/spaced-svc
Restart=always
RestartSec=2
# The default KillMode (control-group) kills every process left in the unit's
# cgroup on stop, workloads included. The stop deadline, shorter than the 90s
# default, covers the daemon's 10s drain plus its 5s telemetry and 5s runtime
# shutdown bounds.
TimeoutStopSec=30
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=space.target
SVCEOF
  systemctl enable spaced.service 2>/dev/null || true
fi

# Keep the guest journal in tmpfs. Sandbox logs are already emitted to the
# console, and persistent guest journals make systemd-journal-flush.service a
# sysinit critical-path dependency during boot.
mkdir -p /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/00-volatile.conf <<'JOURNALCONF'
[Journal]
Storage=volatile
JOURNALCONF
rm -rf /var/log/journal
systemctl mask systemd-journal-flush.service 2>/dev/null || true

# Bypass systemd-resolved: use static resolv.conf with direct nameservers.
# systemd-resolved's stub (127.0.0.53) doesn't work reliably in FC guests
# because it requires DefaultRoute=yes on the link, which link-local addresses
# don't get by default.
systemctl disable systemd-resolved 2>/dev/null || true
rm -f /etc/resolv.conf
printf 'nameserver 8.8.8.8\nnameserver 1.1.1.1\n' > /etc/resolv.conf

# These are part of the single readiness path for a snapshot-optimized guest.
systemctl unmask \
  systemd-tmpfiles-setup.service \
  systemd-tmpfiles-setup-dev.service \
  systemd-tmpfiles-setup-dev-early.service \
  2>/dev/null || true

# Mask services that waste boot time in a Firecracker guest (no console,
# no keyboard, no hardware, no interactive logins, no cloud entropy polling,
# no apt cron).
systemctl mask --force \
  console-setup.service \
  keyboard-setup.service \
  setvtrgb.service \
  pollinate.service \
  dbus.service \
  dbus.socket \
  systemd-networkd-persistent-storage.service \
  systemd-networkd.service \
  systemd-networkd.socket \
  systemd-networkd-varlink.socket \
  systemd-networkd-resolve-hook.socket \
  systemd-networkd-wait-online.service \
  e2scrub_reap.service \
  systemd-logind.service \
  systemd-hostnamed.service \
  systemd-hostnamed.socket \
  systemd-binfmt.service \
  modprobe@configfs.service \
  modprobe@drm.service \
  modprobe@efi_pstore.service \
  ldconfig.service \
  systemd-pstore.service \
  systemd-network-generator.service \
  systemd-udevd.service \
  systemd-udev-trigger.service \
  systemd-modules-load.service \
  systemd-journal-catalog-update.service \
  systemd-update-done.service \
  systemd-remount-fs.service \
  systemd-sysusers.service \
  systemd-sysctl.service \
  sys-kernel-debug.mount \
  dev-hugepages.mount \
  dev-mqueue.mount \
  tmp.mount \
  getty@tty1.service \
  gpu-manager.service \
  secureboot-db.service \
  sshd-keygen.service \
  sshd@sshd-keygen.service \
  sshd-vsock.socket \
  sshd-unix-local.socket \
  lxd-installer.socket \
  unattended-upgrades.service \
  ua-reboot-cmds.service \
  ubuntu-advantage.service \
  update-notifier-download.service \
  2>/dev/null || true

# Disable timers not needed in sandbox VMs
systemctl disable \
  apt-daily.timer \
  apt-daily-upgrade.timer \
  dpkg-db-backup.timer \
  e2scrub_all.timer \
  fstrim.timer \
  motd-news.timer \
  ua-timer.timer \
  update-notifier-download.timer \
  update-notifier-motd.timer \
  2>/dev/null || true

# udev is masked for faster boots. Keep a static vsock node in the image as a
# fallback; spaced.service also recreates it after devtmpfs is mounted.
mknod -m 666 /dev/vsock c 10 241 2>/dev/null || true

echo '/dev/vda / ext4 defaults,noatime 0 1' > /etc/fstab
echo 'space-sandbox' > /etc/hostname
echo '127.0.0.1 space-sandbox' >> /etc/hosts
mkdir -p /etc/spaced
printf '%s\n' '${SPACED_VERSION}' > /etc/spaced/version
printf '%s\n' '${SPACED_COMPATIBILITY_FAMILY}' > /etc/spaced/compatibility-family

rm -rf /tmp/* /var/tmp/*
apt-get clean
rm -rf /var/lib/apt/lists/*

# Remove Ubuntu minimal MOTD nag
rm -f /etc/update-motd.d/60-unminimize
"

# Install the SPACE MITM root CA(s) if the builder staged one at this fixed
# path (mirrors the sandbox SSH key — a staged file, no env var). csplit
# because update-ca-certificates wants one certificate per .crt file.
MITM_ROOT_CA="/space/etc/space-mitm/root-ca.pem"
if [[ -f "$MITM_ROOT_CA" ]]; then
  echo "=== Installing SPACE MITM root CA into guest trust store ==="
  install -d "${CHROOT}/usr/local/share/ca-certificates"
  csplit -z -s -f "${CHROOT}/usr/local/share/ca-certificates/space-mitm-root-" \
    -b '%02d.crt' "$MITM_ROOT_CA" '/-----BEGIN CERTIFICATE-----/' '{*}'
  chroot "${CHROOT}" /bin/bash -c "update-ca-certificates"
fi

# ── Tear down mounts ─────────────────────────────────────────────────────────
trap - EXIT
safe_unmount
chmod 1777 "${CHROOT}/tmp" "${CHROOT}/var/tmp" 2>/dev/null || true

# ── Pack into ext4 ────────────────────────────────────────────────────────────
echo "=== Creating ext4 image ==="
# Don't chown -R root:root — it strips setuid bits from sudo, ping, etc.
# The squashfs has correct ownership already. Only fix /home/user.
[[ -d "${CHROOT}/home/user" ]] && chown -R 2000:2000 "${CHROOT}/home/user"

# Keep seed image small for fast transfer. The builder resizes the CoW clone
# to the configured build disk size before installing packages.
IMG_SIZE="1024M"
truncate -s "$IMG_SIZE" "${OUTPUT}/rootfs.ext4"
mkfs.ext4 -O dir_nlink,large_dir -J size=4 -m 1 -d "${CHROOT}" -F "${OUTPUT}/rootfs.ext4"

# Shrink to minimum + 100 MB headroom (SSH host keys, logs, journal)
e2fsck -fy "${OUTPUT}/rootfs.ext4" 2>/dev/null || true
resize2fs -M "${OUTPUT}/rootfs.ext4"
# Get actual filesystem size (resize2fs -M shrinks fs but not the file)
FS_BLOCKS=$(dumpe2fs -h "${OUTPUT}/rootfs.ext4" 2>/dev/null | awk '/Block count:/{print $NF}')
BLOCK_SIZE=$(dumpe2fs -h "${OUTPUT}/rootfs.ext4" 2>/dev/null | awk '/Block size:/{print $NF}')
if [[ -z "$FS_BLOCKS" || -z "$BLOCK_SIZE" ]]; then
  echo "ERROR: could not parse filesystem size from dumpe2fs" >&2
  exit 1
fi
FS_SIZE=$(( FS_BLOCKS * BLOCK_SIZE ))
HEADROOM=$(( 100 * 1024 * 1024 ))
TARGET_SIZE=$(( FS_SIZE + HEADROOM ))
truncate -s "$TARGET_SIZE" "${OUTPUT}/rootfs.ext4"
resize2fs "${OUTPUT}/rootfs.ext4"

# ── Metadata ──────────────────────────────────────────────────────────────────
cat > "${OUTPUT}/metadata.json" <<EOF
{
  "schema_version": 2,
  "execution": {
    "runtime": {
      "kind": "${RUNTIME}",
      "version": "${RUNTIME_VERSION}"
    },
    "guest": {
      "os": "linux",
      "architecture": "${ARCH}"
    },
    "state_format": {
      "format": "firecracker-snapshot",
      "version": "${STATE_FORMAT_VERSION}"
    },
    "components": [${KERNEL_COMPONENT}]
  },
  "id": "${ID}",
  "name": "seed",
  "description": "Minimal bootable Ubuntu with SSH and networking",
  "base": "",
  "built_at": "$(date -Iseconds)",
  "base_image": "${SQUASHFS_URL}",
  "arch": "${ARCH}",
  "spaced_version": "${SPACED_VERSION}",
  "spaced_compatibility_family": "${SPACED_COMPATIBILITY_FAMILY}",
  "recipe": "",
  "steps_count": 0,
  "vcpu": 0,
  "memory_mib": 0
}
EOF

echo ""
echo "=== Seed template built ==="
echo "  rootfs:    ${OUTPUT}/rootfs.ext4"
echo "  metadata:  ${OUTPUT}/metadata.json"
ls -lh "${OUTPUT}/"
