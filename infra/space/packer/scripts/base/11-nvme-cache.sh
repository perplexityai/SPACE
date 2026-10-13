#!/usr/bin/env bash
# Install a systemd service that formats and mounts local NVMe instance store
# as btrfs /space/cache at boot. On instances without instance store, the
# service exits cleanly and the existing loopback mount remains.
set -euxo pipefail
AL2023_RELEASEVER="${AL2023_RELEASEVER:-latest}"

# Ensure required tools are available
if command -v dnf &>/dev/null; then
  dnf --releasever="$AL2023_RELEASEVER" install -y nvme-cli cloud-utils-growpart
elif command -v apt-get &>/dev/null; then
  apt-get update -qq && apt-get install -y nvme-cli cloud-guest-utils
fi

# Install the boot-time mount script
cat > /usr/local/sbin/mount-space-cache-nvme.sh << 'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail

# Two boot paths run this script: space-cache-nvme.service, and the node's
# user data, which isn't ordered against that unit. Both would unmount and
# force-format the same devices. The lock serializes them, and a run that
# finishes leaves a marker that makes any later run this boot a no-op. The
# marker is written last, so a run that failed partway is redone in full.
exec 9>/run/space-cache-nvme.lock
flock 9
if [[ -e /run/space-cache-nvme.done ]]; then
  echo "/space/cache was already set up this boot; nothing to do"
  exit 0
fi

grow_root() {
  # Grow root partition to fill EBS volume if launched with a larger disk than
  # the baked AMI. Keep this best-effort so cache mounting is not blocked.
  local root_source resolved_root_source root_fstype root_block root_sys root_disk part_num
  root_source="$(findmnt -n -o SOURCE /)"
  resolved_root_source="$(readlink -f "$root_source" 2>/dev/null || true)"
  if [[ -n "$resolved_root_source" ]]; then
    root_source="$resolved_root_source"
  fi
  root_fstype="$(findmnt -n -o FSTYPE /)"

  if [[ "$root_source" != /dev/* ]]; then
    echo "Root source is not a block device, skipping grow: $root_source"
    return 0
  fi

  root_block="$(basename "$root_source")"
  root_sys="/sys/class/block/$root_block"
  if [[ ! -e "$root_sys" ]]; then
    echo "Could not resolve sysfs block device for root source, skipping grow: $root_source"
    return 0
  fi

  if [[ -f "$root_sys/partition" ]]; then
    part_num="$(<"$root_sys/partition")"
    root_disk="/dev/$(basename "$(dirname "$(readlink -f "$root_sys")")")"
    growpart "$root_disk" "$part_num" || true
  fi

  case "$root_fstype" in
    xfs)
      xfs_growfs / || true
      ;;
    ext2 | ext3 | ext4)
      resize2fs "$root_source" || true
      ;;
    *)
      echo "Unsupported root filesystem for grow, skipping: $root_fstype"
      ;;
  esac
}

grow_root

# Find local NVMe instance store devices.
# Instance store: model contains "Instance Storage"
# EBS volumes: model contains "Elastic Block Store"
CACHE_DEVS=()
for dev in /dev/nvme*n1; do
  [[ -b "$dev" ]] || continue
  devname=$(basename "$dev")
  # Check model via sysfs (no nvme-cli needed as fallback)
  model=$(cat "/sys/block/${devname}/device/model" 2>/dev/null || true)
  if [[ "$model" == *"Instance Storage"* ]]; then
    CACHE_DEVS+=("$dev")
  fi
done

if [[ ${#CACHE_DEVS[@]} -eq 0 ]]; then
  echo "No instance store NVMe found, keeping default /space/cache"
  exit 0
fi

echo "Found ${#CACHE_DEVS[@]} instance store NVMe device(s): ${CACHE_DEVS[*]}"

# Drain pending udev events so AL2023 instance-store init releases the devices
# before mkfs.btrfs tries to claim them.
udevadm settle --timeout=30 || true

# Unmount loopback if present
if mountpoint -q /space/cache; then
  umount --lazy /space/cache
fi

# mkfs.btrfs occasionally races AL2023 instance-store init and aborts with
# "Device or resource busy" on the first device. Retry on EBUSY before giving
# up, and dump the holder for postmortem if we still lose the race.
mkfs_btrfs_with_retry() {
  local attempts=5
  local delay=2
  local attempt rc out
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    rc=0
    out="$(mkfs.btrfs "$@" 2>&1)" || rc=$?
    echo "$out"
    if [[ $rc -eq 0 ]]; then
      return 0
    fi
    if ! grep -q "Device or resource busy" <<<"$out"; then
      return $rc
    fi
    if [[ $attempt -eq $attempts ]]; then
      echo "mkfs.btrfs still busy after ${attempts} attempts; dumping device holders" >&2
      for dev in "${CACHE_DEVS[@]}"; do
        echo "--- holders for $dev ---" >&2
        fuser -v "$dev" >&2 2>&1 || true
        lsof "$dev" >&2 2>&1 || true
      done
      return 1
    fi
    echo "mkfs.btrfs busy on attempt ${attempt}/${attempts}, retrying in ${delay}s"
    sleep "$delay"
    udevadm settle --timeout=10 || true
  done
}

# Format as btrfs (RAID0 if multiple drives for max throughput)
if [[ ${#CACHE_DEVS[@]} -eq 1 ]]; then
  mkfs_btrfs_with_retry -f -L cache "${CACHE_DEVS[0]}"
  mount -o defaults,noatime "${CACHE_DEVS[0]}" /space/cache
else
  mkfs_btrfs_with_retry -f -L cache -d raid0 -m raid0 "${CACHE_DEVS[@]}"
  mount -o defaults,noatime "${CACHE_DEVS[0]}" /space/cache
fi

# Create expected directory structure
mkdir -p /space/cache/firecracker/{assets,templates,sandboxes,jails,builds}

# Rehydrate the independently versioned guest-kernel inventory from the root-backed vendor
# directory. The instance-store mount replaces the AMI-baked cache at every boot.
default_fc_version="$(basename "$(readlink -f /space/vendor/firecracker/default)" 2>/dev/null || true)"
default_kernel_version="$(basename "$(readlink -f /space/vendor/kernels/default)" 2>/dev/null || true)"
legacy_kernel_version="$(basename "$(readlink -f /space/vendor/kernels/legacy-default)" 2>/dev/null || true)"
found_kernel=false
for vendor_kernel in /space/vendor/kernels/*; do
  [[ -d "$vendor_kernel" && -f "${vendor_kernel}/vmlinux.bin" ]] || continue
  kernel_version="$(basename "$vendor_kernel")"
  [[ "$kernel_version" == "default" || "$kernel_version" == "legacy-default" ]] && continue
  install -D -o root -g root -m 0644 \
    "${vendor_kernel}/vmlinux.bin" \
    "/space/cache/firecracker/kernels/${kernel_version}/vmlinux.bin"
  found_kernel=true
done
if [[ "$found_kernel" == true && -n "$default_kernel_version" ]]; then
  ln -sfn "${default_kernel_version}" /space/cache/firecracker/kernels/default
  if [[ -n "$legacy_kernel_version" ]]; then
    ln -sfn "${legacy_kernel_version}" /space/cache/firecracker/kernels/legacy-default
  fi
  for vendor_version in /space/vendor/firecracker/v[0-9]*; do
    [[ -d "$vendor_version" ]] || continue
    fc_version="$(basename "$vendor_version")"
    install -d -o root -g root -m 0755 "/space/cache/firecracker/assets/${fc_version}/kernel"
    rm -f "/space/cache/firecracker/assets/${fc_version}/kernel/vmlinux.bin"
    ln "/space/cache/firecracker/kernels/${default_kernel_version}/vmlinux.bin" \
      "/space/cache/firecracker/assets/${fc_version}/kernel/vmlinux.bin"
  done
  if [[ -n "$default_fc_version" ]]; then
    ln -sfn "${default_fc_version}" /space/cache/firecracker/assets/default
  else
    echo "WARNING: default Firecracker vendor release is missing" >&2
  fi
else
  echo "WARNING: guest-kernel vendor inventory missing; cache assets not populated" >&2
fi

chown -R root:space-cache-rw /space/cache 2>/dev/null || true
chmod -R 2775 /space/cache
chown root:root /space/cache/firecracker/jails
chmod 0755 /space/cache/firecracker/jails
# Kernel and asset roots are staged by root and read by launches: keep them
# root-only after the cache-wide group grant above, which also made their files
# group-writable. The two shared parents are sticky so the cache services
# cannot rename those roots away.
for tree in /space/cache/firecracker/kernels /space/cache/firecracker/assets; do
  [[ -d "$tree" && ! -L "$tree" ]] || continue
  chown -R root:root "$tree"
  find "$tree" -xdev -type d -exec chmod u=rwx,go=rx,ug-s,a-t {} +
  find "$tree" -xdev -type f -exec chmod u=rw,go=r,ug-s,a-t {} +
done
chmod +t /space/cache /space/cache/firecracker

echo "Mounted /space/cache on NVMe instance store (btrfs RAID0)"
df -Th /space/cache
touch /run/space-cache-nvme.done
SCRIPT
chmod +x /usr/local/sbin/mount-space-cache-nvme.sh

# Install systemd unit
cat > /etc/systemd/system/space-cache-nvme.service << 'UNIT'
[Unit]
Description=Format and mount NVMe instance store as /space/cache (btrfs)
After=local-fs.target
Before=kubelet.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/mount-space-cache-nvme.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable space-cache-nvme.service
