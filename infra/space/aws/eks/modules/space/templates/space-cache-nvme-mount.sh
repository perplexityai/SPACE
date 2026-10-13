mount_space_cache_nvme_if_available() {
  if [[ ! -d /space/cache ]]; then
    return 0
  fi

  if [[ -x /usr/local/sbin/mount-space-cache-nvme.sh ]]; then
    /usr/local/sbin/mount-space-cache-nvme.sh
    return 0
  fi

  local cache_devs=()
  local dev devname model
  for dev in /dev/nvme*n1; do
    [[ -b "$dev" ]] || continue
    devname="$(basename "$dev")"
    model="$(cat "/sys/block/${devname}/device/model" 2>/dev/null || true)"
    if [[ "$model" == *"Instance Storage"* ]]; then
      cache_devs+=("$dev")
    fi
  done

  if [[ ${#cache_devs[@]} -eq 0 ]]; then
    echo "No instance-store NVMe devices found; keeping existing /space/cache"
    return 0
  fi

  if ! command -v mkfs.btrfs >/dev/null 2>&1; then
    dnf --releasever=latest install -y btrfs-progs || {
      echo "mkfs.btrfs is unavailable; keeping existing /space/cache"
      return 0
    }
  fi

  # Best-effort install of holder-diagnostics tools so the EBUSY dump path has
  # something actionable to show. Don't fail the mount if these are missing.
  if ! command -v fuser >/dev/null 2>&1 || ! command -v lsof >/dev/null 2>&1; then
    dnf --releasever=latest install -y psmisc lsof || true
  fi

  # Drain pending udev events so AL2023 instance-store init releases the
  # devices before mkfs.btrfs tries to claim them.
  udevadm settle --timeout=30 || true

  if mountpoint -q /space/cache; then
    umount --lazy /space/cache
  fi

  local attempts=5
  local delay=2
  local attempt rc out
  local mkfs_args=(-f -L cache)
  if [[ ${#cache_devs[@]} -gt 1 ]]; then
    mkfs_args+=(-d raid0 -m raid0)
  fi
  mkfs_args+=("${cache_devs[@]}")

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    rc=0
    out="$(mkfs.btrfs "${mkfs_args[@]}" 2>&1)" || rc=$?
    echo "$out"
    if [[ $rc -eq 0 ]]; then
      break
    fi
    if ! grep -q "Device or resource busy" <<<"$out"; then
      return $rc
    fi
    if [[ $attempt -eq $attempts ]]; then
      echo "mkfs.btrfs still busy after ${attempts} attempts; dumping device holders" >&2
      for dev in "${cache_devs[@]}"; do
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

  mount -o defaults,noatime "${cache_devs[0]}" /space/cache

  mkdir -p /space/cache/firecracker/{templates,sandboxes,jails,builds}
  chown -R root:space-cache-rw /space/cache 2>/dev/null || true
  chmod -R 2775 /space/cache
  chown root:root /space/cache/firecracker/jails
  chmod 0755 /space/cache/firecracker/jails
  chmod +t /space/cache /space/cache/firecracker /space/cache/firecracker/builds
  for build_root in /space/cache/firecracker/builds/{oci,seed-cache}; do
    [[ ! -L "$build_root" ]] || { echo "Refusing linked build directory: $build_root" >&2; exit 1; }
    install -d -m 00755 -o 0 -g 0 "$build_root"
  done
  df -Th /space/cache
}

mount_space_cache_nvme_if_available
