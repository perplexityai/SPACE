#!/usr/bin/env bash
# Install a systemd service that formats and mounts local NVMe instance store
# as btrfs /space/cache at boot. On instances without instance store, the
# service exits cleanly and the existing loopback mount remains.
set -euxo pipefail
AL2023_RELEASEVER="${AL2023_RELEASEVER:-latest}"

# Ensure required tools are available
if command -v dnf &>/dev/null; then
  dnf --releasever="$AL2023_RELEASEVER" install -y nvme-cli cloud-utils-growpart cryptsetup
elif command -v apt-get &>/dev/null; then
  apt-get update -qq && apt-get install -y nvme-cli cloud-guest-utils cryptsetup-bin
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

# Tenant-encrypted cache. A launch template opts a node in by tagging the
# instance with SpaceCacheEncryptionKmsKeyArn (instance metadata tags must be
# enabled). The cache devices are then LUKS2 volumes keyed by a data key from
# that KMS key, so revoking the key makes the cache unreadable once the
# watchdog suspends the volumes or the node stops. Every failure on this path
# stops the boot: a node tagged for encryption never mounts a plaintext cache.
ENCRYPTION_STATE_DIR=/run/space-cache-encryption
ENCRYPTED_CACHE_MARKER=/space/cache/.space-cache-encrypted

imds_get() {
  # Prints the body and returns 0 on 200, returns 4 on 404, 1 otherwise.
  local path="$1" token code body
  token="$(curl -fsS -m 2 -X PUT http://169.254.169.254/latest/api/token \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 300')" || return 1
  body="$(mktemp)"
  code="$(curl -sS -m 2 -o "$body" -w '%{http_code}' \
    -H "X-aws-ec2-metadata-token: ${token}" \
    "http://169.254.169.254/latest/meta-data/${path}")" || { rm -f "$body"; return 1; }
  case "$code" in
    200) cat "$body"; rm -f "$body"; return 0 ;;
    404) rm -f "$body"; return 4 ;;
    *) rm -f "$body"; return 1 ;;
  esac
}

imds_get_retry() {
  local path="$1" attempt rc out
  for attempt in $(seq 1 30); do
    rc=0
    out="$(imds_get "$path")" || rc=$?
    if [[ $rc -eq 0 || $rc -eq 4 ]]; then
      printf '%s' "$out"
      return "$rc"
    fi
    sleep 2
  done
  echo "instance metadata unreachable reading ${path}" >&2
  return 1
}

CACHE_KMS_KEY_ARN=""
tags_rc=0
imds_get_retry tags/instance >/dev/null || tags_rc=$?
if [[ $tags_rc -eq 1 ]]; then
  exit 1
fi
if [[ $tags_rc -eq 0 ]]; then
  key_rc=0
  CACHE_KMS_KEY_ARN="$(imds_get_retry tags/instance/SpaceCacheEncryptionKmsKeyArn)" || key_rc=$?
  if [[ $key_rc -eq 1 ]]; then
    exit 1
  fi
  if [[ $key_rc -eq 4 ]]; then
    CACHE_KMS_KEY_ARN=""
  fi
fi

if [[ -n "$CACHE_KMS_KEY_ARN" ]]; then
  if [[ "$CACHE_KMS_KEY_ARN" != arn:aws*:kms:*:*:key/* && "$CACHE_KMS_KEY_ARN" != arn:aws*:kms:*:*:alias/* ]]; then
    echo "SpaceCacheEncryptionKmsKeyArn is not a KMS key or alias ARN: ${CACHE_KMS_KEY_ARN}" >&2
    exit 1
  fi
  for tool in cryptsetup aws base64; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "encrypted cache requested but ${tool} is not installed" >&2
      exit 1
    fi
  done
  CACHE_KMS_REGION="$(cut -d: -f4 <<<"$CACHE_KMS_KEY_ARN")"
  INSTANCE_ID="$(imds_get_retry instance-id)"
  echo "Encrypting /space/cache under ${CACHE_KMS_KEY_ARN}"
fi

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
  if [[ -n "$CACHE_KMS_KEY_ARN" ]]; then
    echo "encrypted cache requested but this instance has no instance-store NVMe" >&2
    exit 1
  fi
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

if [[ -n "$CACHE_KMS_KEY_ARN" ]]; then
  # One data key per boot, wrapped by the tenant key and bound to this
  # instance through the encryption context. Only the wrapped copy outlives
  # this block; the plaintext is a key file on tmpfs, removed once the volumes
  # are open. Instance-store contents never survive a stop, so nothing needs
  # the key across boots.
  install -d -m 0700 "$ENCRYPTION_STATE_DIR"
  for mapped in /dev/mapper/space-cache-*; do
    [[ -e "$mapped" ]] || continue
    cryptsetup close "$(basename "$mapped")" || true
  done
  umask 077
  encryption_context="{\"space:purpose\":\"node-cache\",\"space:instance\":\"${INSTANCE_ID}\"}"
  data_key_json="$(aws kms generate-data-key \
    --region "$CACHE_KMS_REGION" \
    --key-id "$CACHE_KMS_KEY_ARN" \
    --key-spec AES_256 \
    --encryption-context "$encryption_context" \
    --query '[CiphertextBlob,Plaintext,KeyId]' --output text)"
  read -r wrapped_key plaintext_key resolved_key_id <<<"$data_key_json"
  unset data_key_json
  printf '%s' "$wrapped_key" >"${ENCRYPTION_STATE_DIR}/wrapped-key.b64"
  printf '%s\n' "$resolved_key_id" >"${ENCRYPTION_STATE_DIR}/key-id"
  printf '%s\n' "$CACHE_KMS_REGION" >"${ENCRYPTION_STATE_DIR}/region"
  printf '%s\n' "$encryption_context" >"${ENCRYPTION_STATE_DIR}/encryption-context"
  key_file="${ENCRYPTION_STATE_DIR}/plaintext.key"
  base64 -d <<<"$plaintext_key" >"$key_file"
  unset plaintext_key wrapped_key

  MAPPED_DEVS=()
  index=0
  for dev in "${CACHE_DEVS[@]}"; do
    name="space-cache-${index}"
    # The data key is 256 random bits, so key stretching buys nothing; the
    # minimum PBKDF2 cost keeps boot fast on many devices.
    cryptsetup luksFormat --batch-mode --type luks2 \
      --cipher aes-xts-plain64 --key-size 512 \
      --pbkdf pbkdf2 --pbkdf-force-iterations 1000 \
      --key-file "$key_file" "$dev"
    cryptsetup open --type luks2 --key-file "$key_file" \
      --perf-no_read_workqueue --perf-no_write_workqueue \
      "$dev" "$name"
    MAPPED_DEVS+=("/dev/mapper/${name}")
    index=$((index + 1))
  done
  shred -u "$key_file" 2>/dev/null || rm -f "$key_file"
  umask 022
  printf '%s\n' "${MAPPED_DEVS[@]}" >"${ENCRYPTION_STATE_DIR}/devices"
  CACHE_DEVS=("${MAPPED_DEVS[@]}")
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

if [[ -n "$CACHE_KMS_KEY_ARN" ]]; then
  # Read by sandbox-manager: a node with this marker never uploads snapshots
  # to the shared bucket.
  printf '%s\n' "$CACHE_KMS_KEY_ARN" >"$ENCRYPTED_CACHE_MARKER"
  chmod 0444 "$ENCRYPTED_CACHE_MARKER"
  systemctl enable --now space-cache-key-watchdog.timer
  echo "Mounted /space/cache on LUKS2 over NVMe instance store under ${CACHE_KMS_KEY_ARN}"
else
  rm -f "$ENCRYPTED_CACHE_MARKER"
  echo "Mounted /space/cache on NVMe instance store (btrfs RAID0)"
fi
df -Th /space/cache
touch /run/space-cache-nvme.done
SCRIPT
chmod +x /usr/local/sbin/mount-space-cache-nvme.sh

# Install systemd unit
cat > /etc/systemd/system/space-cache-nvme.service << 'UNIT'
[Unit]
Description=Format and mount NVMe instance store as /space/cache (btrfs)
After=local-fs.target network-online.target
Wants=network-online.target
Before=kubelet.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/mount-space-cache-nvme.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
UNIT

# Key watchdog for tenant-encrypted caches. Started by the mount script only
# when the cache was encrypted. It proves every minute that the tenant key can
# still unwrap this boot's data key. A definitive refusal (the key was
# disabled, scheduled for deletion, or its grant to this node revoked) suspends
# the LUKS volumes, which drops their keys from the kernel and blocks all
# cache I/O, then powers the node off. Transient failures (network,
# throttling) are logged and retried.
cat > /usr/local/sbin/space-cache-key-watchdog.sh << 'WATCHDOG'
#!/usr/bin/env bash
set -uo pipefail
state=/run/space-cache-encryption
[[ -s "${state}/wrapped-key.b64" ]] || exit 0
denials_file="${state}/consecutive-denials"
threshold="${SPACE_CACHE_KEY_DENIAL_THRESHOLD:-2}"

wrapped="$(mktemp)"
trap 'rm -f "$wrapped"' EXIT
base64 -d "${state}/wrapped-key.b64" >"$wrapped"
rc=0
out="$(aws kms decrypt \
  --region "$(cat "${state}/region")" \
  --key-id "$(cat "${state}/key-id")" \
  --ciphertext-blob "fileb://${wrapped}" \
  --encryption-context "$(cat "${state}/encryption-context")" \
  --query KeyId --output text 2>&1)" || rc=$?

if [[ $rc -eq 0 ]]; then
  rm -f "$denials_file"
  exit 0
fi

case "$out" in
  *AccessDeniedException* | *DisabledException* | *KMSInvalidStateException* | \
    *NotFoundException* | *IncorrectKeyException* | *InvalidCiphertextException*)
    denials=$(( $(cat "$denials_file" 2>/dev/null || echo 0) + 1 ))
    echo "$denials" >"$denials_file"
    echo "cache key refused (${denials}/${threshold}): ${out}" >&2
    if (( denials < threshold )); then
      exit 0
    fi
    echo "cache key revoked; suspending encrypted cache and powering off" >&2
    while read -r mapped; do
      [[ -n "$mapped" ]] || continue
      cryptsetup luksSuspend "$(basename "$mapped")" || true
    done <"${state}/devices"
    systemctl poweroff --force --force
    ;;
  *)
    echo "cache key check failed transiently: ${out}" >&2
    exit 0
    ;;
esac
WATCHDOG
chmod 0755 /usr/local/sbin/space-cache-key-watchdog.sh

cat > /etc/systemd/system/space-cache-key-watchdog.service << 'UNIT'
[Unit]
Description=Power off when the tenant cache encryption key is revoked
ConditionPathExists=/run/space-cache-encryption/wrapped-key.b64

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/space-cache-key-watchdog.sh
UNIT

cat > /etc/systemd/system/space-cache-key-watchdog.timer << 'UNIT'
[Unit]
Description=Check the tenant cache encryption key every minute

[Timer]
OnActiveSec=60
OnUnitActiveSec=60
AccuracySec=5

[Install]
WantedBy=timers.target
UNIT

systemctl daemon-reload
systemctl enable space-cache-nvme.service
