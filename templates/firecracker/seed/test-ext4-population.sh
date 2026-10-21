#!/usr/bin/env bash
# Component test: run as root on Linux with loop mounts, GNU tar, e2fsprogs,
# attr, acl and libcap tools. All mounts and images belong to this test.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# shellcheck source=templates/firecracker/seed/rootfs-builder.sh
source "$repo/templates/firecracker/seed/rootfs-builder.sh"
work="$(mktemp -d)"
cleanup() {
  local status=$?
  local target
  for target in "$work/source/nested" "$work/mount"; do
    if mountpoint -q "$target" && ! umount "$target"; then
      echo "ERROR: test mount still active at $target" >&2
      exit 1
    fi
  done
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$work/source" "$work/mount"
source_tree="$work/source"
printf 'small inline payload\n' > "$source_tree/small"
dd if=/dev/urandom of="$source_tree/metadata" bs=8192 count=1 status=none
ln "$source_tree/metadata" "$source_tree/hardlink"
chown 1234:2345 "$source_tree/metadata"
chmod 4751 "$source_tree/metadata"
setfattr -n user.synthetic -v preserved "$source_tree/metadata"
setfacl -m u:3456:r-- "$source_tree/metadata"
cp /bin/true "$source_tree/capability"
setcap cap_net_bind_service=ep "$source_tree/capability"
truncate -s 16M "$source_tree/sparse"
printf 'tail' | dd of="$source_tree/sparse" bs=1 seek=$((16 * 1024 * 1024 - 4)) conv=notrunc status=none
link_target="$(printf 'x%.0s' {1..120})"
ln -s "$link_target" "$source_tree/link"

for feature in '^inline_data' inline_data; do
  image="$work/image.ext4"
  truncate -s 128M "$image"
  mkfs.ext4 -q -F -O "dir_nlink,large_dir,$feature" -J size=4 -m 1 "$image"
  populate_ext4 "$image" "$source_tree"
  e2fsck -fn "$image"

  # Reproduce an xattr addition to a long symlink. Userspace
  # population with inline_data creates an inline symlink and fails fsck;
  # kernel population leaves it block-backed and safely resizable.
  debugfs -w -R 'ea_set /link user.synthetic 123456789012345678901234567890123' "$image"
  # Force the check timestamp behind the mount timestamp without a sleep.
  # Read-only fsck alone cannot make this image eligible for resizing.
  debugfs -w -R 'set_super_value lastcheck 1' "$image"
  check_ext4_for_resize "$image"
  resize2fs -M "$image"
  check_ext4_for_resize "$image"
  truncate -s 128M "$image"
  resize2fs "$image"
  e2fsck -fn "$image"

  mount -t ext4 -o loop,ro "$image" "$work/mount"
  dest="$work/mount"
  for file in small metadata hardlink capability sparse; do
    cmp "$source_tree/$file" "$dest/$file"
  done
  [[ "$(readlink "$dest/link")" == "$link_target" ]]
  [[ "$(stat -c '%u:%g:%a' "$source_tree/metadata")" == "$(stat -c '%u:%g:%a' "$dest/metadata")" ]]
  [[ "$(stat -c '%i' "$dest/metadata")" == "$(stat -c '%i' "$dest/hardlink")" ]]
  [[ "$(getfattr --only-values -n user.synthetic "$dest/metadata")" == preserved ]]
  [[ "$(getfacl -cpn "$source_tree/metadata")" == "$(getfacl -cpn "$dest/metadata")" ]]
  [[ "$(getcap "$dest/capability")" == "$dest/capability cap_net_bind_service=ep" ]]
  [[ "$(stat -c '%b' "$dest/sparse")" -lt 1024 ]]
  umount "$dest"
  rm "$image"
  echo "PASS: $feature population, metadata and fsck/shrink/grow"
done

mkdir "$work/staging"
truncate -s 16M "$work/image.ext4"
mkfs.ext4 -q -F "$work/image.ext4"

# Reject both mount types: tar skips different filesystems but traverses
# same-filesystem bind mounts, potentially importing host files.
mkdir "$source_tree/nested" "$work/other"
for kind in tmpfs bind; do
  if [[ "$kind" == tmpfs ]]; then
    mount -t tmpfs tmpfs "$source_tree/nested"
  else
    mount --bind "$work/other" "$source_tree/nested"
  fi
  if TMPDIR="$work/staging" bash -c 'source "$1"; populate_ext4 "$2" "$3"' bash \
    "$repo/templates/firecracker/seed/rootfs-builder.sh" "$work/image.ext4" "$source_tree"; then
    echo "ERROR: source with $kind mount was accepted" >&2
    exit 1
  fi
  umount "$source_tree/nested"
  [[ -z "$(ls -A "$work/staging")" ]]
done
echo 'PASS: nested filesystems and bind mounts are rejected'

# A failed archive must fail the build and release its mount, not allow fsck
# or resize against a still-mounted filesystem. Force a real ENOSPC failure.
mkdir "$work/too-large"
dd if=/dev/urandom of="$work/too-large/payload" bs=1M count=32 status=none
if TMPDIR="$work/staging" bash -c 'source "$1"; populate_ext4 "$2" "$3"' bash \
  "$repo/templates/firecracker/seed/rootfs-builder.sh" "$work/image.ext4" "$work/too-large"; then
  echo 'ERROR: failed archive was accepted' >&2
  exit 1
fi
[[ -z "$(ls -A "$work/staging")" ]]
e2fsck -fn "$work/image.ext4"
echo 'PASS: failed archive releases its mount and staging directory'

# A container may have loop-control but no node for the free loop index.
# Replace /dev only in a private mount namespace; no host nodes are removed.
truncate -s 128M "$work/image.ext4"
mkfs.ext4 -q -F "$work/image.ext4"
# shellcheck disable=SC2016 # The child shell expands its own positional arguments.
unshare --mount --propagation private bash -c '
  set -euo pipefail
  source "$1"
  mount -t tmpfs tmpfs /dev
  mknod /dev/loop-control c 10 237
  mknod /dev/null c 1 3
  populate_ext4 "$2" "$3"
  e2fsck -fn "$2"
' bash "$repo/templates/firecracker/seed/rootfs-builder.sh" "$work/image.ext4" "$source_tree"
echo 'PASS: missing container loop-device nodes are created'

# Deliver TERM immediately after the mount syscall succeeds, before the
# caller can record any state. Cleanup must inspect the actual mount.
mkdir "$work/bin"
cat > "$work/bin/mount" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
/usr/bin/mount "$@"
kill -TERM "$PPID"
SH
chmod +x "$work/bin/mount"
status=0
PATH="$work/bin:$PATH" TMPDIR="$work/staging" bash -c 'source "$1"; populate_ext4 "$2" "$3"' bash \
  "$repo/templates/firecracker/seed/rootfs-builder.sh" "$work/image.ext4" "$source_tree" || status=$?
[[ "$status" == 143 ]]
[[ -z "$(ls -A "$work/staging")" ]]
echo 'PASS: TERM immediately after mounting releases the mount'

# Never certify or repair a filesystem whose read-only check fails.
debugfs -w -R 'set_super_value lastcheck 1' "$work/image.ext4"
debugfs -w -R 'set_inode_field /small links_count 9' "$work/image.ext4"
before="$(sha256sum "$work/image.ext4")"
if bash -c 'source "$1"; check_ext4_for_resize "$2"' bash \
  "$repo/templates/firecracker/seed/rootfs-builder.sh" "$work/image.ext4"; then
  echo 'ERROR: corrupt filesystem was certified for resize' >&2
  exit 1
fi
[[ "$(sha256sum "$work/image.ext4")" == "$before" ]]
echo 'PASS: corrupt filesystem is rejected without changing bytes'
