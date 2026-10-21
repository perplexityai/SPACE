#!/usr/bin/env bash
# Component test: run as root on Linux with loop mounts, GNU tar, e2fsprogs,
# attr, acl and libcap tools. All mounts and images belong to this test.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=templates/firecracker/seed/rootfs-builder.sh
source "$repo/templates/firecracker/seed/rootfs-builder.sh"
work="$(mktemp -d)"
mounted=0
cleanup() {
  local status=$?
  if [[ "$mounted" == 1 ]] && ! umount "$work/mount"; then
    echo "ERROR: test mount still active at $work/mount" >&2
    exit 1
  fi
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

  # Reproduce e2fsprogs #295's xattr addition to a long symlink. Userspace
  # population with inline_data creates an inline symlink and fails fsck;
  # kernel population leaves it block-backed and safely resizable.
  debugfs -w -R 'ea_set /link user.synthetic 123456789012345678901234567890123' "$image"
  e2fsck -fn "$image"
  resize2fs -M "$image"
  e2fsck -fn "$image"
  truncate -s 128M "$image"
  resize2fs "$image"
  e2fsck -fn "$image"

  mount -t ext4 -o loop,ro "$image" "$work/mount"
  mounted=1
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
  mounted=0
  rm "$image"
  echo "PASS: $feature population, metadata and fsck/shrink/grow"
done

# A failed archive must fail the build and release its mount, not allow fsck
# or resize against a still-mounted filesystem.
mkdir "$work/staging"
truncate -s 128M "$work/image.ext4"
mkfs.ext4 -q -F "$work/image.ext4"
if TMPDIR="$work/staging" bash -c 'source "$1"; populate_ext4 "$2" "$3"' bash \
  "$repo/templates/firecracker/seed/rootfs-builder.sh" "$work/image.ext4" "$work/missing-source"; then
  echo 'ERROR: failed archive was accepted' >&2
  exit 1
fi
[[ -z "$(ls -A "$work/staging")" ]]
e2fsck -fn "$work/image.ext4"
echo 'PASS: failed archive releases its mount and staging directory'
