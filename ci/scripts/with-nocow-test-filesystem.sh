#!/usr/bin/env bash
# A disposable loop-backed btrfs filesystem for NoCoW tests. Never format or
# mount a caller-supplied device; the loop belongs to this newly created image.
set -euo pipefail

if [[ $(uname -s) != Linux || $(id -u) != 0 || $# == 0 ]]; then
    echo "Usage (Linux root): $0 command [args...]" >&2
    exit 2
fi
for tool in mkfs.btrfs losetup mount umount mountpoint truncate; do
    command -v "$tool" >/dev/null
done

fixture=$(mktemp -d "${TMPDIR:-/tmp}/space-nocow-tests.XXXXXX")
loop_device=
cleanup() {
    local status=$?
    trap - EXIT
    if mountpoint -q "$fixture/mount"; then
        if ! umount "$fixture/mount"; then
            echo "Failed to unmount owned fixture $fixture; leaving image attached for diagnosis" >&2
            exit 1
        fi
    fi
    if [[ -n "$loop_device" ]]; then
        if ! losetup --detach "$loop_device"; then
            echo "Failed to detach owned loop $loop_device; retaining $fixture" >&2
            exit 1
        fi
    fi
    rm -f -- "$fixture/filesystem.img"
    if [[ -d "$fixture/mount" ]]; then
        rmdir "$fixture/mount"
    fi
    rmdir "$fixture"
    exit "$status"
}
trap cleanup EXIT
mkdir "$fixture/mount"
truncate -s 512M "$fixture/filesystem.img"
loop_device=$(losetup --find --show "$fixture/filesystem.img")
mkfs.btrfs -q "$loop_device"
mount -t btrfs -o noatime "$loop_device" "$fixture/mount"
export SPACE_NOCOW_TEST_BTRFS_DIR="$fixture/mount"
"$@"
