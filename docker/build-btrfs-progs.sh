#!/usr/bin/env bash
# Temporary packaging workaround: worker and CI distro packages must contain
# the inline-LZO receive fix (upstream e69e94b, included in 7.3). Retire this
# source build once their supported packages include it, including a verified
# distro backport; a version string alone does not establish backport status.
# Removal steps live under "Snapshot receive" in the node-manager README.
set -euo pipefail

apt-get update
apt-get install -y --no-install-recommends \
    gcc make libc6-dev pkg-config curl ca-certificates xz-utils \
    libblkid-dev uuid-dev liblzo2-dev zlib1g-dev libzstd-dev
rm -rf /var/lib/apt/lists/*

curl --fail --show-error --location --retry 5 --retry-all-errors \
      https://www.kernel.org/pub/linux/kernel/people/kdave/btrfs-progs/btrfs-progs-v7.3.tar.xz \
      -o /tmp/btrfs-progs.tar.xz \
    && echo 'd37b63403d25fe025704c758e6a3da0979ae50c30c5f02aa78e4410e7b346e3a  /tmp/btrfs-progs.tar.xz' | sha256sum -c - \
    && tar -C /tmp -xJf /tmp/btrfs-progs.tar.xz \
    && cd /tmp/btrfs-progs-v7.3 \
    && ./configure --disable-documentation --disable-python --disable-convert \
         --disable-libudev --disable-backtrace \
    && make -j"$(nproc)" btrfs.static \
    && install -D -m 0755 btrfs.static /opt/space/btrfs-progs/btrfs \
    && /opt/space/btrfs-progs/btrfs --version
