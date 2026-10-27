#!/usr/bin/env bash
# Shared static btrfs build for worker and Rust CI images.
set -euo pipefail

curl --fail --show-error --location --retry 5 \
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
