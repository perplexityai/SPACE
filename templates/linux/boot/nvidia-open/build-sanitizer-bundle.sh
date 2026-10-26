#!/usr/bin/env bash
set -euo pipefail
[[ "${BOOT_INITRAMFS_SHA256:-}" =~ ^[0-9a-f]{64}$ ]]
printf '%s  /input/initrd.img\n' "$BOOT_INITRAMFS_SHA256" | sha256sum --check --strict
mkdir -p /build/initrd /output
cd /build/initrd
gzip -dc /input/initrd.img | cpio -idm --no-absolute-filenames
mkdir -p usr/bin usr/lib etc
cp -a /usr/bin/python3 /usr/bin/python3.12 usr/bin/
cp -a /usr/lib/python3.12 /usr/lib/x86_64-linux-gnu usr/lib/
cp -a /usr/lib64 usr/
ln -s usr/lib lib
ln -s usr/lib64 lib64
cp /etc/ld.so.cache etc/
printf 'root:x:0:0:root:/:/bin/sh\n' > etc/passwd
printf 'root:x:0:\n' > etc/group
install -m 755 /input/init init
install -m 644 /input/sanitizer.py sanitizer.py
find . -name __pycache__ -type d -prune -exec rm -rf {} +
find . -exec touch -h -d @0 {} +
find . -print0 | LC_ALL=C sort -z | cpio --null -o --format=newc --reproducible --owner=0:0 | gzip -n > /output/sanitizer-initrd.img
install -d /output/licenses
cp -L --parents /usr/share/doc/*/copyright /output/licenses/
cp -r /usr/share/common-licenses /output/licenses/
dpkg-query -W -f='${Package}\t${Version}\t${source:Package}\t${source:Version}\n' | LC_ALL=C sort > /output/packages.tsv
python3 - <<'PY'
from pathlib import Path
import hashlib
import json
import os

def digest(path):
    data = Path(path).read_bytes()
    return {'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)}

Path('/output/sanitizer.json').write_text(json.dumps({
    'schema_version': 1,
    'coverage_kind': 'cuda_allocations',
    'qualification': 'unqualified',
    'source_boot_initramfs_sha256': os.environ['BOOT_INITRAMFS_SHA256'],
    'program': digest('/input/sanitizer.py'),
    'init': digest('/input/init'),
    'image': digest('/output/sanitizer-initrd.img'),
}, indent=2) + '\n')
PY
