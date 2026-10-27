#!/usr/bin/env bash
set -euo pipefail
: "${KERNEL_VERSION:?}"
: "${NVIDIA_VERSION:?}"
readonly jobs="${BUILD_JOBS:-16}"
[[ "$jobs" =~ ^[1-9][0-9]*$ && "$jobs" -le 128 ]]
mkdir -p /output
: > /output/.write-check
rm /output/.write-check
cd /build/linux
# Neither failed builds nor successful output may retain the private key.
trap 'rm -f /build/linux/certs/signing_key.pem /signing/kernel.pem' EXIT
[[ "$(stat -f -c %T /signing)" == tmpfs ]]
[[ "$(make -s kernelversion)" == "$KERNEL_VERSION" ]]
mkdir -p "/build/firmware/nvidia/$NVIDIA_VERSION"
cp -L "/lib/firmware/nvidia/$NVIDIA_VERSION/gsp_tu10x.bin" \
  "/build/firmware/nvidia/$NVIDIA_VERSION/gsp_tu10x.bin"
# External UVM needs the hidden MMU notifier helper. Expose that one option
# instead of enabling an unrelated in-tree device or nested-VM driver.
if ! patch --dry-run --reverse -p1 < /build/gpu-module-kconfig.patch >/dev/null 2>&1; then
  patch --forward -p1 < /build/gpu-module-kconfig.patch
fi
scripts/kconfig/merge_config.sh -m /build/base.config /build/gpu.config
make olddefconfig
python3 - <<'PY'
from pathlib import Path
resolved = set(Path('.config').read_text().splitlines())
for line in Path('/build/gpu.config').read_text().splitlines():
    if line.startswith('# CONFIG_') and line.endswith(' is not set'):
        # Kconfig omits disabled symbols whose parent dependency is disabled.
        symbol = line.split()[1]
        if any(entry.startswith(symbol + '=') for entry in resolved):
            raise SystemExit(f'Required GPU kernel option is enabled: {symbol}')
    elif line and line not in resolved:
        raise SystemExit(f'Required GPU kernel configuration did not resolve: {line}')
if any(line.endswith('=m') for line in resolved):
    raise SystemExit('Unexpected in-tree loadable module')
PY
export SOURCE_DATE_EPOCH="$(cat /build/source-date-epoch)"
export KBUILD_BUILD_TIMESTAMP="@$SOURCE_DATE_EPOCH"
# Keep the key off image layers, even if the builder is killed before its trap.
openssl req -new -nodes -utf8 -sha512 -days 36500 -batch -x509 \
  -newkey rsa:4096 -subj '/CN=SPACE GPU kernel modules/' \
  -addext 'keyUsage=critical,digitalSignature' \
  -addext 'basicConstraints=critical,CA:FALSE' \
  -keyout /signing/kernel.pem -out /signing/kernel.pem
ln -s /signing/kernel.pem certs/signing_key.pem
make -j"$jobs" bzImage modules
for section in .BTF .BTF_ids; do
  size="$(objdump --section-headers vmlinux | awk -v section="$section" '$2 == section { print $3; exit }')"
  [[ "$size" =~ ^[0-9a-fA-F]*[1-9a-fA-F][0-9a-fA-F]*$ ]] || {
    echo "ERROR: kernel is missing $section" >&2
    exit 1
  }
done
release="$(make -s kernelrelease)"
[[ "$release" == "$KERNEL_VERSION-space-gpu" ]]
# Relink before signing on a retry; never append a signature to an already
# signed module from a failed packaging attempt with a discarded key.
rm -f /build/nvidia/kernel-open/nvidia.ko /build/nvidia/kernel-open/nvidia-uvm.ko
make -C /build/nvidia -j"$jobs" modules \
  SYSSRC=/build/linux SYSOUT=/build/linux \
  NV_KERNEL_MODULES='nvidia nvidia-uvm'
mkdir -p /output /build/initrd/{bin,dev,proc,sys,newroot,modules}
for module in nvidia nvidia-uvm; do
  source="/build/nvidia/kernel-open/$module.ko"
  [[ "$(modinfo -F version "$source")" == "$NVIDIA_VERSION" ]]
  scripts/sign-file sha512 certs/signing_key.pem certs/signing_key.x509 "$source"
  [[ -n "$(modinfo -F signer "$source")" ]]
  [[ "$(modinfo -F sig_hashalgo "$source")" == sha512 ]]
  install -m 644 "$source" "/build/initrd/modules/$module.ko"
done
# The firmware is built into the authenticated kernel. After switch_root,
# requests still resolve to those bytes rather than tenant-owned /lib/firmware.
install -m 755 /bin/busybox /build/initrd/bin/busybox
install -m 755 /build/initramfs-init /build/initrd/init
find /build/initrd -exec touch -h -d "@$SOURCE_DATE_EPOCH" {} +
(
  cd /build/initrd
  find . -print0 | LC_ALL=C sort -z | cpio --null -o --format=newc --reproducible --owner=0:0 | gzip -n > /output/initrd.img
)
install -m 644 arch/x86/boot/bzImage /output/vmlinux.bin
install -m 644 .config /output/config
install -m 644 certs/signing_key.x509 /output/module-signing-certificate.der
openssl x509 -inform DER -in certs/signing_key.x509 -noout -fingerprint -sha256 > /output/module-signing-certificate.sha256
rm -f certs/signing_key.pem /signing/kernel.pem
install -d /output/licenses
cp -L --parents /usr/share/doc/*/copyright /output/licenses/
cp -r /usr/share/common-licenses /output/licenses/
cp /build/nvidia/COPYING /output/licenses/nvidia-open-COPYING
cp /build/linux/COPYING /output/licenses/linux-COPYING
cp -r /build/linux/LICENSES /output/licenses/linux-LICENSES
dpkg-query -W -f='${Package}\t${Version}\t${source:Package}\t${source:Version}\n' | LC_ALL=C sort > /output/packages.tsv
export KERNEL_RELEASE="$release"
python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path
output = Path('/output')
def identity(path):
    data = path.read_bytes()
    return {'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)}
artifacts = [
    dict(file=name, kind=kind, role=role, format=fmt, **identity(output / name))
    for name, kind, role, fmt in [
        ('vmlinux.bin', 'kernel', 'boot', 'linux-bzimage'),
        ('initrd.img', 'initrd', 'initramfs', 'linux-initramfs'),
    ]
]
manifest = {
    'schema_version': 1,
    'architecture': 'x86_64',
    'kernel_source_commit': '069c93d2b7bddda87c472453c38c450faf86f999',
    'kernel_release': os.environ['KERNEL_RELEASE'],
    'kernel_patch': identity(Path('/build/gpu-module-kconfig.patch')),
    'nvidia_version': os.environ['NVIDIA_VERSION'],
    'module_signing': 'ephemeral-build-key',
    'firmware_delivery': 'built-into-kernel',
    'firmware': identity(Path('/build/firmware/nvidia') / os.environ['NVIDIA_VERSION'] / 'gsp_tu10x.bin'),
    'modules': {p.name: identity(p) for p in sorted(Path('/build/initrd/modules').glob('*.ko'))},
    'artifacts': artifacts,
}
(output / 'bundle.json').write_text(json.dumps(manifest, indent=2) + '\n')
PY
