#!/usr/bin/env bash
set -euo pipefail

: "${KERNEL_RELEASE:?kernel ABI is required}"
: "${NVIDIA_VERSION:?matching driver version is required}"

for module in nvidia nvidia_uvm; do
  version="$(modinfo -k "$KERNEL_RELEASE" -F version "$module")"
  signer="$(modinfo -k "$KERNEL_RELEASE" -F signer "$module")"
  if [[ "$version" != "$NVIDIA_VERSION" || "$signer" != 'Canonical Ltd. Kernel Module Signing' ]]; then
    echo "ERROR: $module does not match the signed kernel/driver contract" >&2
    exit 1
  fi
done

install -d /output /etc/modprobe.d /etc/initramfs-tools
cat > /etc/initramfs-tools/modules <<'MODULES'
virtio_pci
virtio_blk
virtio_net
vmw_vsock_virtio_transport
ext4
nvidia
nvidia_uvm
MODULES
# UVM's HMM device-memory reservation does not fit the qualified VM physical
# address layout. Ordinary CUDA allocations and explicit transfers remain usable.
printf '%s\n' 'options nvidia_uvm uvm_disable_hmm=1' > /etc/modprobe.d/nvidia-uvm.conf
printf '%s\n' 'RESUME=none' > /etc/initramfs-tools/conf.d/resume
SOURCE_DATE_EPOCH=0 mkinitramfs -o /output/initrd.img "$KERNEL_RELEASE"
lsinitramfs /output/initrd.img > /tmp/initrd-contents
for module in nvidia nvidia_uvm; do
  module_path="$(readlink -f "$(modinfo -k "$KERNEL_RELEASE" -F filename "$module")")"
  if ! grep -Fx "${module_path#/}" /tmp/initrd-contents; then
    echo "ERROR: initramfs is missing $module" >&2
    exit 1
  fi
done
if ! grep -F "nvidia/$NVIDIA_VERSION/gsp_tu10x.bin" /tmp/initrd-contents; then
  echo 'ERROR: initramfs is missing Turing GSP firmware' >&2
  exit 1
fi
install -m 644 "/boot/vmlinuz-$KERNEL_RELEASE" /output/vmlinux.bin
dpkg-query -W -f='${Package}\t${Version}\t${source:Package}\t${source:Version}\n' \
  | LC_ALL=C sort > /output/packages.tsv
install -d /output/licenses
cp -L --parents /usr/share/doc/*/copyright /output/licenses/
cp -r /usr/share/common-licenses /output/licenses/

python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path

output = Path('/output')
artifacts = []
for name, kind, role, image_format in [
    ('vmlinux.bin', 'kernel', 'boot', 'linux-bzimage'),
    ('initrd.img', 'initrd', 'initrd', 'linux-initramfs'),
]:
    data = (output / name).read_bytes()
    artifacts.append({
        'file': name, 'kind': kind, 'role': role, 'format': image_format,
        'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data),
    })
manifest = {
    'schema_version': 1,
    'architecture': 'x86_64',
    'kernel_release': os.environ['KERNEL_RELEASE'],
    'nvidia_version': os.environ['NVIDIA_VERSION'],
    'required_kernel_arguments': ['module.sig_enforce=1'],
    'artifacts': artifacts,
}
(output / 'bundle.json').write_text(json.dumps(manifest, indent=2) + '\n')
PY
