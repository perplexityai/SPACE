#!/usr/bin/env bash
# Configure every x86 worker image for PCI GPU passthrough when hardware is present.
# Runs as root during Packer builds.
set -euo pipefail

if [[ "$(uname -m)" != "x86_64" ]]; then
  echo "ERROR: GPU passthrough is supported only by the x86_64 worker recipe" >&2
  exit 1
fi
if ! command -v grubby >/dev/null 2>&1; then
  echo "ERROR: grubby is required to configure every installed kernel" >&2
  exit 1
fi

KERNEL_ARGS=(intel_iommu=on iommu=pt)
VFIO_MODULES=(vfio vfio_pci vfio_iommu_type1)

grubby --update-kernel=ALL --args="${KERNEL_ARGS[*]}"

# The GPU stays on its normal host driver. Device binding is a runtime allocator
# decision and must use the selected PCI address, never a shared vendor/device ID.
printf '%s\n' "${VFIO_MODULES[@]}" > /etc/modules-load.d/space-vfio.conf

mapfile -t configured_kernel_args < <(
  grubby --info=ALL | sed -n 's/^args="\(.*\)"$/\1/p'
)
if [[ ${#configured_kernel_args[@]} -eq 0 ]]; then
  echo "ERROR: grubby reported no installed kernel command lines" >&2
  exit 1
fi
for args in "${configured_kernel_args[@]}"; do
  for required in "${KERNEL_ARGS[@]}"; do
    if ! grep -Fqx "$required" < <(tr ' ' '\n' <<<"$args"); then
      echo "ERROR: installed kernel is missing $required: $args" >&2
      exit 1
    fi
  done
done
for module in "${VFIO_MODULES[@]}"; do
  modinfo "$module" >/dev/null
  modprobe "$module"
done

install -d -m 0755 -o root -g root /opt/space/bin
install -m 0755 -o root -g root /tmp/space-gpu-preflight /opt/space/bin/space-gpu-preflight
