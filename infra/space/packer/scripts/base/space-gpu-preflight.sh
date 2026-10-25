#!/usr/bin/env bash
# Verify a running worker's IOMMU/VFIO state, optionally for one bound GPU.
set -euo pipefail

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [PCI_ADDRESS]" >&2
  exit 2
fi
if [[ "$(uname -m)" != "x86_64" ]]; then
  echo "ERROR: GPU passthrough preflight supports only x86_64 workers" >&2
  exit 1
fi

for required in intel_iommu=on iommu=pt; do
  if ! grep -Fqx "$required" < <(tr ' ' '\n' </proc/cmdline); then
    echo "ERROR: running kernel is missing $required" >&2
    exit 1
  fi
done
for module in vfio vfio_pci vfio_iommu_type1; do
  if [[ ! -d "/sys/module/$module" ]]; then
    echo "ERROR: VFIO module is not loaded: $module" >&2
    exit 1
  fi
done
if [[ ! -c /dev/vfio/vfio ]]; then
  echo "ERROR: VFIO control device is unavailable: /dev/vfio/vfio" >&2
  exit 1
fi

gpu_count=0
for device in /sys/bus/pci/devices/*; do
  [[ -e "$device" ]] || continue
  class="$(cat "$device/class")"
  [[ "$class" == 0x03* ]] || continue
  gpu_count=$((gpu_count + 1))
  if [[ ! -L "$device/iommu_group" ]]; then
    echo "ERROR: GPU has no IOMMU group: ${device##*/}" >&2
    exit 1
  fi
done
if [[ $gpu_count -eq 0 ]]; then
  echo "ERROR: no PCI display-class GPU is present" >&2
  exit 1
fi

if [[ -z "${1:-}" ]]; then
  echo "GPU passthrough preflight passed for $gpu_count display-class PCI device(s)"
  exit 0
fi

pci_address="${1,,}"
if [[ ! "$pci_address" =~ ^[[:xdigit:]]{4}:[[:xdigit:]]{2}:[[:xdigit:]]{2}\.[[:xdigit:]]$ ]]; then
  echo "ERROR: PCI address must have the form 0000:00:00.0" >&2
  exit 2
fi
device="/sys/bus/pci/devices/$pci_address"
if [[ ! -e "$device" || "$(cat "$device/class")" != 0x03* ]]; then
  echo "ERROR: PCI address is not a display-class GPU: $pci_address" >&2
  exit 1
fi
driver="$(basename "$(readlink -f "$device/driver" 2>/dev/null || true)")"
if [[ "$driver" != "vfio-pci" ]]; then
  echo "ERROR: selected GPU is not bound to vfio-pci: $pci_address" >&2
  exit 1
fi
group="$(basename "$(readlink -f "$device/iommu_group")")"
if [[ ! "$group" =~ ^[0-9]+$ || ! -c "/dev/vfio/$group" ]]; then
  echo "ERROR: VFIO group device is unavailable for $pci_address" >&2
  exit 1
fi
for group_device in "$device/iommu_group/devices"/*; do
  if [[ -L "$group_device/driver" ]]; then
    group_driver="$(basename "$(readlink -f "$group_device/driver")")"
  else
    # Bridges and other non-assignable members can be valid without a driver.
    continue
  fi
  if [[ "$group_driver" != "vfio-pci" ]]; then
    echo "ERROR: IOMMU group $group contains a device still using $group_driver: ${group_device##*/}" >&2
    exit 1
  fi
done

echo "GPU passthrough preflight passed: device=$pci_address group=$group"
