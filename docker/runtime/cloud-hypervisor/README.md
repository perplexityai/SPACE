# Cloud Hypervisor build with VFIO Landlock support

Build an x86-64 static binary from the pinned upstream v54.0 source archive:

```sh
docker buildx build --platform linux/amd64 --target export \
  --output type=local,dest=/tmp/cloud-hypervisor-vfio \
  docker/runtime/cloud-hypervisor
```

The patch fixes VFIO's Landlock rules. A PCI device path names the device, not
its IOMMU group: read its `iommu_group` link to identify the group device node.
Permit read access to the selected device subtree as well. This does not grant
access to other PCI devices or make sysfs writable.

The output is a distinct build, with its own digest and source/patch record.
It must receive its own reviewed catalog identity before SPACE can execute it.
Do not replace the upstream v54.0 pin or overwrite an existing managed runtime.
The build does not publish artifacts or modify the catalog.

Landlock does not replace the host adapter's checks: the entire IOMMU/reset
unit must be exclusively leased, only its device nodes and read-only sysfs
subtree may be exposed, and the VMM must run unprivileged with its control
socket inaccessible to guest-facing services. Device release requires separate
sanitization qualification.
