# Linux boot bundle with signed NVIDIA open modules

This builds an x86-64 kernel and initramfs from matching Ubuntu kernel, NVIDIA
open-module and GSP firmware packages. The initramfs owns the kernel modules
and firmware; a guest template supplies the matching NVIDIA userspace libraries.
It does not assign a GPU, install artifacts into SPACE, or approve an environment.

```sh
bash templates/linux/boot/nvidia-open/build.sh /tmp/nvidia-boot-bundle
```

The output contains the kernel, initramfs, binary/source package inventory,
package copyright notices, and a manifest with the artifact SHA-256 digests.
Record reviewed digests in the execution catalog
before admitting these bytes. The manifest is build output, not a trust root.
The base image and kernel/module/firmware versions are pinned; supporting tools
are resolved from the signed distribution repository and recorded in
`packages.tsv`. This is not a claim of byte-identical builds across repository
updates.

Boot with `module.sig_enforce=1`. The modules are precompiled and signed by the
distribution key trusted by this kernel; no generated DKMS key is accepted.
The driver and GSP firmware version is `580.178.04`. GPU userspace in the template
must match that version. Download those packages during the template build;
do not commit vendor binaries into this repository.

HMM is disabled in this bundle because its device-memory address reservation
fails in the qualified VM layout. CUDA device allocations and explicit memory
transfers remain available. This bundle does not qualify managed-memory use,
GPU snapshots, cross-tenant device reuse, or other device families. Device
assignment and release require separate qualification and durable ownership.
