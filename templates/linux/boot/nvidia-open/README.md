# Linux boot bundle with signed NVIDIA open modules

This builds an x86-64 kernel and initramfs from matching Ubuntu kernel, NVIDIA
open-module and GSP firmware packages. The initramfs owns the kernel modules
and firmware; a guest template supplies the matching NVIDIA userspace libraries.
It does not assign a GPU, install artifacts into SPACE, or approve an environment.
This distribution-kernel bundle is qualification tooling. It uses Canonical's
module-signing trust root and does not inherit the hardened SPACE guest-kernel
configuration; a passing CUDA experiment does not qualify it for production.
The initramfs component has kind `initrd`, role `initramfs`, and format
`linux-initramfs`.

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

## Source-built kernel and modules

The separate source build compiles Linux 6.1.155 and NVIDIA open modules
580.178.04 together. It preserves the signed-module profile's enforced signatures,
BTF/tracing support, and disabled FUSE, module unload and vsock loopback. The
Firecracker kernel builder and its outputs are unchanged. A small, recorded
Kconfig patch exposes the hidden MMU notifier option required by external UVM;
it does not change its implementation or enable an unrelated in-tree driver.

```sh
bash templates/linux/boot/nvidia-open/build-hardened.sh /tmp/gpu-source-bundle
```

The build signs only the compute modules (`nvidia` and `nvidia-uvm`) with the
kernel's freshly generated key, then deletes that private key in the same build
step. The exported public certificate identifies the trust root. Updating a
module requires rebuilding the kernel/module pair; outputs from different builds
are different immutable environments, even if their version labels match.

Matching GSP firmware is built into the kernel. Firmware requests therefore keep
using authenticated boot bytes after switching to the template root filesystem.
On GPU-bearing boots the initramfs loads the signed compute modules before
entering the template and refuses to continue if loading fails. Without a GPU it
skips module loading, so template builds do not need a device lease. The template supplies the corresponding
580.178.04 userspace libraries, as required by the [NVIDIA module release](https://github.com/NVIDIA/open-gpu-kernel-modules/tree/580.178.04).
HMM remains disabled pending separate qualification.

The source revision, module-source archive and base config are pinned. Build-tool
packages come from signed distribution repositories and are recorded in the
output; the per-build key deliberately prevents byte-identical rebuilds. The
manifest records the actual kernel, initramfs, signed modules and firmware.
This build is a candidate until direct boot, signature enforcement, CUDA and
lifecycle tests pass. It does not by itself qualify device identity, sanitization,
or cross-tenant reuse, and does not add an allowed execution environment.

## Template userspace

`userspace.toml` builds matching compute userspace through build-manager. First
install and allow the compiled kernel/initramfs environment, then build a scratch
seed with that exact environment reference and no conflicting recipe kernel pin.
Use the seed's returned content digest as `--base` when building this recipe;
standalone builders cannot resolve cluster release tags. Pass the same exact
`--environment` reference for the derived build.

The recipe installs only the matching userspace packages from the distribution's
authenticated package repository. It fails if that version is unavailable,
rather than installing another driver release or DKMS modules. The build VM
receives no GPU, and the resulting template retains both the kernel and initramfs
identities. Loading `libcuda` during the build verifies library availability,
not device operation. Actual GPU execution still requires an admitted device
profile and a checked CUDA workload after assignment.
