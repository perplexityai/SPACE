# Firecracker guest kernel

Build a pinned x86_64 Linux kernel from the checksum-pinned Firecracker CI base
config and commit-pinned Amazon Linux source. The default is Linux 6.1.155 from
Firecracker CI v1.15. No vendor fragment is required.

```bash
./templates/firecracker/kernel/build.sh
```

Docker with BuildKit is required. The default artifacts are written to
`templates/firecracker/kernel/output/vmlinux-6.1.155/`: `vmlinux.bin`, the resolved
`config`, and `vmlinux.bin.sha256`. Source and resolved kernel versions must match
the requested version; the base config digest is verified before compilation.

The build and QEMU smoke images share a digest-pinned Ubuntu base and a dated
[Ubuntu package snapshot](https://snapshot.ubuntu.com/). Update the base digest
and `UBUNTU_SNAPSHOT` in `Dockerfile` together to pick up dependency fixes, using
a snapshot no older than the base image. The base is pinned by its linux/amd64
manifest digest, which its line in `ci/mirrored-images.txt` must name too. The
checksum-pinned CA bundle bootstraps HTTPS for APT; update its URL and checksum
when needed. Validate pin updates with a kernel build and `smoke-boot.sh` before
publishing artifacts.

## Optional BPF/module-signing profile

Place a configuration fragment in this directory and pass its filename explicitly:

```bash
CONFIG_FRAGMENT=custom.config ./templates/firecracker/kernel/build.sh
```

Fragment builds use the `vmlinux-6.1.155-bpf-modsig/` output directory. This profile
requires BTF (including module BTF), tracing/probes, built-in BPF networking,
forced SHA-512 module signatures, no loadable modules, and disabled FUSE, module
unload, and vsock loopback. The build rejects fragments that do not resolve to
these requirements and verifies nonempty `.BTF` and `.BTF_ids` sections after
stripping DWARF. Its ephemeral signing key is removed after compilation and is
never exported. A requested missing fragment is an error, not a base-profile
fallback. The default base build does not claim these additional capabilities.

The fragment is mounted read-only during the Docker build. No private repository
path or environment-specific input is required by this directory.

## In-repository variants

A variant is a named, reviewed change to the base config kept in `variants/`:

```bash
KERNEL_VARIANT=nft ./templates/firecracker/kernel/build.sh
```

Each variant has `<name>.config` (the fragment, built-in symbols only),
`<name>.required` (lines the resolved config must contain verbatim, including
symbols the fragment only selects) and `<name>.disabled` (symbols that must
resolve to `is not set`). Every variant keeps `CONFIG_MODULES` disabled, and a
resolved config with any `=m` line is rejected. An optional `<name>.boot-log`
holds extended regular expressions that `smoke-boot.sh <kernel> <version>
<name>` must find in a boot with `initcall_debug`. Output goes to
`vmlinux-6.1.155-<name>/`. `KERNEL_VARIANT` and `CONFIG_FRAGMENT` are mutually
exclusive, and an unknown variant is an error.

The `nft` variant adds nf_tables with the iptables-nft compatibility layer,
`inet` tables, `ct state`, `reject`, NAT/masquerade and `fib`, plus the legacy
`raw` table and the `state` match, so that iptables 1.8 with its default
nf_tables backend, nft rulesets and Docker's iptables rules work in the guest.
ARP, netdev and bridge nf_tables families, flow offload, ipset, IPVS, conntrack
netlink and nfqueue stay disabled.

## Other pinned versions

Override all inputs together:

```bash
KERNEL_VERSION=6.1.XXX \
FIRECRACKER_CI_VERSION=v1.XX \
KERNEL_SOURCE_COMMIT=<full-amazon-linux-commit> \
BASE_CONFIG_SHA256=<firecracker-config-sha256> \
  ./templates/firecracker/kernel/build.sh
```

Only x86_64 is supported; aarch64 snapshot restore remains unvalidated. A kernel
is separate from the seed root filesystem and is identified by its binary digest.
A build does not select a fleet default or change an existing template's kernel.

## Main CI

Main CI runs `ci.sh publish`, which builds the default kernel and smoke-boots it
unless a pass of this directory's exact contents is already recorded in GHCR.
Any file in this directory, with its executable bit, is part of that record's
key, so keep everything the check depends on here. While Docker Hub and
snapshot.ubuntu.com work, a registry problem costs time but not the check:
lookups and writes have deadlines and only warn, and a build that fails or
stalls taking layers from the image mirror or the toolchain cache runs once
more without them, as does the rest of the run. `build.sh` and `smoke-boot.sh` take extra Docker build
options from `KERNEL_DOCKER_BUILD_OPTIONS`; `ci.sh` uses it for its builder, the
mirrored Ubuntu base and the toolchain cache. Run `ci.sh check` for the same
check without writing anything.

## Publication

`ci/scripts/run-guest-kernel-publish.sh` builds and validates the kernel before
publishing immutable bundles. Non-PR main builds require a comma-separated list
of destination buckets in `SPACE_GUEST_KERNEL_BUCKETS`. Pull-request and
non-main builds validate without publishing. With `KERNEL_VARIANT` set, it
builds, smoke-boots and publishes that variant as `vmlinux-<version>-<name>`.
