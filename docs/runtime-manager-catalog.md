# Runtime manager release and kernel catalog

Diagnostic discovery recognizes Cloud Hypervisor release directories containing
`bin/cloud-hypervisor`. A valid executable is reported without a memory-state
format: recognizing the installation does not enable execution, snapshots or
warm pools. The install catalog offers pinned ARM64 and x86-64 releases as described below.

The compiled catalog pins installable Firecracker releases and guest kernels for
both `x86_64` and `aarch64`. Installing an artifact does not select it as a node's
default. Node-manager selects compiled E0 (Firecracker 1.15.1 / Linux 6.1.155)
by default for new work and stock legacy fallback. Verified managed copies can
supply this binding; other compiled environments require explicit execution
policy permission.

| Firecracker | Snapshot format | Kernel options | Upstream kernel directory |
| --- | --- | --- | --- |
| 1.15.1 | 9.0.0 | 6.1.155 | `v1.15` |
| 1.16.0, 1.16.1 | 10.0.0 | 5.10.253, 6.1.172 | `20260603-e656faf58d6c-0` |
| 1.16.2 | 11.0.0 | 5.10.253, 6.1.172 | `20260603-e656faf58d6c-0` |
| 1.17.0 | 12.0.0 | 6.1.186, 6.18.44 | `20260909-a8e1c3830545-0` |

Kernel directories are relative to `s3://spec.ccfc.min/firecracker-ci`, with
objects named `<directory>/<architecture>/vmlinux-<version>`. The dated
snapshots were available immediately before the corresponding release line
shipped. Every patch release within a line uses the same options. These are
reviewed install candidates; byte verification does not establish that a
particular SPACE workload boots successfully on them.

The public bucket no longer publishes `v1.16` or `v1.17` directories. The S3
source maps those lines to the fixed snapshots above; it never selects a latest
directory. Custom S3 roots retain the `v<line>/<architecture>/vmlinux-<version>`
layout. Additional catalog JSON pins alone do not define new source paths.

## Provenance and verification

Firecracker archives come from the official
[release assets](https://github.com/firecracker-microvm/firecracker/releases).
For 1.17.0, both archives were checked against their published `.sha256.txt`
files, then the `firecracker` and `jailer` members were hashed independently and
their ELF architectures inspected. The snapshot format comes from the tagged
[`SNAPSHOT_VERSION`](https://github.com/firecracker-microvm/firecracker/blob/v1.17.0/src/vmm/src/persist.rs)
constant, not from a guess based on the release number. The existing 1.16.x
archive and member pins were also rechecked. Tagged source identifies 1.16.2's
format as 11.0.0: it cannot restore 1.16.0/1.16.1 format-10 snapshots. This corrects
an earlier catalog entry that incorrectly grouped all three patch releases.

Kernel families follow upstream's tagged test filters for
[1.16](https://github.com/firecracker-microvm/firecracker/blob/v1.16.0/tests/framework/artifacts.py)
and [1.17](https://github.com/firecracker-microvm/firecracker/blob/v1.17.0/tests/framework/artifacts.py).
Each newly pinned object was downloaded, checked against its listed byte count,
inspected for the expected boot-image architecture and Linux release banner,
and hashed with SHA-256. The bucket does not publish independent SHA-256
checksums; the compiled pins identify those reviewed bytes. Pin values live in
`crates/libs/execution/catalog/src/catalog.rs`.

The September snapshot's `vmlinux-5.10.260` is deliberately excluded: both
architectures report `Linux version 5.10.255-260`, which contradicts the listed
version and would be rejected by the installer. Debug and no-ACPI variants are
not included in these additions.

Multipart S3 ETags are not content MD5 hashes. An absent MD5 is supported, while
catalog SHA-256 and size checks remain mandatory in runtime-manager and
node-manager. Existing files must match the SHA-256 pin before a new install
reports them as already provided. An MD5, when published, is checked as an
additional transfer checksum.

## Managed E0 execution and rollout

The managed store at `/space/vendor/artifacts/{firecracker,kernels}` is separate
from external vendor layouts. Releases occupy `v<version>--<first-12-release-digest-hex>`
and kernels `<version>--<first-12-kernel-digest-hex>`. Release identity hashes the pinned
binary names and hashes in layout order; it is not the download-archive hash.
Different builds sharing a label occupy different directories. The approved
catalog still admits one executable runtime identity per version/platform.

Node-manager owns the compiled E0 binding without a selection database or
activation RPC. On x86_64 its runtime release digest is
`sha256:7225701e6d651bb351c6858e3587994fb75a9932fb16d744dfec30fb8bfdd027`;
its kernel digest is
`sha256:e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2`.
Runtime-manager stages downloads unprivileged under `/space/cache/.rtmgr-staging`.
Node-manager independently checks catalog pins and copies bytes into fresh
root-owned files under an interprocess store lock. Installing candidates does
not qualify or select them.

Execution discovery verifies E0 binaries, preferring a managed copy over an
identical baked copy. It stages managed E0's kernel as a real, independently
verified file under `/space/cache/firecracker/managed/kernels`, on the jail
filesystem. It does not rewrite image aliases or adopt external files. Supported
explicit historical runtime/component bindings remain resolvable from external
layouts. Absent kernel metadata binds by E0 digest only for stock-image legacy
bindings or empty managed-only layouts. A custom or unresolved legacy kernel
blocks unannotated existing boots/restores until their identity is made explicit;
it does not silently become E0. An external runtime default that differs from E0
produces a diagnostic; managed new boots still select E0. An empty inventory can
be reported, but cannot resolve a launch. New cold boots never fall back to the
newest installed candidate.

E0 publication builds and stages a replacement snapshot before swapping it in;
readers retain the previous valid snapshot during the copy. Installing/removing
unselected candidates does not rebuild executable inventory. Verification or
staging failures withdraw only affected E0 artifacts; explicit unrelated runtime
and kernel bindings remain usable. Requested failed bindings retain the original
error classification and diagnostic. Permanent failures retry when observed files
or permissions change; transient I/O retries on an inventory request after a
ten-second interval. Service restart discovers the durable store again. Cached
inventory checks metadata before reuse; cache loss forces reconstruction without
downloading. Existing valid cache files are verified and reused, never overwritten
or pruned under running jails.

Shared execution-cache parents must be root-owned and sticky when group-writable.
Both the image cache bootstrap and EKS NVMe fallback set this on `/space/cache`
and `/space/cache/firecracker`; durable artifact-store parents remain strictly
non-writable by other accounts. Existing EKS nodes need the corrected bootstrap
permissions before provisioning managed E0. Changing the template does not repair
a running node.

Node-manager refuses removal of E0 identities even with empty inventory or after
restart. Other managed candidates remain removable because they cannot execute
from that store. External execution files cannot be removed through the API.
Removing E0 in place is not a supported upgrade mechanism.

Deploy on baked images first. Upgrade node-manager and runtime-manager together,
with artifact mutations quiesced during the layout transition: an old publisher
or remover does not understand the new layout/protection contract. Old prototype
version-only managed copies are not adopted into execution. Reinstall through the
new services to create digest-addressed copies; retain the old copies until a
separate operator cleanup. Do not treat an old completed operation's path as
proof that the digest-addressed copy exists. No bootstrap reconciler, placement
enforcement, builder selection API or image-payload removal is introduced here.
Those are subsequent rollout stages.

A damaged selected identity fails closed rather than being overwritten. Repair
requires host-administrator ownership of the affected node, quiesced artifact
mutations and execution users, and the store/cache lock appropriate to the
operation. Do not remove a root, replace its inode, modify a kernel linked into a
jail, or recursively re-own live files. Runtime-manager never acquires write
access to execution files. Unsafe/overlapping roots require provisioning repair;
node-manager does not rewrite links to make them pass. Prototype nodes with
overlapping roots retain their existing external execution path and alias
selection, while all managed-store mutations stay refused. This bounded
compatibility path does not activate managed E0; migrate the layout before
provisioning. Unreadable roots are not treated as evidence of a prototype layout.

Deploy node-manager support before enabling mutations. During a partial rollout,
old node-manager RPCs or catalog entries may reject requests. Runtime-manager's
worker retries transient transport/busy/old-service failures up to three
attempts with shared-worker backoff, with a 30-minute bound per attempt and a
60-second download idle limit. Permanent policy or identity failures end the
operation immediately. `space-rtmgr operation <id>` reports the result; after
repair or rollout, an operator resubmits a failed operation. No deployment gate
waits on runtime-manager for execution readiness.

The only runtime-manager table beyond shared jobs is `operation_results`, which
records mutation intent before the RPC and the outcome afterward. A restart
requeues interrupted jobs; a lost reply is resolved by repeating the same
idempotent mutation. Artifact presence is observed from the store, not an
ownership ledger. Terminal failures can report an uncertain mutation outcome;
a later request is safe to repeat. `AlreadyInstalled` means the verified bytes
were present at publication, including after a lost reply; it does not attribute
the original installation. `Uninstalled` means the managed copy is absent,
including when it was already absent. Staging cleanup failure is reported
separately and does not replace a completed mutation or permanent refusal.

The lifecycle API is `ListAvailableArtifacts`, `InstallArtifact`, and
`UninstallArtifact`. Identities distinguish runtime releases from components;
both carry a version and digest. Runtime compatibility is separate from identity.
Managed storage includes the digest in each slot name; catalog approval remains
separate from storage identity. Availability presence only reports a local slot; mutation rechecks the
bytes against the identity at node-manager. CLI `runtimes`, `kernels`, and
`components` are conveniences over the same API. The kernel shorthand refuses
ambiguous versions; explicit component operations take a digest.

```sh
space-rtmgr runtimes available
space-rtmgr runtimes install fc-1.16.2
space-rtmgr kernels available --runtime fc-1.16.2
space-rtmgr kernels install 6.1.172 --runtime fc-1.16.2
space-rtmgr runtimes install fc-1.17.0
space-rtmgr kernels install 6.18.44 --runtime fc-1.17.0
space-rtmgr kernels uninstall 6.18.44
space-rtmgr runtimes uninstall fc-1.17.0
```

Acceptance must cover both a fresh baked image and an upgrade from the currently
supported image. Before and after install, repeat install, candidate removal,
lost-reply retry, runtime-manager restart/outage and host reboot, verify:

- New launches and builds still resolve the original pair; existing sandboxes
  and compatible snapshot restores keep working.
- External execution aliases and bytes are unchanged. Published execution paths
  may move to verified managed copies of the same E0 identity.
- Managed artifacts survive reboot; unselected candidates can be removed, while
  E0 removal is rejected even after restart or cache loss.
- A bad unselected candidate does not disable E0. A damaged selected artifact
  fails execution closed; diagnostics identify the cause and repair is explicit.
- Cache loss reconstructs managed E0 without downloads, preserves existing jail
  inodes, and permits subsequent execution.
- Disabling runtime-manager leaves execution usable. Rollback uses the supported
  deployment path and does not roll back or reinterpret prototype database state.

This acceptance requires Linux/KVM and deployment evidence; source review and
syntax checks do not establish continuity on existing nodes.

## Cloud Hypervisor installation

On Linux/x86_64 and Linux/arm64, the compiled catalog includes the upstream Cloud
Hypervisor v54.0 static executables. Each download and installed-file SHA-256 pin
names the same bytes; the catalog also carries its fs-verity measurement (SHA-256,
4096-byte blocks, no salt). The upstream asset digest verifies transfer
consistency, not independent build provenance. Source:
[Cloud Hypervisor v54.0](https://github.com/cloud-hypervisor/cloud-hypervisor/releases/tag/v54.0).

Upgrade node-manager first, then runtime-manager. Node-manager provisions the
new managed CH directory; until it exists, `space-rtmgr status` reports that
root as absent and exits non-zero. After both services are upgraded, local
operators can use:

```sh
space-rtmgr runtimes available --runtime cloud-hypervisor
space-rtmgr runtimes install cloud-hypervisor-54.0
space-rtmgr list --kind runtime --wide
```

The managed release is published under `/space/vendor/artifacts/cloud-hypervisor`,
independently of Firecracker's store. Runtime-manager's
`--cloud-hypervisor-release-source` can point at a mirror with the same release
asset layout; it cannot change the compiled pins. An unsupported host architecture
is refused with `FailedPrecondition`. Omitting `--runtime` from `runtimes available`
lists Firecracker releases; pass `--runtime cloud-hypervisor` to list CH.

Release sources must use credential-free HTTPS URLs. Userinfo, query parameters
and fragments are refused; authentication must not be embedded in these URLs.

Installation does not add a runnable environment, change defaults, or qualify
any guest kernel, snapshot format or GPU. Diagnostics report the installed CH
binary without a fake jailer or memory-state format. Template and sandbox
execution require the CH adapter and a qualified environment separately.

Finish upgrading both artifact services before submitting CH installs. An older
node-manager lacks the pin and refuses publication. Such a failed operation
must be resubmitted after the upgrade; existing Firecracker work is unaffected.

The ARM64 boot environment also pins upstream `Image-arm64` from
[the Cloud Hypervisor Linux release](https://github.com/cloud-hypervisor/linux/releases/tag/ch-release-v7.2.8-20261009).
The embedded kernel banner is `7.2.8+`; its file length is 24,177,152 bytes.
The content SHA-256 is
`8f8f3d5a983809f87a4e1e9a13053a3361fda47ef04e09c771a41c5e037cb6c2`;
the SHA-256/4096-byte/no-salt fs-verity measurement is
`26a86972db2461c90fea9945b577e24734f4ec7fcb9e4060c2dba401e5832176`.
The latter was checked with the Linux enable/measure ioctls on a disposable
btrfs copy. The HTTPS source uses the same bounded transport as release downloads;
node-manager independently authenticates the staged bytes against its catalog.

```sh
space-rtmgr kernels available --runtime cloud-hypervisor-54.0
space-rtmgr kernels install '7.2.8+' --runtime cloud-hypervisor-54.0
```

Artifacts referenced by compiled environments are retained by the removal guard,
including CH. Installation remains separate from execution permission. See
[the local builder and worker exercise](execution-environments.md#local-x86-64-builder-and-worker-exercise)
for explicit policy configuration, template builds and sandbox creation.

### Compact locator rollout

Upgrade runtime-supervisor before node-manager or runtime-manager creates compact
execution paths. The updated supervisor accepts both the historical 64-hex and
compact 12-hex locator for the exact compiled digest. The old supervisor does not
accept compact paths. Keep the updated supervisor in rollback images once compact
paths have been published. Inventory and removal recognize both store layouts;
when both exist, the compact copy is preferred and both are authenticated before
removal. A short-name collision fails full-content verification rather than
replacing another artifact. Historical paths remain read-compatible.
