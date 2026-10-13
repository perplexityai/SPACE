# Runtime manager release and kernel catalog

The compiled catalog pins installable Firecracker releases and guest kernels for
both `x86_64` and `aarch64`. Installing an artifact does not select it as a node's
default. Execution continues to use the image-provided Firecracker 1.15.1 /
Linux 6.1.155 pair.

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
`crates/libs/node/runtime/core/src/catalog.rs`.

The September snapshot's `vmlinux-5.10.260` is deliberately excluded: both
architectures report `Linux version 5.10.255-260`, which contradicts the listed
version and would be rejected by the installer. Debug and no-ACPI variants are
not included in these additions.

Multipart S3 ETags are not content MD5 hashes. An absent MD5 is supported, while
catalog SHA-256 and size checks remain mandatory in runtime-manager and
node-manager. Existing files must match the SHA-256 pin before a new install
reports them as already provided. An MD5, when published, is checked as an
additional transfer checksum.

## Install/delete prototype and rollout

Execution keeps reading the existing image-provided Firecracker and kernel
paths, including their default/legacy aliases and cache copies. The managed
store at `/space/vendor/artifacts/{firecracker,kernels}` is separate: launch/build/
restore discovery and control-plane execution inventory do not read it. There
is no activation API, selection database, bootstrap reconciler or new runtime
backend. The store deliberately does not reuse `/space/vendor/node`, which
earlier prototype images linked into execution roots. Installing candidates does not establish that a workload boots or
restores successfully with them.

Node-manager creates missing managed directories without moving, re-owning or
linking any execution artifact. It is the only writer of the managed store.
Runtime-manager downloads into `/space/cache/.rtmgr-staging`; node-manager
copies verified bytes into fresh root-owned files under an interprocess lock.
Uninstall addresses an immutable identity within that store only. Any managed
copy whose required files can be measured and match the requested digest can be
removed, including copies of Firecracker 1.15.1 / Linux 6.1.155 and the last
candidate. Execution files and aliases are outside that store and cannot be
removed through this API.

A missing required file or unreadable identity needs operator repair; reinstall
never overwrites a conflicting directory. First disable management and stop its
worker, then let any in-flight node-manager artifact RPC finish. With exclusive
access to the affected store root's directory `flock` (the same lock node-manager
uses), remove only the damaged version directory beneath that managed root.
Never remove the root itself, replace its inode, or touch execution files or
aliases. Release the lock, restart management, and resubmit the install. This
manual recovery requires host-administrator access; the ordinary API deliberately
cannot delete a directory without evidence of its identity.

Fresh images bake the fixed pair into the execution layout and leave the
managed store empty. Existing images need only the new account, protected
state/staging directories and matching services. No filesystem adoption or
prototype ownership import runs. If an earlier prototype granted runtime-manager
write access to execution artifacts, its startup audit disables management and
reports the unsafe paths. Execution is not gated on this audit. Track these
images for replacement before enabling downloads. Disabling management does not revoke the account's existing write authority over execution
files; it only keeps the installer disabled. Any alternative repair needs its
own ownership handoff design; recursively changing ownership of live files is
insufficient when another process may retain writable descriptors. Node-manager
also refuses mutations if execution roots resolve into the managed store, as
on an earlier prototype image. It does not rewrite those links or gate launches.

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
The Firecracker adapter has one kernel slot per version and refuses conflicting
bytes. Availability presence only reports a local slot; mutation rechecks the
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

Acceptance must cover both a fresh image and an upgrade from the currently
supported image. Before and after install, repeat install, delete, lost-reply
retry, runtime-manager restart/outage and host reboot, verify:

- New launches and builds still resolve the original pair; existing sandboxes
  and compatible snapshot restores keep working.
- Execution aliases, bytes and published execution inventory are unchanged.
- Managed artifacts survive reboot; deleting all managed candidates works.
- An unavailable/unsafe managed store refuses management without blocking
  execution. Diagnostics identify the cause and operator retry is explicit.
- Disabling runtime-manager leaves execution usable. Rollback uses the supported
  deployment path and does not roll back or reinterpret prototype database state.

This acceptance requires Linux/KVM and deployment evidence; source review and
syntax checks do not establish continuity on existing nodes.
