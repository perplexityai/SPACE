# Retained block-volume experiment

## Client-managed deletion

`GET /v1/volumes/{volume_id}` inspects a sandbox-created volume using the same
owner authorization as its sandbox. Responses expose logical identity, the
required mount, resource state and `delete_requested`, not cloud identifiers.

`DELETE /v1/volumes/{volume_id}` requires `Idempotency-Key` and returns a
`PublicOperationRecord` with HTTP 202. Poll the existing operation endpoint:
success requires confirmed backing deletion. The operation can time out while
durable cleanup continues; inspect the volume before retrying with a new key.
The operation's target identifies the volume; its optional `result` remains
absent to preserve the stable operation-result union. Inspect the retained
volume tombstone for resource state.
Deletion requires a stopped owner, no retained memory checkpoint, no device
owner and observed detach. A suspended sandbox is not eligible. A deleted
required volume remains a tombstone and prevents sandbox restart; it is not
silently replaced and its mount is never redirected to rootfs.

Mutation admission additionally requires `volume_management: true` in the
existing `SPACE_EBS_ADMISSION_POLICY` for the allowlisted organization. It
defaults false. Deploy compatible API and operation-dispatcher consumers
before enabling it; disable admission and finish outstanding volume operations
before rolling back a dispatcher. Older API binaries cannot decode the new
operation kind: retain compatible readers until those records
have aged out of operation retention, including terminal operations.

The opt-in typed scenario
`native.retained_volume_delete_blocks_restart` exercises inspection, active-use
rejection, stop/detach, idempotent explicit deletion and rejected subsequent
wake. It requires separately approved deployment admission and an EBS resource
budget. Merely adding or discovering the scenario is not deployed qualification;
independent provider inventory checks are still required.
This is source-level management work, not authorization to reopen an experiment
or resize provisioned disks. Resize and retained-disk cold restart are not yet
qualified.

This is a default-off experiment, not a generally available storage API. The
native sandbox-create request accepts one creation-time disk:

```json
{
  "template": "seed",
  "volume_mounts": [
    {
      "type": "ebs",
      "size_gib": 128,
      "permissions": "read_write",
      "mount": "/data"
    }
  ]
}
```

The experiment admits only explicitly configured organizations, Firecracker
guests, one 128-GiB disk per sandbox and at most two experiment disks in total.
`read_only` is also supported. Paths must be normalized absolute paths outside
system directories. The disk is encrypted gp3, formatted as ext4 once, and
owned by that sandbox. Organization BYOK, warm adoption, shared disks,
Multi-Attach and user-supplied device paths are unsupported.

Native sandbox get/list responses include `volume_mounts` for retained disks:
the logical `volume_id`, immutable create-time mount fields, and current
resource `state`. This is separate from sandbox readiness. AWS identifiers,
attachment epochs and initialization claims are never included. The field is
omitted for ordinary root-only sandboxes.

## Ownership and provisioning

The control plane transactionally reserves a durable disk record with cold
sandbox creation. AWS provisioning and attachment happen asynchronously in the
cluster controller, not in the HTTP request. The controller verifies its AWS
account, inventory, availability zone, ownership tags and attachment targets.
Creating, retained and cleanup-pending disks count toward the two-disk limit.
An ambiguous create is reconciled through durable intent and inventory, not
replaced with another unaccounted disk.

Volume-manager relays authenticated node requests to the control plane.
Attachment grants are fenced by registered node, sandbox execution generation,
activation attempt and attachment epoch. Node-manager verifies and pins the
whole NVMe namespace through its EBS serial, model and geometry. It formats only
under a durable never-exposed initialization claim and retains a local release
receipt. An ambiguous format outcome fails closed instead of formatting again.

The verified device is bound into the execution jail as
`/block-volumes/data0`. Firecracker exposes the stable `data0` virtio-blk
topology as guest `/dev/vdb`. The guest agent verifies the ext4 UUID and mounts
it at the requested path before sandbox readiness. A writable fresh filesystem
root is made writable to the sandbox user. Read-only access is enforced by
Firecracker, not merely a guest mount flag.

The host never mounts a guest-modified ext4 filesystem. A guest can corrupt its
own disk, but cannot choose host devices or grant itself another sandbox's disk.

## Suspend and restore

Pause keeps the VM and attachment resident. Suspend flushes the guest
filesystems, captures VM memory and the root filesystem, publishes a versioned
`retained_live_disk_v1` dependency, and retires the VM and device references
before acknowledging release. Only then may the controller detach the EBS disk.

An EBS disk is a live dependency, not snapshot contents. **This implementation
does not create EBS snapshots.** Restore must use the same sandbox's exact latest
checkpoint and disk generation, in the same AZ, with the identical drive
topology. The checkpoint is consumed before writable execution. Resuming after
a failed suspend withdraws its checkpoint before allowing more writes.

Attachment epochs and disk generations are distinct. Moving a disk changes
attachment ownership; executing writes makes older memory checkpoints stale.
Fork, rollback, manual/rolling snapshots, export and warm-donor cloning are
rejected for retained disks. Existing host bindings continue to reject cold
restart of an already exposed disk. A binding can explicitly opt into the
experimental cold-start authority with `retained_cold_start: true` in
`SPACE_EBS_NODE_BINDINGS`; this defaults false and is not a qualification claim.

That opt-in requires a fresh execution and attachment epoch after positively
observed detach, completed initialization, and no retained checkpoint. It
advances the disk generation once and preserves filesystem identity without
granting another format claim. A missing or incompatible RAM checkpoint never
falls back to cold boot, and a valid retained checkpoint still prevents cold
activation. Suspending and then stopping is not yet a qualified checkpoint-
discard workflow. Cold-start rootfs behavior and mounted data continuity still
require deployed qualification before enabling this option.

Deleting the sandbox leaves durable cleanup authority. AWS deletion requires
verified release and an unattached owned volume; a missing heartbeat or a
deleted sandbox row is not proof that a device is safe to detach. Forced detach
is never used.

## Default-off configuration and rollout

All configuration values are operator-supplied; this repository contains no
deployment-specific accounts, organizations, node names or AWS resource IDs.

| Variable | Consumer | Contract |
| --- | --- | --- |
| `SPACE_EBS_ADMISSION_POLICY` | API | JSON with `organization_ids`, `availability_zone`, and `qualified_guest_images`, a map from immutable template content hashes to the qualified MountDataDrive protocol version (`1`). The controller checks the resolved image before provisioning; absent, empty, unknown-version, or unqualified image entries never authorize a disk. This experiment uses explicit operator qualification, not automatic guest-capability discovery. |
| `SPACE_EBS_PLACEMENT_POLICY` | API | JSON with `availability_zone` and one or two qualified Kubernetes `node_names`; intersects every existing required affinity alternative. Optional `tolerations` admit explicitly isolated worker pools. Optional `hold_for_placement: true` adds the `space.pplx.net/block-volume-placement` scheduling gate, allowing an authorized fixture to add a narrower hostname selector before removing that gate. Both options default off/empty; neither replaces existing CPU or AZ constraints. A missing fixture leaves the pod unscheduled, not placed elsewhere. |
| `SPACE_EBS_EXPERIMENT_POLICY` | Cluster controller | JSON with `account_id`, `region`, `availability_zone`, `cluster`, `experiment` and one or two permitted `instance_ids`; absent disables cloud reconciliation |
| `SPACE_EBS_NODE_BINDINGS` | Internal API | Explicit registered-node UUID to instance/AZ bindings, validated against the authenticated request origin; keys are not Kubernetes hostnames. A current authenticated diskless execution may resolve an absent disk without an EBS host grant; any existing disk still requires that grant. |
| `SPACE_EBS_NODE_ID` | Worker disk consumers | Explicit registered node UUID or Kubernetes node name and worker opt-in; node names use the normal worker-role identity derivation and must agree with the fixed control-plane UUID bindings. Volume-manager also requires the normal authenticated ClusterApi endpoint, TLS trust and projected-token configuration. |

Deploy migrations and protocol consumers before enabling producers. First
upgrade the guest agent/template, node-manager, volume-manager, sandbox-manager
and internal API on the qualified workers; verify their actual installed
versions. Then deploy the controller and API with admission still disabled.
Qualify VM snapshot compatibility independently before selecting a worker pair.
Same AZ and matching instance type alone do not prove CPU/MSR compatibility.
Never remove CPU compatibility gates to make the experiment pass.

Additive messages and tables allow old code to continue serving ordinary
root-only sandboxes while the experiment is off. They do **not** make an old
worker safe to run a retained-disk sandbox: old readers can ignore dependency
metadata. Enabling admission therefore requires a verified consumer-first
rollout and placement restricted to those consumers.

Do not roll disk consumers back while any retained-disk sandbox, checkpoint,
attachment or cleanup receipt remains. Disable admission, gracefully delete
the experiment-owned sandboxes, and verify control-plane and AWS cleanup first.
Down migrations are not an operational rollback procedure.

## Evidence required

The typed manual scenario
`native.retained_ebs_suspend_preserves_open_fd` checks two suspend/restore
cycles, an in-memory nonce and counter, root/data hashes, an unlinked surviving
open file descriptor, a second sandbox's isolation and host-enforced read-only
access. It creates at most two disks and uses fixture-owned cleanup.

That scenario alone does not prove worker movement or AWS cleanup. Deployment
qualification must additionally record A→B→A actual worker identities, an
unchanged AWS volume ID, detach-before-attach ordering, stale-checkpoint and
unsupported-operation rejection, and deletion from both SPACE and AWS.
Compilation, mocked AWS responses and same-node restores are not substitutes
for this evidence.

The one-disk manual scenario `native.retained_volume_cold_start` writes durable
root/data markers, stops and waits for observed detach, then cold-starts twice.
It requires a different guest boot identity on each start and the same ext4
UUID, logical volume ID and marker hashes. It requires the default-off host
opt-in described above and a qualified stopped-rootfs retention path. It does
not test checkpoint discard or establish cross-host stopped-rootfs portability.
Record provider volume identity independently; tenant responses intentionally
do not expose it.

`native.retained_volume_benchmark` compares the local rootfs (`/var/tmp`) and
retained data disk (`/data`) inside one sandbox using the standard
[fio I/O tester](https://fio.readthedocs.io/en/master/fio_doc.html).
Four pairs alternate which arm runs first and retain fio's original JSON,
tool version and binary hash. Each arm uses a 48-MiB working set: direct
sequential writes, direct 4-KiB random reads, buffered 4-KiB overwrites and
appends with periodic `fdatasync` after each write and a final `fsync`, and
fio's `filecreate` engine. Jobs use one synchronous I/O at a time and refill
write buffers rather than repeatedly submitting one unchanged buffer.
Latency observations include p50, p95 and p99. Each random phase stops after
five seconds or 20,000 operations; append and creation phases have 128 samples
each. File creation does not measure content writes or durable creation.
Fio reports write and sync latencies separately; do not add their percentiles
and call that a transaction percentile.

The qualified guest must include fio 3.x; no custom benchmark fallback or
implicit package installation is performed. Files and fio auxiliary state
are private to the scenario and removed afterward, including on failure.
Missing direct-I/O support fails rather than silently measuring page-cache reads.

The baseline is named `local_rootfs`, not NVMe. Establish its backing device
independently before reporting an NVMe/EBS comparison. Record filesystem types
and options (included in the guest evidence), host storage layout and CoW policy:
this compares complete guest filesystem paths, not just storage media. No
benchmark results are implied by the existence of the scenario.

These are bounded microbenchmark observations, not steady-state, provider
saturation, attachment-density or fleet-cost evidence. Include worker type,
provider capacity, IOPS/throughput settings and software versions in deployment
evidence. Direct I/O bypasses the guest page cache, not every host/provider
cache. The creation phase measures create-to-ready, not isolated provider
attachment latency. Offline growth and post-growth benchmarking still require
their own implemented maintenance path.
