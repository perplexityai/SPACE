# Retained block-volume experiment

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
rejected for retained disks. Stop followed by a fresh cold start of an already
exposed disk is not supported in this experiment; use suspend/restore.

Deleting the sandbox leaves durable cleanup authority. AWS deletion requires
verified release and an unattached owned volume; a missing heartbeat or a
deleted sandbox row is not proof that a device is safe to detach. Forced detach
is never used.

## Default-off configuration and rollout

All configuration values are operator-supplied; this repository contains no
deployment-specific accounts, organizations, node names or AWS resource IDs.

| Variable | Consumer | Contract |
| --- | --- | --- |
| `SPACE_EBS_ADMISSION_POLICY` | API | JSON with `organization_ids` and `availability_zone`; absent denies new disk requests |
| `SPACE_EBS_PLACEMENT_POLICY` | API | JSON with `availability_zone` and one or two qualified Kubernetes `node_names`; intersects every existing required affinity alternative |
| `SPACE_EBS_EXPERIMENT_POLICY` | Cluster controller | JSON with `account_id`, `region`, `availability_zone`, `cluster`, `experiment` and one or two permitted `instance_ids`; absent disables cloud reconciliation |
| `SPACE_EBS_NODE_BINDINGS` | Internal API | Explicit registered-node to instance/AZ bindings, validated against the authenticated request origin |
| `SPACE_EBS_NODE_ID` | Worker disk consumers | Explicit registered node identity and worker opt-in; must agree with the fixed control-plane bindings |

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
