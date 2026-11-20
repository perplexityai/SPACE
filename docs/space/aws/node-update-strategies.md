# Worker node-update strategies

A worker node holds sandbox state locally: running Firecracker VMs, their
snapshots, and the rootfs cache. Any change that a node only picks up at boot
replaces nodes. That includes the AMI, the launch template (LT) and its user
data, instance type, subnets, and the Kubernetes version carried by a custom
AMI. This page compares the ways the Terraform modules under
`infra/space/aws/eks/modules/` can adopt such a change and says which one to
use for which node group. Operator procedures live in each deployment's
runbooks; this page covers the mechanisms and their tradeoffs.

Status: the EKS native update, the per-group launch template pin, and the
single-pair sandbox-aware rollout exist. Per-entry instance type and subnet
overrides for rollout targets, and running several rollout pairs in
parallel, are future work and not scheduled.

## What decides the mechanism

Publishing a new LT version and adopting it are separate events. Terraform
publishes a new version of the shared worker LT, `aws_launch_template.workers`,
whenever its rendered user data or image changes, including a change to a
boot-time script inlined with `file()`. What happens next depends on the
node group resource:

| Node group resource | LT version it tracks | Effect of a new LT version on apply |
| --- | --- | --- |
| `module.workers_1`, `workers_rollout_cohorts` (`rollout-mng`), `aws_eks_node_group.rollout_managed` | Version at creation; `launch_template` is in `ignore_changes` | None. Adoption happens only through a sandbox-aware rollout onto a new MNG. |
| `aws_eks_node_group.worker_capacity` (`worker_capacity_groups`) | The entry's `launch_template_version` if set, otherwise `latest_version` | Unpinned groups: EKS native in-place update, empty or not. Pinned groups: none until the pin moves. |
| `aws_eks_node_group.custom` (for example `builders`) | `builders_launch_template_version` if set, otherwise `latest_version` | Unpinned: EKS native in-place update with `builders_update_strategy`. Pinned: none until the pin moves. |

Unpinned capacity groups follow `latest_version` on purpose. A group
sitting at zero nodes must not scale out later on a stale bootstrap, and an
empty group adopts a new version quickly, with no node replacement. An
unpinned nonempty group has the same plan diff, `launch_template.version`
moving to the next number, and the apply starts a native rolling update of
live sandbox workers. Pin populated groups to prevent that; see
[Pin each capacity group's launch template](#pin-each-capacity-groups-launch-template).

## EKS native in-place update

An `UpdateNodegroupVersion` call, which is what Terraform issues when a
managed node group's LT version changes, runs the phases in
[Understand each phase of node updates](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-update-behavior.html):

1. Scale up. The ASG's desired and max sizes grow by the larger of twice
   the group's Availability Zone count and `maxUnavailable`. The phase
   succeeds only when every AZ that has an old node also has a new node in
   `Ready`, and each new node must join within 15 minutes of the ASG
   resize. Old nodes are then marked unschedulable.
2. Upgrade. EKS selects up to `maxUnavailable` old nodes, evicts their pods
   through the Kubernetes eviction API, waits 60 seconds, and terminates
   them. A pod that has not left after 15 minutes fails the update with
   `PodEvictionFailure` unless the update was forced.
3. Scale down. The ASG returns to its pre-update size.

The two update strategies differ in the upgrade phase:

- `DEFAULT` launches replacements before terminating old nodes. It needs
  surge capacity of the same instance type in each AZ the group uses.
- `MINIMAL` cordons every node in the group at the start, then terminates
  up to `maxUnavailable` nodes before the ASG launches their replacements.
  Capacity never exceeds the configured size, so a replacement can reuse the
  slot the old node released.

In both cases the whole group becomes unschedulable at the start of the
update, not `maxUnavailable` nodes at a time; `maxUnavailable` only limits
how many nodes are replaced at once. With `max_unavailable = 1`, which both
`worker_capacity` and `custom` set, the group is replaced one node at a time
and wall time grows linearly with group size. For a large worker group it
runs for hours.

Effects on sandbox workers:

- Capacity loss. All of the group's nodes stop accepting new sandboxes for
  most of the update. Several groups updating at once remove all of them
  together, so new creates and restores concentrate on the remaining nodes.
  Expect create latency to rise, typically in pod IP assignment on those
  nodes, and new nodes to draw bursts of placements as they join.
- Sandbox loss. Kubernetes eviction is not sandbox-aware. Evicted sandbox
  pods are not migrated by the cluster-controller drain pipeline, so
  resident VMs are stopped rather than evacuated.
- Capacity and AZ requirements. `DEFAULT` needs a free slot of the same
  type in each affected AZ within 15 minutes. When every reserved slot is
  in use and on-demand capacity is short (`InsufficientInstanceCapacity`),
  the update fails and EKS rolls it back. ASG `AZRebalance` can terminate a
  surge node mid-update and cause the same failure. `MINIMAL` avoids the
  surge but leaves the group short by `maxUnavailable` nodes per step, and
  another consumer of an open reservation can take the freed slot first.
- Apply duration. An update that runs inside `terraform apply` must finish
  within the node group's update timeout and the apply runner's own limit.

Use the native update only for:

- groups with no sandboxes resident, such as empty capacity groups;
- stateless pools such as `builders`, one cluster at a time and when
  someone is watching; and
- emergencies, one nonempty worker group at a time, with explicit approval.

### Builders

`builders` is a stateless custom pool. Replacing a builder loses at most
the builds in flight on it, which callers retry, so a native update is
appropriate. The `space` module always sets its strategy from
`builders_update_strategy`, which defaults to `MINIMAL` and cannot be null:
EKS keeps returning a strategy once one has been set and cannot clear it,
so an unset value would plan `<value> -> null` on every run. The tradeoff
between the two values:

- `MINIMAL`, the default, terminates a builder first and relaunches into
  the slot it released, so a roll never needs capacity beyond the group's
  size. With two or more builders and `max_unavailable = 1`, the others keep
  serving while one is replaced. A single-node group has no builder for
  roughly one node boot, and template build submissions fail as unavailable
  until the replacement joins. The released slot is guaranteed only if the
  reservation is `targeted` to the builders rather than `open` to every
  matching launch in the account.
- `DEFAULT` launches the replacement first, so it keeps every builder
  serving, but needs a spare instance of the builder's type in the same AZ
  within 15 minutes. When builders draw from a shared reservation that is
  fully used, that surge falls back to on-demand capacity and can fail, and
  EKS rolls the update back.

Run at least two builders in every cluster. The module defaults
`builders_min_size` and `builders_desired_size` to 1, so set both to 2 or
more; with one builder, `MINIMAL` leaves the cluster without a builder for
each replacement. Apply a strategy change on its own, before any LT or AMI
change to the group: in a combined apply the node group version update
runs first, under the old strategy. `builders_launch_template_version`
can hold the builders on a version, like the capacity group pin below.

Keep builder LT changes in their own apply, separate from worker changes, so
a builder capacity failure does not hold up unrelated work.

## Sandbox-aware two-MNG blue/green

`scripts/space-rollout-mng.py` moves sandboxes from a source MNG to a target
MNG through the cluster-controller drain reconciler, which evacuates each
sandbox onto the target instead of evicting it. The target MNG declares a
cohort taint that only evacuation pods tolerate. As each source node
terminates, the script removes the taint from one Ready target node, which
then accepts user creates. It aims to keep `H` Ready target nodes tainted
for incoming evacuations, and untaints those last ones when the source is
empty. `H` is intended headroom, not a guarantee: the drain gate counts
every Ready target node, tainted or not, so it can start a drain while
fewer than `H` of them are tainted. The target module and taint are
described in [`rollout-mng`](../../../infra/space/aws/eks/modules/rollout-mng/README.md).

The script's two knobs:

- `K`, `--max-concurrent-node-drains`, caps how many source nodes drain at
  once.
- `H`, `--headroom-count`, is the number of extra target nodes the script
  aims to keep ahead of the drains. The script requires `1 <= K <= H`.

The script holds the two groups' combined desired size at `N + H`, where
`N` is the source's starting size: the target's desired size starts at `H`
and grows by one for each source node terminated, ending at `N + H`. Ready
capacity can dip below that while a replacement target node boots, because
a source terminate lowers the source at once and the target node takes
time to become Ready.

The target must carry the `space.pplx.net/rollout-cohort` taint and its
matching node label, as `rollout-mng` declares them. The script reads target
taints on every tick. Its sanity check fails the run when target nodes lack
the taint, except that on a resumed rollout it allows one untainted target
node per source node already terminated. It repeats that check each tick
only until it sees one tainted Ready target node, and does not re-check the
taints of target nodes that join after that. For a custom-AMI node group,
EKS applies node group taints after the node registers, unless the bootstrap
passes them to kubelet with `--register-with-taints`; the shared worker
bootstrap does not. A new target node can therefore accept a user pod before
its taint arrives, and an operator check cannot close that window. Use a
`rollout-mng` or `workers_rollout_cohorts` target. A
`worker_capacity_groups` entry does not set either today, so it can be a
rollout source but not a target.

The target must also be able to grow to `N + H`. A `rollout-mng` or
`workers_rollout_cohorts` target takes its `max_size` from the caller. The
99-node ceiling of a capacity group is a planning limit for the source:
keep groups that will be rolled into a same-sized target small enough to
leave room for `H`, or first move some of the source's sandboxes elsewhere.

Tradeoffs:

- Capacity loss is bounded by `K` draining nodes; the rest of the fleet
  keeps accepting work.
- Sandboxes are migrated, not stopped.
- It needs `H` extra instances of the target's type for the whole rollout.
  With reservations, check free slots per AZ and instance type, not just the
  pool total.
- Wall time is set by `K` and by how long each wave takes: evacuation,
  target readiness, and source termination. Bare-metal instances can take
  tens of minutes to terminate, and a terminating source node still counts
  against `K`, so small `K` on bare metal is slow.
- Each rollout is operator-driven and needs supporting changes: the
  target's Terraform entry, the cluster-controller's
  `drainEvacuationPlacement.cohortValue` set to the target's cohort, and a
  cleanup change that removes the cohort taint and records the final sizes.

Today the orchestrator handles one source and one target per run, and the
evacuation placement selects one cohort value cluster-wide.
`workers_rollout_cohorts` entries inherit `workers-1`'s instance types and
subnets, so they can only roll a fleet shaped like `workers-1`. Capacity
groups with a different instance type or a single-AZ subnet pool need
per-entry overrides before they can be blue/green targets.

## Pin each capacity group's launch template

Each `worker_capacity_groups` entry has an optional
`launch_template_version`, a version number such as `"3"`:

- Unset, the group follows `latest_version`. That suits empty groups,
  which then never scale out on a stale bootstrap.
- Set, the group stays on that version, so a new LT version is published
  without being adopted. Adoption is a reviewed one-line change to the pin,
  made after the group is empty or through a blue/green rollout. Moving
  the pin of a populated group is a native in-place update of that group.

The non-blocking `worker_capacity_groups_on_latest_launch_template` check
warns while any pin lags the latest version. During a plan that changes the
LT, the latest version is not known until apply, so the check reports as
unknown. A pin above the template's latest version fails a precondition:
at plan, or at apply when the same apply changes the template. In the
second case it fails only when that node group is applied, and other node
groups that do not depend on it may already be updating. The module does not require
a populated group to be pinned, so make it a rule: pin a group to the
version it runs before scaling it out from zero and before any change that
publishes a new LT version. An unpinned group that is populated when the LT
changes rolls on that apply.

The builders pool has the same control, `builders_launch_template_version`,
with the `custom_node_groups_on_latest_launch_template` check and the same
precondition. It applies only to a custom-AMI pool.

With populated groups pinned, a change to the shared LT updates only empty
groups in place, and nonempty groups adopt it when an operator chooses.
Empty groups adopt it on their own: a group that is zero today picks up the
latest version and serves it when it scales out. A group can be drained to
zero, its pin moved or cleared, and then refilled on the new version
without a blue/green pair, provided the rest of the fleet can hold its
sandboxes meanwhile.

Bump pins deliberately: one group per change, reviewed, applied while
someone watches node availability, and only after the previous group has
finished and its nodes are back to Ready and schedulable. Never batch pin
bumps for several populated groups into one apply.

## Operating guardrails

The mechanisms above assume a few standing practices:

- Reserved capacity covers every running worker and builder. Size
  reservations per instance type and AZ to the live node counts, plus the
  `H` headroom of any planned blue/green rollout. A `DEFAULT` surge or a
  replacement that falls back to on-demand capacity can fail with
  `InsufficientInstanceCapacity`.
- Node availability is monitored. Alert on each node group's Ready and
  schedulable node counts, on nodes stuck NotReady or cordoned, and on node
  group updates that run long or fail, so an unintended or stalled roll is
  caught while it is still one group.
- Every populated worker group is pinned, and every cluster runs at least
  two builders on `MINIMAL`.

## Parallel pairs (future)

Rolling several groups at once with one blue/green pair each would cut
wall time roughly by the number of pairs. It is not implemented. It needs:

- evacuation placement routed per source, so each source drains only onto
  its own target rather than any node with the shared cohort;
- a cluster-wide drain budget that counts every pair;
- reserved headroom per pair, by AZ and instance type; and
- automatic pause conditions, such as sustained create latency, IP
  allocation latency, or create errors above a threshold.

## Choosing a strategy

| Change | Strategy |
| --- | --- |
| Empty capacity group | Native update; it adopts the latest LT on apply. |
| Boot-only or cosmetic LT change on nonempty groups | Keep the groups pinned, let empty groups adopt it and roll the rest at the next planned rollout. |
| Scale-out of a capacity group | Pin the group to the version it will run before scaling it out. |
| Planned worker AMI, kernel or bootstrap change | Pin, or drain to zero, every nonempty capacity group before the apply that publishes the LT. Then roll sandbox-aware blue/green, one pair at a time, with room for `N + H` in the target, or bump one group's pin at a time. |
| Builders | Native update with `builders_update_strategy` (`MINIMAL` by default) and at least two builders, one cluster at a time. |
| Emergency on nonempty workers | Blue/green with larger `K` and `H` if headroom exists; otherwise native update one group at a time, with approval. |

Avoid native updates of several nonempty worker groups in parallel: each
one makes its entire group unschedulable for the duration.
