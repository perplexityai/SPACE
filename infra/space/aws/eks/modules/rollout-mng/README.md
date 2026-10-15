# `rollout-mng`

Reusable Terraform module for the **new managed node group** in a SPACE
two-MNG blue-green node rollout. The MNG itself is permanent — it
survives the rollout and becomes the steady-state worker pool. Only the
cohort taint is transient: it fences steady-state user sandbox pods off
the new MNG while sandboxes migrate from the incumbent MNG, then is
dropped in the cleanup PR once the incumbent is empty and gets
destroyed.

## What this module does

Declares a single `aws_eks_node_group` carrying a cohort taint
(`space.pplx.net/rollout-cohort=<cohort-tag>:NoSchedule` by default) so
that steady-state SPACE user sandbox pods will not land on it while
sandboxes are migrating, while the cluster-controller's evacuation pods
— which carry a matching toleration and a matching required
nodeAffinity term — will. Once the rollout completes and the incumbent
MNG is destroyed, this MNG **is** the worker pool; the cleanup PR
drops the cohort taint by setting `cohort_taint_value = null`.

In addition to the taint, the module declares a Kubernetes label with
the same key and value (e.g.
`space.pplx.net/rollout-cohort=<cohort-tag>`) on every node in the
MNG. The taint by itself only fences user pods *off* the new MNG; the
matching label is what pulls cluster-controller evacuation pods *onto*
it, via the required nodeAffinity term the cluster-controller derives
from its `clusterController.drainEvacuationPlacement.cohortValue` Helm
value. Both the taint and the label drop out when
`cohort_taint_value` is `null`.

The cohort taint is declared in the EKS `taints` block rather than in
user-data so EKS applies it at kubelet registration time. This closes
the pre-registration race where a node could briefly advertise itself
as schedulable before a user-data taint script ran. The cohort label
goes through the EKS `labels` argument for the same reason.

The module is intentionally narrow: it owns the MNG resource only. The
IAM role, launch template, and subnets are caller-owned and reused from
the incumbent worker pool wherever possible.

## What this module does **not** do

- It does not create or modify a launch template. Pass an existing
  `launch_template_id` + `launch_template_version` (typically a new LT
  version produced by the same PR that adds the rollout MNG).
- It does not create the node IAM role. Pass
  `aws_iam_role.workers_pool[0].arn` — the hoisted, shared worker
  role declared as a standalone resource in
  [`../space/iam.tf`](../space/iam.tf).
  Every `workers-N` MNG references the same role so per-pool IAM
  lifecycle does not block deletion of prior `workers-N` MNGs after
  their rollback windows expire.
- It does not orchestrate the rollout. The drain reconciler and the
  operator-driven rollout script handle node-by-node migration; see
  [Rollout flow at a glance](#rollout-flow-at-a-glance).
- It does not automatically remove the cohort taint or cohort label
  from currently-registered nodes at rollout completion. The cleanup
  PR sets `cohort_taint_value = null`, which drops both from the EKS
  node group configuration (the `dynamic "taint"` block disappears and
  the cohort key is removed from the `labels` merge). The cleanup
  behavior differs by resource type:

  - **Cohort taint — currently-registered nodes**: EKS does **not**
    reconcile MNG taint changes back onto live nodes. Run:
    ```
    kubectl taint nodes -l eks.amazonaws.com/nodegroup=<new-mng> <taint-key>-
    ```
    Without this, live nodes keep the taint until they are replaced
    (recycled by ASG), which means the cohort toleration is still
    required for any new pods placed on them.

  - **Cohort label — currently-registered nodes**: EKS **does**
    reconcile MNG label changes onto live nodes via the
    `eks:node-manager` controller. Once Terraform applies
    `cohort_taint_value = null`, EKS strips the cohort label from all
    running nodes in the MNG automatically. No `kubectl label nodes`
    command is needed.

  - **Future scale-out / replacement nodes**: both the taint and the
    label are absent from the MNG configuration after the cleanup PR,
    so new nodes register as plain workers with no cohort marking.

## Lifecycle of `min_size` / `desired_size` / `max_size`

All three scaling fields are Terraform-set **only at MNG creation**.
The module declares

```hcl
lifecycle {
  ignore_changes = [
    scaling_config[0].min_size,
    scaling_config[0].desired_size,
    scaling_config[0].max_size,
  ]
}
```

so the rollout orchestration script can drive the full scaling triple
via the AWS EKS API (`update-nodegroup-config`) wave-by-wave without an
unrelated Atlantis apply mid-rollout reverting it. The script
maintains a `min == desired == max` invariant on both MNGs at
steady-state observation points — source shrinks `N/N/N` →
`(N-1)/(N-1)/(N-1)` per drained node; target grows symmetrically.

Consequence: after the MNG exists, Terraform never re-asserts these
fields. To resize the MNG outside the orchestration script — e.g.
after promotion to steady-state — call the AWS EKS API directly
(`aws eks update-nodegroup-config --scaling-config
minSize=N,desiredSize=N,maxSize=N ...`), or remove this `ignore_changes`
block in the cleanup PR (in the same change that records the new
steady-state size in tfvars).

## Prerequisites in the cluster

The MNG this module creates is **inert on its own**. The rollout needs
two other pieces in place; one still has to be wired up before the
first real rollout.

**Already shipped (plumbing exists; configure before kicking off a rollout):**

- **Cluster-controller evacuation placement.** The
  `space-cluster-controller` Helm value
  `clusterController.drainEvacuationPlacement.cohortValue` (env var
  `SPACE_CLUSTER_CONTROLLER_DRAIN_EVACUATION_COHORT_VALUE`) decides
  the target cohort the cluster-controller's evacuation `Restore`
  pods are pinned to. The cohort key is fixed at
  `space.pplx.net/rollout-cohort`. When the value is non-empty the
  cluster-controller renders a matching `Equal`-operator toleration *and* a required
  nodeAffinity term on each evacuation pod, so it can only land on
  the target MNG. Default is empty (no placement). Before a rollout,
  set the value to the `cohort_taint_value` declared on the target
  MNG (its `cohort_taint_key` must be the default). Without this, evacuation pods
  schedule against the incumbent pool (or stay `Pending`), defeating
  the rollout.
- **Per-drain `replace_after_terminate` flag**. The
  orchestration script posts `:drain` with `mode: forced`,
  `replace_after_terminate: false` and `require_undrained: true`, so the
  incumbent MNG shrinks monotonically (`forced` because the default drain
  mode is gentle, which waits for residents instead of evacuating them) and
  the request can never escalate a drain somebody else started. A hand
  drain with `space-admin-cli drain-node` has no start-only precondition.
  Against a node already gentle-draining, `--forced` alone escalates the
  drain in place on its existing terms; adding
  `--no-replace-after-terminate` escalates only if the drain in flight was
  started with `replace_after_terminate: false` (by that flag or the
  API), and is a 409 otherwise, because a drain's replacement intent
  is fixed when it starts. The `nodes.terminate_decrement_capacity` column and
  the corresponding `Ec2InstanceTerminator` path are already on `main`.

**Still pending — wire up before the first real rollout:**

- **Node-local DaemonSets tolerate the cohort taint.** The SPACE node
  service DaemonSets — `space-node-api`, `space-node-ctl`,
  `space-node-manager`, `space-runtime-manager`, `space-sandbox-manager`,
  `space-template-manager`, `space-volume-manager` — and any other
  DaemonSet expected to run on every worker must carry the matching
  toleration in their Helm values, otherwise the new nodes register
  but never become drain targets. The toleration shape is the same
  `Exists`-operator on `space.pplx.net/rollout-cohort`.

## Rollout flow at a glance

A two-MNG rollout proceeds in three phases. Each worker group uses a unique,
monotonically increasing `workers-N` index.

1. **Add new MNG (this module).** A Terraform PR introduces a module
   instance using the next unallocated `workers-M` index above every rollout
   and permanent capacity group, with the new cohort's configuration
   delta. The convention is `min_size = 0, desired_size = H, max_size
   = H` so EKS provisions the headroom nodes at apply time and the
   rollout can begin draining as soon as those nodes are `Ready`.
2. **Drive the rollout.** The operator runs the rollout script. It
   ratchets the new MNG's `desired_size` up wave-by-wave, calls the
   admin `:drain` endpoint on incumbent nodes with
   `replace_after_terminate=false` and `mode=forced`, and waits for the
   cluster-controller to evacuate sandboxes onto the new MNG. The mode
   is explicit because the endpoint's default is `gentle`, which cordons
   and waits for residents to leave on their own — a rollout replacing
   the node cannot wait on a long-lived sandbox.
3. **Cleanup PR.** Once `workers-N` (the incumbent) has reached zero
   nodes, a follow-up Terraform PR
   - holds `workers-N` in Terraform at `desired_size = 0` as the
     rollback artifact (kept across the next rollout cycle),
   - deletes the explicitly recorded older *rollout* rollback artifact — never
     a permanent capacity group selected by index arithmetic — to return the
     rollout lineage to two-MNG steady state (skip on the first rollout from
     `workers-cgnat`),
   - sets `cohort_taint_value = null` on this module instance so both
     the `dynamic "taint"` block and the cohort key in the `labels`
     merge drop out of the EKS node group configuration — later
     scale-out / replacement nodes register untainted and unlabeled,
   - and unconditionally runs `kubectl taint nodes -l eks.amazonaws.com/nodegroup=workers-M <taint-key>-`
     out-of-band to clear the taint from any nodes that registered
     before the Terraform apply. The cohort **label** does not need a
     matching `kubectl label` command — EKS reconciles label removals
     onto live nodes automatically via the `eks:node-manager`
     controller once the MNG configuration is updated.

   EKS does not replace nodes for a taint or label change in practice;
   verify on a non-production cluster before applying in production.

## Which configuration dimensions can drive a rollout

Any change to one or more of the following triggers a rollout. The
module inputs map one-to-one:

| Dimension                | Module input(s)                                  | Notes                                                                   |
|--------------------------|--------------------------------------------------|-------------------------------------------------------------------------|
| Launch template / AMI    | `launch_template_id`, `launch_template_version`  | The most common dimension; AMI / kernel / user-data bumps live here.    |
| Instance shape           | `instance_types`                                 | E.g. `m5.metal` → `r6id.metal` for higher ENI / pod density.            |
| EKS Kubernetes version   | `launch_template_id`, `launch_template_version`  | Carried by the AMI — see below.                                         |
| Capacity type            | `capacity_type`                                  | `ON_DEMAND` ↔ `SPOT`.                                                   |
| AZ topology              | `subnet_ids`                                     | Rebalance the pool across a different AZ subset.                        |

A K8s-version rollout has no input of its own: the kubelet minor comes
from the custom AMI, and AWS rejects an `UpdateNodegroupVersion` that
carries both a version and a custom-AMI launch template. So it is a
launch-template rollout like any other AMI change.

The cohort taint value (`cohort_taint_value`) **must** be the new
MNG's running-number index as a string (`"1"`, `"2"`, …) to match the
`workers-N` naming convention.
It does not encode which dimension changed — that lives in the PR
description.

## Usage

Worker MNGs follow the `workers-N` naming convention (monotonic
integer index). The first rollout produces `workers-1`; subsequent
rollouts produce `workers-2`, `workers-3`, etc. Never reuse an index already
assigned to a rollout or permanent capacity group.

```hcl
module "workers_1" {
  source = "../rollout-mng"

  cluster_name    = module.eks.node_group_cluster_name
  node_group_name = "workers-1"
  node_role_arn   = aws_iam_role.workers_pool[0].arn   # count-gated on workers_<N>_headroom_count

  subnet_ids     = local.secondary_private_subnet_ids
  instance_types = var.workers_instance_types
  capacity_type  = var.workers_capacity_type

  # 0/H/H so EKS provisions the headroom nodes at Terraform apply
  # time and the rollout can begin draining as soon as those nodes
  # are `Ready`. `H` is the operator's chosen --headroom-count.
  desired_size = var.workers_next_headroom_count   # H
  min_size     = 0
  max_size     = var.workers_next_headroom_count   # H

  labels = {
    "space.pplx.net/node-pool"   = "workers"
    "space.pplx.net/subnet-tier" = "secondary"
    "workload"                   = "workers"
  }

  launch_template_id      = aws_launch_template.workers_1.id
  launch_template_version = aws_launch_template.workers_1.latest_version

  cohort_taint_value = "1"   # matches the new MNG's index

  tags = local.common_tags
}
```

## Atlantis / multi-platform lock files

This module ships no lock file of its own — Terraform lock files live
with root stacks, not with shared modules. The first root stack to
consume this module is responsible for ensuring its
`.terraform.lock.hcl` includes `h1:` checksums for `linux_amd64`
(Atlantis), `linux_arm64`, `darwin_amd64`, and `darwin_arm64`. From
the caller directory, run

```bash
terraform providers lock \
  -platform=linux_amd64 -platform=linux_arm64 \
  -platform=darwin_amd64 -platform=darwin_arm64
```

and commit the regenerated lock file.

## Inputs

See `variables.tf`.

## Outputs

| Name              | Description                                                                                                  |
|-------------------|--------------------------------------------------------------------------------------------------------------|
| `node_group_name` | Name of the new MNG. Pass to the rollout script's `--target-mng` flag.                                       |
| `node_group_arn`  | ARN of the new MNG.                                                                                          |
| `cohort_taint`    | Object `{ key, value }` describing the declared cohort taint (also surfaced as a matching node label with the same key+value), or `null` once both have been cleared. The cluster-controller's `drainEvacuationPlacement.cohortValue` Helm value must match this value, and the key must be `space.pplx.net/rollout-cohort`. |
