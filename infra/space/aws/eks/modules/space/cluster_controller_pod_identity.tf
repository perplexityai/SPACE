# Extra inline policies attached to the existing `space-cluster-controller`
# EKS Pod Identity role provisioned in `db_client_pod_identity.tf`. Grant
# the AWS API access the node-drain pipeline needs:
#
#   * `autoscaling:TerminateInstanceInAutoScalingGroup` on the SPACE worker
#     ASG(s) so the `node_drain_reconciler` can complete the
#     `terminating -> deleted` transition.
#   * `autoscaling:SuspendProcesses` on the same ASG(s) so the reconciler
#     can pause `ReplaceUnhealthy` / `Launch` during a node drain and let
#     the targeted termination through without the ASG racing to replace
#     it with a fresh instance the operator doesn't want yet.
#   * `autoscaling:DescribeAutoScalingGroups` (no resource-level scope
#     supported by AWS — `Resource = "*"`). Read-only state read used to
#     verify suspended-process state and instance lifecycle before issuing
#     the terminate.
#   * `eks:DescribeNodegroup` and `eks:UpdateNodegroupConfig` on the
#     worker EKS nodegroup(s). The reconciler uses these to read live
#     scaling state and (during a multi-node rollout) shrink/expand the
#     nodegroup `desired_size` in step with the drain plan.
#   * `ec2:DescribeInstances` (no resource-level scope) for the
#     `orphaned_nodes` reconciler.
#
# Why this is a separate `aws_iam_role_policy` on the existing role
# instead of a new `aws_eks_pod_identity_association`: EKS Pod Identity
# only allows one association per (cluster, namespace, service_account),
# and `db_client_pod_identity.tf` already binds the
# `cluster-controller-db-pod-identity` role to
# `space/space-cluster-controller`. Multiple inline policies on that one
# role compose into the pod's effective permissions.
#
# Scope: every node pool labeled `space.pplx.net/node-pool=workers`. The label
# is the authoritative "this pool runs sandbox workloads" marker — sandbox
# pods are scheduled with that nodeSelector by the API, and node drain
# only ever operates on capacity that hosts sandboxes. Builder and
# general pools are excluded because they don't carry the label.

locals {
  # Every pool-keyed node group: the core module's and the legacy
  # rollout-managed one. A pool lands in exactly one of them, so keys don't
  # collide across the merge. Rollout MNGs (e.g. `module.workers_1`) aren't
  # pool-keyed and are handled separately below via the module's
  # `asg_names` output.
  cluster_controller_all_node_groups = merge(
    module.eks.node_groups,
    aws_eks_node_group.rollout_managed,
  )

  # Names of every ASG backing a SPACE worker node group. EKS backs each
  # managed node group with one ASG spanning all its subnets, but the provider
  # exposes it as a list under `resources[*].autoscaling_groups[*]`, so flatten
  # across that nesting AND across pools. `flatten` is recursive, so the triple
  # nesting collapses to a flat list.
  #
  # Three sources, concatenated then flattened:
  #
  #   1. Top-level node-group resources whose pool entry carries the
  #      `space.pplx.net/node-pool=workers` label. On a cluster that
  #      hasn't retired it, this is the legacy `workers-cgnat` MNG; after
  #      retirement the set is empty.
  #   2. `module.workers_1[*].asg_names` — the rollout-mng module
  #      publishes its own ASG names because module-created MNGs aren't
  #      reachable from the parent's top-level resource addresses.
  #      Future rollouts to `workers-2`, `workers-3`, ... each add a
  #      separate `module "workers_N"` block in `rollout.tf` and an
  #      additional entry below.
  #   3. `var.additional_worker_asg_names` — worker MNGs the root declares
  #      beside this module (the rollout smoke-test pools) whose nodes the
  #      same `node_drain_reconciler` terminator path must be able to
  #      terminate; without this scope the `:terminate` transition fails
  #      with AccessDenied.
  #
  # This local also feeds the `worker_asg_propagated_tag_entries` for_each in
  # `node_groups.tf`, whose keys must be known at plan time — which is why tag
  # propagation has to be off for any apply that creates one of these ASGs
  # (see var.worker_asg_tag_propagation_enabled). A root ASG whose name AWS
  # only assigns at create time goes through
  # `var.additional_worker_asg_names_policy_only` instead, which is
  # concatenated straight into `cluster_controller_worker_asg_arns` below (a
  # policy-`resources` value, not a for_each key, so an unknown-until-apply
  # ARN is fine) and never tagged by this module.
  cluster_controller_worker_asg_names = flatten(concat(
    [
      for key, ng in local.cluster_controller_all_node_groups :
      [for r in ng.resources : [for asg in r.autoscaling_groups : asg.name]]
      if try(local.node_pools[key].labels["space.pplx.net/node-pool"], null) == "workers"
    ],
    module.workers_1[*].asg_names,
    var.additional_worker_asg_names,
  ))

  # IAM resource ARNs for those ASGs.
  worker_capacity_asg_arns = [
    for name in values(local.worker_capacity_asg_names) :
    "${local.autoscaling_group_arn_prefix}${name}"
  ]

  worker_capacity_nodegroup_arns = [
    for node_group in aws_eks_node_group.worker_capacity : node_group.arn
  ]

  cluster_controller_worker_asg_arns = concat(
    [
      for name in local.cluster_controller_worker_asg_names :
      "${local.autoscaling_group_arn_prefix}${name}"
    ],
    # Root-declared worker ASGs whose names are unknown until apply: fine here
    # because this is an IAM policy `resources` value, not a for_each key.
    [
      for name in var.additional_worker_asg_names_policy_only :
      "${local.autoscaling_group_arn_prefix}${name}"
    ],
    local.worker_capacity_asg_arns,
  )

  # EKS nodegroup ARNs for the same worker pools: top-level
  # `aws_eks_node_group` resources whose pool entry carries the
  # `space.pplx.net/node-pool=workers` label, then each rollout-mng module's
  # `node_group_arn` output (`workers_1`), the ARNs of worker MNGs the calling
  # root declares itself (`additional_worker_node_group_arns`), and the
  # `workers-N` capacity groups. Unlike the ASG-names list, the root's MNGs
  # are safe to include directly here because this local feeds only IAM policy
  # `resources` (no for_each), so an unknown-until-apply ARN is fine. AWS EKS
  # supports resource-level permissions on `eks:DescribeNodegroup` and
  # `eks:UpdateNodegroupConfig`, so the cluster-controller's reach is scoped to
  # worker nodegroups (builders and the general pool are off-limits even though
  # the controller's role can name them).
  cluster_controller_worker_nodegroup_arns = concat(
    [
      for key, ng in local.cluster_controller_all_node_groups : ng.arn
      if try(local.node_pools[key].labels["space.pplx.net/node-pool"], null) == "workers"
    ],
    module.workers_1[*].node_group_arn,
    var.additional_worker_node_group_arns,
    local.worker_capacity_nodegroup_arns,
  )

}

data "aws_iam_policy_document" "cluster_controller_asg_terminate" {
  # Despite the resource name, this policy now covers every ASG-scoped
  # action the drain pipeline needs: termination of a specific instance
  # and process-suspension during the drain window. Keeping the
  # `aws_iam_role_policy.cluster_controller_asg_terminate` name avoids a
  # disruptive rename on Atlantis-managed accounts (inline policy
  # delete+recreate on a live role) for a no-op identity refactor.
  statement {
    actions = [
      "autoscaling:TerminateInstanceInAutoScalingGroup",
      "autoscaling:SuspendProcesses",
    ]

    resources = local.cluster_controller_worker_asg_arns
  }
}

# Read-only ASG state for the drain reconciler. AWS does not support
# resource-level permissions on `autoscaling:DescribeAutoScalingGroups`,
# so the policy must use `Resource = "*"` (the AWS-documented shape for
# this action).
data "aws_iam_policy_document" "cluster_controller_asg_describe" {
  statement {
    sid = "NodeDrainReconcilerDescribeAutoScalingGroups"

    actions = [
      "autoscaling:DescribeAutoScalingGroups",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "cluster_controller_asg_describe" {
  name   = "${local.name}-cluster-controller-asg-describe"
  role   = aws_iam_role.db_client_pod_identity["cluster-controller"].name
  policy = data.aws_iam_policy_document.cluster_controller_asg_describe.json
}

# EKS nodegroup management for the drain reconciler. Scoped to the
# worker nodegroup ARNs so the cluster-controller can read live scaling
# state (`DescribeNodegroup`) and resize the nodegroup
# (`UpdateNodegroupConfig`) during a multi-node rollout. Builder and
# general nodegroups are intentionally outside the resource set.
data "aws_iam_policy_document" "cluster_controller_eks_nodegroup_manage" {
  statement {
    sid = "NodeDrainReconcilerManageWorkerNodegroups"

    actions = [
      "eks:DescribeNodegroup",
      "eks:UpdateNodegroupConfig",
    ]

    resources = local.cluster_controller_worker_nodegroup_arns
  }
}

resource "aws_iam_role_policy" "cluster_controller_eks_nodegroup_manage" {
  name   = "${local.name}-cluster-controller-eks-nodegroup-manage"
  role   = aws_iam_role.db_client_pod_identity["cluster-controller"].name
  policy = data.aws_iam_policy_document.cluster_controller_eks_nodegroup_manage.json
}

# No `count` gate: every supported environment has at least one worker
# MNG declared in config. When `workers_cgnat_retired = false`, the
# legacy `aws_eks_node_group.rollout_managed["workers-cgnat"]` is alive
# and its ASG name flows through `cluster_controller_all_node_groups`;
# when `true`, `module.workers_1` is alive and its ASG name flows in
# via `module.workers_1[*].asg_names`. The two are mutually exclusive
# in this PR's gating but the union is always non-empty, so the policy
# statement always has at least one resource.
resource "aws_iam_role_policy" "cluster_controller_asg_terminate" {
  name   = "${local.name}-cluster-controller-asg-terminate"
  role   = aws_iam_role.db_client_pod_identity["cluster-controller"].name
  policy = data.aws_iam_policy_document.cluster_controller_asg_terminate.json
}

// Read-only EC2 access for the `orphaned_nodes` reconciler, which calls
// `ec2:DescribeInstances` filtered by `private-dns-name` to verify a node
// row's underlying instance is gone before hard-deleting the row. EC2
// `DescribeInstances` does not support resource-level permissions, so the
// policy is `Resource = "*"` (the AWS-documented shape for this action).
data "aws_iam_policy_document" "cluster_controller_ec2_describe_instances" {
  statement {
    sid = "OrphanedNodesReconcilerDescribeInstances"

    actions = [
      "ec2:DescribeInstances",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "cluster_controller_ec2_describe_instances" {
  name   = "${local.name}-cluster-controller-ec2-describe-instances"
  role   = aws_iam_role.db_client_pod_identity["cluster-controller"].name
  policy = data.aws_iam_policy_document.cluster_controller_ec2_describe_instances.json
}

// S3 reap permissions for the `snapshot_storage_sweeper` reconciler.
// The sweeper lists every object under a suspend-snapshot
// prefix and batch-deletes them when the matching
// `snapshot_storage_locations` row is in the orphaned state. Scoped to
// the same `snapshots_bucket` that sbx-mgr writes to; no other buckets
// are touched. `ListBucket` is bucket-scoped (S3 requirement for List);
// `DeleteObject` + `DeleteObjects` are object-scoped.
data "aws_iam_policy_document" "cluster_controller_snapshots_reap" {
  statement {
    sid = "SnapshotStorageSweeperListBucket"

    actions = [
      "s3:ListBucket",
    ]

    resources = [
      local.snapshots_bucket_arn,
    ]
  }

  statement {
    sid = "SnapshotStorageSweeperDeleteObjects"

    actions = [
      "s3:DeleteObject",
    ]

    resources = [
      local.snapshots_bucket_objects,
    ]
  }
}

resource "aws_iam_role_policy" "cluster_controller_snapshots_reap" {
  name   = "${local.name}-cluster-controller-snapshots-reap"
  role   = aws_iam_role.db_client_pod_identity["cluster-controller"].name
  policy = data.aws_iam_policy_document.cluster_controller_snapshots_reap.json
}
