# Two-MNG blue/green node rollout: the workers-1 MNG.
#
# The first rollout from the
# legacy `workers-cgnat` MNG adds a `workers-1` MNG via `../rollout-mng`,
# drains cgnat → workers-1 via `scripts/space-rollout-mng.py`, then drops
# the cohort taint and (in a dedicated retirement PR) flips
# `workers_cgnat_retired = true` to destroy the legacy MNG.
# The MNG itself is permanent — workers-1 becomes the steady-state worker
# pool once cgnat retires. Later rollouts add a target cohort to
# `var.workers_rollout_cohorts` (the `workers_rollout_cohort` module below),
# keyed by the next unallocated workers-N name above every permanent worker
# MNG, including zero-sized capacity groups. The source then stays at zero as
# the rollback artifact, with its size recorded through the `workers_1_*`
# sizing inputs (or its own entry, for a cohort). workers-1 can't be removed
# once `workers_cgnat_retired` is set: after a later rollout it stays a
# zero-sized group in the worker IAM scope.
#
# Two states a cluster can be in:
#   - Promoted: workers-1 is the steady-state pool
#     (`workers_cgnat_retired = true`).
#   - Legacy (`workers_cgnat_retired = false`): workers-cgnat is still the
#     steady-state worker pool; `workers_1_headroom_count` temporarily
#     instantiates workers-1 during that cluster's rollout.

module "workers_1" {
  # workers-1 exists in two phases:
  #   1. In-flight rollout: the cluster sets `workers_1_headroom_count`, so
  #      workers-cgnat and workers-1 coexist while the orchestrator
  #      drains source nodes onto the tainted target cohort.
  #   2. Steady state: `workers_cgnat_retired = true`, so workers-1
  #      stays alive without the rollout cohort taint.
  count  = local.workers_pool_enabled ? 1 : 0
  source = "../rollout-mng"

  cluster_name    = module.eks.node_group_cluster_name
  node_group_name = "workers-1"
  node_role_arn   = aws_iam_role.workers_pool[0].arn

  subnet_ids     = local.secondary_private_subnet_ids
  instance_types = var.workers_instance_types
  capacity_type  = var.workers_capacity_type

  # During the rollout, create only the operator-selected headroom
  # (`0/H/H`) so the drain orchestrator can ratchet target capacity up
  # wave by wave. After promotion, the same module reads the steady-state
  # `workers_*` triple. The `workers_1_*` sizing inputs override either,
  # so tfvars can record workers-1 at zero once a later rollout has moved
  # the fleet onto a `workers_rollout_cohorts` target. Live sizes are the
  # orchestrator's, so these only take effect when the MNG is created.
  desired_size = coalesce(var.workers_1_desired_size, var.workers_cgnat_retired ? var.workers_desired_size : coalesce(var.workers_1_headroom_count, 0))
  min_size     = coalesce(var.workers_1_min_size, var.workers_cgnat_retired ? var.workers_min_size : 0)
  max_size     = coalesce(var.workers_1_max_size, var.workers_cgnat_retired ? var.workers_max_size : coalesce(var.workers_1_headroom_count, 0))

  # Mirror the legacy `workers-cgnat` label set so node-local DaemonSets
  # (which select on `space.pplx.net/node-pool=workers`) and sandbox pods
  # (which carry the same `nodeSelector`) land here without an API-side
  # selector change.
  labels = local.worker_rollout_labels

  # Standalone worker LT (state-migrated out of the per-pool for_each
  # so retiring `workers-cgnat` from `local.node_pools` doesn't drag
  # the LT away from this MNG).
  launch_template_id      = aws_launch_template.workers.id
  launch_template_version = aws_launch_template.workers.latest_version

  # Fence user sandbox pods off workers-1 while the cluster is draining from
  # workers-cgnat. The cleanup PR flips `workers_cgnat_retired = true`
  # and drops the taint from the promoted steady-state pool. After that,
  # `workers_1_rollback_cohort_taint_value` re-taints workers-1 for a
  # rollback from a `workers_rollout_cohorts` target back onto it.
  cohort_taint_value = var.workers_cgnat_retired ? var.workers_1_rollback_cohort_taint_value : var.workers_1_cohort_taint_value
  # The same isolation taints as every other worker pool.
  extra_taints = var.workers_taints

  tags = local.worker_common_tags

  # The module's `aws_eks_node_group.this` itself only references the role
  # ARN, but EKS launches nodes immediately on create — they need to assume
  # the role with WorkerNode + ECR + S3-read permissions before kubelet
  # registration. Force Terraform to attach those policies before the MNG
  # is created. Mirrors the `depends_on` on the core module's node
  # groups and on `aws_eks_node_group.rollout_managed` in `node_groups.tf`.
  #
  # Also depends on `aws_eks_access_entry.workers_pool` — kubelet on each
  # instance authenticates to the EKS API server via the role; without an
  # access entry binding the role to `system:nodes`, the API rejects node
  # registration as `Unauthorized` and the MNG times out in `CREATE_FAILED`.
  depends_on = [
    aws_iam_role_policy_attachment.workers_pool_managed,
    aws_iam_role_policy_attachment.workers_pool_s3_read,
    aws_eks_access_entry.workers_pool,
  ]
}

# Target cohorts for rollouts after workers-1 is the steady-state pool, one
# MNG per `var.workers_rollout_cohorts` entry. Each mirrors workers-1 (role,
# subnets, instance types, labels, isolation taints, tags) and launches from
# the same shared worker launch template, at its latest version when the
# entry is created. The capacity groups and any root-declared canary group
# can only take that template (EKS can update an existing node group to
# another version of its launch template, not to a different template), so a
# rollout bumps the template and moves the fleet onto a fresh cohort created
# from it; the source keeps the version it launched with, which is what a
# rollback brings back. That same bump plans an in-place launch template
# update on every capacity group (`worker_capacity.tf`), so empty a nonempty
# one first. A cohort shares workers-1's instance types, capacity type and
# subnets, so it carries a launch template delta only: changing those inputs
# replaces workers-1 and every cohort. A capacity type change also replaces
# every capacity group, and a subnet change replaces any capacity group
# whose resolved subnets change (`worker_capacity.tf`). The variable's
# validations keep the names inside the workers-N namespace and off the
# capacity groups' names.
module "workers_rollout_cohort" {
  for_each = var.workers_rollout_cohorts
  source   = "../rollout-mng"

  cluster_name    = module.eks.node_group_cluster_name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.workers_pool[0].arn

  subnet_ids     = local.secondary_private_subnet_ids
  instance_types = var.workers_instance_types
  capacity_type  = var.workers_capacity_type

  desired_size = each.value.desired_size
  min_size     = each.value.min_size
  max_size     = each.value.max_size

  # The workers-1 label set, so node-local DaemonSets and sandbox pods
  # select these nodes the same way.
  labels = local.worker_rollout_labels

  launch_template_id      = aws_launch_template.workers.id
  launch_template_version = aws_launch_template.workers.latest_version

  cohort_taint_value = each.value.cohort_taint_value
  extra_taints       = var.workers_taints

  tags = local.worker_common_tags

  # Same reason as on workers-1: nodes register as soon as EKS creates the
  # MNG, so the role's policies and access entry have to exist first.
  depends_on = [
    aws_iam_role_policy_attachment.workers_pool_managed,
    aws_iam_role_policy_attachment.workers_pool_s3_read,
    aws_eks_access_entry.workers_pool,
  ]
}

locals {
  workers_rollout_cohort_asg_names = {
    for name, cohort in module.workers_rollout_cohort : name => one(cohort.asg_names)
  }

  # EKS node-group tags don't propagate to the backing ASG or its instances.
  # Key by the stable cohort/tag pair, as the capacity groups do, so for_each
  # is known before AWS assigns the ASG name, and adding a cohort doesn't need
  # `worker_asg_tag_propagation_enabled` turned off for its first apply.
  workers_rollout_cohort_asg_tag_entries = merge(
    {},
    [
      for cohort_name in keys(var.workers_rollout_cohorts) : {
        # `Name` and `NodeGroup` as on the capacity groups, so a cohort's
        # instances aren't named after the shared launch template's
        # `workers-cgnat` tag.
        for key, value in merge(local.worker_common_tags, {
          Name      = "${local.name}-${cohort_name}"
          NodeGroup = cohort_name
          }) : "${cohort_name}/${key}" => {
          cohort_name = cohort_name
          key         = key
          value       = value
        }
      }
    ]...
  )
}

resource "aws_autoscaling_group_tag" "workers_rollout_cohort" {
  for_each = local.workers_rollout_cohort_asg_tag_entries

  autoscaling_group_name = local.workers_rollout_cohort_asg_names[each.value.cohort_name]

  tag {
    key                 = each.value.key
    value               = each.value.value
    propagate_at_launch = true
  }
}
