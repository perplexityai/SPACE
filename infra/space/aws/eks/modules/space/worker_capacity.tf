# Permanent worker-fleet groups use the next generic workers-N names and share
# the steady-state worker AMI, launch template, identity, bootstrap, and base
# scheduling labels. Environment tfvars select only the instance type,
# optional named subnet pool, per-group ceiling, and optional launch template
# version pin. Every group is created at zero nodes; scaling remains an
# explicit operator action outside Terraform.
#
# Launch template adoption. An unpinned group (`launch_template_version`
# null) tracks the shared worker template's latest version, so an empty or
# dormant group never scales out later on an obsolete bootstrap. A populated
# group should be pinned to the version it runs: a template change then only
# publishes a new version, which unpinned (empty) groups adopt at no cost,
# and the populated group's plan stays empty.
#
# Changing a populated group's pin, or leaving a populated group unpinned
# while the template changes, starts an EKS managed node group update. EKS
# marks every existing node in the group unschedulable up front, then
# replaces nodes max_unavailable (1) at a time. Each replacement drains its
# node through Kubernetes eviction rather than SPACE's sandbox-aware drain and
# takes minutes, so a group near the 99-node ceiling stays mostly
# unschedulable for hours. Move a populated group to a new version with the
# sandbox-aware two-MNG rollout instead (`../rollout-mng/README.md`,
# `scripts/space-rollout-mng.py`), or drain it to zero first and then move the
# pin. The worker_capacity_groups_on_latest_launch_template check warns while
# any pin lags the latest version.
#
# A 99-node MNG ceiling limits one group's operational blast radius. It does
# not restore full scheduler scoring once a sandbox pod can match more than
# 100 workers across the fleet. Check scheduler scoring coverage across all
# eligible workers before scaling past that boundary.

locals {
  worker_capacity_group_subnet_ids = {
    for name, group in var.worker_capacity_groups : name => (
      group.availability_zone == null ? (
        group.subnet_pool == null ? local.secondary_private_subnet_ids : lookup(
          local.worker_subnet_pool_ids,
          group.subnet_pool,
          [],
        )
      ) : try(data.aws_subnets.worker_capacity_group[name].ids, [])
    )
  }
}

resource "aws_eks_node_group" "worker_capacity" {
  for_each = var.worker_capacity_groups

  cluster_name    = module.eks.node_group_cluster_name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.workers_pool[0].arn
  subnet_ids      = local.worker_capacity_group_subnet_ids[each.key]

  ami_type       = "CUSTOM"
  capacity_type  = var.workers_capacity_type
  instance_types = each.value.instance_types

  # A pin holds the group on that version; null tracks the latest. Changing
  # the resolved version of a populated group rolls every node in it; see the
  # top of this file.
  launch_template {
    id      = aws_launch_template.workers.id
    version = each.value.launch_template_version != null ? each.value.launch_template_version : aws_launch_template.workers.latest_version
  }

  # The same isolation taints as every other worker pool.
  dynamic "taint" {
    for_each = var.workers_taints

    content {
      key    = taint.value.key
      value  = taint.value.value
      effect = taint.value.effect
    }
  }

  # No `node-restriction.kubernetes.io/space-node-group` here, deliberately.
  # That key is the org-scoped placement boundary, and no Terraform writes it:
  # the control plane does, per node — the cluster-controller's
  # `node_group_labeler` while the default group is the only one, an
  # admin-credentialed patch after that. Group membership is control-plane
  # state, while an MNG label can only ever be a per-group constant. Nothing
  # rejects the declaration — EKS applies MNG labels with control-plane
  # credentials, not the kubelet's, so NodeRestriction does not apply — which is
  # the hazard: a Terraform label can name a group with no `node_groups` row.
  # The labeler never overwrites an existing label, and the mirror records NULL
  # for an unknown group, leaving those nodes ineligible for sandbox placement.
  # `var.workers_labels` is validated against the same key.
  labels = merge(
    var.workers_labels,
    {
      "space.pplx.net/node-pool"   = "workers"
      "space.pplx.net/subnet-tier" = "secondary"
      "workload"                   = "workers"
    },
    each.value.subnet_pool == null ? {} : {
      "space.pplx.net/subnet-pool" = each.value.subnet_pool
    },
  )

  scaling_config {
    desired_size = 0
    min_size     = 0
    max_size     = each.value.max_size
  }

  update_config {
    max_unavailable = 1
  }

  tags = merge(local.worker_common_tags, {
    Name      = "${local.name}-${each.key}"
    NodeGroup = each.key
  })

  timeouts {
    update = "5h"
  }

  lifecycle {
    # The cluster-controller owns current fleet size. Terraform still owns the
    # <=99 ceiling and launch-template version: ignoring either would let a
    # dormant group scale months later with an obsolete AMI or an unbounded
    # ASG. Pin a populated group's version instead (launch_template_version).
    # A plan that changes the launch template version of a nonempty group is
    # a worker rollout of the whole group; drain/zero the group or use the
    # sandbox-aware rollout procedure before applying it.
    ignore_changes = [
      scaling_config[0].desired_size,
      scaling_config[0].min_size,
    ]

    precondition {
      condition = each.value.subnet_pool == null ? true : contains(
        keys(var.worker_subnet_pools),
        each.value.subnet_pool,
      )
      error_message = "Worker group `${each.key}` selects unknown subnet_pool `${coalesce(each.value.subnet_pool, "<default>")}`; declare that pool in worker_subnet_pools first."
    }

    precondition {
      condition     = each.value.availability_zone == null || each.value.subnet_pool != null
      error_message = "Worker group `${each.key}` selects availability_zone `${coalesce(each.value.availability_zone, "<default>")}` without a named subnet_pool."
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.workers_pool_managed,
    aws_iam_role_policy_attachment.workers_pool_s3_read,
    aws_eks_access_entry.workers_pool,
  ]
}

# A warning, as for the general groups: a pinned group behind the template's
# latest version is intended until someone moves it, but it keeps launching
# nodes from its pinned version, including on scale-out. During a plan that
# changes the template, the latest version isn't known until apply, so the
# check reports as unknown.
locals {
  worker_capacity_groups_behind_launch_template = {
    for name, group in aws_eks_node_group.worker_capacity : name => group.launch_template[0].version
    if group.launch_template[0].version != tostring(aws_launch_template.workers.latest_version)
  }
}

check "worker_capacity_groups_on_latest_launch_template" {
  assert {
    condition     = length(local.worker_capacity_groups_behind_launch_template) == 0
    error_message = "Worker capacity groups ${join(", ", [for name, version in local.worker_capacity_groups_behind_launch_template : "${name} (pinned to version ${version})"])} run an older version of the worker launch template than its latest, ${aws_launch_template.workers.latest_version}, and an apply doesn't move them. Moving a populated group's pin rolls every node in it; the comment at the top of worker_capacity.tf in the space module describes how to move them."
  }
}

locals {
  worker_capacity_asg_names = {
    for name, node_group in aws_eks_node_group.worker_capacity : name => one(flatten([
      for resources in node_group.resources : [
        for asg in resources.autoscaling_groups : asg.name
      ]
    ]))
  }

  # EKS node-group tags do not propagate to the backing ASG or its instances.
  # Key by the stable group/tag pair so for_each is known before AWS assigns an
  # ASG name; the name itself can remain an apply-time value.
  worker_capacity_asg_tag_entries = merge(
    {},
    [
      for group_name in keys(var.worker_capacity_groups) : {
        for key, value in merge(local.worker_common_tags, {
          Name      = "${local.name}-${group_name}"
          NodeGroup = group_name
          }) : "${group_name}/${key}" => {
          group_name = group_name
          key        = key
          value      = value
        }
      }
    ]...
  )
}

resource "aws_autoscaling_group_tag" "worker_capacity" {
  for_each = local.worker_capacity_asg_tag_entries

  autoscaling_group_name = local.worker_capacity_asg_names[each.value.group_name]

  tag {
    key                 = each.value.key
    value               = each.value.value
    propagate_at_launch = true
  }
}
