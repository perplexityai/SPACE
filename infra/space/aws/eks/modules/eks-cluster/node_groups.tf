resource "aws_launch_template" "node_group" {
  for_each = var.node_pools

  name_prefix            = "${var.name}-${each.key}-"
  image_id               = each.value.ami_id
  update_default_version = true
  vpc_security_group_ids = concat(
    [
      aws_security_group.nodes.id,
      aws_eks_cluster.this.vpc_config[0].cluster_security_group_id,
    ],
    each.value.additional_security_group_ids,
  )

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      delete_on_termination = true
      encrypted             = true
      volume_size           = each.value.disk_size_gib
      volume_type           = "gp3"
    }
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_put_response_hop_limit = var.node_instance_metadata_hop_limit
    http_tokens                 = "required"
    instance_metadata_tags      = "enabled"
  }

  dynamic "capacity_reservation_specification" {
    for_each = each.value.capacity_reservation_preference == null && each.value.capacity_reservation_resource_group_arn == null ? [] : [1]

    content {
      capacity_reservation_preference = each.value.capacity_reservation_preference

      dynamic "capacity_reservation_target" {
        for_each = each.value.capacity_reservation_resource_group_arn == null ? [] : [1]

        content {
          capacity_reservation_resource_group_arn = each.value.capacity_reservation_resource_group_arn
        }
      }
    }
  }

  # EKS-managed AMIs bootstrap themselves; a custom AMI runs the caller's script.
  user_data = each.value.ami_id == null ? null : base64encode(var.node_pool_user_data[each.key])

  tag_specifications {
    resource_type = "instance"

    tags = merge(var.common_tags, {
      Name = "${var.name}-${each.key}"
    })
  }

  # EBS root volumes take tags only from their own tag_specifications.
  tag_specifications {
    resource_type = "volume"

    tags = merge(var.common_tags, each.value.volume_tags, {
      Name = "${var.name}-${each.key}"
    })
  }

  tags = merge(var.common_tags, {
    Name = "${var.name}-${each.key}"
  })
}

# Custom-AMI pools. Terraform owns the launch template version here, so a
# template change rolls the group at max_unavailable = 1.
resource "aws_eks_node_group" "custom" {
  for_each = {
    for key, value in var.node_pools : key => value
    if value.ami_id != null && !value.launch_template_and_role_only
  }

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.node[each.key].arn
  subnet_ids      = each.value.subnet_ids

  ami_type       = "CUSTOM"
  capacity_type  = each.value.capacity_type
  instance_types = each.value.instance_types

  launch_template {
    id      = aws_launch_template.node_group[each.key].id
    version = aws_launch_template.node_group[each.key].latest_version
  }

  labels = each.value.labels

  scaling_config {
    desired_size = each.value.desired_size
    max_size     = each.value.max_size
    min_size     = each.value.min_size
  }

  update_config {
    max_unavailable = 1
    update_strategy = each.value.update_strategy
  }

  dynamic "taint" {
    for_each = each.value.taints

    content {
      effect = taint.value.effect
      key    = taint.value.key
      value  = taint.value.value
    }
  }

  depends_on = [
    aws_eks_addon.pre_node,
    aws_iam_role_policy_attachment.node_additional,
    aws_iam_role_policy_attachment.node_managed,
  ]

  tags = merge(var.common_tags, {
    Name = "${var.name}-${each.key}"
  })

  # Default 60min is too short for large pools rolling at max_unavailable=1.
  timeouts {
    update = var.node_group_update_timeout
  }
}

resource "aws_eks_node_group" "managed" {
  for_each = {
    for key, value in var.node_pools : key => value
    if value.ami_id == null && !value.launch_template_and_role_only
  }

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.node[each.key].arn
  subnet_ids      = each.value.subnet_ids

  ami_type       = each.value.ami_type
  capacity_type  = each.value.capacity_type
  instance_types = each.value.instance_types

  # `version` is deliberately unset. EKS accepts only the cluster's own current
  # version at create time, so a pinned node version would fail CreateNodegroup
  # on any cluster built while the control plane leads the AMI. Groups start at
  # their cluster's version and lag only by staying put through later bumps.

  launch_template {
    id      = aws_launch_template.node_group[each.key].id
    version = aws_launch_template.node_group[each.key].latest_version
  }

  labels = each.value.labels

  scaling_config {
    desired_size = each.value.desired_size
    max_size     = each.value.max_size
    min_size     = each.value.min_size
  }

  update_config {
    max_unavailable = 1
    update_strategy = each.value.update_strategy
  }

  dynamic "taint" {
    for_each = each.value.taints

    content {
      effect = taint.value.effect
      key    = taint.value.key
      value  = taint.value.value
    }
  }

  depends_on = [
    aws_eks_addon.pre_node,
    aws_iam_role_policy_attachment.node_additional,
    aws_iam_role_policy_attachment.node_managed,
  ]

  tags = merge(var.common_tags, {
    Name = "${var.name}-${each.key}"
  })

  timeouts {
    update = var.node_group_update_timeout
  }

  # min_size and max_size are code-owned: node_pools decides them, and a change
  # lands through an apply. desired_size isn't. Something else moves it: an
  # operator through the EKS API, or a cluster-autoscaler the caller runs on
  # one of these groups. Without this, every unrelated apply would reset each
  # group to its configured desired size and could scale it down under
  # whatever is running on it. node_pools' desired_size still decides what a
  # new group comes up with.
  #
  # The provider sends all three fields in one UpdateNodegroupConfig call,
  # desired_size as it stood in state when the plan was made, and EKS requires
  # min_size <= desired_size <= max_size. So raising min_size above a group's
  # live desired size, or lowering max_size below it (say after
  # cluster-autoscaler has grown the pool), can't apply: the managed_node_groups
  # output refuses the plan. And an apply of a saved plan, on a pool
  # cluster-autoscaler has resized since the plan, sends the old desired size
  # back and resizes the pool to it. Both go away if the live group already
  # has the new bounds when the plan is made: it then has no bound change and
  # sends no size. Make that EKS change immediately before planning the
  # matching code change, since any other apply in between reverts it (see
  # below). To raise bounds, raise max, then desired if it's below the new
  # min, then min. To lower them, lower min, then shrink the pool to the new
  # max, then lower max. Shrink by draining a chosen node and terminating it
  # with `aws autoscaling terminate-instance-in-auto-scaling-group
  # --should-decrement-desired-capacity`, one at a time, each from the zone
  # with the most nodes: a smaller desired size lets the ASG pick which
  # instances go, and a multi-zone ASG left uneven runs AZRebalance, neither
  # of which honors PodDisruptionBudgets.
  #
  # Otherwise, on a cluster running the autoscaler, plan immediately before
  # applying a min/max change and check the plan's desired_size against the
  # live group.
  #
  # An out-of-band min/max change through the EKS API is drift: every later
  # apply on the cluster, whatever it's for, plans to revert it, and the plan
  # fails while the live desired size sits outside the configured bounds.
  # Record an emergency resize in node_pools rather than leaving it live-only.
  #
  # The launch-template version is held too. A group starts on the
  # template's latest version, and later versions don't roll it. A change to
  # what the template launches with bumps the version, a tag in common_tags
  # included. EKS fails a rolling update if a node's pods haven't left within
  # about 15 minutes of its drain (PodEvictionFailure), so a pool whose pods
  # can take longer to leave, through a long termination grace period or a
  # tight PodDisruptionBudget, can't take a new version that way. These
  # groups take their AMI from EKS, not the template, but the provider's
  # template roll sent no Kubernetes or AMI version, so it could also move
  # them to the cluster's Kubernetes version and its newest AMI. With the
  # hold, only an apply that replaces a group does that, since a new group
  # starts at the cluster's version; otherwise it takes
  # update-nodegroup-version, below. The template ID isn't held: EKS can't
  # move a group to another template, so a new ID still plans the group's
  # replacement, which deletes it with its nodes before creating it again.
  #
  # A group left behind keeps launching nodes from its old version, so move
  # it before a change it needs, such as a new security group or metadata
  # option, is relied on. A group whose pods all leave within EKS's window can
  # roll in place:
  # `aws eks update-nodegroup-version --cluster-name <cluster>
  # --nodegroup-name <pool> --launch-template id=<id>,version=<version>`.
  # The ID is the group's launchTemplate.id in `aws eks describe-nodegroup`,
  # and the version is that template's LatestVersionNumber in
  # `aws ec2 describe-launch-templates`. Without `--kubernetes-version` and
  # `--release-version`, the call can also upgrade the group's AMI and
  # Kubernetes version. To change only the template, pass the group's current
  # version and releaseVersion from describe-nodegroup; EKS's documentation
  # doesn't say how it treats unchanged values next to a new template
  # version, so confirm the update's parameters with `aws eks describe-update`
  # before relying on it. A group running
  # slower pods can't roll in place, and draining it first doesn't help: its
  # pods land on its other old-version nodes. `--force` gets the roll past
  # PodEvictionFailure only by deleting those pods regardless of their
  # disruption budgets, so don't use it on them. Move the workload to a new
  # group instead, which starts on the latest version; that's also how to
  # finish a roll that failed partway and left the group on mixed versions.
  # The space module's general pools have that path built in, general-2 and
  # the per-AZ groups. Any other pool needs a second pool key added for it,
  # which for the space module's other pools means changing the module.
  # Don't `-replace` a group that runs nodes: EKS deletes them with it.
  # The next refresh records the version the group runs, so neither path
  # leaves drift. Until a group is moved, the check below warns about it.
  lifecycle {
    ignore_changes = [
      scaling_config[0].desired_size,
      launch_template[0].version,
    ]
  }
}

# A warning, not an error: a held group behind its template is expected until
# someone moves it. The plan that changes a template can't name the groups,
# since the new version isn't known until apply, so it reports the check as
# known after apply; the apply and every later plan name them.
locals {
  managed_node_groups_behind_launch_template = {
    for name, group in aws_eks_node_group.managed : name => group.launch_template[0].version
    if group.launch_template[0].version != tostring(aws_launch_template.node_group[name].latest_version)
  }
}

check "managed_node_groups_on_latest_launch_template" {
  assert {
    condition     = length(local.managed_node_groups_behind_launch_template) == 0
    error_message = "Node groups ${join(", ", [for name, version in local.managed_node_groups_behind_launch_template : "${name} (version ${version}, latest ${aws_launch_template.node_group[name].latest_version})"])} run an older version of their launch template, and an apply doesn't move them. The comment on aws_eks_node_group.managed in the eks-cluster module describes how to move them."
  }
}

locals {
  managed_node_groups_outside_bounds = [
    for name, group in aws_eks_node_group.managed : name
    if group.scaling_config[0].desired_size < group.scaling_config[0].min_size ||
    group.scaling_config[0].desired_size > group.scaling_config[0].max_size
  ]
}
