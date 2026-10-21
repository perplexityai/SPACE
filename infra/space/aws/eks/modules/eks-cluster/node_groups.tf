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
  # applying a min/max change, check the plan's desired_size against the live
  # group, and keep the change out of an apply that also rolls the group: the
  # provider rolls first, which can take hours, and only then sends the
  # desired size it planned.
  #
  # An out-of-band min/max change through the EKS API is drift: every later
  # apply on the cluster, whatever it's for, plans to revert it, and the plan
  # fails while the live desired size sits outside the configured bounds.
  # Record an emergency resize in node_pools rather than leaving it live-only.
  lifecycle {
    ignore_changes = [
      scaling_config[0].desired_size,
    ]
  }
}

locals {
  managed_node_groups_outside_bounds = [
    for name, group in aws_eks_node_group.managed : name
    if group.scaling_config[0].desired_size < group.scaling_config[0].min_size ||
    group.scaling_config[0].desired_size > group.scaling_config[0].max_size
  ]
}
