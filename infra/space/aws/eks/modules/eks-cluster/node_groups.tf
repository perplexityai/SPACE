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

  # These groups' size moves only outside Terraform: an operator resizing them
  # through the EKS API, or a cluster-autoscaler the caller runs on one of them,
  # which sets the ASG's desired capacity between the min and max it reads live
  # from that ASG. Without this, the next apply for any unrelated change would
  # reset each group to its creation-time sizing and scale it down under
  # whatever is running on it. The sizes in node_pools still decide what a new
  # cluster comes up with.
  lifecycle {
    ignore_changes = [
      scaling_config[0].min_size,
      scaling_config[0].desired_size,
      scaling_config[0].max_size,
    ]
  }
}
