# Standalone launch template shared by SPACE worker MNG types. Its consumers
# deliberately have different lifecycle contracts:
#
# - Rollout-managed MNGs (`workers-cgnat` and `module "workers_N"`) ignore
#   launch-template changes. A new template version is adopted through a new,
#   sandbox-aware rollout MNG instead of an EKS in-place node recycle.
# - Permanent `aws_eks_node_group.worker_capacity` MNGs reconcile the latest
#   version so a dormant group cannot later scale from a stale AMI/bootstrap.
#   Applying that change to a nonempty capacity group is therefore a worker
#   rollout: drain and scale the group to zero first, or use the sandbox-aware
#   rollout procedure.
#
# This resource is hoisted out of the per-pool launch templates (now
# `module.eks.aws_launch_template.node_group`) so flipping
# `var.workers_cgnat_retired` to `true` (and dropping `workers-cgnat` from
# `local.node_pools`) doesn't cascade-destroy the LT that workers-1 still
# references in EKS state.
#
# `name_prefix` keeps the legacy `workers-cgnat-` token so the AWS-side
# LT name (and therefore the LT identity) survives the state-only `moved`
# below. Changing `name_prefix` would force LT replacement — and because
# `module.workers_1`'s `ignore_changes = [launch_template]` deliberately
# hides LT diffs from EKS, workers-1 would continue launching from the
# now-deleted LT id and scale-out would silently fail. The cosmetic
# carry-over is intentional and goes away naturally on the next per-
# rollout LT (the workers-N → workers-(N+1) rollout adds its own
# `aws_launch_template.workers_N+1` block).
resource "aws_launch_template" "workers" {
  name_prefix            = "${local.name}-workers-cgnat-"
  image_id               = local.workers_ami_id
  update_default_version = true
  vpc_security_group_ids = [
    module.eks.node_security_group_id,
    module.eks.cluster_security_group_id,
  ]

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      delete_on_termination = true
      encrypted             = true
      volume_size           = var.workers_disk_size_gib
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
    for_each = var.workers_capacity_reservation_preference == null && local.workers_capacity_reservation_resource_group_arn == null ? [] : [1]

    content {
      capacity_reservation_preference = var.workers_capacity_reservation_preference

      dynamic "capacity_reservation_target" {
        for_each = local.workers_capacity_reservation_resource_group_arn == null ? [] : [1]

        content {
          capacity_reservation_resource_group_arn = local.workers_capacity_reservation_resource_group_arn
        }
      }
    }
  }

  user_data = local.workers_ami_id == null ? null : base64encode(templatefile(
    "${path.module}/templates/worker-bootstrap.sh.tftpl",
    {
      cluster_endpoint           = module.eks.cluster_endpoint
      cluster_name               = module.eks.cluster_name
      cluster_service_cidr       = var.cluster_service_cidr
      cluster_ca_data            = module.eks.cluster_certificate_authority_data
      space_cache_nvme_mount     = file("${path.module}/templates/space-cache-nvme-mount.sh")
      bootstrap_bucket           = var.bootstrap_bucket
      shim_dogstatsd_socket_path = var.worker_shim_dogstatsd_socket_path
      shim_environment           = var.environment
    },
  ))

  tag_specifications {
    resource_type = "instance"

    tags = merge(local.worker_common_tags, {
      Name = "${local.name}-workers-cgnat"
    })
  }

  tag_specifications {
    resource_type = "volume"

    tags = merge(local.worker_common_tags, {
      Name = "${local.name}-workers-cgnat"
    })
  }

  tags = merge(local.worker_common_tags, {
    Name = "${local.name}-workers-cgnat"
  })
}

locals {
  # Keyed by ASG name, which AWS assigns when the worker nodegroups are
  # created — so the for_each below cannot be evaluated on the very first
  # apply of a fresh environment. The variable gate lets bootstrap runs skip
  # tag propagation until the nodegroups exist in state.
  worker_asg_propagated_tag_entries = !var.worker_asg_tag_propagation_enabled ? {} : merge(
    {},
    [
      for asg_name in local.cluster_controller_worker_asg_names : {
        for key, value in local.worker_common_tags : "${asg_name}/${key}" => {
          asg_name = asg_name
          key      = key
          value    = value
        }
      }
    ]...
  )
}

resource "aws_autoscaling_group_tag" "worker_common" {
  for_each = local.worker_asg_propagated_tag_entries

  autoscaling_group_name = each.value.asg_name

  tag {
    key                 = each.value.key
    value               = each.value.value
    propagate_at_launch = true
  }
}

# Custom-AMI worker pools whose scaling is driven out-of-band by the
# two-MNG rollout script (`scripts/space-rollout-mng.py`): source MNG
# `min`/`desired`/`max` shrink wave-by-wave as nodes drain. Without
# `lifecycle.ignore_changes` on all three scaling fields, an unrelated
# Atlantis apply mid-rollout would reset the source pool to its tfvars
# values, re-launching terminated nodes and breaking the rollout.
# Outside rollouts, steady-state resizing also has to go through the EKS
# API followed by a tfvars bump to keep the recorded size honest; the
# `rollout-mng` module follows the same pattern intentionally.
resource "aws_eks_node_group" "rollout_managed" {
  # Unfiltered: workers_ami_id's validation guarantees a worker AMI, so the
  # keys don't depend on an AMI ID that may be unknown until apply.
  for_each = local.legacy_worker_node_pools

  # Resolves after the pre-node addons, so nodes can find a CNI.
  cluster_name    = module.eks.node_group_cluster_name
  node_group_name = each.key
  # Legacy per-pool IAM for the workers-cgnat MNG. The two-MNG rollout
  # pattern moves to a shared standalone `aws_iam_role.workers_pool` (see
  # `iam.tf`) starting with the workers-1 MNG; workers-cgnat keeps its
  # per-pool role until retired (touching `node_role_arn` on a live MNG
  # is destructive — `aws_eks_node_group.node_role_arn` is `ForceNew` in
  # the AWS provider).
  node_role_arn = aws_iam_role.node[each.key].arn
  subnet_ids    = each.value.subnet_ids

  ami_type       = "CUSTOM"
  capacity_type  = each.value.capacity_type
  instance_types = each.value.instance_types

  # Standalone worker LT. Only ever `workers-cgnat` lands in this
  # resource (`local.legacy_worker_node_pools` holds at most that pool),
  # so the reference doesn't need a key dispatch.
  # Same LT is referenced by `module.workers_1` in `rollout.tf`.
  launch_template {
    id      = aws_launch_template.workers.id
    version = aws_launch_template.workers.latest_version
  }

  labels = each.value.labels

  scaling_config {
    desired_size = each.value.desired_size
    max_size     = each.value.max_size
    min_size     = each.value.min_size
  }

  update_config {
    max_unavailable = 1
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
    aws_iam_role_policy_attachment.node_managed,
    aws_iam_role_policy_attachment.node_s3_read,
  ]

  tags = merge(local.worker_common_tags, {
    Name = "${local.name}-${each.key}"
  })

  timeouts {
    update = "5h"
  }

  # `launch_template` is ignored for the same reason as on the rollout-mng
  # module (see `modules/rollout-mng/main.tf`): bumping `latest_version`
  # via an in-place LT body edit would push EKS into a rolling update of
  # every node at `max_unavailable=1`, which on `m5d.metal` / `r6id.metal`
  # workers is hours-to-days of replacement. Any LT delta belongs in the
  # next workers-N rollout, not an in-place roll of this MNG.
  lifecycle {
    ignore_changes = [
      scaling_config[0].min_size,
      scaling_config[0].desired_size,
      scaling_config[0].max_size,
      launch_template,
    ]
  }
}

# A second managed-AMI general pool gives a cluster an additive blue/green
# target without replacing the fixed-name `general` MNG. It deliberately
# reuses general's role, launch template, subnets, labels, and taints so normal
# general workloads can move between the two pools without a Helm change.
resource "aws_eks_node_group" "general_blue_green" {
  count = var.general_blue_green_node_group == null ? 0 : 1

  # Both resolve only once the core module's pre-node addons and the general
  # role's policy attachments exist.
  cluster_name    = module.eks.node_group_cluster_name
  node_group_name = "general-2"
  node_role_arn   = module.eks.node_role_arns["general"]
  subnet_ids      = local.node_pools.general.subnet_ids

  ami_type       = "AL2023_x86_64_STANDARD"
  capacity_type  = local.node_pools.general.capacity_type
  instance_types = var.general_blue_green_node_group.instance_types

  launch_template {
    id      = module.eks.launch_templates["general"].id
    version = module.eks.launch_templates["general"].latest_version
  }

  labels = local.node_pools.general.labels

  scaling_config {
    desired_size = var.general_blue_green_node_group.recorded_active_size
    max_size     = var.general_blue_green_node_group.max_size
    min_size     = var.general_blue_green_node_group.recorded_active_size
  }

  update_config {
    max_unavailable = 1
  }

  dynamic "taint" {
    for_each = local.node_pools.general.taints

    content {
      effect = taint.value.effect
      key    = taint.value.key
      value  = taint.value.value
    }
  }

  tags = merge(var.common_tags, {
    Name = "${local.name}-general-2"
  })

  timeouts {
    create = "2h"
    update = "5h"
    delete = "2h"
  }

  # After creation, operators scale this pool between zero and its active
  # target through EKS, and while it serves, cluster-autoscaler moves its
  # desired size inside those bounds. Unrelated Terraform applies must not
  # wake a dormant pool or scale down one that is carrying the active general
  # workload.
  lifecycle {
    ignore_changes = [
      scaling_config[0].min_size,
      scaling_config[0].desired_size,
      scaling_config[0].max_size,
    ]
  }
}

# general_node_group_retired deletes the general MNG together with any node it
# still runs, so the general_serving_node_group output refuses the plan until
# general is parked: its ASG wants no instances and none are left. Each lookup
# below runs only if the one before it found its target, and lists rather than
# reads, so none fails on a missing cluster or node group. Once retirement has
# landed, general isn't among the cluster's node groups and the check passes.
# The cluster name is read from local.name rather than module.eks so that the
# lookups run at plan time even when the cluster itself has pending changes.
#
# Retirement also needs a cluster that exists. A new cluster comes up with
# general: its deployment-backed addons wait only on node groups inside
# module.eks, while general-2 waits only on the pre-node addons (vpc-cni,
# kube-proxy), so a cluster created retired would install its other addons
# with no node group they wait for.
data "aws_eks_clusters" "retiring_general" {
  count = var.general_node_group_retired ? 1 : 0
}

data "aws_eks_node_groups" "retiring_general" {
  count = length([for clusters in data.aws_eks_clusters.retiring_general : true if contains(clusters.names, local.name)])

  cluster_name = local.name
}

data "aws_eks_node_group" "retiring_general" {
  count = length([for groups in data.aws_eks_node_groups.retiring_general : true if contains(groups.names, "general")])

  cluster_name    = local.name
  node_group_name = "general"
}

locals {
  retiring_general_asg_names = toset(flatten([
    for group in data.aws_eks_node_group.retiring_general : [
      for r in group.resources : [for asg in r.autoscaling_groups : asg.name]
    ]
  ]))
}

data "aws_autoscaling_group" "retiring_general" {
  for_each = local.retiring_general_asg_names

  name = each.value
}

# A desired capacity of zero only says the ASG has stopped asking for nodes;
# the ones it's removing can still be draining. Every instance still alive in
# the ASG counts until it's gone.
data "aws_instances" "retiring_general" {
  for_each = local.retiring_general_asg_names

  instance_tags = {
    "aws:autoscaling:groupName" = each.value
  }
  instance_state_names = ["pending", "running", "shutting-down", "stopping", "stopped"]
}

data "aws_iam_policy_document" "irsa_assume_role" {
  # Excludes `workers-cgnat` unconditionally — its IRSA role is now
  # owned by the standalone `aws_iam_role.workers_service_account`
  # below so retiring the legacy MNG via `workers_cgnat_retired = true`
  # doesn't break the workers DaemonSet IRSA (the Helm values in
  # `deploy/<env>/space-worker.values.yaml` still reference the role's
  # AWS-side name and ARN).
  for_each = {
    for key, value in local.node_pools : key => value
    if key != "workers-cgnat"
  }

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer_hostpath}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer_hostpath}:sub"
      values = [
        "system:serviceaccount:${each.value.service_account.namespace}:${each.value.service_account.name}",
      ]
    }

    principals {
      identifiers = [module.eks.oidc_provider_arn]
      type        = "Federated"
    }
  }
}

resource "aws_iam_role" "service_account" {
  for_each = {
    for key, value in local.node_pools : key => value
    if key != "workers-cgnat"
  }

  assume_role_policy = data.aws_iam_policy_document.irsa_assume_role[each.key].json
  name               = "${local.name}-${each.key}-sa"

  tags = merge(var.common_tags, {
    Name = "${local.name}-${each.key}-sa"
  })
}

resource "aws_iam_role_policy_attachment" "service_account_s3_read" {
  for_each = length(var.s3_read_bucket_names) > 0 ? {
    for key, value in local.node_pools : key => value
    if key != "workers-cgnat"
  } : {}

  policy_arn = aws_iam_policy.s3_read[0].arn
  role       = aws_iam_role.service_account[each.key].name
}

# Standalone IRSA role for the SPACE worker ServiceAccounts
# (`space/workers` and `space/space-node-manager`). Hoisted out of the per-pool
# `aws_iam_role.service_account` for_each so retiring `workers-cgnat`
# from `local.node_pools` doesn't cascade-destroy the role that the
# Helm-deployed `workers` Kubernetes ServiceAccount still annotates as
# `eks.amazonaws.com/role-arn` (see
# `infra/space/aws/eks/deploy/<env>/space-worker.values.yaml`).
#
# The role's AWS-side name keeps the legacy `${local.name}-workers-cgnat-sa`
# token so the AWS role ARN survives the state-only `moved` below.
# Changing the name would force role recreation and a new ARN, which
# would silently break IRSA on every worker DaemonSet pod until the
# Helm values are bumped and the workloads roll. The cosmetic carry-
# over goes away on the next Helm values refresh that retires the
# legacy annotation.
data "aws_iam_policy_document" "workers_service_account_irsa_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer_hostpath}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer_hostpath}:sub"
      values = [
        "system:serviceaccount:${var.workers_service_account_namespace}:${var.workers_service_account_name}",
        # Stable and canary node-managers use this dedicated SA to stage guest kernels.
        "system:serviceaccount:${var.workers_service_account_namespace}:space-node-manager",
      ]
    }

    principals {
      identifiers = [module.eks.oidc_provider_arn]
      type        = "Federated"
    }
  }
}

resource "aws_iam_role" "workers_service_account" {
  assume_role_policy = data.aws_iam_policy_document.workers_service_account_irsa_assume_role.json
  name               = "${local.name}-workers-cgnat-sa"

  tags = merge(var.common_tags, {
    Name = "${local.name}-workers-cgnat-sa"
  })
}

resource "aws_iam_role_policy_attachment" "workers_service_account_s3_read" {
  count = length(var.s3_read_bucket_names) > 0 ? 1 : 0

  policy_arn = aws_iam_policy.s3_read[0].arn
  role       = aws_iam_role.workers_service_account.name
}
