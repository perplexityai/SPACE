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
    # As on general: the recorded floor only while this pool serves.
    min_size = var.general_serving_node_group == "general-2" ? var.general_blue_green_node_group.recorded_active_size : 0
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

  # Same ownership split as module.eks's managed groups, and the same
  # constraint on raising min_size above the live desired size: min and max
  # are code-owned, desired moves through EKS or, while this pool serves,
  # cluster-autoscaler. So switching general_serving_node_group to a pool
  # parked at zero, this one, general or a per-AZ group, needs it grown
  # through EKS first, and the switch, like any bound change, should be
  # planned immediately before it's applied (see the per-AZ steps below).
  #
  # It also stays on the launch-template version it was created or last
  # rolled with, as module.eks's managed groups do and for the same reason:
  # general can run pods that take longer to leave than a rolling update
  # waits. So a parked pool can fall behind general's template, and the check
  # after the per-AZ groups warns while it does. To switch
  # general_serving_node_group to one that has, first empty it if it runs
  # nodes, the way step 2 below empties an old group (cordon, drain through
  # the eviction API, then scale to zero through EKS). Then replace it, in an
  # apply with nothing else in it and no saved plan. Nothing in this module
  # refuses the replacement while the group runs nodes, and EKS deletes a
  # group with its nodes, so check that its ASG runs no instances immediately
  # before running `terraform apply -replace='<its address>'`, quoted because
  # the address has brackets. The apply creates it again on the template's
  # latest version, the cluster's Kubernetes version and its newest AMI, at
  # its configured desired size. Then plan and apply the switch.
  #
  # A serving pool takes a new version by moving its workload to a pool
  # that's already on it: the steps below for the per-AZ groups, and the same
  # declare, switch, drain and park sequence for general-2 or general,
  # declaring the target again first if it was removed.
  lifecycle {
    ignore_changes = [
      scaling_config[0].desired_size,
      launch_template[0].version,
    ]
  }
}

# One general pool per availability zone, each in that zone's private subnet
# only. A multi-AZ ASG rebalances once a scale-down leaves its zones uneven:
# AZRebalance launches in the short zone and terminates elsewhere, and EKS
# drains that termination without honoring PodDisruptionBudgets and kills the
# node after 15 minutes. A single-zone ASG has nothing to rebalance. Like
# general-2, each group runs on general's launch template, role, labels and
# taints, so cluster-autoscaler sees the groups as similar and can balance
# scale-ups across them, and general workloads move without a Helm change.
#
# Moving a cluster onto them takes three applies, each with nothing else in it:
# 1. Declare general_per_az_node_groups. The groups come up at their floors
#    beside the serving group, which keeps serving. Their minimums stay zero
#    until they serve.
# 2. Set general_serving_node_group = "general-per-az". The discovery tags move,
#    so cluster-autoscaler stops scaling the old group and balances the new
#    ones, and the per-AZ minimums rise to each zone's min_size while the old
#    group's falls to zero. The plan refuses a minimum above a group's live
#    desired size, so grow any per-AZ group scaled below its floor through EKS
#    and plan again. Each bound change sends the desired size from the plan,
#    and cluster-autoscaler is still scaling the old group until this apply
#    moves its tag, so plan immediately before applying: a saved plan applied
#    after the autoscaler has grown the old group shrinks it back.
#    Then cordon the old group's nodes and drain them through the
#    eviction API, which honors PodDisruptionBudgets and each pod's grace
#    period; the evicted pods land on the per-AZ groups, which
#    cluster-autoscaler grows as they go Pending (without it, grow them through
#    EKS first). Only then scale the old group
#    to zero through EKS. A smaller desired size doesn't honor the budgets, and
#    EKS terminates a node 15 minutes into its drain, so it must find the nodes
#    empty.
# 3. Once the old group runs no nodes, remove it: general_node_group_retired
#    for general, or drop general_blue_green_node_group for general-2. The plan
#    refuses either while the group still wants or runs an instance. Removing
#    the ASG also removes any process suspended on it, AZRebalance included; an
#    old group that's kept instead needs its suspended processes resumed.
#
# The plan refuses removing any group cluster-autoscaler still discovers, a
# zone dropped while the per-AZ groups serve among them: it keeps scaling that
# group, smallest first, until the apply that removes it. Serve from another
# group in an earlier apply first.
locals {
  # Keyed by node group name, with each group's zone and sizes.
  general_per_az_node_groups = var.general_per_az_node_groups == null ? {} : {
    for zone, sizes in var.general_per_az_node_groups.zones : "general-${zone}" => merge(sizes, { zone = zone })
  }
  general_per_az_floors = { for name, group in local.general_per_az_node_groups : name => group.min_size }
}

resource "aws_eks_node_group" "general_per_az" {
  for_each = local.general_per_az_node_groups

  cluster_name    = module.eks.node_group_cluster_name
  node_group_name = each.key
  node_role_arn   = module.eks.node_role_arns["general"]
  subnet_ids      = [data.aws_subnet.private[each.value.zone].id]

  ami_type       = "AL2023_x86_64_STANDARD"
  capacity_type  = local.node_pools.general.capacity_type
  instance_types = var.general_per_az_node_groups.instance_types

  launch_template {
    id      = module.eks.launch_templates["general"].id
    version = module.eks.launch_templates["general"].latest_version
  }

  labels = local.node_pools.general.labels

  scaling_config {
    desired_size = each.value.min_size
    max_size     = each.value.max_size
    # As on general-2: each zone's floor only while the per-AZ groups serve.
    min_size = var.general_serving_node_group == "general-per-az" ? each.value.min_size : 0
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
    Name = "${local.name}-${each.key}"
  })

  timeouts {
    create = "2h"
    update = "5h"
    delete = "2h"
  }

  # The same ownership split as general-2, and the same launch-template hold.
  # A parked per-AZ group keeps the nodes it came up with, so empty it before
  # replacing it, as general-2's comment describes.
  lifecycle {
    ignore_changes = [
      scaling_config[0].desired_size,
      launch_template[0].version,
    ]
  }
}

# A warning, as for module.eks's managed groups, and with the same timing:
# general-2 and the per-AZ groups stay on their launch-template version until
# someone moves them.
locals {
  general_node_groups_behind_launch_template = {
    for name, group in merge(
      { for g in aws_eks_node_group.general_blue_green : "general-2" => g },
      aws_eks_node_group.general_per_az,
    ) : name => group.launch_template[0].version
    if group.launch_template[0].version != tostring(module.eks.launch_templates["general"].latest_version)
  }
}

check "general_node_groups_on_latest_launch_template" {
  assert {
    condition     = length(local.general_node_groups_behind_launch_template) == 0
    error_message = "Node groups ${join(", ", [for name, version in local.general_node_groups_behind_launch_template : "${name} (version ${version})"])} run an older version of general's launch template than its latest, ${module.eks.launch_templates["general"].latest_version}, and an apply doesn't move them. The comment on aws_eks_node_group.general_blue_green in the space module describes how to move them."
  }
}

# Removing a general MNG deletes it together with any node it still runs, so
# the general_serving_node_group output refuses the plan until the group is
# parked: its ASG wants no instances and none are left. A group is being
# removed when EKS still reports it and this configuration no longer declares
# it: general once general_node_group_retired, general-2 once
# general_blue_green_node_group is dropped, and general-<zone> once its zone
# leaves general_per_az_node_groups. The lookups run only while general is
# retired or the per-AZ groups are declared. Running them everywhere would add
# AWS reads to every cluster's plan, and fail it under a module-level
# depends_on. So, unless general is retired, two removals aren't checked and
# need parking by hand first: dropping general-2 from a cluster with no per-AZ
# groups, and setting general_per_az_node_groups back to null. Each lookup below runs
# only if the one before it found its target, and lists rather than reads, so
# none fails on a missing cluster or node group. Once the removal has landed,
# the group isn't among the cluster's node groups and the check passes. The
# cluster name is read from local.name rather than module.eks so that the
# lookups run at plan time even when the cluster itself has pending changes.
#
# Retiring general also needs a cluster that exists. A new cluster comes up
# with general: its deployment-backed addons wait only on node groups inside
# module.eks, while general-2 and the per-AZ groups wait only on the pre-node
# addons (vpc-cni, kube-proxy), so a cluster created retired would install its
# other addons with no node group they wait for.
locals {
  retiring_general_checked = var.general_node_group_retired || var.general_per_az_node_groups != null
  # Matched against what EKS reports rather than built from the zones still
  # configured, so a zone dropped from availability_zones in the same apply is
  # still checked.
  general_node_group_name_pattern = "^general(-2|-[a-z]{2}(-[a-z]+)+-[0-9]+([a-z]|-[a-z]+-[0-9]+[a-z]))?$"
  declared_general_node_group_names = concat(
    var.general_node_group_retired ? [] : ["general"],
    var.general_blue_green_node_group == null ? [] : ["general-2"],
    keys(local.general_per_az_node_groups),
  )
}

data "aws_eks_clusters" "retiring_general" {
  count = local.retiring_general_checked ? 1 : 0
}

data "aws_eks_node_groups" "retiring_general" {
  count = length([for clusters in data.aws_eks_clusters.retiring_general : true if contains(clusters.names, local.name)])

  cluster_name = local.name
}

data "aws_eks_node_group" "retiring_general" {
  for_each = toset(flatten([
    for groups in data.aws_eks_node_groups.retiring_general :
    [
      for name in groups.names : name
      if can(regex(local.general_node_group_name_pattern, name)) && !contains(local.declared_general_node_group_names, name)
    ]
  ]))

  cluster_name    = local.name
  node_group_name = each.value
}

locals {
  retiring_general_asg_names = toset(flatten([
    for group in values(data.aws_eks_node_group.retiring_general) : [
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

# A per-AZ group's instance type, capacity type, AMI type and subnet force its
# replacement (so does its node role, which isn't compared: it can be unknown
# until apply, and the comparison has to be known at plan time), and
# its fixed name means EKS deletes the old group, nodes and all, before the new
# one exists. So the general_serving_node_group output refuses a change to any
# of the four while the per-AZ groups serve, or while a changing group still
# wants or runs a node. Each declared group that EKS reports is read live; the
# reads ride on the same cluster listing as the removal check.
data "aws_eks_node_group" "general_per_az_live" {
  for_each = toset(flatten([
    for groups in data.aws_eks_node_groups.retiring_general :
    [for name in keys(local.general_per_az_node_groups) : name if contains(groups.names, name)]
  ]))

  cluster_name    = local.name
  node_group_name = each.value
}

locals {
  general_per_az_replacing = {
    for name, group in data.aws_eks_node_group.general_per_az_live : name => group
    if(
      tolist(group.instance_types) != tolist(var.general_per_az_node_groups == null ? [] : var.general_per_az_node_groups.instance_types) ||
      group.capacity_type != local.node_pools.general.capacity_type ||
      group.ami_type != "AL2023_x86_64_STANDARD" ||
      toset(group.subnet_ids) != toset([data.aws_subnet.private[local.general_per_az_node_groups[name].zone].id])
    )
  }
  general_per_az_replacing_asg_names = toset(flatten([
    for group in values(local.general_per_az_replacing) : [
      for r in group.resources : [for asg in r.autoscaling_groups : asg.name]
    ]
  ]))
}

# cluster-autoscaler can scale a replacing group up again until its discovery
# tag is gone, so the tag is read live, as for a removal.
data "aws_autoscaling_group" "general_per_az_replacing" {
  for_each = local.general_per_az_replacing_asg_names

  name = each.value
}

data "aws_instances" "general_per_az_replacing" {
  for_each = local.general_per_az_replacing_asg_names

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
