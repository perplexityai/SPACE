# cluster-autoscaler for the serving general node groups, and nothing else.
#
# It discovers those groups by a SPACE-specific tag this module puts on each
# one's ASG, not by the stock `k8s.io/cluster-autoscaler/enabled` +
# `k8s.io/cluster-autoscaler/<cluster>` pair, because EKS stamps that pair on
# every managed node group's ASG: workers, builders and cred-authority as well
# as general. Worker nodes carry Firecracker sandboxes whose drain path
# cluster-autoscaler knows nothing about, so the stock pair would let it scale
# those pools down under live sandboxes. The IAM write actions are scoped to
# the same ASG ARNs for the same reason.
#
# Discovery rather than `--nodes=<min>:<max>:<asg>` because a discovered group's
# bounds are the ASG's own min and max, read live: the caller's apply sets them,
# and cluster-autoscaler follows without its arguments changing. The caller
# leaves the group's desired size to cluster-autoscaler, so only an apply that
# changes min or max sends a desired size back to the group, the one in its
# plan, or one that creates or replaces the group, which comes up at its
# configured size.
#
# Several groups are the general pool split one per zone. A single-zone ASG
# never runs AZRebalance, which EKS drains without honoring PodDisruptionBudgets
# and cuts off after 15 minutes. --balance-similar-node-groups then spreads each
# scale-up over the groups, adding to the smallest first. Scale-down doesn't
# balance: it removes the nodes it finds unneeded wherever they are, and the
# next scale-up evens the zones out again.

locals {
  # Pinned to the control plane's minor, as upstream recommends. Add the next
  # minor here before bumping `kubernetes_version` on a cluster that runs it;
  # the precondition on the release fails the plan until then. Upstream marks
  # --scale-down-enabled (set below) deprecated, so check each new minor still
  # accepts it. The chart's RBAC is built for appVersion 1.35.0 (chart 9.59.0),
  # a minor behind the 1.36 image; check the cluster role against each new
  # image too.
  cluster_autoscaler_image_tags = {
    "1.35" = "v1.35.2"
    "1.36" = "v1.36.1"
  }

  # The default cutoff, spelled out so it's a reviewable literal rather than an
  # upstream default that can move under a chart bump. Pods below it are
  # expendable: cluster-autoscaler adds no node to run one and doesn't count one
  # as a reason to keep a node. Every PriorityClass a general-pool workload
  # names has to sit at or above it, the SPACE ones in
  # infra/space/kind/manifests/priorityclasses.yaml included.
  cluster_autoscaler_expendable_pods_priority_cutoff = -10

  # The discovery tag, unique per cluster so a tagged ASG can't be picked up by
  # another cluster's autoscaler in the same account.
  cluster_autoscaler_discovery_tag_key = "space.pplx.net/cluster-autoscaler/${var.cluster_name}"

  cluster_autoscaler_node_groups = var.cluster_autoscaler == null ? [] : var.cluster_autoscaler.node_groups
  cluster_autoscaler_balanced    = length(local.cluster_autoscaler_node_groups) >= 2
}

# Moving general_serving_node_group retargets these tags, so the old groups
# drop out of discovery and the new ones join it in the same apply. A single
# group keeps the indexed address it had before there could be several. Several
# are keyed by group name, so adding or removing one leaves the others' tags
# alone: replacing an indexed tag whose ASG shifted could delete a tag another
# index had just created on that ASG, since both name the same key. A list that
# goes from one group to several shouldn't keep its first group: that group's
# indexed tag and its keyed tag would be the same tag, raced the same way. The
# space module never does this; a single group and a per-AZ set never share a
# group.
resource "aws_autoscaling_group_tag" "cluster_autoscaler" {
  count = length(local.cluster_autoscaler_node_groups) == 1 ? 1 : 0

  autoscaling_group_name = local.cluster_autoscaler_node_groups[0].autoscaling_group_name

  lifecycle {
    precondition {
      condition     = local.cluster_autoscaler_node_groups[0].autoscaling_group_name != null
      error_message = "cluster-autoscaler needs node group ${local.cluster_autoscaler_node_groups[0].name}'s ASG name, and the group reports none; check the node group's health in EKS."
    }
    precondition {
      condition     = length(local.cluster_autoscaler_discovery_tag_key) <= 128
      error_message = "The discovery tag key ${local.cluster_autoscaler_discovery_tag_key} is longer than the 128 characters AWS allows; the cluster name is too long for it."
    }
  }

  tag {
    key                 = local.cluster_autoscaler_discovery_tag_key
    value               = "owned"
    propagate_at_launch = false
  }
}

resource "aws_autoscaling_group_tag" "cluster_autoscaler_balanced" {
  for_each = local.cluster_autoscaler_balanced ? { for group in local.cluster_autoscaler_node_groups : group.name => group } : {}

  autoscaling_group_name = each.value.autoscaling_group_name

  lifecycle {
    precondition {
      condition     = each.value.autoscaling_group_name != null
      error_message = "cluster-autoscaler needs node group ${each.key}'s ASG name, and the group reports none; check the node group's health in EKS."
    }
    precondition {
      condition     = length(local.cluster_autoscaler_discovery_tag_key) <= 128
      error_message = "The discovery tag key ${local.cluster_autoscaler_discovery_tag_key} is longer than the 128 characters AWS allows; the cluster name is too long for it."
    }
  }

  tag {
    key                 = local.cluster_autoscaler_discovery_tag_key
    value               = "owned"
    propagate_at_launch = false
  }
}

data "aws_iam_policy_document" "cluster_autoscaler_assume_role" {
  count = var.cluster_autoscaler == null ? 0 : 1

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_issuer_host}:sub"
      values   = ["system:serviceaccount:kube-system:cluster-autoscaler"]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_issuer_host}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster_autoscaler" {
  count = var.cluster_autoscaler == null ? 0 : 1

  name               = var.cluster_autoscaler.role_name
  assume_role_policy = data.aws_iam_policy_document.cluster_autoscaler_assume_role[0].json
}

data "aws_iam_policy_document" "cluster_autoscaler" {
  count = var.cluster_autoscaler == null ? 0 : 1

  # The Describe and Get calls take no resource-level scoping.
  statement {
    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeLaunchConfigurations",
      "autoscaling:DescribeScalingActivities",
      "autoscaling:DescribeTags",
      "ec2:DescribeImages",
      "ec2:DescribeInstanceTypes",
      "ec2:DescribeLaunchTemplateVersions",
      "ec2:GetInstanceTypesFromInstanceRequirements",
    ]
    resources = ["*"]
  }

  statement {
    actions   = ["eks:DescribeNodegroup"]
    resources = local.cluster_autoscaler_node_groups[*].node_group_arn
  }

  statement {
    actions = [
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
    ]
    resources = local.cluster_autoscaler_node_groups[*].autoscaling_group_arn
  }
}

resource "aws_iam_role_policy" "cluster_autoscaler" {
  count = var.cluster_autoscaler == null ? 0 : 1

  name   = var.cluster_autoscaler.role_name
  role   = aws_iam_role.cluster_autoscaler[0].id
  policy = data.aws_iam_policy_document.cluster_autoscaler[0].json
}

resource "helm_release" "cluster_autoscaler" {
  count = var.cluster_autoscaler == null ? 0 : 1

  name       = "cluster-autoscaler"
  repository = "https://kubernetes.github.io/autoscaler"
  chart      = "cluster-autoscaler"
  namespace  = "kube-system"
  version    = "9.59.0"

  values = [yamlencode({
    cloudProvider    = "aws"
    awsRegion        = var.aws_region
    fullnameOverride = "cluster-autoscaler"
    # The chart renders `--node-group-auto-discovery=asg:tag=<tags>` once
    # clusterName is set; `tags` replaces the stock pair rather than adding to
    # it, so only the ASG carrying this key matches.
    autoDiscovery = {
      clusterName = var.cluster_name
      tags        = [local.cluster_autoscaler_discovery_tag_key]
    }
    image = {
      tag = lookup(local.cluster_autoscaler_image_tags, var.cluster_autoscaler.kubernetes_version, null)
    }
    # A pod with an emptyDir (a scratch or upload spool, say) would otherwise
    # pin its node: skip-nodes-with-local-storage keeps any such node, and a
    # workload that runs one on most general nodes would pin most of the pool.
    # An emptyDir dies with its pod on every rollout anyway.
    # skip-nodes-with-system-pods only delays kube-system pods with no PDB: once
    # one is older than --blocking-system-pod-distruption-timeout (default 1h)
    # it's evicted like any other. The Deployments in the kube-system Helm
    # releases this module installs on general all carry budgets; give any the
    # calling root adds there one too before turning scale-down on. The flip
    # side: a single-replica `maxUnavailable: 1` budget allows no disruption
    # while its pod isn't Ready, and the pdb drainability rule blocks the node
    # on that with no timeout, whatever the budget's
    # `unhealthyPodEvictionPolicy`, so a crashlooping singleton pins its node
    # from scale-down until it recovers. Other drains go through the eviction
    # API, which honors that policy: every single-replica budget here comes
    # from the in-repo chart, which sets `AlwaysAllow`, so an unready singleton
    # never blocks a drain.
    #
    # The drain cap and the scale-up delay are added only where a cluster sets
    # them, so a cluster that doesn't keeps the upstream defaults (600s and 0s)
    # with no change to its values. A null can't stand in for "unset": the chart
    # renders a null extraArgs entry as the bare flag.
    extraArgs = merge(
      {
        "scale-down-enabled"              = var.cluster_autoscaler.scale_down_enabled
        "scale-down-unneeded-time"        = "30m"
        "scale-down-delay-after-add"      = "30m"
        "skip-nodes-with-system-pods"     = true
        "skip-nodes-with-local-storage"   = false
        "expendable-pods-priority-cutoff" = local.cluster_autoscaler_expendable_pods_priority_cutoff
      },
      # Caps each evicted pod's grace period during a scale-down drain, and
      # bounds how long the drain waits for the pods to go (the cap plus 30s).
      # A pod with a shorter grace keeps its own. Upstream refuses to start if
      # --drain-priority-config is also set.
      var.cluster_autoscaler.max_graceful_termination_sec == null ? {} : {
        "max-graceful-termination-sec" = var.cluster_autoscaler.max_graceful_termination_sec
      },
      # Pods younger than this aren't considered for scale-up, so a rollout
      # surge pod whose room a terminating pod frees within the delay doesn't
      # add a node.
      var.cluster_autoscaler.new_pod_scale_up_delay == null ? {} : {
        "new-pod-scale-up-delay" = var.cluster_autoscaler.new_pod_scale_up_delay
      },
      # Only with several groups, so a single-group cluster's values don't
      # change. Similar groups need identical labels apart from an ignore list
      # (zone, hostname and eks.amazonaws.com/nodegroup among them). The groups
      # share a launch template, but each one rolls to a new AMI or template
      # version on its own, and during that roll its nodes carry a different
      # nodegroup-image or sourceLaunchTemplateVersion than the other groups'.
      # The chart drops everything from the first `_` in a key, which is how
      # one flag is passed twice. Two maps, so the flag keeps its bool rather
      # than being unified with the labels into strings.
      !local.cluster_autoscaler_balanced ? {} : {
        "balance-similar-node-groups" = true
      },
      !local.cluster_autoscaler_balanced ? {} : {
        "balancing-ignore-label_image"  = "eks.amazonaws.com/nodegroup-image"
        "balancing-ignore-label_launch" = "eks.amazonaws.com/sourceLaunchTemplateVersion"
      },
    )
    priorityClassName = "system-cluster-critical"
    # One replica, and a budget that lets it go: the pod lives on the pool it
    # scales, so a budget that refused its eviction would pin whichever general
    # node it landed on. A rescheduled replica rebuilds its view from the API
    # server and the ASG, losing in-memory state such as scale-down timers and
    # scale-up backoff. The budget comes from disruption_budgets.tf, which can
    # set `AlwaysAllow`; null drops the chart's own.
    replicaCount        = 1
    podDisruptionBudget = null
    # Memory is per cluster because cluster-autoscaler's shared informer caches
    # every pod in the cluster, sandboxes included, and no flag narrows it
    # (main.go builds it with only a managedFields-stripping transform). Request
    # equals limit, so the pod never counts on memory its node hasn't reserved
    # for it. GOMEMLIMIT at 90% of the limit makes the Go GC collect harder as
    # the runtime's total memory (heap, stacks, GC metadata) nears it, instead
    # of letting the initial pod list's garbage push RSS past the limit. The
    # other 10% is for memory the Go runtime doesn't manage.
    resources = {
      requests = {
        cpu    = "100m"
        memory = "${var.cluster_autoscaler.memory_mib}Mi"
      }
      limits = {
        memory = "${var.cluster_autoscaler.memory_mib}Mi"
      }
    }
    extraEnv = {
      GOMEMLIMIT = "${floor(var.cluster_autoscaler.memory_mib * 0.9)}MiB"
    }
    nodeSelector = {
      "space.pplx.net/node-pool" = "general"
    }
    rbac = {
      serviceAccount = {
        create = true
        name   = "cluster-autoscaler"
        annotations = {
          "eks.amazonaws.com/role-arn" = aws_iam_role.cluster_autoscaler[0].arn
        }
      }
    }
  })]

  lifecycle {
    precondition {
      condition     = contains(keys(local.cluster_autoscaler_image_tags), var.cluster_autoscaler.kubernetes_version)
      error_message = "cluster-autoscaler has no image pinned for Kubernetes ${var.cluster_autoscaler.kubernetes_version}; add one to local.cluster_autoscaler_image_tags."
    }
    # Like the other controllers here, its pod pins to the general node pool.
    precondition {
      condition     = var.nodes_ready
      error_message = "The general node pool must exist before cluster-autoscaler installs."
    }
  }

  depends_on = [
    aws_autoscaling_group_tag.cluster_autoscaler,
    aws_autoscaling_group_tag.cluster_autoscaler_balanced,
    aws_iam_role_policy.cluster_autoscaler,
  ]
}
