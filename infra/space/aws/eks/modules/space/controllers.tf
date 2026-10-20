locals {
  # Every general MNG this cluster declares, keyed by name: general unless it's
  # retired, and general-2 when it's declared. general comes from the
  # managed-group output rather than node_groups, which merges every pool and
  # would hold its consumers behind a custom-AMI pool update. Consumers still
  # wait on both general pools whichever one serves: Terraform orders by
  # reference, not by the branch a value takes, and they must be able to follow
  # general-2 once general is retired.
  general_node_groups = merge(
    { for name, group in module.eks.managed_node_groups : name => group if name == "general" },
    {
      for group in aws_eks_node_group.general_blue_green : "general-2" => {
        arn            = group.arn
        asg_names      = flatten([for r in group.resources : [for asg in r.autoscaling_groups : asg.name]])
        scaling_config = group.scaling_config
      }
    },
  )

  # The one general_serving_node_group names. Its validations guarantee the
  # key exists: general can only be retired while general-2 serves, and
  # general-2 can only serve once it's declared.
  serving_general_node_group = local.general_node_groups[var.general_serving_node_group]
}

module "controllers" {
  source = "../controllers"

  aws_region                             = var.aws_region
  cluster_name                           = module.eks.cluster_name
  vpc_id                                 = data.aws_vpc.shared.id
  oidc_provider_arn                      = module.eks.oidc_provider_arn
  oidc_issuer_host                       = local.oidc_issuer_hostpath
  external_dns_enabled                   = var.external_dns_records_enabled
  hosted_zone_id                         = var.external_dns_records_enabled ? var.internal_dns_zone_id : null
  cluster_vpc_id                         = data.aws_vpc.shared.id
  alb_controller_role_name               = "${local.name}-aws-load-balancer-controller"
  external_dns_role_name                 = "${local.name}-external-dns"
  disruption_budget_chart_path           = "${var.charts_dir}/pod-disruption-budget"
  resource_tags                          = var.common_tags
  aws_load_balancer_controller_resources = var.aws_load_balancer_controller_resources
  # The serving general group carries the general pool's labels, so it's the
  # one the releases schedule onto, and once general is retired it's the only
  # one.
  nodes_ready = local.serving_general_node_group.arn != ""

  # A managed node group is backed by one ASG. one() fails the plan if it
  # reports several, and gives null if it reports none, which the controllers
  # module's precondition on the discovery tag turns into a named error. The
  # ARN is "" then, so it doesn't fail first on interpolating the null.
  cluster_autoscaler = var.cluster_autoscaler_enabled ? {
    role_name              = "${local.name}-cluster-autoscaler"
    kubernetes_version     = var.kubernetes_version
    autoscaling_group_arn  = one(local.serving_general_node_group.asg_names) == null ? "" : "${local.autoscaling_group_arn_prefix}${one(local.serving_general_node_group.asg_names)}"
    autoscaling_group_name = one(local.serving_general_node_group.asg_names)
    node_group_arn         = local.serving_general_node_group.arn
    scale_down_enabled     = var.cluster_autoscaler_scale_down_enabled
    memory_mib             = var.cluster_autoscaler_memory_mib

    max_graceful_termination_sec = var.cluster_autoscaler_max_graceful_termination_sec
    new_pod_scale_up_delay       = var.cluster_autoscaler_new_pod_scale_up_delay
  } : null
}
