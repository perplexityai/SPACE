locals {
  # Every general MNG this cluster declares, keyed by name: general unless it's
  # retired, general-2 when it's declared, and general-<zone> for each per-AZ
  # group. general comes from the managed-group output rather than
  # node_groups, which merges every pool and would hold its consumers behind a
  # custom-AMI pool update. Consumers still wait on every general pool whichever
  # one serves: Terraform orders by reference, not by the branch a value takes,
  # and they must be able to follow another group once general is retired.
  general_node_groups = merge(
    { for name, group in module.eks.managed_node_groups : name => group if name == "general" },
    {
      for group in concat(aws_eks_node_group.general_blue_green, values(aws_eks_node_group.general_per_az)) : group.node_group_name => {
        arn            = group.arn
        asg_names      = flatten([for r in group.resources : [for asg in r.autoscaling_groups : asg.name]])
        scaling_config = group.scaling_config
      }
    },
  )

  # The groups general_serving_node_group names, each with the recorded floor
  # its live min has to meet: one group, or every per-AZ group for
  # general-per-az. Its validations guarantee the names exist in
  # general_node_groups: general can only be retired while another group
  # serves, and general-2 or the per-AZ groups can only serve once they're
  # declared.
  serving_general_node_group_floors = var.general_serving_node_group == "general-per-az" ? local.general_per_az_floors : {
    (var.general_serving_node_group) = (
      var.general_serving_node_group == "general-2" ? var.general_blue_green_node_group.recorded_active_size :
      var.general_blue_green_node_group == null && var.general_per_az_node_groups == null ? 1 : var.general_min_size
    )
  }
  serving_general_node_groups = {
    for name in keys(local.serving_general_node_group_floors) : name => local.general_node_groups[name]
  }
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
  # The serving general groups carry the general pool's labels, so they're the
  # ones the releases schedule onto, and once general is retired they're the
  # only ones.
  nodes_ready = alltrue([for group in values(local.serving_general_node_groups) : group.arn != ""])

  # A managed node group is backed by one ASG. one() fails the plan if it
  # reports several, and gives null if it reports none, which the controllers
  # module's precondition on the discovery tag turns into a named error. The
  # ARN is "" then, so it doesn't fail first on interpolating the null.
  cluster_autoscaler = var.cluster_autoscaler_enabled ? {
    role_name          = "${local.name}-cluster-autoscaler"
    kubernetes_version = var.kubernetes_version
    node_groups = [
      for name, group in local.serving_general_node_groups : {
        name                   = name
        autoscaling_group_arn  = one(group.asg_names) == null ? "" : "${local.autoscaling_group_arn_prefix}${one(group.asg_names)}"
        autoscaling_group_name = one(group.asg_names)
        node_group_arn         = group.arn
      }
    ]
    scale_down_enabled = var.cluster_autoscaler_scale_down_enabled
    memory_mib         = var.cluster_autoscaler_memory_mib

    max_graceful_termination_sec = var.cluster_autoscaler_max_graceful_termination_sec
    new_pod_scale_up_delay       = var.cluster_autoscaler_new_pod_scale_up_delay
  } : null
}
