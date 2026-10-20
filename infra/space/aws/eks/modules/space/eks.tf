# The cluster, its control-plane IAM, addons, and every node pool except the
# legacy `workers-cgnat`. The module takes values, not names to look up; this
# module resolves them from its own conventions and SPACE's pool definitions.
module "eks" {
  source = "../eks-cluster"

  name       = local.name
  vpc_id     = data.aws_vpc.shared.id
  subnet_ids = local.private_subnet_ids

  common_tags                        = var.common_tags
  kubernetes_version                 = var.kubernetes_version
  control_plane_scaling_tier         = var.control_plane_scaling_tier
  cluster_endpoint_private_access    = var.cluster_endpoint_private_access
  cluster_endpoint_public_access     = var.cluster_endpoint_public_access
  cluster_public_access_cidrs        = var.cluster_public_access_cidrs
  cluster_log_retention_days         = var.cluster_log_retention_days
  enabled_cluster_log_types          = var.enabled_cluster_log_types
  eks_cluster_tags                   = var.eks_cluster_tags
  cluster_addons                     = var.cluster_addons
  cluster_addon_configuration_values = local.cluster_addon_configuration_values
  cluster_admin_principal_arns       = local.cluster_admin_access_entries
  cluster_admin_principals_ready     = alltrue([for role in aws_iam_role.ci_helm_deploy : role.arn != ""])
  node_instance_metadata_hop_limit   = var.node_instance_metadata_hop_limit

  node_pools          = local.core_node_pools
  node_pool_user_data = local.core_node_pool_user_data

  # Once general-2 serves, it's general capacity the addons can run on, even
  # with general retired.
  external_node_group_hosts_addons = var.general_serving_node_group == "general-2"
}

# Joins the general and cred-authority pools' launch templates through
# `additional_security_group_ids` in `local.core_node_pools`; RDS and Redis
# admit it (`general_node_security_group_id`).
resource "aws_security_group" "general_nodes" {
  description = "Additional security group for SPACE general-purpose nodes"
  name        = "${local.name}-general-nodes"
  vpc_id      = data.aws_vpc.shared.id

  egress {
    cidr_blocks      = ["0.0.0.0/0"]
    from_port        = 0
    ipv6_cidr_blocks = ["::/0"]
    protocol         = "-1"
    to_port          = 0
  }

  tags = merge(var.common_tags, {
    Name = "${local.name}-general-nodes"
  })
}
