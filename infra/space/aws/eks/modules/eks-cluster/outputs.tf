output "cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.this.name
}

output "cluster_arn" {
  description = "EKS cluster ARN"
  value       = aws_eks_cluster.this.arn
}

output "cluster_endpoint" {
  description = "EKS API server endpoint"
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64 cluster CA"
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_oidc_issuer" {
  description = "OIDC issuer URL for IRSA"
  value       = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider ARN, the federated principal of IRSA trust policies"
  value       = aws_iam_openid_connect_provider.this.arn
}

output "oidc_issuer_host" {
  description = "OIDC issuer host and path used in IAM trust-policy conditions"
  value       = local.oidc_issuer_hostpath
}

output "cluster_security_group_id" {
  description = "Security group EKS creates for the cluster and attaches to every node"
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "node_security_group_id" {
  description = "Node-to-node security group attached to every node launch template"
  value       = aws_security_group.nodes.id
}

output "node_group_cluster_name" {
  description = "Cluster name that resolves only once the pre-node addons (vpc-cni, kube-proxy) are installed. A node group declared outside this module takes its cluster_name from here so its nodes can become Ready."
  value       = aws_eks_cluster.this.name

  depends_on = [aws_eks_addon.pre_node]
}

output "node_role_arns" {
  description = "Node IAM role ARN by pool. Resolves once every policy attachment on the role exists, so a node group created from it outside this module can register its nodes."
  value       = { for name, role in aws_iam_role.node : name => role.arn }

  depends_on = [
    aws_iam_role_policy_attachment.node_additional,
    aws_iam_role_policy_attachment.node_managed,
  ]
}

output "node_managed_policy_arns" {
  description = "AWS managed policies every node role in this module carries; attach the same set to node roles declared outside it"
  value       = local.node_managed_policy_arns
}

output "launch_templates" {
  description = "Launch template id and latest version by pool"
  value = {
    for name, template in aws_launch_template.node_group : name => {
      id             = template.id
      latest_version = template.latest_version
    }
  }
}

output "node_groups" {
  description = "Managed node group by pool: arn, the backing autoscaling groups under resources, and scaling_config"
  value = {
    for name, group in merge(aws_eks_node_group.custom, aws_eks_node_group.managed) : name => {
      arn            = group.arn
      resources      = group.resources
      scaling_config = group.scaling_config
    }
  }
}

output "vpc_cni_role_arn" {
  description = "IRSA role ARN of the kube-system/aws-node ServiceAccount"
  value       = aws_iam_role.vpc_cni.arn
}

output "managed_node_groups" {
  description = "EKS-managed-AMI node groups by pool: arn, the backing ASG names, and scaling_config. It depends only on those groups, so a consumer doesn't wait on custom-AMI pools."
  value = {
    for name, group in aws_eks_node_group.managed : name => {
      arn            = group.arn
      asg_names      = flatten([for r in group.resources : [for asg in r.autoscaling_groups : asg.name]])
      scaling_config = group.scaling_config
    }
  }

  # desired_size is ignored, so the plan carries the size EKS reported when it
  # refreshed, and a min/max change sends that size with the new bounds. EKS
  # refuses one outside them partway through the apply; this refuses the plan.
  precondition {
    condition     = length(local.managed_node_groups_outside_bounds) == 0
    error_message = "The live desired size of ${join(", ", local.managed_node_groups_outside_bounds)} is outside its configured min_size and max_size, and EKS refuses those bounds. Move the live group to the new bounds through the EKS API, in the order the comment on aws_eks_node_group.managed in the eks-cluster module gives, and re-plan; or change the configured sizes, which for a group being created or replaced are its creation sizes."
  }
}

output "managed_node_group_arns" {
  description = "ARNs of the EKS-managed-AMI node groups by pool. Depends only on those groups, so a readiness edge through it doesn't wait on custom-AMI pools."
  value       = { for name, group in aws_eks_node_group.managed : name => group.arn }
}

output "cluster_service_cidr" {
  description = "Kubernetes service CIDR EKS assigned the cluster; a custom-AMI node's nodeadm config needs it"
  value       = try(aws_eks_cluster.this.kubernetes_network_config[0].service_ipv4_cidr, null)
}
