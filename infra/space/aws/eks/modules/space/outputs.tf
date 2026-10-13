output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_oidc_issuer" {
  description = "OIDC issuer URL for IRSA"
  value       = module.eks.cluster_oidc_issuer
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider ARN for root-owned Kubernetes integrations"
  value       = module.eks.oidc_provider_arn
}

output "oidc_issuer_host" {
  description = "OIDC issuer host and path used in IAM trust-policy conditions"
  value       = local.oidc_issuer_hostpath
}

output "external_secrets_operator_ready" {
  description = "Readiness token for root-owned releases that create External Secrets resources"
  value       = module.controllers.ready
}

output "controllers_ready" {
  description = "Readiness token for root-owned releases that must follow every cluster controller release, not only External Secrets"
  value       = true

  depends_on = [module.controllers]
}

output "private_subnet_ids" {
  description = "Private subnet IDs used by the cluster"
  value       = local.private_subnet_ids
}

output "secondary_private_subnet_ids" {
  description = "Secondary private subnet IDs selected for existing worker MNGs"
  value       = local.secondary_private_subnet_ids
}

output "shared_vpc_id" {
  description = "VPC ID containing the EKS cluster"
  value       = data.aws_vpc.shared.id
}

output "node_group_role_arns" {
  description = "IAM role ARNs for node groups"
  value = merge(
    module.eks.node_role_arns,
    { for name, role in aws_iam_role.node : name => role.arn },
  )
}

output "service_account_role_arns" {
  description = "IRSA role ARNs keyed by node pool. The `workers-cgnat` key is preserved across the workers-cgnat → workers-1 retirement so consumers that resolve the workers ServiceAccount's IRSA annotation from this map don't break; the underlying role is the hoisted standalone `aws_iam_role.workers_service_account`."
  value = merge(
    {
      for name, role in aws_iam_role.service_account : name => role.arn
    },
    {
      "workers-cgnat" = aws_iam_role.workers_service_account.arn
    },
  )
}

output "service_account_annotations" {
  description = "Service accounts and role ARNs to annotate in Kubernetes. The `workers-cgnat` key is preserved across retirement (see `service_account_role_arns`)."
  value = merge(
    {
      for name, role in aws_iam_role.service_account : name => {
        namespace = local.node_pools[name].service_account.namespace
        name      = local.node_pools[name].service_account.name
        role_arn  = role.arn
      }
    },
    {
      "workers-cgnat" = {
        namespace = var.workers_service_account_namespace
        name      = var.workers_service_account_name
        role_arn  = aws_iam_role.workers_service_account.arn
      }
    },
  )
}

output "general_node_security_group_id" {
  description = "Additional security group ID attached only to the general node group."
  value       = aws_security_group.general_nodes.id
}

output "general_serving_node_group" {
  description = "Operator-maintained record of the general MNG currently intended to serve SPACE workloads."
  value       = var.general_serving_node_group

  precondition {
    # Existing single-pool environments retain their positive-capacity gate.
    # Blue/green configurations additionally enforce the operator-recorded floor.
    condition = var.general_blue_green_node_group == null ? (
      module.eks.node_groups["general"].scaling_config[0].min_size > 0
      ) : var.general_serving_node_group == "general" ? (
      module.eks.node_groups["general"].scaling_config[0].min_size >= var.general_min_size
      ) : (
      aws_eks_node_group.general_blue_green[0].scaling_config[0].min_size >= var.general_blue_green_node_group.recorded_active_size
    )
    error_message = "The recorded general_serving_node_group must be live and, for blue/green configurations, meet its recorded capacity floor."
  }
}

output "general_serving_min_size" {
  description = "Larger configured floor of the two permanent general pools. Either may carry the active workload, so a root-owned DaemonSet that must roll on the general pool gates on this while the inactive pool stays at zero; referencing it also orders the consumer after a floor update."
  value = max(
    module.eks.node_groups["general"].scaling_config[0].min_size,
    try(aws_eks_node_group.general_blue_green[0].scaling_config[0].min_size, 0),
  )
}

output "vpc_cni_service_account_role_arn" {
  description = "IAM role ARN used by the kube-system/aws-node ServiceAccount for Amazon VPC CNI EC2 ENI/IP permissions."
  value       = module.eks.vpc_cni_role_arn
}

output "credential_manager_role_arn" {
  description = "IAM role ARN backing the credmgr ServiceAccount via IRSA. Wire this into deploy/{env}/space-worker.values.yaml under credentialManager.serviceAccount.annotations."
  value       = module.credential_manager_iam.role_arn
}

output "credential_manager_service_account_annotations" {
  description = "Annotation map to apply to the credmgr Kubernetes ServiceAccount. Cloud-specific (EKS emits eks.amazonaws.com/role-arn); the chart consumes this opaquely."
  value       = module.credential_manager_iam.service_account_annotations
}

output "db_client_pod_identity_role_arns" {
  description = "IAM role ARNs keyed by SPACE DB client workload for EKS Pod Identity."
  value = {
    for name, role in aws_iam_role.db_client_pod_identity : name => role.arn
  }
}

output "db_client_pod_identity_association_ids" {
  description = "EKS Pod Identity association IDs keyed by SPACE DB client workload."
  value = {
    for name, association in aws_eks_pod_identity_association.db_client : name => association.association_id
  }
}

output "cluster_controller_asg_terminate_policy_name" {
  description = "Inline policy on the cluster-controller Pod Identity role granting autoscaling:TerminateInstanceInAutoScalingGroup and autoscaling:SuspendProcesses on worker ASGs."
  value       = aws_iam_role_policy.cluster_controller_asg_terminate.name
}

output "cluster_controller_asg_describe_policy_name" {
  description = "Inline policy on the cluster-controller Pod Identity role granting autoscaling:DescribeAutoScalingGroups (Resource=* per AWS, which does not support resource-level scoping for this action)."
  value       = aws_iam_role_policy.cluster_controller_asg_describe.name
}

output "cluster_controller_eks_nodegroup_manage_policy_name" {
  description = "Inline policy on the cluster-controller Pod Identity role granting eks:DescribeNodegroup and eks:UpdateNodegroupConfig on the worker EKS nodegroup(s)."
  value       = aws_iam_role_policy.cluster_controller_eks_nodegroup_manage.name
}

output "space_hostname" {
  description = "Internal hostname for the SPACE UI ingress"
  value       = var.hostname
}

output "space_api_hostname" {
  description = "Internal hostname for the SPACE API ingress"
  value       = var.api_hostname
}

output "space_public_ingress_alb_name" {
  description = "Deterministic AWS load balancer name for the SPACE public ingress ALB"
  value       = local.public_ingress_alb_name
}

output "space_ecw_api_hostname" {
  description = "E2B-compatible hostname for the SPACE API ingress"
  value       = var.ecw_api_hostname
}

output "space_ecw_hostname" {
  description = "E2B-compatible base hostname for ECW sandbox traffic"
  value       = var.ecw_hostname
}

output "cluster_certificate_authority_data" {
  description = "Base64 cluster CA, for the root's Kubernetes-facing provider configuration"
  value       = module.eks.cluster_certificate_authority_data
}

# For worker MNGs the root declares beside this module: they share the
# module's worker launch template, node role, subnets, and tags.
output "workers_launch_template_id" {
  description = "Shared worker launch template id"
  value       = aws_launch_template.workers.id
}

output "workers_launch_template_latest_version" {
  description = "Shared worker launch template latest version"
  value       = aws_launch_template.workers.latest_version
}

output "workers_pool_node_role_arn" {
  description = "Shared workers-pool node role ARN, or null while the pool is disabled. Its policies and EKS access entry are attached before this value resolves, so an MNG created from it can register nodes."
  value       = one(aws_iam_role.workers_pool[*].arn)

  depends_on = [
    aws_iam_role_policy_attachment.workers_pool_managed,
    aws_iam_role_policy_attachment.workers_pool_s3_read,
    aws_eks_access_entry.workers_pool,
  ]
}

output "workers_shared_identity_needed" {
  description = "Whether the shared workers-pool role exists in this configuration"
  value       = local.workers_shared_identity_needed
}

output "worker_common_tags" {
  description = "Tags every worker MNG carries"
  value       = local.worker_common_tags
}

output "credmgr_s3_vend_role_arn" {
  description = "ARN of the S3-only role the credmgr authority assumes to vend S3 credentials; the chart's s3VendRoleArn. Null when credmgr_authority_s3_access_enabled is false."
  value       = one(aws_iam_role.credmgr_s3_vend[*].arn)
}

output "credmgr_secrets_reader_role_arn" {
  description = "ARN of the role the credmgr authority assumes per organization to read that organization's tagged Secrets Manager secrets; the authority's SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN."
  value       = aws_iam_role.credmgr_secrets_reader.arn
}
