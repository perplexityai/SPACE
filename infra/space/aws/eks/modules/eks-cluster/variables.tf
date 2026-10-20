# Every input is a value, never a name to look up. The module reads no remote
# state and makes no account, VPC, subnet, or AMI lookups, so a root with no
# access to the target account can still plan it; see examples/minimal.

variable "name" {
  description = "Cluster name, and the prefix of every resource name the module creates (IAM roles, security groups, launch templates, the control-plane log group)"
  type        = string

  validation {
    condition     = trimspace(var.name) != ""
    error_message = "name must not be empty."
  }
}

variable "vpc_id" {
  description = "VPC the cluster and node security groups are created in"
  type        = string
}

variable "subnet_ids" {
  description = "Subnets the EKS control plane places its network interfaces in"
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) >= 2
    error_message = "EKS requires subnets in at least two availability zones."
  }
}

variable "common_tags" {
  description = "Tags applied to every resource"
  type        = map(string)
  default     = {}
}

variable "kubernetes_version" {
  description = "Kubernetes minor version for the EKS control plane"
  type        = string
}

variable "control_plane_scaling_tier" {
  description = "EKS Provisioned Control Plane scaling tier; null leaves the block out, so clusters that never opt in produce no diff"
  type        = string
  default     = null
}

variable "cluster_endpoint_private_access" {
  description = "Whether the EKS API endpoint is reachable from inside the VPC"
  type        = bool
  default     = true
}

variable "cluster_endpoint_public_access" {
  description = "Whether the EKS API endpoint is reachable from the internet"
  type        = bool
  default     = false
}

variable "cluster_public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "cluster_log_retention_days" {
  description = "Retention for the control-plane CloudWatch log group"
  type        = number
  default     = 30
}

variable "enabled_cluster_log_types" {
  description = "EKS control-plane log types to ship to CloudWatch"
  type        = list(string)
  default     = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
}

variable "eks_cluster_tags" {
  description = "Tags applied only to the EKS cluster resource, on top of common_tags"
  type        = map(string)
  default     = {}
}

variable "cluster_addons" {
  description = "EKS addon name to the exact version to pin it at. vpc-cni and kube-proxy install before the node groups, because a node can't become Ready without a CNI; every other addon installs after them. Required with no default: an unset addon_version doesn't track the AWS default, since EKS resolves it once at cluster creation and never advances it."
  type        = map(string)

  validation {
    condition = alltrue([
      for version in values(var.cluster_addons) :
      can(regex("^v[0-9]+\\.[0-9]+\\.[0-9]+-eksbuild\\.[0-9]+$", version))
    ])
    error_message = "Every cluster_addons value must be a full EKS addon version such as \"v1.23.0-eksbuild.1\"."
  }

  # Node roles deliberately lack AmazonEKS_CNI_Policy; the CNI's permissions
  # reach it only through the vpc-cni addon's IRSA role. Without the addon,
  # nodes can't assign pod IPs and never become Ready.
  validation {
    condition     = contains(keys(var.cluster_addons), "vpc-cni")
    error_message = "cluster_addons must include vpc-cni: node roles carry no CNI policy, so nodes need the addon's IRSA role to become Ready."
  }
}

variable "cluster_addon_configuration_values" {
  description = "EKS addon name to its configuration_values JSON; an addon absent from the map gets the addon default"
  type        = map(string)
  default     = {}
}

variable "iam_role_permissions_boundary" {
  description = "Managed policy ARN set as the permissions boundary of every IAM role the module creates (the cluster role, each node role, and the VPC CNI role). For a caller whose own IAM grant only allows creating roles under a fixed boundary; null creates them without one."
  type        = string
  default     = null
}

variable "cluster_admin_principal_arns" {
  description = "IAM principals granted AmazonEKSClusterAdminPolicy through EKS access entries"
  type        = list(string)
  default     = []
}

variable "node_instance_metadata_hop_limit" {
  description = "IMDSv2 PUT response hop limit on every node launch template; 2 lets pods on the node network reach IMDS"
  type        = number
  default     = 2
}

variable "node_pools" {
  description = <<-EOT
    Managed node groups, keyed by node group name. Each pool gets its own node
    IAM role, launch template, and managed node group.

    `ami_id` null runs the EKS-managed AMI named by `ami_type` (x86_64 AL2023
    by default; `AL2023_ARM_64_STANDARD` for Graviton instance types); set, the
    group runs that AMI as `CUSTOM`, `ami_type` is ignored, and
    `node_pool_user_data` must carry the pool's bootstrap script. `additional_policy_arns` keys must be known at plan time; their
    values may not be. For a pool with no `ami_id`, `min_size`,
    `desired_size`, and `max_size` apply at creation only: afterwards the
    group is resized through the EKS API (or its ASG's desired capacity by a
    cluster-autoscaler the caller runs on it) and Terraform ignores all three. A custom-AMI pool
    keeps its size under Terraform. `update_strategy` is the group's
    rolling-update strategy: null leaves EKS's default, which launches
    replacements before draining; `MINIMAL` drains first, so a roll never runs
    more nodes than the group's size.

    `launch_template_and_role_only` true keeps only the pool's launch template,
    node IAM role, and the role's policy attachments, and creates no node
    group, so a node group declared outside this module can run on them (the
    `launch_templates` and `node_role_arns` outputs). The pool's node-group
    fields (sizes, instance and capacity types, labels, taints, subnets,
    `update_strategy`) are then unused.
  EOT
  type = map(object({
    instance_types                          = list(string)
    capacity_type                           = string
    min_size                                = number
    desired_size                            = number
    max_size                                = number
    disk_size_gib                           = number
    labels                                  = map(string)
    taints                                  = list(object({ key = string, value = string, effect = string }))
    subnet_ids                              = list(string)
    ami_id                                  = optional(string)
    ami_type                                = optional(string, "AL2023_x86_64_STANDARD")
    capacity_reservation_preference         = optional(string)
    capacity_reservation_resource_group_arn = optional(string)
    additional_security_group_ids           = optional(list(string), [])
    additional_policy_arns                  = optional(map(string), {})
    volume_tags                             = optional(map(string), {})
    update_strategy                         = optional(string)
    launch_template_and_role_only           = optional(bool, false)
  }))
  default = {}

  validation {
    condition = alltrue([
      for pool in values(var.node_pools) : pool.update_strategy == null ? true : contains(["DEFAULT", "MINIMAL"], pool.update_strategy)
    ])
    error_message = "A node pool's update_strategy must be null, \"DEFAULT\" or \"MINIMAL\"."
  }

  # Deployment-backed addons (coredns, metrics-server, ...) install after this
  # module's node groups and can't become healthy with no node to run on.
  validation {
    condition = alltrue([
      for name in keys(var.cluster_addons) : contains(["vpc-cni", "kube-proxy"], name)
      ]) || var.external_node_group_hosts_addons || anytrue([
      for pool in values(var.node_pools) : !pool.launch_template_and_role_only && pool.desired_size > 0
    ])
    error_message = "Addons other than vpc-cni and kube-proxy need a node pool with a node group and desired_size above zero to schedule on: one in this module, or one declared outside it with external_node_group_hosts_addons set."
  }
}

variable "external_node_group_hosts_addons" {
  description = "Whether a node group the caller declares outside this module, on a pool's launch template and role, has nodes the deployment-backed addons (coredns, metrics-server, ...) can schedule on. Set it when that group is the only such capacity, so the addon check in node_pools counts it. This module can't wait for that group: the group takes its cluster name from this module's outputs, so the addons only become healthy once it has nodes."
  type        = bool
  default     = false
  nullable    = false
}

variable "node_group_update_timeout" {
  description = "How long Terraform waits for a node group update, such as a roll to a new launch template, before giving up. A caller whose apply runs under a shorter job limit should lower it, or the job is killed mid-apply with the state lock held."
  type        = string
  default     = "5h"
  nullable    = false
}

# Separate from node_pools on purpose. A bootstrap script embeds the cluster's
# endpoint and CA, which this module outputs; inside node_pools, that dependency
# would reach every consumer of the pool map, including the node IAM roles,
# which would then wait for the cluster to exist.
variable "node_pool_user_data" {
  description = "Node pool name to its bootstrap script, plain text; required for every pool with an ami_id and ignored otherwise"
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for name, pool in var.node_pools : pool.ami_id == null || contains(keys(var.node_pool_user_data), name)
    ])
    error_message = "Every node pool with an ami_id needs an entry in node_pool_user_data."
  }
}

variable "cluster_admin_principals_ready" {
  description = "Readiness token for cluster_admin_principal_arns principals the caller creates in the same apply. Pass an expression that stays unknown until they exist so the access entries wait for them; the default suits principals that already exist."
  type        = bool
  default     = true
  nullable    = false
}
