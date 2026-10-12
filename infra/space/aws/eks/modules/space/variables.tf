variable "aws_region" {
  description = "AWS region for the cluster"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
}

variable "sibling_stack_prefix" {
  description = "Resource-name prefix the sibling infra/space/aws/postgres and infra/space/aws/elasticache stacks use in this environment (their `prefix` input). Deterministic Secrets Manager names for the RDS credentials, Redis connection string, and DB bootstrap-role credentials are derived from it."
  type        = string

  validation {
    condition     = trimspace(var.sibling_stack_prefix) != ""
    error_message = "sibling_stack_prefix must not be empty."
  }
}

variable "name_prefix" {
  description = "Prefix for cluster and related resources"
  type        = string
  default     = "space"
}

variable "cluster_id" {
  description = "Cluster identity that distinguishes multiple clusters sharing one environment. environment selects the DD_ENV / Environment-tag posture, while cluster_id selects the physical cluster: e.g. two production clusters can both run with environment=prod but cluster_id prod-a and prod-b. Feeds local.name (name_prefix joined with cluster_id), which names every cluster-scoped AWS resource (EKS cluster, IAM roles, node groups, ALB), so co-located clusters in one account don't collide. Defaults to environment so single-cluster environments need not set it."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.cluster_id == null ? true : trimspace(var.cluster_id) != ""
    error_message = "cluster_id must be null or non-empty."
  }
}

variable "kubernetes_version" {
  description = "Kubernetes minor version for the EKS control plane, e.g. \"1.36\". Required with no default: an omitted version resolves to the AWS default, which is not necessarily the newest offered version, so a new cluster would silently land on whatever AWS defaults to that month. Choose a supported control-plane version and compatible worker AMIs separately; this input does not select a worker AMI."
  type        = string

  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+$", var.kubernetes_version))
    error_message = "kubernetes_version must be a MAJOR.MINOR string such as \"1.36\"."
  }
}

variable "control_plane_scaling_tier" {
  description = "EKS Provisioned Control Plane scaling tier. Controls API request concurrency, pod scheduling rate, and cluster database size. Valid values: standard, tier-xl, tier-2xl, tier-4xl, tier-8xl."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.control_plane_scaling_tier == null ? true : contains(["standard", "tier-xl", "tier-2xl", "tier-4xl", "tier-8xl"], var.control_plane_scaling_tier)
    error_message = "control_plane_scaling_tier must be null or one of: standard, tier-xl, tier-2xl, tier-4xl, tier-8xl."
  }
}

variable "worker_subnet_pools" {
  description = "Secondary-subnet pools to discover by space.pplx.net/subnet-pool tag, with an exact expected membership count. A non-empty map must identify exactly one default pool for existing worker MNGs; other entries are available to explicitly selected future MNGs. Changing that default replaces the subnet attachment of every existing worker-shaped MNG and must be handled as an explicit node migration."
  type = map(object({
    default_for_existing_mngs = optional(bool, false)
    expected_subnet_count     = number
  }))
  default = {}

  validation {
    condition = alltrue([
      for name in keys(var.worker_subnet_pools) :
      length(name) <= 63 &&
      can(regex("^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$", name))
    ])
    error_message = "worker_subnet_pools keys must be non-empty lowercase DNS labels no longer than 63 characters."
  }

  validation {
    condition = alltrue([
      for pool in values(var.worker_subnet_pools) :
      pool.expected_subnet_count >= 1 &&
      floor(pool.expected_subnet_count) == pool.expected_subnet_count
    ])
    error_message = "Each worker_subnet_pools expected_subnet_count must be an integer of at least one."
  }

  validation {
    condition = (
      length(var.worker_subnet_pools) == 0 ||
      length([for pool in values(var.worker_subnet_pools) : pool if pool.default_for_existing_mngs]) == 1
    )
    error_message = "A non-empty worker_subnet_pools map must mark exactly one pool default_for_existing_mngs."
  }
}

variable "availability_zones" {
  description = "Availability zones where EKS node groups should run"
  type        = list(string)

  validation {
    condition     = length(var.availability_zones) > 0
    error_message = "availability_zones must include at least one availability zone."
  }
}

variable "cluster_public_access_cidrs" {
  description = "CIDR ranges allowed to access the public EKS endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "cluster_endpoint_public_access" {
  description = "Whether the EKS API endpoint is publicly reachable"
  type        = bool
  default     = false
}

variable "cluster_endpoint_private_access" {
  description = "Whether the EKS API endpoint is privately reachable from the VPC"
  type        = bool
  default     = true
}

variable "cluster_log_retention_days" {
  description = "Retention period for EKS control plane logs"
  type        = number
  default     = 30
}

variable "enabled_cluster_log_types" {
  description = "Enabled EKS control plane log types"
  type        = list(string)
  default = [
    "api",
    "audit",
    "authenticator",
    "controllerManager",
    "scheduler",
  ]
}

variable "cluster_addons" {
  description = <<-EOT
    The EKS addons to install, each mapped to the exact version to pin it at.

    One map rather than a list plus a sibling version map, because an addon
    without a version and a version without an addon are both mistakes nobody
    should be able to write. Required with no default: an omitted
    `addon_version` does not track the AWS default, since EKS resolves the
    default once at cluster-*create* time and never advances it, so an unpinned
    fleet freezes each cluster on whatever was default the day it was created.

    The calling root must choose addon versions compatible with its EKS
    version; this module does not enforce a fleet-wide version floor.
  EOT
  type        = map(string)

  validation {
    condition = alltrue([
      for version in values(var.cluster_addons) :
      can(regex("^v[0-9]+\\.[0-9]+\\.[0-9]+-eksbuild\\.[0-9]+$", version))
    ])
    error_message = "Every cluster_addons value must be a full EKS addon version such as \"v1.23.0-eksbuild.1\"."
  }

  # The credmgr-authority, db-client, and rollout-mng Pod Identity associations
  # are unconditional; without the agent on the nodes they apply cleanly and
  # then hand those pods no credentials.
  validation {
    condition     = contains(keys(var.cluster_addons), "eks-pod-identity-agent")
    error_message = "cluster_addons must include eks-pod-identity-agent: this module's Pod Identity associations deliver no credentials without it."
  }
}

variable "metrics_server_resources" {
  description = "Optional memory request and limit override for the EKS metrics-server addon. Once applied, retain the block: roll back by setting the pinned version's reviewed defaults, then update that retained default pair with future addon version bumps. Null stops managing EKS's stored optional/computed configuration; it does not clear it."
  type = object({
    memory_request = string
    memory_limit   = string
  })
  default = null
}

variable "vpc_cni_enable_prefix_delegation" {
  description = "Whether to enable prefix delegation for the aws-vpc-cni addon."
  type        = bool
  default     = false
}

variable "vpc_cni_minimum_ip_target" {
  description = "Minimum number of total IPs to allocate per node for the aws-vpc-cni addon."
  type        = number
}

variable "vpc_cni_warm_ip_target" {
  description = "Free IPs to retain for VPC CNI growth beyond the minimum allocation. Null leaves WARM_IP_TARGET unset; with a positive minimum IP target, this prevents further pool growth and WARM_ENI_TARGET is ignored."
  type        = number
  default     = null

  validation {
    condition     = var.vpc_cni_warm_ip_target == null ? true : var.vpc_cni_warm_ip_target > 0 && floor(var.vpc_cni_warm_ip_target) == var.vpc_cni_warm_ip_target
    error_message = "vpc_cni_warm_ip_target must be a positive integer when set."
  }
}

variable "vpc_cni_node_agent_resources" {
  description = "Memory requested and limited for the VPC CNI network-policy agent on every node."
  type = object({
    memory_request = string
    memory_limit   = string
  })
  default = {
    memory_request = "256Mi"
    memory_limit   = "8Gi"
  }

  validation {
    condition = try(
      can(regex("^[1-9][0-9]*(Mi|Gi)$", var.vpc_cni_node_agent_resources.memory_request)) &&
      can(regex("^[1-9][0-9]*(Mi|Gi)$", var.vpc_cni_node_agent_resources.memory_limit)) &&
      (endswith(var.vpc_cni_node_agent_resources.memory_request, "Gi") ? tonumber(trimsuffix(var.vpc_cni_node_agent_resources.memory_request, "Gi")) * 1024 : tonumber(trimsuffix(var.vpc_cni_node_agent_resources.memory_request, "Mi"))) <=
      (endswith(var.vpc_cni_node_agent_resources.memory_limit, "Gi") ? tonumber(trimsuffix(var.vpc_cni_node_agent_resources.memory_limit, "Gi")) * 1024 : tonumber(trimsuffix(var.vpc_cni_node_agent_resources.memory_limit, "Mi"))),
      false,
    )
    error_message = "VPC CNI node-agent memory_request and memory_limit must use positive Mi/Gi values, with request no greater than limit."
  }
}

variable "aws_load_balancer_controller_resources" {
  description = "Optional AWS Load Balancer Controller memory reservation and limit override."
  type = object({
    memory_request = string
    memory_limit   = string
  })
  default = null

  validation {
    condition = alltrue([
      for resources in var.aws_load_balancer_controller_resources[*] : try(
        can(regex("^[1-9][0-9]*(Mi|Gi)$", resources.memory_request)) &&
        can(regex("^[1-9][0-9]*(Mi|Gi)$", resources.memory_limit)) &&
        (endswith(resources.memory_request, "Gi") ? tonumber(trimsuffix(resources.memory_request, "Gi")) * 1024 : tonumber(trimsuffix(resources.memory_request, "Mi"))) <=
        (endswith(resources.memory_limit, "Gi") ? tonumber(trimsuffix(resources.memory_limit, "Gi")) * 1024 : tonumber(trimsuffix(resources.memory_limit, "Mi"))),
        false,
      )
    ])
    error_message = "AWS Load Balancer Controller memory_request and memory_limit must use positive Mi/Gi values, with request no greater than limit."
  }
}

variable "hostname" {
  description = "SPACE UI ingress hostname"
  type        = string
}

variable "api_hostname" {
  description = "SPACE native API ingress hostname"
  type        = string
}

variable "ecw_api_hostname" {
  description = "E2B-compatible API ingress hostname"
  type        = string
}

variable "ecw_hostname" {
  description = "E2B-compatible base hostname for ECW sandbox traffic"
  type        = string
}

variable "external_dns_records_enabled" {
  description = "Whether to run external-dns for the SPACE ingress: installs external-dns and its IAM role, looks up internal_dns_zone_id, and associates that zone with the cluster VPC. Requires the zone, so it defaults off."
  type        = bool
  default     = false
  nullable    = false

  validation {
    condition = !var.external_dns_records_enabled || try(
      var.internal_dns_zone_id != "" && trimspace(var.internal_dns_zone_id) == var.internal_dns_zone_id,
      false,
    )
    error_message = "internal_dns_zone_id must name the Route53 zone, without surrounding whitespace, when external_dns_records_enabled is true."
  }
}

variable "cluster_service_cidr" {
  description = "Kubernetes service CIDR used when bootstrapping custom AMIs"
  type        = string
  default     = "172.20.0.0/16"
}

variable "node_instance_metadata_hop_limit" {
  description = "IMDSv2 PUT response hop limit for EKS node group launch templates."
  type        = number
  default     = 2
}

variable "s3_read_bucket_names" {
  description = "S3 bucket names that SPACE workloads should be able to read from"
  type        = list(string)
  default     = []
}

variable "workers_instance_types" {
  description = "EC2 instance types for the workers node group; use bare metal SKUs for Firecracker hosts"
  type        = list(string)
  default     = ["m5d.metal"]
}

variable "workers_min_size" {
  description = "Minimum size of the workers node group"
  type        = number
  default     = 1
}

variable "workers_max_size" {
  description = "Maximum size of the workers node group"
  type        = number
  default     = 6
}

variable "workers_desired_size" {
  description = "Desired size of the workers node group"
  type        = number
  default     = 2
}

variable "workers_capacity_type" {
  description = "Capacity type for the workers node group"
  type        = string
  default     = "ON_DEMAND"
}

variable "workers_capacity_reservation_preference" {
  description = "Optional EC2 Capacity Reservation preference for the workers node group launch template. Leave null when targeting a Capacity Reservation resource group with fallback semantics."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.workers_capacity_reservation_preference == null ? true : contains(["capacity-reservations-only", "none", "open"], var.workers_capacity_reservation_preference)
    error_message = "workers_capacity_reservation_preference must be null, capacity-reservations-only, none, or open."
  }
}

variable "workers_capacity_reservation_resource_group_arn" {
  description = "Optional EC2 Capacity Reservation resource group ARN for the workers node group launch template; defaults to the space pool from capacity reservations remote state when configured"
  type        = string
  default     = null
  nullable    = true
}

variable "workers_disk_size_gib" {
  description = "Root disk size for the workers node group"
  type        = number
  default     = 100
}

variable "workers_ami_id" {
  description = "Custom AMI ID for the worker node groups; custom AMIs must include EKS bootstrap tooling. A worker AMI selector is required: this, or one of workers_ami_git_sha and workers_ami_name_pattern (mutually exclusive), which this overrides. When a lookup selector is also set, this must be known at plan time."
  type        = string
  default     = null
  nullable    = true

  # Every worker MNG (workers-cgnat, workers-1, the workers-N capacity groups)
  # is `ami_type = "CUSTOM"` on the shared worker launch template. With no
  # selector, workers-cgnat is silently filtered out of `rollout_managed` and
  # the others fail at apply with a launch template that has no image.
  validation {
    condition = anytrue([
      try(trimspace(var.workers_ami_id) != "", false),
      try(trimspace(var.workers_ami_git_sha) != "", false),
      try(trimspace(var.workers_ami_name_pattern) != "", false),
    ])
    error_message = "Set one non-empty worker AMI selector: workers_ami_id, workers_ami_git_sha, or workers_ami_name_pattern. Every worker node group runs a custom AMI."
  }

  validation {
    condition     = var.workers_ami_id == null ? true : trimspace(var.workers_ami_id) != ""
    error_message = "workers_ami_id must be null or non-empty; an empty value is not the same as unset."
  }
}

variable "workers_ami_name_pattern" {
  description = "Optional name pattern used to resolve the most recent workers AMI when workers_ami_id is unset"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = !(var.workers_ami_name_pattern != null && var.workers_ami_git_sha != null)
    error_message = "workers_ami_name_pattern and workers_ami_git_sha are mutually exclusive; set only one AMI lookup selector."
  }

  validation {
    condition     = var.workers_ami_name_pattern == null ? true : trimspace(var.workers_ami_name_pattern) != ""
    error_message = "workers_ami_name_pattern must be null or non-empty; an empty value is not the same as unset."
  }
}

variable "workers_ami_git_sha" {
  description = "Optional Git SHA tag used to resolve a workers AMI when workers_ami_id is unset"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.workers_ami_git_sha == null ? true : trimspace(var.workers_ami_git_sha) != ""
    error_message = "workers_ami_git_sha must be null or non-empty; an empty value is not the same as unset."
  }
}

variable "workers_labels" {
  description = "Additional labels to add to workers nodes"
  type        = map(string)
  default     = {}
  # `null` means unset, not a null map: without this, a tfvars setting
  # `workers_labels = null` makes the `keys()` below fail with Terraform's
  # "argument must not be null" and the validation's own message is never
  # reached.
  nullable = false

  # Merged into this module's worker-MNG label surfaces: the legacy
  # workers-cgnat pool (`locals.tf`), workers-1 (`rollout.tf`), and the
  # `workers-N` capacity groups (`worker_capacity.tf`). `locals.tf` and
  # `rollout.tf` merge it *last*, so a key set here wins over the literal label
  # map rather than losing to it. Worker MNGs a calling root declares itself
  # (a canary group, smoke or test nodes) carry their own labels and never
  # merge this variable, so this validation does not fence them; the root has
  # to hold them to the same contract.
  #
  # The invariant is that **no Terraform writes the node-group boundary
  # label** — stated this way on purpose. "The labeler is its only writer"
  # holds only while the default group is the only row in `node_groups`; the
  # labeler suppresses all label writes once a second group exists, and a
  # later group's label comes from an admin-credentialed patch instead.
  validation {
    condition = !contains(
      keys(var.workers_labels),
      "node-restriction.kubernetes.io/space-node-group",
    )
    error_message = "workers_labels must not set node-restriction.kubernetes.io/space-node-group: no Terraform in this stack may declare the node-group boundary label, which the control plane writes per node."
  }
}

variable "workers_taints" {
  description = "Taints for every worker node group: workers-cgnat, workers-1, and the workers-N capacity groups"
  type = list(object({
    effect = string
    key    = string
    value  = string
  }))
  default = []
}

variable "workers_service_account_name" {
  description = "Service account name for worker workloads"
  type        = string
  default     = "workers"
}

variable "workers_service_account_namespace" {
  description = "Service account namespace for worker workloads"
  type        = string
  default     = "space"
}

# Per-cohort inputs for in-flight workers-N rollouts (two-MNG blue/green
# pattern) live here while
# the rollout is active and are dropped from this stack's variables on
# the rollout's cleanup PR. The promoted steady-state worker MNG consumes `workers_min_size`
# / `workers_desired_size` / `workers_max_size` / `workers_labels`
# inputs that the legacy `workers-cgnat` pool used. "Headroom" and the
# cohort taint are both rollout-time concepts and do not appear on a
# promoted steady-state pool.
#
# `workers_1_headroom_count` and `workers_1_cohort_taint_value` are the
# per-cohort inputs for a cluster's `workers-cgnat -> workers-1`
# rollout. They are only used while `workers_cgnat_retired = false` and
# are dropped again in the rollout cleanup PR after workers-1 is promoted
# to steady state. A future `workers-1 -> workers-2` rollout PR will
# re-introduce `workers_2_*` variables for its duration, and the matching
# cleanup PR will drop them again.
variable "workers_1_headroom_count" {
  description = "Initial rollout headroom (`H`) for the `workers-cgnat -> workers-1` migration. Setting this to a non-null value creates the target `workers-1` MNG at `min=0, desired=H, max=H` while the legacy MNG still exists. Leave null outside an active rollout window."
  type        = number
  default     = null
  nullable    = true

  validation {
    condition     = var.workers_1_headroom_count == null ? true : var.workers_1_headroom_count >= 1
    error_message = "workers_1_headroom_count must be null or at least 1."
  }
}

variable "workers_1_cohort_taint_value" {
  description = "Rollout cohort value for the `workers-1` target MNG. Must match `clusterController.drainEvacuationPlacement.cohortValue` while the rollout is in flight."
  type        = string
  default     = "1"

  validation {
    condition     = length(var.workers_1_cohort_taint_value) > 0
    error_message = "workers_1_cohort_taint_value must be non-empty."
  }
}

# Set to `true` once an environment has finished its workers-cgnat →
# workers-1 rollout and the legacy MNG can be retired in this stack.
# Flipping to `true` cascades the destruction of
# `aws_eks_node_group.rollout_managed["workers-cgnat"]`, the per-pool IAM
# role + policy attachments, and the service-account role. The legacy LT
# itself is preserved via a `moved` block in `node_groups.tf` (its
# identity has to survive so `module.workers_1` can keep referencing it
# without rolling), so the AWS-side LT name keeps its `workers-cgnat-`
# prefix until the next workers-N rollout produces a fresh per-cohort
# LT.
#
# Keep the legacy MNG cordoned + drained to zero before flipping this to
# `true`; the rollout-mng module's `ignore_changes` covers the scaling
# triple, so Terraform's apply doesn't itself drain nodes.
variable "workers_cgnat_retired" {
  description = "Whether the legacy `workers-cgnat` MNG has been retired for this environment. Default `false` keeps the MNG, its IAM role, and per-pool service-account role in place. On an existing cluster, set `true` only after the workers-cgnat → workers-1 rollout completes and the legacy MNG has been drained to zero nodes. A new cluster can start with `true`, running workers-1 as its steady-state pool and never creating the legacy MNG."
  type        = bool
  default     = false
}

variable "worker_capacity_groups" {
  description = "Optional permanent worker MNGs for capacity expansion. Keys reserve entries in the cluster-wide generic workers-N sequence after workers-1; a later rollout must use an index above every allocated capacity or rollout MNG. Every group starts at min=0 and desired=0, uses the normal worker launch template and identity, and has a Terraform-enforced ceiling below 100 nodes. An optional availability_zone narrows a named subnet pool to exactly one subnet in that zone. Splitting MNGs does not bound the scheduler's cross-group feasible set; validate placement before the total matching worker fleet exceeds 100."
  type = map(object({
    instance_types    = list(string)
    subnet_pool       = optional(string)
    availability_zone = optional(string)
    max_size          = optional(number, 99)
  }))
  default = {}

  validation {
    condition = alltrue([
      for name in keys(var.worker_capacity_groups) :
      can(regex("^workers-(?:[2-9]|[1-9][0-9]+)$", name))
    ])
    error_message = "worker_capacity_groups keys must continue the generic workers-N sequence at workers-2 or later."
  }

  validation {
    condition = alltrue([
      for group in values(var.worker_capacity_groups) :
      length(group.instance_types) > 0 &&
      alltrue([for instance_type in group.instance_types : trimspace(instance_type) != ""])
    ])
    error_message = "Each worker capacity group must declare at least one non-empty instance type."
  }

  validation {
    condition = alltrue([
      for group in values(var.worker_capacity_groups) :
      group.max_size >= 1 && group.max_size <= 99 && floor(group.max_size) == group.max_size
    ])
    error_message = "Each worker capacity group max_size must be a whole number between 1 and 99."
  }

  validation {
    condition = alltrue([
      for group in values(var.worker_capacity_groups) :
      group.subnet_pool == null ? true : trimspace(group.subnet_pool) != ""
    ])
    error_message = "worker capacity subnet_pool values must be null or non-empty."
  }

  validation {
    condition = alltrue([
      for group in values(var.worker_capacity_groups) :
      group.availability_zone == null ? true : trimspace(group.availability_zone) != ""
    ])
    error_message = "worker capacity availability_zone values must be null or non-empty."
  }

  validation {
    condition = alltrue([
      for group in values(var.worker_capacity_groups) :
      group.availability_zone == null || group.subnet_pool != null
    ])
    error_message = "A worker capacity availability_zone requires a named subnet_pool."
  }
}

variable "builders_instance_types" {
  description = "EC2 instance types for the builders node group; use bare metal SKUs for template builders"
  type        = list(string)
  default     = ["m5d.metal"]
}

variable "builders_min_size" {
  description = "Minimum size of the builders node group"
  type        = number
  default     = 1
}

variable "builders_max_size" {
  description = "Maximum size of the builders node group"
  type        = number
  default     = 3
}

variable "builders_desired_size" {
  description = "Desired size of the builders node group"
  type        = number
  default     = 1
}

variable "builders_capacity_type" {
  description = "Capacity type for the builders node group"
  type        = string
  default     = "ON_DEMAND"
}

variable "builders_capacity_reservation_preference" {
  description = "Optional EC2 Capacity Reservation preference for the builders node group launch template. Leave null when targeting a Capacity Reservation resource group with fallback semantics."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.builders_capacity_reservation_preference == null ? true : contains(["capacity-reservations-only", "none", "open"], var.builders_capacity_reservation_preference)
    error_message = "builders_capacity_reservation_preference must be null, capacity-reservations-only, none, or open."
  }
}

variable "builders_capacity_reservation_resource_group_arn" {
  description = "Optional EC2 Capacity Reservation resource group ARN for the builders node group launch template; defaults to the space pool from capacity reservations remote state when configured"
  type        = string
  default     = null
  nullable    = true
}

variable "builders_disk_size_gib" {
  description = "Root disk size for the builders node group"
  type        = number
  default     = 100
}

variable "builders_ami_id" {
  description = "Optional custom AMI ID for the builders node group; custom AMIs must include EKS bootstrap tooling. Null runs the EKS-managed AMI. Whether it's null decides which node-group resource the builders pool uses, so a non-null value must be known at plan time."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.builders_ami_id == null ? true : trimspace(var.builders_ami_id) != ""
    error_message = "builders_ami_id must be null or non-empty; an empty value is not the same as unset."
  }
}

variable "builders_ami_name_pattern" {
  description = "Optional name pattern used to resolve the most recent builders AMI when builders_ami_id is unset"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = !(var.builders_ami_name_pattern != null && var.builders_ami_git_sha != null)
    error_message = "builders_ami_name_pattern and builders_ami_git_sha are mutually exclusive; set only one AMI lookup selector."
  }

  validation {
    condition     = var.builders_ami_name_pattern == null ? true : trimspace(var.builders_ami_name_pattern) != ""
    error_message = "builders_ami_name_pattern must be null or non-empty; an empty value is not the same as unset."
  }
}

variable "builders_ami_git_sha" {
  description = "Optional Git SHA tag used to resolve a builders AMI when builders_ami_id is unset"
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.builders_ami_git_sha == null ? true : trimspace(var.builders_ami_git_sha) != ""
    error_message = "builders_ami_git_sha must be null or non-empty; an empty value is not the same as unset."
  }
}

variable "builders_labels" {
  description = "Additional labels to add to builders nodes"
  type        = map(string)
  default     = {}
}

variable "builders_taints" {
  description = "Taints for builder nodes"
  type = list(object({
    effect = string
    key    = string
    value  = string
  }))
  default = [
    {
      effect = "NO_SCHEDULE"
      key    = "space.pplx.net/workload"
      value  = "builders"
    },
  ]
}

variable "builders_service_account_name" {
  description = "Service account name for builder workloads"
  type        = string
  default     = "builders"
}

variable "builders_service_account_namespace" {
  description = "Service account namespace for builder workloads"
  type        = string
  default     = "space"
}

variable "builder_ecr_pull_repository_arns" {
  description = "Exact private ECR repositories the build-manager IRSA role may pull as template inputs"
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for arn in var.builder_ecr_pull_repository_arns :
      can(regex("^arn:[^:]+:ecr:[^:]+:[0-9]{12}:repository/.+$", arn))
    ])
    error_message = "builder_ecr_pull_repository_arns must contain ECR repository ARNs."
  }
}

variable "general_instance_types" {
  description = "EC2 instance types for the general node group"
  type        = list(string)
  default     = ["m7i.large"]
}

variable "general_min_size" {
  description = "Minimum size of the general node group"
  type        = number
  default     = 1
}

variable "general_max_size" {
  description = "Maximum size of the general node group"
  type        = number
  default     = 4
}

variable "general_desired_size" {
  description = "Desired size of the general node group"
  type        = number
  default     = 2
}

variable "general_capacity_type" {
  description = "Capacity type for the general node group"
  type        = string
  default     = "ON_DEMAND"
}

variable "general_disk_size_gib" {
  description = "Root disk size for the general node group"
  type        = number
  default     = 80
}

variable "general_labels" {
  description = "Additional labels to add to general-purpose nodes"
  type        = map(string)
  default     = {}
}

variable "general_taints" {
  description = "Taints for general-purpose nodes"
  type = list(object({
    effect = string
    key    = string
    value  = string
  }))
  default = []
}

variable "general_blue_green_node_group" {
  description = "Optional permanent general-2 MNG used for additive blue/green swaps. recorded_active_size creates the pool initially and records its operator-managed active target for capacity checks."
  type = object({
    instance_types       = list(string)
    recorded_active_size = number
    max_size             = number
  })
  default = null

  validation {
    condition = var.general_blue_green_node_group == null ? true : (
      length(var.general_blue_green_node_group.instance_types) == 1 &&
      trimspace(var.general_blue_green_node_group.instance_types[0]) != "" &&
      var.general_blue_green_node_group.recorded_active_size >= 1 &&
      var.general_blue_green_node_group.recorded_active_size <= var.general_blue_green_node_group.max_size &&
      var.general_blue_green_node_group.max_size <= 20
    )
    error_message = "general_blue_green_node_group must select exactly one instance type and use 1 <= recorded_active_size <= max_size <= 20."
  }
}

variable "general_serving_node_group" {
  description = "General MNG that is currently intended to serve SPACE workloads. Keep this aligned with the operator-managed blue/green cutover so capacity checks use the live node shape."
  type        = string
  default     = "general"

  validation {
    condition = contains(["general", "general-2"], var.general_serving_node_group) && (
      var.general_serving_node_group != "general-2" ||
      var.general_blue_green_node_group != null
    )
    error_message = "general_serving_node_group must be general, or general-2 when general_blue_green_node_group is declared."
  }
}

variable "general_service_account_name" {
  description = "Service account name for general workloads"
  type        = string
  default     = "general"
}

variable "general_service_account_namespace" {
  description = "Service account namespace for general workloads"
  type        = string
  default     = "space"
}

variable "bootstrap_bucket" {
  description = "S3 bucket for bootstrap artifacts (shim binary, database migrator, etc.)"
  type        = string
}

variable "template_artifacts_bucket" {
  description = "S3 bucket for rootfs template artifacts"
  type        = string
}

variable "snapshots_bucket" {
  description = "S3 bucket for suspend-snapshot uploads from sbx-mgr"
  type        = string
}

variable "eks_cluster_tags" {
  description = "Additional tags to apply only to the EKS cluster"
  type        = map(string)
  default     = {}
}

variable "worker_asg_tag_propagation_enabled" {
  description = "Propagate worker common tags to the workers-cgnat, workers-1, and additional_worker_asg_names ASGs. That propagation's for_each is keyed by ASG names that exist only after those node groups are created, so it must be false for any apply that creates one: a fresh cluster's first apply, and on an existing cluster any apply that adds workers-1 (setting workers_1_headroom_count, or setting workers_cgnat_retired = true on a cluster that never set it) or a new additional_worker_asg_names entry. Defaults off so a fresh cluster plans; set true and re-apply once the node groups exist. The workers-N capacity groups are keyed by group name and always get their tags."
  type        = bool
  default     = false
  nullable    = false
}

variable "cluster_admin_principal_arns" {
  description = "Additional IAM principal ARNs that should receive EKS cluster-admin access"
  type        = list(string)
  default     = []
}

variable "ci_smoke_test_role_name" {
  description = "IAM role name for the CI smoke-test role, which gets namespace-scoped Kubernetes access; required when ci_smoke_test_trusted_principal_arns is non-empty."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.ci_smoke_test_role_name == null ? true : trimspace(var.ci_smoke_test_role_name) != ""
    error_message = "ci_smoke_test_role_name must be null or non-empty."
  }

  validation {
    condition     = length(var.ci_smoke_test_trusted_principal_arns) == 0 || var.ci_smoke_test_role_name != null
    error_message = "ci_smoke_test_role_name must be set when ci_smoke_test_trusted_principal_arns is set. The module doesn't pick a default: a role's name can't change without replacing it, so a default is a name every caller would be stuck with."
  }
}

variable "ci_smoke_test_trusted_principal_arns" {
  description = "IAM principal ARNs, typically a CI system's role, trusted to assume the smoke-test Kubernetes access role. Empty creates no role."
  type        = list(string)
  default     = []
}

variable "ci_smoke_test_namespaces" {
  description = "Kubernetes namespaces where the CI smoke-test role receives AmazonEKSAdminPolicy access"
  type        = list(string)
  default     = ["space"]

  validation {
    condition = length(var.ci_smoke_test_namespaces) > 0 && alltrue([
      for namespace in var.ci_smoke_test_namespaces : length(trimspace(namespace)) > 0
    ])
    error_message = "ci_smoke_test_namespaces must contain at least one non-empty namespace name."
  }
}

variable "ci_helm_deploy_role_name" {
  description = "IAM role name created in-account for Helm deploys run from outside it, by a CI system or an operator; required when ci_helm_deploy_trusted_principal_arn is set."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.ci_helm_deploy_role_name == null ? true : trimspace(var.ci_helm_deploy_role_name) != ""
    error_message = "ci_helm_deploy_role_name must be null or non-empty."
  }

  validation {
    condition     = var.ci_helm_deploy_trusted_principal_arn == null || var.ci_helm_deploy_role_name != null
    error_message = "ci_helm_deploy_role_name must be set when ci_helm_deploy_trusted_principal_arn is set; IAM role names are account-global, so the module does not pick one."
  }
}

variable "ci_helm_deploy_trusted_principal_arn" {
  description = "Optional AWS principal ARN allowed to assume the Helm deploy role"
  type        = string
  default     = null
  nullable    = true
}

variable "ci_helm_deploy_external_id" {
  description = "Optional external ID required when assuming the Helm deploy role"
  type        = string
  default     = null
  nullable    = true
}

variable "cred_authority_node_count" {
  description = "Node count (min = desired) for the dedicated cred-authority pool that hosts credmgr-authority. Defaults to 1 for clusters without live authority traffic; set 2 on clusters with live authority traffic so the two authority replicas spread across nodes."
  type        = number
  default     = 1

  validation {
    condition     = var.cred_authority_node_count >= 1
    error_message = "cred_authority_node_count must be at least 1."
  }
}

variable "cred_authority_instance_types" {
  description = "EC2 instance types for the dedicated cred-authority pool. The default suits clusters without live authority traffic; clusters with it match the general pool's size so the aws-eks-nodeagent's per-node memory footprint (several GiB, growing with sandbox churn until the upstream network-policy-agent leak is fixed) leaves headroom for the authority replicas. Changing this replaces the managed node group."
  type        = list(string)
  default     = ["m7i.large"]
}

variable "mitm_root_ca_external_secret_enabled" {
  description = "Sync the space-mitm-root-ca Secrets Manager value (the public MITM root CA, sourced from the certificates stack in mitm_root_ca.tf) into the space namespace, where the space-build-manager chart mounts it into seed builds. On only where the certificates stack has a root; the Secrets Manager read grant does not depend on this."
  type        = bool
  default     = false
}

variable "credmgr_authority_s3_access_enabled" {
  description = "Grant the credmgr authority the S3 access it vends to forwarding node-local credmgr agents (template artifact pull and publish, snapshot transfers) and create its S3 vend role. Disable only on a cluster whose node-local credmgr doesn't forward to the authority; if a forwarding cluster disables it, the credentials the authority vends carry no S3 access and template and snapshot operations fail."
  type        = bool
  default     = true
  nullable    = false
}

variable "credmgr_authority_tls_external_secret_enabled" {
  description = "Sync the credmgr-authority server-TLS keypair into the space namespace. Terraform creates that Secrets Manager secret on every cluster (credmgr_authority_tls.tf), so this tracks whether space-credmgr-authority is deployed here rather than whether the value exists — off leaves no synced secret that nothing mounts."
  type        = bool
  default     = false
}

variable "credmgr_intermediate_key_broker_token_external_secret_enabled" {
  description = "Sync the shared bearer the credmgr authority presents to the intermediate-key broker. Its Secrets Manager secret is populated out-of-band and is not Terraform-created, so this stays off until it exists; flip it on together with space-credmgr-authority.intermediateKeyBroker.enabled and the matching token on the broker side."
  type        = bool
  default     = false
}

variable "cluster_smoke_ecw_api_key_external_secret_enabled" {
  description = "Sync this cluster's in-cluster smoke ECW API key into the space namespace as secret/space-smoke. The calling root creates the Secrets Manager container but not its value: the key is minted against this cluster's control plane out of band, and an ExternalSecret pointed at an unpopulated secret reports SecretSyncError. Flip it on once the key exists; the in-cluster smoke Job cannot start without it."
  type        = bool
  default     = false
}

variable "network_credential_smoke_enabled" {
  description = "Provision the fixed testing credential for the network-credential smoke. Requires network_credential_smoke_org_id."
  type        = bool
  default     = false
}

variable "network_credential_smoke_org_id" {
  description = "Organization the network-credential smoke runs as. Tags the smoke fixture space-org-id=<org> so that organization's reader-role session can read it (credmgr_secrets_reader.tf). Required when network_credential_smoke_enabled is true."
  type        = string
  default     = null

  validation {
    condition     = var.network_credential_smoke_org_id == null || can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.network_credential_smoke_org_id))
    error_message = "network_credential_smoke_org_id must be a lowercase organization UUID."
  }
}

variable "cluster_smoke_slack_webhook_external_secret_enabled" {
  description = "Sync the Slack webhook the in-cluster smoke Job posts its own run summary to. Independent of the key gate above: a cluster can run the smoke and report through logs, its summary artifact, and its metrics without ever posting to Slack. Off until the webhook value is in this account."
  type        = bool
  default     = false
}

# Why the empty string rather than the `null` + `nullable = true` shape
# load_test_api_key_secret_id uses: that one is consumed by a `count` on a
# whole module, where a null never reaches an attribute. This one flows into
# an external_secrets entry whose `remote_key` is a non-optional string, so a
# null would have to be coalesced back to "" before the grant's trimspace saw
# it — two spellings of unset instead of one.
variable "eppo_secret_id" {
  description = "AWS Secrets Manager secret ID holding the SPACE Eppo SDK key as JSON with an EPPO_SDK_KEY field, synced into secret/space-eppo. Account-level and shared by every cluster in the account; no stack creates it, so a new cluster names the one its account already has rather than expecting a per-cluster secret to appear. Naming one is also the gate: empty is the provider-free deployment, where the space-eppo ExternalSecret does not render and no Secrets Manager grant is derived for it. Defaulted here rather than in the root stack: this module is reusable, so omitting the input means provider-free, while the root keeps it required so each new cluster it declares states its choice."
  type        = string
  # Not nullable, or `null` would be the second spelling of unset the
  # note above says this shape avoids — and it would fail inside
  # `trimspace` rather than through the error_message below.
  nullable = false
  default  = ""

  validation {
    condition     = var.eppo_secret_id == trimspace(var.eppo_secret_id)
    error_message = "eppo_secret_id must be either empty — the provider-free deployment, which renders no space-eppo ExternalSecret — or a trimmed Secrets Manager secret name. A whitespace-only or space-padded value is neither: it is non-empty, so the entry renders, and the external-secrets read grant is interpolated from the same string."
  }
}
