# Inputs the root resolves from its own environment: sibling-stack state reads
# and paths under the root's directory. The module reads no remote state and
# hardcodes no path outside itself: the chart directory it reads comes in as
# an input (charts_dir), so a consumer from another checkout names it.

variable "common_tags" {
  description = "Tags applied to every resource; the root also sets them as provider default_tags"
  type        = map(string)
}

variable "credential_service_egress_enabled" {
  description = "Whether the dedicated cred-authority node pool is enabled for egress to the caller's private key-service endpoints."
  type        = bool
  default     = false
  nullable    = false
}

variable "credential_service_client_security_group_id" {
  description = "Caller-owned client security group attached to the cred-authority node pool when credential_service_egress_enabled is true. The calling root owns the private egress endpoints that admit it; this module creates none."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = !var.credential_service_egress_enabled || try(
      trimspace(var.credential_service_client_security_group_id) != "",
      false,
    )
    error_message = "credential_service_client_security_group_id must name the caller-owned client security group that the private egress endpoints admit when credential_service_egress_enabled is true."
  }
}

variable "hosting_ecr_repository_arns" {
  description = "ECR repository ARNs from the hosting stack (repository name -> ARN); may be empty"
  type        = map(string)
}

variable "postgres_connection_string_secret_name" {
  description = "Secrets Manager name of the postgres stack's connection-string secret"
  type        = string
}

variable "postgres_database_resource_id" {
  description = "RDS DbiResourceId of the postgres stack's instance, for rds-db:connect grants"
  type        = string
}

variable "capacity_reservation_pool_arns" {
  description = "Capacity-reservation resource-group ARNs by pool name from the capacity-reservations stack"
  type        = map(string)
  default     = {}
}

variable "internal_dns_zone_id" {
  description = "Private Route53 hosted zone external-dns manages; required when external_dns_records_enabled is true. The module associates it with the cluster VPC, so it must not already be associated with that VPC."
  type        = string
  default     = null
}

variable "credmgr_intermediate_ca_arn" {
  description = "Private CA the credential-manager authority may sign under; null grants nothing"
  type        = string
  default     = null
}

variable "mitm_root_ca_pem" {
  description = "PEM of the SPACE MITM root CA delivered to the seed builder; null creates no secret"
  type        = string
  default     = null
}

variable "charts_dir" {
  description = "Directory holding the Helm charts the module installs"
  type        = string
}

variable "worker_shim_dogstatsd_socket_path" {
  description = "Unix socket the worker containerd shim sends DogStatsD metrics to, exported as DD_DOGSTATSD_SOCKET by the shim wrapper the bootstrap writes. null omits it, and the shim sends UDP to DD_AGENT_HOST:DD_DOGSTATSD_PORT instead. The space-worker chart's shim-updater rewrites the same wrapper with its own socket setting, so this governs the wrapper only until the first shim update."
  type        = string
  default     = null
}

# Operator-managed secret containers declared by the calling root. The
# module still owns the shared external-secrets release, so their names cross
# the boundary while their AWS resources stay outside the reusable module.
variable "additional_external_secrets" {
  description = "Caller-owned ExternalSecret entries appended to the module's own, for secrets a deployment syncs into the space namespace without the module naming them. Same shape and validation as the module's entries; the Secrets Manager read grant covers them too."
  type = list(object({
    name               = string
    target_secret_name = string
    refresh_interval   = optional(string)
    enabled            = optional(bool, true)
    data = list(object({
      secret_key = string
      remote_key = string
      property   = optional(string)
    }))
  }))
  default  = []
  nullable = false
}

variable "cluster_smoke_ecw_api_key_secret_name" {
  description = "Secrets Manager name containing the in-cluster smoke ECW API key"
  type        = string
}

variable "cluster_smoke_slack_webhook_secret_name" {
  description = "Secrets Manager name containing the in-cluster smoke Slack webhook"
  type        = string
}

# Worker node groups the root declares beside this module (canaries, rollout
# smoke-test pools, kernel experiments). Their nodes run sandboxes, so the
# cluster controller's drain and terminate grants must cover them; ordering
# within each list is the order the grants are rendered in.
variable "additional_worker_asg_names" {
  description = "ASG names of root-declared worker MNGs, known at plan time; granted drain/terminate and tagged like the module's own worker ASGs"
  type        = list(string)
  default     = []
}

variable "additional_worker_asg_names_policy_only" {
  description = "ASG names of root-declared worker MNGs that may be unknown at plan time; granted drain/terminate only"
  type        = list(string)
  default     = []
}

variable "additional_worker_node_group_arns" {
  description = "EKS node group ARNs of root-declared worker MNGs, for the controller's describe/update grants"
  type        = list(string)
  default     = []
}
