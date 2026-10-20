variable "aws_region" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "cluster_vpc_id" {
  type = string
}

variable "oidc_provider_arn" {
  type = string
}

variable "oidc_issuer_host" {
  type = string
}

variable "hosted_zone_id" {
  description = "Private Route53 hosted zone external-dns manages. This module creates its association with cluster_vpc_id, so it must be private (Route53 associates VPCs with private zones only) and must not already be associated with that VPC. Required when external_dns_enabled is true."
  type        = string
  default     = null

  validation {
    condition = !var.external_dns_enabled || try(
      var.hosted_zone_id != "" && trimspace(var.hosted_zone_id) == var.hosted_zone_id,
      false,
    )
    error_message = "hosted_zone_id must name the Route53 zone, without surrounding whitespace, when external_dns_enabled is true."
  }
}

variable "external_dns_enabled" {
  type     = bool
  nullable = false
}

variable "alb_controller_role_name" {
  type = string
}

variable "external_dns_role_name" {
  type = string
}

variable "disruption_budget_chart_path" {
  description = "Path to the in-repo pod-disruption-budget chart, for controllers whose own chart can't render a budget."
  type        = string
}

variable "resource_tags" {
  type        = map(string)
  description = "Default AWS resource tags applied by in-cluster controllers."
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

variable "cluster_autoscaler" {
  description = "cluster-autoscaler for the one serving general node group, or null to install none of it. It scales between that group's own min and max, read live from its ASG, so a resize through the EKS API moves its bounds too."
  type = object({
    role_name              = string
    kubernetes_version     = string
    autoscaling_group_arn  = string
    autoscaling_group_name = string
    node_group_arn         = string
    # optional so a null takes false: the chart renders a null value as a bare
    # `--scale-down-enabled`, which the flag parser reads as true.
    scale_down_enabled = optional(bool, false)
    memory_mib         = number
    # Each null leaves cluster-autoscaler's own default (600s, 0s) in force.
    max_graceful_termination_sec = optional(number)
    new_pod_scale_up_delay       = optional(string)
  })
  default = null

  validation {
    condition     = try(floor(var.cluster_autoscaler.memory_mib) == var.cluster_autoscaler.memory_mib && var.cluster_autoscaler.memory_mib >= 256, var.cluster_autoscaler == null)
    error_message = "cluster_autoscaler.memory_mib must be set, a whole number of MiB, at least 256."
  }

  validation {
    # Each side carries its own try: `||` short-circuits only from Terraform
    # 1.12, and this module still allows 1.9. The upper bound keeps the value
    # below 1e6, from which Helm prints it in exponent form and
    # cluster-autoscaler's integer flag refuses it.
    condition     = try(var.cluster_autoscaler.max_graceful_termination_sec == null, true) || try(floor(var.cluster_autoscaler.max_graceful_termination_sec) == var.cluster_autoscaler.max_graceful_termination_sec && var.cluster_autoscaler.max_graceful_termination_sec >= 1 && var.cluster_autoscaler.max_graceful_termination_sec <= 86400, false)
    error_message = "cluster_autoscaler.max_graceful_termination_sec must be null or a whole number of seconds from 1 to 86400."
  }

  validation {
    condition     = try(var.cluster_autoscaler.new_pod_scale_up_delay == null, true) || can(regex("^[0-9]+[sm]$", var.cluster_autoscaler.new_pod_scale_up_delay))
    error_message = "cluster_autoscaler.new_pod_scale_up_delay must be null or a whole number of seconds or minutes, such as \"90s\"."
  }
}

variable "nodes_ready" {
  description = "Readiness token for the general node pool that every controller release schedules onto. Pass an expression that stays unknown until the pool exists (the space module passes its serving general node group's ARN check) so the releases wait for it; the default suits callers that order the pool first themselves."
  type        = bool
  default     = true
  nullable    = false
}
