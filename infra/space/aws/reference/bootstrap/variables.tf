variable "region" {
  description = "Region for the state bucket. The cluster root must use the same region in its backend configuration."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for every resource this root creates. The state bucket appends the account id, so the default is globally unique per account."
  type        = string
  default     = "space-reference"
}

variable "github_repository" {
  description = "The `owner/name` repository whose workflows assume this root's roles, as named in role descriptions and tags. Trust itself matches `github_oidc_subject_prefixes`, which can name more than this one repository."
  type        = string
  default     = "perplexityai/SPACE"
}

variable "github_oidc_subject_prefixes" {
  description = "Prefixes of the `sub` claims the trust accepts, one per repository. Each repository uses immutable subjects, which carry the owner and repository ids, so a renamed repository keeps its id and a replacement repository needs its own entry; read a repository's value with `gh api repos/<owner>/<name>/actions/oidc/customization/sub` (`sub_claim_prefix`). The defaults are `perplexityai/SPACE` and the subject its replacement repository will issue once it's renamed to `SPACE`. The replacement's pre-rename subject is left out, so the replacement can't assume the role until the rename. `infra/space/aws/oss-registry` holds the same list for the account's other GitHub OIDC role; change both together."
  type        = list(string)
  nullable    = false
  default = [
    "repo:perplexityai@185426709/SPACE@1400568925",
    "repo:perplexityai@185426709/SPACE@1407686799",
  ]

  validation {
    condition = length(var.github_oidc_subject_prefixes) > 0 && alltrue([
      for prefix in var.github_oidc_subject_prefixes :
      can(regex("^repo:[^/@:]+@[0-9]+/[^/@:]+@[0-9]+$", prefix))
    ])
    error_message = "github_oidc_subject_prefixes must name at least one repository, each as an immutable subject prefix: repo:<owner>@<owner id>/<name>@<repository id>."
  }
}

variable "github_environment" {
  description = "GitHub environment the apply job runs in. The role trusts only tokens issued to this environment, so the environment's branch protection is what keeps pull requests and forks from assuming it."
  type        = string
  default     = "reference-deployment"
}

variable "create_github_oidc_provider" {
  description = "Create the account's GitHub Actions OIDC provider. An account holds at most one provider per issuer URL, so set this to false where one already exists."
  type        = bool
  default     = true
}

variable "state_noncurrent_version_retention_days" {
  description = "Days to keep superseded state versions. Versioning is what makes a bad apply's state recoverable, so keep this comfortably longer than the gap between noticing a failure and acting on it."
  type        = number
  default     = 90
}

variable "cluster_name" {
  description = "The cluster root's `cluster_name`. The apply role's EKS, IAM and log permissions are limited to resources named from it, so change both together."
  type        = string
  default     = "space-reference"
}

variable "cluster_admin_principal_arns" {
  description = "IAM principals granted cluster admin through EKS access entries: the people who run kubectl against the cluster. Each must already exist. The apply role is admin already, as the cluster's creator. Required, with no default: an apply that left it out would empty the list, and the next scheduled cluster apply would remove everyone's access. Pass `[]` for none."
  type        = list(string)
  nullable    = false
}
