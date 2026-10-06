variable "region" {
  description = "Region for the repositories. `artifact-ci.yml` reads the `SPACE_ECR_REGION` repository variable, which must match when it isn't `us-east-1`."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for the push role's name."
  type        = string
  default     = "space-oss"
}

variable "github_repository" {
  description = "The `owner/name` repository whose `main` builds push, as named in the role description and tags. Trust itself matches `github_oidc_subject_prefixes`, which can name more than this one repository."
  type        = string
  default     = "perplexityai/SPACE"
}

variable "github_oidc_subject_prefixes" {
  description = "Prefixes of the `sub` claims the trust accepts, one per repository. Each repository uses immutable subjects, which carry the owner and repository ids, so a renamed repository keeps its id and a replacement repository needs its own entry; read a repository's value with `gh api repos/<owner>/<name>/actions/oidc/customization/sub` (`sub_claim_prefix`). The defaults are `perplexityai/SPACE` and the subject its replacement repository will issue once it's renamed to `SPACE`. The replacement's pre-rename subject is left out, so the replacement can't assume the role until the rename. `infra/space/aws/reference/bootstrap` holds the same list for the account's other GitHub OIDC role; change both together."
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

variable "image_retention_count" {
  description = "Images each repository keeps, newest first. Every publishing `main` CI run pushes one image per repository. Choose a count that retains the required rollback window at your expected publishing frequency."
  type        = number
  default     = 300
}

variable "untagged_image_retention_days" {
  description = "Days before an untagged image expires. Commit tags are immutable, so an image only loses its tags when someone retags or untags it by hand; the grace period leaves time to notice a mistake."
  type        = number
  default     = 7
}
