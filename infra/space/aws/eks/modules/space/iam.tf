data "aws_iam_policy_document" "node_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      identifiers = ["ec2.amazonaws.com"]
      type        = "Service"
    }
  }
}

# The legacy `workers-cgnat` pool's role. Every other pool's node role lives in
# the core module (`module.eks`); this one stays until the pool is retired,
# because `aws_eks_node_group.node_role_arn` is ForceNew and the live MNG must
# keep its role.
resource "aws_iam_role" "node" {
  for_each = local.legacy_worker_node_pools

  assume_role_policy = data.aws_iam_policy_document.node_assume_role.json
  name               = "${local.name}-${each.key}-nodes"

  tags = merge(var.common_tags, {
    Name = "${local.name}-${each.key}-nodes"
  })
}

# Shared worker IAM role for workers-N rollouts and permanent capacity groups.
# Used by the rollout module in `rollout.tf` and the steady-state MNG resource
# in `worker_capacity.tf`.
# In environments that haven't retired the legacy MNG yet
# (`var.workers_cgnat_retired = false`) it keeps its own
# per-pool `aws_iam_role.node["workers-cgnat"]` role and stays untouched —
# IAM role names are immutable and `aws_eks_node_group.node_role_arn` is
# `ForceNew`, so swapping the live MNG's role would force-recreate it.
# New MNGs reference this standalone role so retiring an older workers-N group
# and its local.node_pools entry cannot delete an IAM role still in use.
resource "aws_iam_role" "workers_pool" {
  # Gated on a promoted workers-N steady state, an in-flight workers-cgnat ->
  # workers-1 rollout, or at least one permanent capacity group. The role has
  # to exist before either worker resource launches instances.
  count = local.workers_shared_identity_needed ? 1 : 0

  assume_role_policy = data.aws_iam_policy_document.node_assume_role.json
  name               = "${local.name}-workers-nodes"

  tags = merge(var.common_tags, {
    Name = "${local.name}-workers-nodes"
  })
}

# EKS access entry binding the `workers_pool` role to the `system:nodes`
# Kubernetes group. Without this entry, kubelet on a `workers-N` instance
# authenticates to STS successfully but its `aws eks get-token` payload is
# rejected by the EKS API server with `Unauthorized`, and the node never
# registers (the MNG sits in `CREATE_FAILED` after the EKS-side timeout).
#
# EKS access entries bind a *role's identity* (not just its ARN string).
# Recreating a role with the same name does not update an existing access
# entry: its stored role identity must match the replacement role.
resource "aws_eks_access_entry" "workers_pool" {
  count = local.workers_shared_identity_needed ? 1 : 0

  cluster_name  = module.eks.cluster_name
  principal_arn = aws_iam_role.workers_pool[0].arn
  # EKS auto-binds `EC2_LINUX` access entries to the `system:nodes` group
  # (and assigns the `system:node:{{EC2PrivateDNSName}}` username) based
  # on `type`. Explicitly passing `kubernetes_groups = ["system:nodes"]`
  # or any other `system:*` value is rejected by the API with
  # `InvalidParameterException: ... cannot start with system:` — the
  # value materializes on describe-access-entry but must not be sent at
  # create time.
  type = "EC2_LINUX"

  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "node_managed" {
  # `AmazonEKS_CNI_Policy` moved to the dedicated VPC CNI IRSA role
  # (`module.eks.aws_iam_role.vpc_cni`). `AmazonSSMManagedInstanceCore` is attached
  # to every pool so SSM access does not depend on attaching a policy during
  # troubleshooting. The core module supplies the policies for its own pools.
  for_each = {
    for attachment in flatten([
      for node_group in keys(local.legacy_worker_node_pools) : [
        for policy_arn in module.eks.node_managed_policy_arns : {
          node_group = node_group
          policy_arn = policy_arn
        }
      ]
    ]) : "${attachment.node_group}-${replace(basename(attachment.policy_arn), ":", "-")}" => attachment
  }

  policy_arn = each.value.policy_arn
  role       = aws_iam_role.node[each.value.node_group].name
}

# Policies attached to the shared workers IAM role. Mirrors the per-pool
# attachments `workers-cgnat` gets via `node_managed`: WorkerNode + ECR +
# SSM (the latter restored to keep SSM standing on every worker). CNI lives
# on `module.eks.aws_iam_role.vpc_cni`.
resource "aws_iam_role_policy_attachment" "workers_pool_managed" {
  # Empty for_each in environments where `aws_iam_role.workers_pool` isn't
  # created (see its `count` gate above).
  for_each = local.workers_shared_identity_needed ? toset(module.eks.node_managed_policy_arns) : toset([])

  policy_arn = each.value
  role       = aws_iam_role.workers_pool[0].name
}

data "aws_iam_policy_document" "s3_read" {
  count = length(var.s3_read_bucket_names) > 0 ? 1 : 0

  statement {
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
    ]
    resources = concat(local.s3_read_bucket_arns, local.s3_read_object_arns)
  }
}

resource "aws_iam_policy" "s3_read" {
  count = length(var.s3_read_bucket_names) > 0 ? 1 : 0

  name   = "${local.name}-s3-read"
  policy = data.aws_iam_policy_document.s3_read[0].json

  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "node_s3_read" {
  for_each = length(var.s3_read_bucket_names) > 0 ? local.legacy_worker_node_pools : {}

  policy_arn = aws_iam_policy.s3_read[0].arn
  role       = aws_iam_role.node[each.key].name
}

resource "aws_iam_role_policy_attachment" "workers_pool_s3_read" {
  count = (length(var.s3_read_bucket_names) > 0 && local.workers_shared_identity_needed) ? 1 : 0

  policy_arn = aws_iam_policy.s3_read[0].arn
  role       = aws_iam_role.workers_pool[0].name
}

data "aws_iam_policy_document" "builder_template_artifacts_publish" {
  statement {
    actions = [
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = [
      local.template_artifacts_build_inputs,
      local.template_artifacts_templates_objects,
    ]
  }

  statement {
    actions = [
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      local.template_artifacts_bucket_arn,
    ]
  }
}

resource "aws_iam_policy" "builder_template_artifacts_publish" {
  name   = "${local.name}-builder-template-artifacts-publish"
  policy = data.aws_iam_policy_document.builder_template_artifacts_publish.json

  tags = var.common_tags
}

# Read-side counterpart to `builder_template_artifacts_publish`. Workers and
# builders both pull template artifacts and content-addressed build inputs from
# S3 via space-credmgr-svc (which signs requests with its own IRSA identity).
# ListBucket is included so credmgr can scope HeadBucket / directory
# enumeration calls.
data "aws_iam_policy_document" "template_artifacts_pull" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      local.template_artifacts_build_inputs,
      local.template_artifacts_templates_objects,
    ]
  }

  statement {
    actions = [
      "s3:ListBucket",
    ]
    resources = [
      local.template_artifacts_bucket_arn,
    ]
  }
}

resource "aws_iam_policy" "template_artifacts_pull" {
  name   = "${local.name}-template-artifacts-pull"
  policy = data.aws_iam_policy_document.template_artifacts_pull.json

  tags = var.common_tags
}

# Suspend-snapshot uploads from sbx-mgr (write) and cross-node restore
# pulls (read). The snapshots bucket holds nothing else, so the policy
# grants read/write on the entire bucket rather than scoping to a key
# prefix.
data "aws_iam_policy_document" "snapshots_rw" {
  statement {
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = [
      local.snapshots_bucket_objects,
    ]
  }

  statement {
    actions = [
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      local.snapshots_bucket_arn,
    ]
  }
}

resource "aws_iam_policy" "snapshots_rw" {
  name   = "${local.name}-snapshots-rw"
  policy = data.aws_iam_policy_document.snapshots_rw.json

  tags = var.common_tags
}

# Node role per tenant-encrypted launch template. Separate from the shared
# workers role so only that tenant's nodes can use its KMS key: the key policy
# names this role, and the shared role never appears in it. Otherwise the same
# node permissions as `workers_pool`.
resource "aws_iam_role" "workers_encrypted" {
  for_each = var.encrypted_worker_launch_templates

  assume_role_policy = data.aws_iam_policy_document.node_assume_role.json
  name               = "${local.name}-workers-enc-${each.key}-nodes"

  tags = merge(var.common_tags, {
    Name = "${local.name}-workers-enc-${each.key}-nodes"
  })
}

resource "aws_eks_access_entry" "workers_encrypted" {
  for_each = var.encrypted_worker_launch_templates

  cluster_name  = module.eks.cluster_name
  principal_arn = aws_iam_role.workers_encrypted[each.key].arn
  type          = "EC2_LINUX"

  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "workers_encrypted_managed" {
  for_each = {
    for attachment in flatten([
      for name in keys(var.encrypted_worker_launch_templates) : [
        for policy_arn in module.eks.node_managed_policy_arns : {
          name       = name
          policy_arn = policy_arn
        }
      ]
    ]) : "${attachment.name}-${replace(basename(attachment.policy_arn), ":", "-")}" => attachment
  }

  policy_arn = each.value.policy_arn
  role       = aws_iam_role.workers_encrypted[each.value.name].name
}

resource "aws_iam_role_policy_attachment" "workers_encrypted_s3_read" {
  for_each = length(var.s3_read_bucket_names) > 0 ? var.encrypted_worker_launch_templates : {}

  policy_arn = aws_iam_policy.s3_read[0].arn
  role       = aws_iam_role.workers_encrypted[each.key].name
}

# The cache unit wraps one data key per boot and the watchdog unwraps it every
# minute, both under the node-cache encryption context. Nothing else.
data "aws_iam_policy_document" "workers_encrypted_cache_key" {
  for_each = var.encrypted_worker_launch_templates

  statement {
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [each.value.kms_key_arn]

    condition {
      test     = "StringEquals"
      variable = "kms:EncryptionContext:space:purpose"
      values   = ["node-cache"]
    }
  }
}

resource "aws_iam_role_policy" "workers_encrypted_cache_key" {
  for_each = var.encrypted_worker_launch_templates

  name   = "cache-encryption-key"
  role   = aws_iam_role.workers_encrypted[each.key].id
  policy = data.aws_iam_policy_document.workers_encrypted_cache_key[each.key].json
}
