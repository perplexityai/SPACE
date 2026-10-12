# Organization-scoped Secrets Manager reads for credential references.
#
# Credential Authority never reads a referenced secret with its own identity.
# For each read it assumes this role (SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN,
# the credmgr_secrets_reader_role_arn output) with the session tag
# space-org-id=<sandbox organization> and a session policy limited to secrets
# with that tag. This role's policy also denies, explicitly, any secret whose
# space-org-id tag is missing or differs from the session's, so neither this
# policy nor a secret's resource policy can let one organization's session read
# another organization's secret. Secrets in another account also need a
# resource policy that allows this role, and a secret under a customer-managed
# KMS key (required across accounts) needs that key's policy to allow this role
# kms:Decrypt with kms:ViaService = secretsmanager.<region>.amazonaws.com.
#
# The role grants nothing until a secret is tagged, so it exists in every
# environment, like the authority role it trusts.

data "aws_iam_policy_document" "credmgr_secrets_reader_assume_role" {
  statement {
    sid = "OnlyCredmgrAuthorityMayAssumePerOrganization"
    # sts:TagSession carries the organization tag, and the authority's EKS Pod
    # Identity session tags propagate on every downstream AssumeRole
    # (credmgr_s3_vend.tf).
    actions = [
      "sts:AssumeRole",
      "sts:TagSession",
    ]

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.credmgr_authority.arn]
    }

    # A session without an organization tag reads nothing below; refuse to
    # mint one at all.
    condition {
      test     = "Null"
      variable = "aws:RequestTag/space-org-id"
      values   = ["false"]
    }
  }
}

resource "aws_iam_role" "credmgr_secrets_reader" {
  name               = "${local.name}-credmgr-secrets-reader"
  assume_role_policy = data.aws_iam_policy_document.credmgr_secrets_reader_assume_role.json
  description        = "Role the credmgr authority assumes per organization to read that organization's tagged Secrets Manager secrets"

  tags = var.common_tags
}

data "aws_iam_policy_document" "credmgr_secrets_reader" {
  statement {
    sid       = "ReadOnlySecretsTaggedForTheSessionOrganization"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:*:*:secret:*"]

    condition {
      test     = "StringEquals"
      variable = "secretsmanager:ResourceTag/space-org-id"
      values   = ["$${aws:PrincipalTag/space-org-id}"]
    }
  }

  # Secrets Manager decrypts with the caller's authority. Allow it only inside a
  # Secrets Manager call, which the tag conditions here already authorize; a
  # customer-managed key's policy must also allow this role the same way.
  statement {
    sid       = "DecryptOnlyThroughSecretsManager"
    actions   = ["kms:Decrypt"]
    resources = ["*"]

    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values   = ["secretsmanager.*.amazonaws.com"]
    }
  }

  # A key policy can grant this role kms access on its own; never outside
  # Secrets Manager. StringNotLike is also true when the call is direct.
  statement {
    sid       = "DenyKmsOutsideSecretsManager"
    effect    = "Deny"
    actions   = ["kms:*"]
    resources = ["*"]

    condition {
      test     = "StringNotLike"
      variable = "kms:ViaService"
      values   = ["secretsmanager.*.amazonaws.com"]
    }
  }

  # An Allow above does not stop a same-account resource policy from granting
  # this role access on its own; an explicit Deny does. StringNotEquals is
  # also true when the secret has no space-org-id tag.
  statement {
    sid       = "DenySecretsNotTaggedForTheSessionOrganization"
    effect    = "Deny"
    actions   = ["secretsmanager:*"]
    resources = ["*"]

    condition {
      test     = "StringNotEquals"
      variable = "secretsmanager:ResourceTag/space-org-id"
      values   = ["$${aws:PrincipalTag/space-org-id}"]
    }
  }

  statement {
    sid       = "DenySessionsWithoutAnOrganization"
    effect    = "Deny"
    actions   = ["secretsmanager:*"]
    resources = ["*"]

    condition {
      test     = "Null"
      variable = "aws:PrincipalTag/space-org-id"
      values   = ["true"]
    }
  }
}

resource "aws_iam_role_policy" "credmgr_secrets_reader" {
  name   = "${local.name}-credmgr-secrets-reader"
  role   = aws_iam_role.credmgr_secrets_reader.name
  policy = data.aws_iam_policy_document.credmgr_secrets_reader.json
}

data "aws_iam_policy_document" "credmgr_authority_assume_secrets_reader" {
  statement {
    sid = "AssumeSecretsReaderRolePerOrganization"
    actions = [
      "sts:AssumeRole",
      "sts:TagSession",
    ]
    resources = [aws_iam_role.credmgr_secrets_reader.arn]
  }
}

resource "aws_iam_role_policy" "credmgr_authority_assume_secrets_reader" {
  name   = "${local.name}-credmgr-authority-assume-secrets-reader"
  role   = aws_iam_role.credmgr_authority.name
  policy = data.aws_iam_policy_document.credmgr_authority_assume_secrets_reader.json
}
