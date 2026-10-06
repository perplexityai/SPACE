# Testing-only AWS fixture for the deployed network-credential path. The value
# is deliberately public and fixed: the smoke proves that Credential Authority
# reads it from Secrets Manager and net-gateway injects it, not that it is secret.

locals {
  network_credential_smoke_count = var.network_credential_smoke_enabled ? 1 : 0
  network_credential_smoke_ci_count = (
    var.network_credential_smoke_enabled && local.ci_smoke_test_enabled ? 1 : 0
  )
}

resource "aws_secretsmanager_secret" "network_credential_smoke" {
  count = local.network_credential_smoke_count

  name        = "${local.name}-network-credential-smoke"
  description = "Fixed dummy Authorization value for testing network credential injection"

  tags = var.common_tags
}

resource "aws_secretsmanager_secret_version" "network_credential_smoke" {
  count = local.network_credential_smoke_count

  secret_id     = aws_secretsmanager_secret.network_credential_smoke[0].id
  secret_string = "Bearer space-network-credential-smoke"
}

data "aws_iam_policy_document" "credmgr_authority_secrets_manager_read" {
  count = local.network_credential_smoke_count

  # Scoped to the smoke fixture only. Credential Authority's own role must not
  # read arbitrary secrets in the account.
  statement {
    sid       = "ReadNetworkCredentialSmokeSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.network_credential_smoke[0].arn]
  }
}

resource "aws_iam_role_policy" "credmgr_authority_secrets_manager_read" {
  count = local.network_credential_smoke_count

  name   = "${local.name}-credmgr-authority-secrets-manager-read"
  role   = aws_iam_role.credmgr_authority.name
  policy = data.aws_iam_policy_document.credmgr_authority_secrets_manager_read[0].json
}

data "aws_iam_policy_document" "ci_smoke_test_network_credential_describe" {
  count = local.network_credential_smoke_ci_count

  # The runner needs the generated ARN to register a reference, but it must
  # never read the credential value. Credential Authority remains the only
  # principal in this flow with secretsmanager:GetSecretValue.
  statement {
    sid       = "DescribeNetworkCredentialSmokeSecret"
    actions   = ["secretsmanager:DescribeSecret"]
    resources = [aws_secretsmanager_secret.network_credential_smoke[0].arn]
  }
}

resource "aws_iam_role_policy" "ci_smoke_test_network_credential_describe" {
  count = local.network_credential_smoke_ci_count

  name   = "${var.ci_smoke_test_role_name}-network-credential-describe"
  role   = aws_iam_role.ci_smoke_test[0].id
  policy = data.aws_iam_policy_document.ci_smoke_test_network_credential_describe[0].json
}
