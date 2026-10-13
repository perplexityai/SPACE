# Per-cluster internal RPC CA and listener identities. external-secrets syncs
# them (app_deploy.tf) into space-internal-rpc-ca, which callers mount at
# /etc/space-rpc-ca, and space-<listener>-tls, which only that listener mounts
# at /etc/space-rpc-tls. See docs/internal-rpc-tls.md for the runtime contract.
#
# A cluster-local issuer keeps leaf renewal independent of client trust. Every
# private key here stays in Terraform state, so protect that state as key
# material; leaf keys are also copied to Secrets Manager, the CA key is not.
#
# Replacing the CA re-signs every leaf in the same apply while running callers
# still trust only the old CA, so it refuses destruction. That includes reaching
# its own expiry: rotate it with overlapping trust instead.
resource "tls_private_key" "internal_rpc_ca" {
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"

  lifecycle {
    prevent_destroy = true
  }
}

resource "tls_self_signed_cert" "internal_rpc_ca" {
  private_key_pem = tls_private_key.internal_rpc_ca.private_key_pem
  subject {
    common_name = "${local.name} internal RPC CA"
  }
  is_ca_certificate     = true
  validity_period_hours = 87600
  allowed_uses          = ["cert_signing", "crl_signing"]

  lifecycle {
    prevent_destroy = true
  }
}

resource "tls_private_key" "internal_rpc_server" {
  for_each    = toset(["build-manager", "cluster-api", "ingress-activation", "node-api"])
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"
}

resource "tls_cert_request" "internal_rpc_server" {
  for_each        = tls_private_key.internal_rpc_server
  private_key_pem = each.value.private_key_pem
  subject {
    common_name = "space-${each.key}"
  }
  dns_names = {
    build-manager      = ["space-builder-build-manager"]
    cluster-api        = ["space-internal-api", "space-cluster-api"]
    node-api           = ["space-node-api"]
    ingress-activation = ["space-ingress-activation"]
  }[each.key]
}

resource "tls_locally_signed_cert" "internal_rpc_server" {
  for_each              = tls_cert_request.internal_rpc_server
  cert_request_pem      = each.value.cert_request_pem
  ca_private_key_pem    = tls_private_key.internal_rpc_ca.private_key_pem
  ca_cert_pem           = tls_self_signed_cert.internal_rpc_ca.cert_pem
  validity_period_hours = 8760
  # Renewal happens only when an apply runs inside this window; nothing applies
  # on a schedule. Listeners load identities at startup, so a renewed leaf takes
  # effect at each listener's next restart.
  early_renewal_hours = 1440
  allowed_uses        = ["digital_signature", "server_auth"]
}

resource "aws_secretsmanager_secret" "internal_rpc_ca" {
  name = "${local.name}-internal-rpc-ca"
  tags = var.common_tags
}

resource "aws_secretsmanager_secret_version" "internal_rpc_ca" {
  secret_id     = aws_secretsmanager_secret.internal_rpc_ca.id
  secret_string = jsonencode({ ca_crt = tls_self_signed_cert.internal_rpc_ca.cert_pem })
}

resource "aws_secretsmanager_secret" "build_manager_tls" {
  name = "${local.name}-build-manager-tls"
  tags = var.common_tags
}

resource "aws_secretsmanager_secret_version" "build_manager_tls" {
  secret_id = aws_secretsmanager_secret.build_manager_tls.id
  secret_string = jsonencode({
    tls_crt = tls_locally_signed_cert.internal_rpc_server["build-manager"].cert_pem
    tls_key = tls_private_key.internal_rpc_server["build-manager"].private_key_pem
  })
}

resource "aws_secretsmanager_secret" "cluster_api_tls" {
  name = "${local.name}-cluster-api-tls"
  tags = var.common_tags
}

resource "aws_secretsmanager_secret_version" "cluster_api_tls" {
  secret_id = aws_secretsmanager_secret.cluster_api_tls.id
  secret_string = jsonencode({
    tls_crt = tls_locally_signed_cert.internal_rpc_server["cluster-api"].cert_pem
    tls_key = tls_private_key.internal_rpc_server["cluster-api"].private_key_pem
  })
}

resource "aws_secretsmanager_secret" "node_api_tls" {
  name = "${local.name}-node-api-tls"
  tags = var.common_tags
}

resource "aws_secretsmanager_secret_version" "node_api_tls" {
  secret_id = aws_secretsmanager_secret.node_api_tls.id
  secret_string = jsonencode({
    tls_crt = tls_locally_signed_cert.internal_rpc_server["node-api"].cert_pem
    tls_key = tls_private_key.internal_rpc_server["node-api"].private_key_pem
  })
}

resource "aws_secretsmanager_secret" "ingress_activation_tls" {
  name = "${local.name}-ingress-activation-tls"
  tags = var.common_tags
}

resource "aws_secretsmanager_secret_version" "ingress_activation_tls" {
  secret_id = aws_secretsmanager_secret.ingress_activation_tls.id
  secret_string = jsonencode({
    tls_crt = tls_locally_signed_cert.internal_rpc_server["ingress-activation"].cert_pem
    tls_key = tls_private_key.internal_rpc_server["ingress-activation"].private_key_pem
  })
}
