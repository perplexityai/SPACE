locals {
  # Deterministic names of secrets published by the sibling
  # infra/space/aws/postgres and infra/space/aws/elasticache stacks. Both
  # stacks derive their secret names from the same per-environment `prefix`
  # input, mirrored here as `sibling_stack_prefix`. The bootstrap-role
  # credentials secret is consumed by the operator-run bootstrap-roles Job,
  # never by long-running services. The shared Valkey name is the ElastiCache
  # stack's cluster-mode-enabled connection-string output, separate from the
  # legacy rate-limiter Redis secret.
  postgres_credentials_secret_name            = "${var.sibling_stack_prefix}rds-credentials"
  redis_connection_string_secret_name         = "${var.sibling_stack_prefix}redis-connection-string"
  shared_valkey_connection_string_secret_name = "${var.sibling_stack_prefix}shared-redis-connection-string"
  db_bootstrap_role_credentials_secret_name   = "${var.sibling_stack_prefix}postgres-bootstrap-role-credentials"
}

module "app_deploy" {
  source = "../app_deploy"

  # A value edge, not a module-level depends_on: that would defer this
  # module's data sources whenever anything controllers depends on changes.
  external_secrets_operator_ready = module.controllers.ready

  aws_region                 = var.aws_region
  namespace                  = "space"
  oidc_provider_arn          = module.eks.oidc_provider_arn
  oidc_issuer_host           = local.oidc_issuer_hostpath
  current_account_id         = data.aws_caller_identity.current.account_id
  external_secrets_role_name = "${local.name}-external-secrets"
  external_secret_store_name = "space-aws-secrets"
  chart_path                 = "${var.charts_dir}/space-secrets-bootstrap"
  external_secrets = concat([
    {
      # This and the four listener identities below come from
      # internal_rpc_tls.tf. They change only on a Terraform renewal or
      # rotation, and processes load them at startup, so the chart-default 5m
      # refresh would re-fetch constants.
      name               = "space-internal-rpc-ca"
      target_secret_name = "space-internal-rpc-ca"
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "ca.crt"
          remote_key = aws_secretsmanager_secret.internal_rpc_ca.name
          property   = "ca_crt"
        }
      ]
    },
    {
      name               = "space-cluster-api-tls"
      target_secret_name = "space-cluster-api-tls"
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "tls.crt"
          remote_key = aws_secretsmanager_secret.cluster_api_tls.name
          property   = "tls_crt"
        },
        {
          secret_key = "tls.key"
          remote_key = aws_secretsmanager_secret.cluster_api_tls.name
          property   = "tls_key"
        }
      ]
    },
    {
      name               = "space-build-manager-tls"
      target_secret_name = "space-build-manager-tls"
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "tls.crt"
          remote_key = aws_secretsmanager_secret.build_manager_tls.name
          property   = "tls_crt"
        },
        {
          secret_key = "tls.key"
          remote_key = aws_secretsmanager_secret.build_manager_tls.name
          property   = "tls_key"
        }
      ]
    },
    {
      name               = "space-node-api-tls"
      target_secret_name = "space-node-api-tls"
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "tls.crt"
          remote_key = aws_secretsmanager_secret.node_api_tls.name
          property   = "tls_crt"
        },
        {
          secret_key = "tls.key"
          remote_key = aws_secretsmanager_secret.node_api_tls.name
          property   = "tls_key"
        }
      ]
    },
    {
      name               = "space-ingress-activation-tls"
      target_secret_name = "space-ingress-activation-tls"
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "tls.crt"
          remote_key = aws_secretsmanager_secret.ingress_activation_tls.name
          property   = "tls_crt"
        },
        {
          secret_key = "tls.key"
          remote_key = aws_secretsmanager_secret.ingress_activation_tls.name
          property   = "tls_key"
        }
      ]
    },
    {
      name               = "space-postgres"
      target_secret_name = "space-postgres"
      data = [
        {
          secret_key = "POSTGRES_CONNECTION_STRING"
          remote_key = var.postgres_connection_string_secret_name
        },
        {
          secret_key = "PGHOST"
          remote_key = local.postgres_credentials_secret_name
          property   = "host"
        },
        {
          secret_key = "PGPORT"
          remote_key = local.postgres_credentials_secret_name
          property   = "port"
        },
        {
          secret_key = "PGUSER"
          remote_key = local.postgres_credentials_secret_name
          property   = "username"
        },
        {
          secret_key = "PGPASSWORD"
          remote_key = local.postgres_credentials_secret_name
          property   = "password"
        },
        {
          secret_key = "PGDATABASE"
          remote_key = local.postgres_credentials_secret_name
          property   = "dbname"
        }
      ]
    },
    {
      name               = "space-db-bootstrap-roles"
      target_secret_name = "space-db-bootstrap-roles"
      data = [
        {
          secret_key = "SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD"
          remote_key = local.db_bootstrap_role_credentials_secret_name
          property   = "SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD"
        },
        {
          secret_key = "SPACE_DB_BOOTSTRAP_USER_PASSWORD"
          remote_key = local.db_bootstrap_role_credentials_secret_name
          property   = "SPACE_DB_BOOTSTRAP_USER_PASSWORD"
        },
        {
          secret_key = "SPACE_DB_BOOTSTRAP_RO_PASSWORD"
          remote_key = local.db_bootstrap_role_credentials_secret_name
          property   = "SPACE_DB_BOOTSTRAP_RO_PASSWORD"
        },
        {
          secret_key = "SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD"
          remote_key = local.db_bootstrap_role_credentials_secret_name
          property   = "SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD"
        },
        {
          secret_key = "SPACE_DB_BOOTSTRAP_CREDS_PASSWORD"
          remote_key = local.db_bootstrap_role_credentials_secret_name
          property   = "SPACE_DB_BOOTSTRAP_CREDS_PASSWORD"
        }
      ]
    },
    {
      name               = "space-redis"
      target_secret_name = "space-redis"
      data = [
        {
          secret_key = "SPACE_REDIS_URL"
          remote_key = local.redis_connection_string_secret_name
        }
      ]
    },
    {
      name               = "space-shared-valkey"
      target_secret_name = "space-shared-valkey"
      data = [
        {
          secret_key = "SPACE_SHARED_VALKEY_URL"
          remote_key = local.shared_valkey_connection_string_secret_name
        }
      ]
    },
    {
      name               = "space-internal"
      target_secret_name = "space-internal"
      data = [
        {
          secret_key = "SPACE_INTERNAL_TOKEN"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_INTERNAL_TOKEN"
        },
        {
          secret_key = "SPACE_INTERNAL_CONTROL_TOKEN"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_INTERNAL_CONTROL_TOKEN"
        },
        {
          secret_key = "SPACE_PUBLIC_CURSOR_SECRET"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_PUBLIC_CURSOR_SECRET"
        },
        {
          secret_key = "SPACE_ECW_CREDENTIAL_KEY"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_ECW_CREDENTIAL_KEY"
        }
      ]
    },
    {
      name               = "space-api-route-signing"
      target_secret_name = "space-api-route-signing"
      data = [
        {
          secret_key = "SPACE_API_ROUTE_ED25519_SIGNING_KEY"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_API_ROUTE_ED25519_SIGNING_KEY"
        },
        {
          secret_key = "SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID"
        },
        {
          secret_key = "SPACE_API_ROUTE_ED25519_ISSUER"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_API_ROUTE_ED25519_ISSUER"
        }
      ]
    },
    {
      name               = "space-api-route-verification"
      target_secret_name = "space-api-route-verification"
      data = [
        {
          secret_key = "SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE"
          remote_key = aws_secretsmanager_secret.space_internal.name
          property   = "SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE"
        }
      ]
    },
    {
      # Eppo SDK key: one externally-owned secret per AWS account, shared by
      # every cluster in it.
      # Nothing in this repo creates it, so the name has to be a variable each
      # cluster's tfvars sets. Deriving it from local.name would name a
      # per-cluster secret no account has, and would do so convincingly — the
      # interpolation, the IAM grant, and the rendered ExternalSecret all look
      # right whether or not anyone made the secret.
      #
      # Naming the secret is also the gate, rather than a boolean beside it.
      # Every other gated entry here reads a secret whose name this stack knows
      # either way, so its `enabled` says "is the value there yet"; this one's
      # name is itself the deployment choice, and a separate boolean would be a
      # second knob free to disagree with it. A deployment that configures no
      # flag provider names nothing and syncs nothing; modules/app_deploy owns
      # what an unnamed entry does to the Secrets Manager grant.
      name               = "space-eppo"
      target_secret_name = "space-eppo"
      enabled            = var.eppo_secret_id != ""
      data = [
        {
          secret_key = "EPPO_SDK_KEY"
          remote_key = var.eppo_secret_id
          property   = "EPPO_SDK_KEY"
        }
      ]
    },
    {
      # Server-TLS keypair the space-credmgr-authority Deployment mounts at
      # /etc/space-credmgr-tls. The SM secret stores the PEMs under
      # underscore property names (tls_crt / tls_key) because external-secrets
      # resolves `property` with gjson path syntax, where a dot is a path
      # separator; the synced Kubernetes secret keys keep the conventional
      # dotted tls.crt / tls.key filenames the container expects.
      #
      # Off where space-credmgr-authority itself is off: the SM secret exists on
      # every cluster (credmgr_authority_tls.tf creates it; referencing the
      # resource orders the two), so syncing it early would work, it would just
      # leave a synced secret nothing mounts. Flip it with the Deployment.
      name               = "space-credmgr-authority-tls"
      target_secret_name = "space-credmgr-authority-tls"
      enabled            = var.credmgr_authority_tls_external_secret_enabled
      data = [
        {
          secret_key = "tls.crt"
          remote_key = aws_secretsmanager_secret.credmgr_authority_tls.name
          property   = "tls_crt"
        },
        {
          secret_key = "tls.key"
          remote_key = aws_secretsmanager_secret.credmgr_authority_tls.name
          property   = "tls_key"
        }
      ]
    },
    {
      # Shared bearer the credmgr authority presents to the intermediate-key
      # broker. The SAME value has to be provisioned on the broker side as the
      # token it expects from its branching-key provider.
      #
      # The SM secret is populated out-of-band and is not created by Terraform,
      # so this stays off until it exists — an ExternalSecret pointed at an
      # absent SM secret reports SecretSyncError forever. Flip it on together
      # with space-credmgr-authority.intermediateKeyBroker.enabled, per cluster,
      # once the value is in that account. Its remote key names a secret this
      # module doesn't create, so the gate is the whole safeguard, and it
      # defaults off — nothing here can see into an account.
      name               = "space-credmgr-branching-provider-token"
      target_secret_name = "space-credmgr-branching-provider-token"
      enabled            = var.credmgr_intermediate_key_broker_token_external_secret_enabled
      data = [
        {
          secret_key = "token"
          remote_key = "${local.name}-credmgr-branching-provider-token"
          property   = "token"
        }
      ]
    },
    {
      # ECW API key the in-cluster smoke Job authenticates with.
      # In this cluster's own account, which is what lets a cluster join smoke
      # coverage without a key filed in another account for an agent looking in
      # from outside.
      #
      # Off until the key is minted against this cluster: Terraform creates the
      # container, not the value, and the Job treats the secret as required —
      # syncing an empty one would trade a clear SecretSyncError for a smoke
      # that 401s on every request.
      name               = "space-smoke"
      target_secret_name = "space-smoke"
      enabled            = var.cluster_smoke_ecw_api_key_external_secret_enabled
      # The key changes only when an operator rotates it, so the chart-default
      # 5m refresh would re-fetch a constant all year.
      refresh_interval = "1h"
      data = [
        {
          secret_key = "E2B_API_KEY"
          remote_key = var.cluster_smoke_ecw_api_key_secret_name
          property   = "E2B_API_KEY"
        }
      ]
    },
    {
      # Slack webhook the in-cluster smoke Job posts its own run summary to, for
      # the runs no CI step is watching. Gated separately from the key:
      # the Job reports through its logs, its summary artifact, and its metrics
      # whether or not this exists.
      name               = "space-smoke-slack"
      target_secret_name = "space-smoke-slack"
      enabled            = var.cluster_smoke_slack_webhook_external_secret_enabled
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "SPACE_SMOKE_SLACK_WEBHOOK_URL"
          remote_key = var.cluster_smoke_slack_webhook_secret_name
          property   = "SPACE_SMOKE_SLACK_WEBHOOK_URL"
        }
      ]
    },
    {
      # Per-cluster sandbox SSH keypair (sandbox_ssh.tf). Synced on every
      # cluster — Terraform creates the SM secret in this same apply, and a
      # synced secret nothing mounts is inert. The consumer gate is the
      # space-build-manager chart's `sandboxSsh.enabled`, flipped per cluster
      # after the seed trusts the corresponding public key.
      # The key changes only during an operator-driven cutover; hourly refresh
      # avoids repeatedly fetching an unchanged value.
      name               = "space-sandbox-ssh"
      target_secret_name = "space-sandbox-ssh"
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "sandbox_ed25519"
          remote_key = aws_secretsmanager_secret.sandbox_ssh.name
          property   = "sandbox_ed25519"
        },
        {
          secret_key = "sandbox_ed25519.pub"
          remote_key = aws_secretsmanager_secret.sandbox_ssh.name
          property   = "sandbox_ed25519_pub"
        }
      ]
    },
    {
      # Public SPACE MITM root CA (mitm_root_ca.tf), mounted into build-manager
      # via the chart's mitmRootCa. Enabled per cluster from tfvars, on only
      # where the certificates stack has a root to source from.
      name               = "space-mitm-root-ca"
      target_secret_name = "space-mitm-root-ca"
      enabled            = var.mitm_root_ca_external_secret_enabled
      refresh_interval   = "1h"
      data = [
        {
          secret_key = "root-ca.pem"
          remote_key = "${local.name}-mitm-root-ca"
          property   = "root_ca_pem"
        }
      ]
    }
  ], var.additional_external_secrets)
}
