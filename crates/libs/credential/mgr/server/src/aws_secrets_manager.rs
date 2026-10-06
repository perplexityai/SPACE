//! AWS Secrets Manager provider for reference-based credential injection.
//!
//! Every read runs in a session scoped to the requesting organization: the
//! authority assumes the reader role named by
//! `SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN` with the session tag
//! `space-org-id=<organization>` and a session policy that allows
//! `GetSecretValue` only on secrets tagged with that same organization, and
//! `kms:Decrypt` only through Secrets Manager. The session policy caps the
//! session whatever the role's own policy or a secret's resource policy grants
//! the role, so a reference cannot read another organization's secret even
//! where the authority's own identity could. A secret under a customer-managed
//! key also needs that key's policy to allow the role `kms:Decrypt` through
//! Secrets Manager. The committed effective network policy limits destinations.

use std::num::NonZeroUsize;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use lru::LruCache;
use pplx_space_credential_injection_core::secret_arn::SecretArn;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::credential_provider::{
    CredentialFetch, CredentialProviderResolver, PreparedCredential, ResolutionContext,
    ResolutionError,
};

/// IAM role the authority assumes, per organization, for every secret read.
pub const SECRETS_MANAGER_ROLE_ARN_ENV: &str = "SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN";
/// Session tag carrying the requesting organization's ID.
pub const ORGANIZATION_SESSION_TAG: &str = "space-org-id";

const MAX_RESOLVED_SECRET_BYTES: usize = 16 * 1024;
const AWS_SECRET_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(10);
/// Organizations whose sessions stay cached; the least recently used is
/// evicted and its session assumed again on its next read.
const MAX_CACHED_ORGANIZATIONS: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{SECRETS_MANAGER_ROLE_ARN_ENV} must be an IAM role ARN")]
pub struct ReaderRoleArnError;

/// A structurally valid IAM role ARN for the organization-scoped reader role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderRoleArn(String);

impl ReaderRoleArn {
    pub fn parse(raw: String) -> Result<Self, ReaderRoleArnError> {
        if crate::iam_role_arn::is_iam_role_arn(&raw) {
            Ok(Self(raw))
        } else {
            Err(ReaderRoleArnError)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The configured reader role, or `None` when this authority resolves no
/// credential references.
pub fn reader_role_arn_from_env() -> Result<Option<ReaderRoleArn>, ReaderRoleArnError> {
    parse_reader_role_arn(std::env::var(SECRETS_MANAGER_ROLE_ARN_ENV).ok())
}

fn parse_reader_role_arn(
    value: Option<String>,
) -> Result<Option<ReaderRoleArn>, ReaderRoleArnError> {
    value
        .filter(|value| !value.is_empty())
        .map(ReaderRoleArn::parse)
        .transpose()
}

/// Session policy limiting an organization's session to secrets tagged with
/// that organization. Secrets Manager decrypts with the caller's authority, so
/// the session also needs `kms:Decrypt`, but only inside a Secrets Manager call
/// that the tag condition already authorized.
fn session_policy(organization_id: Uuid) -> String {
    let mut tag_matches = serde_json::Map::new();
    tag_matches.insert(
        format!("secretsmanager:ResourceTag/{ORGANIZATION_SESSION_TAG}"),
        organization_id.to_string().into(),
    );
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "secretsmanager:GetSecretValue",
                "Resource": "*",
                "Condition": { "StringEquals": tag_matches },
            },
            {
                "Effect": "Allow",
                "Action": "kms:Decrypt",
                "Resource": "*",
                "Condition": { "StringLike": { "kms:ViaService": "secretsmanager.*.amazonaws.com" } },
            },
        ]
    })
    .to_string()
}

/// Resolved current `SecretString`. The value is zeroized on drop; callers
/// should move it directly into their redacted transport type.
pub struct ResolvedAwsSecret {
    value: Zeroizing<String>,
}

impl ResolvedAwsSecret {
    pub(crate) fn new(value: Zeroizing<String>) -> Self {
        Self { value }
    }

    pub fn into_value(self) -> Zeroizing<String> {
        self.value
    }
}

impl std::fmt::Debug for ResolvedAwsSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedAwsSecret")
            .field("value", &"<redacted>")
            .finish()
    }
}

struct AwsSecretFetch {
    client: aws_sdk_secretsmanager::Client,
    secret_arn: String,
}

/// AWS resolver that reads every secret through a session scoped to the
/// requesting organization.
pub struct AwsSecretsManagerResolver {
    base_aws_config: aws_config::SdkConfig,
    reader_role_arn: ReaderRoleArn,
    organization_configs: Mutex<LruCache<Uuid, aws_config::SdkConfig>>,
}

impl AwsSecretsManagerResolver {
    pub fn new(base_aws_config: &aws_config::SdkConfig, reader_role_arn: ReaderRoleArn) -> Self {
        Self {
            base_aws_config: base_aws_config.clone(),
            reader_role_arn,
            organization_configs: Mutex::new(LruCache::new(MAX_CACHED_ORGANIZATIONS)),
        }
    }

    /// SDK configuration whose credentials are the organization's reader-role
    /// session. Cached so its regional clients share one credentials cache,
    /// which renews the session before it expires.
    async fn organization_config(&self, organization_id: Uuid) -> aws_config::SdkConfig {
        if let Some(config) = self.cache().get(&organization_id) {
            return config.clone();
        }
        let session = aws_config::sts::AssumeRoleProvider::builder(self.reader_role_arn.as_str())
            .session_name(format!("space-org-{organization_id}"))
            .tags([(ORGANIZATION_SESSION_TAG, organization_id.to_string())])
            .policy(session_policy(organization_id))
            .configure(&self.base_aws_config)
            .build()
            .await;
        let config = self
            .base_aws_config
            .clone()
            .into_builder()
            .credentials_provider(SharedCredentialsProvider::new(session))
            .identity_cache(aws_config::identity::IdentityCache::lazy().build())
            .build();
        self.cache().put(organization_id, config.clone());
        config
    }

    fn cache(&self) -> MutexGuard<'_, LruCache<Uuid, aws_config::SdkConfig>> {
        self.organization_configs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait]
impl CredentialProviderResolver for AwsSecretsManagerResolver {
    type Reference = SecretArn;
    type Material = ResolvedAwsSecret;

    async fn prepare(
        &self,
        context: ResolutionContext,
        reference: &SecretArn,
    ) -> Result<PreparedCredential<ResolvedAwsSecret>, ResolutionError> {
        let config = self.organization_config(context.organization_id).await;
        Ok(PreparedCredential::new(AwsSecretFetch {
            client: build_client(&config, reference.region()),
            secret_arn: reference.as_str().to_owned(),
        }))
    }
}

#[async_trait]
impl CredentialFetch<ResolvedAwsSecret> for AwsSecretFetch {
    async fn fetch(self: Box<Self>) -> Result<ResolvedAwsSecret, ResolutionError> {
        tokio::time::timeout(
            AWS_SECRET_RESOLUTION_TIMEOUT,
            get_secret_value(&self.client, &self.secret_arn),
        )
        .await
        .map_err(|_| ResolutionError::ProviderUnavailable)?
    }
}

fn build_client(
    base_aws_config: &aws_config::SdkConfig,
    region: &str,
) -> aws_sdk_secretsmanager::Client {
    let builder = aws_sdk_secretsmanager::config::Builder::from(base_aws_config).region(
        aws_sdk_secretsmanager::config::Region::new(region.to_owned()),
    );
    aws_sdk_secretsmanager::Client::from_conf(builder.build())
}

async fn get_secret_value(
    client: &aws_sdk_secretsmanager::Client,
    secret_arn: &str,
) -> Result<ResolvedAwsSecret, ResolutionError> {
    let mut output = client
        .get_secret_value()
        .secret_id(secret_arn)
        .send()
        .await
        .map_err(|error| {
            match error
                .as_service_error()
                .and_then(ProvideErrorMetadata::code)
            {
                Some(
                    "AccessDeniedException"
                    | "DecryptionFailure"
                    | "InvalidParameterException"
                    | "InvalidRequestException"
                    | "ResourceNotFoundException",
                ) => ResolutionError::CredentialUnavailable,
                _ => ResolutionError::ProviderUnavailable,
            }
        })?;
    let secret_string = output.secret_string.take().map(Zeroizing::new);
    let secret_binary = output
        .secret_binary
        .take()
        .map(|value| Zeroizing::new(value.into_inner()));
    if secret_binary.is_some() {
        return Err(ResolutionError::MalformedCredential);
    }
    let value = secret_string.ok_or(ResolutionError::MalformedCredential)?;
    if value.is_empty() || value.len() > MAX_RESOLVED_SECRET_BYTES {
        return Err(ResolutionError::MalformedCredential);
    }
    Ok(ResolvedAwsSecret::new(value))
}

#[cfg(test)]
mod tests;
