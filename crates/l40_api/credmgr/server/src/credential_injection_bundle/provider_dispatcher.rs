//! Routes validated credential locators to their provider adapters.
//!
//! A new provider adds its typed adapter and one locator arm here.
//! Header-value validation remains provider-independent.

use std::sync::Arc;

use pplx_space_credential_injection_core::secret_arn::SecretArn;
use uuid::Uuid;

use super::ResolvedCredentialValue;
use crate::aws_secrets_manager::ResolvedAwsSecret;
use crate::credential_provider::{
    CredentialProviderResolver, DestinationMatcher, ResolutionContext, ResolutionError,
};
use pplx_space_credential_injection_core::validation::ValidatedCredentialReference;

type AwsSecretsManagerProvider =
    dyn CredentialProviderResolver<Reference = SecretArn, Material = ResolvedAwsSecret>;

/// Configured credential-provider adapters for this Credential Authority.
#[derive(Clone, Default)]
pub struct CredentialProviderDispatcher {
    aws_secrets_manager: Option<Arc<AwsSecretsManagerProvider>>,
}

impl CredentialProviderDispatcher {
    pub fn with_aws_secrets_manager(mut self, provider: Arc<AwsSecretsManagerProvider>) -> Self {
        self.aws_secrets_manager = Some(provider);
        self
    }

    pub(super) async fn resolve_for_destinations(
        &self,
        organization_id: Uuid,
        reference: &ValidatedCredentialReference,
        destinations: &[DestinationMatcher],
    ) -> Result<ResolvedCredentialValue, ResolutionError> {
        match reference {
            ValidatedCredentialReference::AwsSecretsManager(secret_arn) => {
                let provider = self
                    .aws_secrets_manager
                    .as_ref()
                    .ok_or(ResolutionError::ProviderNotConfigured)?;
                let prepared = provider
                    .prepare(ResolutionContext { organization_id }, secret_arn)
                    .await?;
                let secret = prepared.resolve_for_destinations(destinations).await?;
                Ok(ResolvedCredentialValue::from_text(secret.into_value()))
            }
        }
    }
}
