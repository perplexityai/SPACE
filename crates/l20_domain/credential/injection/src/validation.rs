//! Validation for reference-only credential locators.

use crate::secret_arn::SecretArn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("credential reference is invalid: {0}")]
pub struct ReferenceValidationError(pub &'static str);

#[derive(Clone)]
pub enum ValidatedCredentialReference {
    AwsSecretsManager(SecretArn),
}

impl ValidatedCredentialReference {
    pub fn aws_secrets_manager(
        secret_arn: impl Into<String>,
    ) -> Result<Self, ReferenceValidationError> {
        SecretArn::parse(secret_arn)
            .map(Self::AwsSecretsManager)
            .map_err(|_| ReferenceValidationError("credential reference is malformed"))
    }
}

impl std::fmt::Debug for ValidatedCredentialReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ValidatedCredentialReference(<redacted>)")
    }
}
