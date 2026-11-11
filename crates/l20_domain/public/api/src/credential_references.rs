#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)
)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum RegisterCredentialReferenceRequest {
    AwsSecretsManager {
        request_id: String,
        secret_arn: String,
    },
}

impl RegisterCredentialReferenceRequest {
    #[must_use]
    pub fn request_id(&self) -> &str {
        match self {
            Self::AwsSecretsManager { request_id, .. } => request_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CredentialReference {
    pub credential_reference_id: String,
}
