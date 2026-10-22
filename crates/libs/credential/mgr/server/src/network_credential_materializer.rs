//! Materializes policy-bound credential references into secret-bearing
//! network credentials.
//!
//! Each reference is resolved concurrently through its provider and its value
//! is checked as an HTTP header value. Permanent per-reference failures omit
//! that credential; transient provider failures fail the whole resolution.
//!
//! This module does not authenticate callers, read durable policy state, mint
//! server certificates, or install material into netgtw. Credential Authority
//! owns authentication and state.

use crate::credential_provider::{DestinationMatcher, ResolutionError};
use futures_util::future::try_join_all;
use pplx_space_common_svc::core::types::CredentialReferenceId;
use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
use pplx_space_credential_injection_core::validation::ValidatedCredentialReference;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};
mod provider_dispatcher;
pub use provider_dispatcher::CredentialProviderDispatcher;

/// Resolves and materializes the references bound by one network policy.
#[derive(Clone)]
pub struct NetworkCredentialMaterializer {
    providers: CredentialProviderDispatcher,
}

impl NetworkCredentialMaterializer {
    pub fn new(provider_dispatcher: CredentialProviderDispatcher) -> Self {
        Self {
            providers: provider_dispatcher,
        }
    }

    pub(crate) async fn materialize_network_credentials(
        &self,
        organization_id: Uuid,
        references: Vec<NetworkCredentialMaterialization>,
    ) -> Result<Vec<ResolvedNetworkCredential>, NetworkCredentialMaterializationError> {
        let resolved = try_join_all(references.iter().map(|reference| async {
            match materialize_network_credential(reference, organization_id, &self.providers).await
            {
                Ok(credential) => Ok(Some(credential)),
                Err(error) if network_credential_is_permanently_unavailable(&error) => Ok(None),
                Err(error) => Err(error),
            }
        }))
        .await?;
        Ok(resolved.into_iter().flatten().collect())
    }
}

fn network_credential_is_permanently_unavailable(
    error: &NetworkCredentialMaterializationError,
) -> bool {
    match error {
        NetworkCredentialMaterializationError::Provider(ResolutionError::ProviderUnavailable)
        | NetworkCredentialMaterializationError::InvalidRules(_) => false,
        NetworkCredentialMaterializationError::Provider(_)
        | NetworkCredentialMaterializationError::InvalidResolvedCredential(_) => true,
    }
}

pub(crate) struct NetworkCredentialMaterialization {
    pub(crate) reference_id: CredentialReferenceId,
    pub(crate) reference: ValidatedCredentialReference,
    pub(crate) destinations: Vec<DestinationMatcher>,
}

impl std::fmt::Debug for NetworkCredentialMaterialization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkCredentialMaterialization")
            .field("reference_id", &self.reference_id)
            .field("reference", &"<redacted>")
            .field("destinations", &self.destinations)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkCredentialMaterializationError {
    #[error("stored credential references are invalid: {0}")]
    InvalidRules(&'static str),
    #[error("resolved credential cannot be injected: {0}")]
    InvalidResolvedCredential(&'static str),
    #[error(transparent)]
    Provider(#[from] ResolutionError),
}

/// Provider-neutral secret text. It is redacted in `Debug` output and zeroized
/// after the network credential has been materialized.
pub(crate) struct ResolvedCredentialValue(Zeroizing<String>);

impl ResolvedCredentialValue {
    fn from_text(value: Zeroizing<String>) -> Self {
        Self(value)
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Debug for ResolvedCredentialValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedCredentialValue(<redacted>)")
    }
}

async fn materialize_network_credential(
    input: &NetworkCredentialMaterialization,
    organization_id: Uuid,
    providers: &CredentialProviderDispatcher,
) -> Result<ResolvedNetworkCredential, NetworkCredentialMaterializationError> {
    let value = providers
        .resolve_for_destinations(organization_id, &input.reference, &input.destinations)
        .await?;
    let mut bytes = value.as_bytes().to_vec();
    if bytes.is_empty() || !bytes.iter().copied().all(is_valid_header_value_byte) {
        bytes.zeroize();
        return Err(
            NetworkCredentialMaterializationError::InvalidResolvedCredential(
                "resolved credential is not a valid HTTP header value",
            ),
        );
    }
    Ok(ResolvedNetworkCredential {
        credential_reference_id: input.reference_id.to_string(),
        value: bytes,
    })
}

// RFC 9110 field-value bytes: visible ASCII, obs-text, or horizontal tab.
// Validate in place before ownership moves into the zeroizing
// `ResolvedNetworkCredential` field.
const fn is_valid_header_value_byte(byte: u8) -> bool {
    (byte >= b' ' && byte != 0x7f) || byte == b'\t'
}

#[cfg(test)]
mod tests;
