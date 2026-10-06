//! Shared contract for reference-based credential provider adapters.
//!
//! Each provider owns its typed locator and material type. Credential
//! Authority supplies the committed policy's destination scope when resolving
//! a provider-neutral prepared credential.

use async_trait::async_trait;
use uuid::Uuid;

/// One exact policy destination authorized to receive a resolved credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationMatcher {
    pub exact_host: String,
    pub port: u16,
}

/// Provider-neutral classification used at the Credential Authority boundary.
/// Messages deliberately contain no provider response or caller-controlled
/// identifiers so they are safe to return through redacted RPC failures.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionError {
    #[error("credential provider is not configured")]
    ProviderNotConfigured,
    #[error("credential reference is malformed")]
    MalformedReference,
    #[error("credential reference is not authorized")]
    ReferenceUnauthorized,
    #[error("credential provider is temporarily unavailable")]
    ProviderUnavailable,
    #[error("credential is unavailable")]
    CredentialUnavailable,
    #[error("credential value is malformed or unsupported")]
    MalformedCredential,
}

/// Common authority context supplied to every provider adapter.
/// `organization_id` is the sandbox's owner from the authority's own state,
/// never a caller-supplied value.
#[derive(Debug, Clone, Copy)]
pub struct ResolutionContext {
    pub organization_id: Uuid,
}

/// One-shot provider fetch created after locator validation. Implementations
/// own any SDK client, locator, or upstream grant needed for the read.
#[async_trait]
pub trait CredentialFetch<Material>: Send + 'static {
    async fn fetch(self: Box<Self>) -> Result<Material, ResolutionError>;
}

/// Reference whose fetch follows an authorized nonempty destination scope.
pub struct PreparedCredential<Material> {
    fetch: Box<dyn CredentialFetch<Material>>,
}

impl<Material: 'static> PreparedCredential<Material> {
    #[must_use]
    pub fn new(fetch: impl CredentialFetch<Material>) -> Self {
        Self {
            fetch: Box::new(fetch),
        }
    }

    /// Resolve once for a nonempty destination scope supplied by the policy.
    pub async fn resolve_for_destinations(
        self,
        destinations: &[DestinationMatcher],
    ) -> Result<Material, ResolutionError> {
        if destinations.is_empty() {
            return Err(ResolutionError::ReferenceUnauthorized);
        }
        self.fetch.fetch().await
    }
}

impl<Material> std::fmt::Debug for PreparedCredential<Material> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedCredential")
            .field("fetch", &"<redacted>")
            .finish()
    }
}

/// Common interface implemented by each typed upstream provider adapter.
///
/// Associated types preserve provider-owned locator and material schemas while
/// making context and errors uniform.
///
/// Implementations must read upstream material with authority scoped to
/// `ResolutionContext::organization_id`: a reference registered by one
/// organization must not resolve another organization's credential, even
/// where Credential Authority's own identity could read it.
#[async_trait]
pub trait CredentialProviderResolver: Send + Sync + 'static {
    type Reference: Sync + 'static;
    type Material: Send + 'static;

    /// Validate the typed locator and prepare a fetch without reading material.
    async fn prepare(
        &self,
        context: ResolutionContext,
        reference: &Self::Reference,
    ) -> Result<PreparedCredential<Self::Material>, ResolutionError>;
}

#[cfg(test)]
mod tests;
