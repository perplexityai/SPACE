//! Pluggable per-organization key wrapping for DEK issuance.
//!
//! The credential manager mints fresh Data Encryption Keys (DEKs) on
//! demand and wraps them under a per-organization Key Encryption Key
//! (KEK). The caller picks the concrete KEK by supplying
//! `(provider, master_key_identifier)`, both resolved from the
//! `org_master_keys` row that the encrypted-fields sidecar's
//! `master_key_id` points at. The KEK itself never leaves the
//! wrapping backend.
//!
//! Backends implement [`KeyWrapper`]. Each impl is expected to handle
//! exactly one [`KekProvider`] variant and to fail closed
//! ([`WrapError::UnsupportedProvider`] /
//! [`UnwrapError::UnsupportedProvider`]) for other providers; the
//! credmgr binary stitches per-backend impls together with
//! [`CompositeKeyWrapper`] so the service code can hold a single
//! `Arc<dyn KeyWrapper>` regardless of which providers are configured.
//!
//! The in-tree backends live next to this module:
//!
//! - [`crate::in_memory::InMemoryKeyWrapper`] — explicitly constructed test
//!   stub; KEKs live in process memory and do not survive restart.
//! - [`crate::intermediate_key_broker::IntermediateKeyWrapper`] — production
//!   backend that obtains a broker-wrapped intermediate key through the public
//!   broker contract and holds plaintext only in a bounded cache.

use async_trait::async_trait;
use pplx_space_common_svc::core::types::{KekProvider, MasterKeyIdentifier, OrgId, SandboxId};

/// Byte length and intended algorithm class of a freshly minted DEK.
///
/// Typed mirror of the proto `DekSpec` enum; boundary code parses the
/// wire enum into this once and passes the typed value through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DekKeySpec {
    /// 256-bit key, 32 bytes; intended for AES-256-GCM / AES-256-SIV.
    Aes256,
    /// 128-bit key, 16 bytes; intended for AES-128-GCM.
    Aes128,
}

impl DekKeySpec {
    /// Returns the DEK length in bytes.
    pub const fn byte_len(self) -> usize {
        match self {
            Self::Aes256 => 32,
            Self::Aes128 => 16,
        }
    }
}

/// Output of [`KeyWrapper::generate_and_wrap_dek`].
#[derive(Debug)]
pub struct GenerateDekOutput {
    /// Plaintext DEK. Sensitive: the credmgr returns it on the wire
    /// once and then forgets it.
    pub plaintext_dek: Vec<u8>,
    /// Wrapped DEK, opaque ciphertext for storage.
    pub wrapped_dek: Vec<u8>,
}

/// Errors returned by [`KeyWrapper::generate_and_wrap_dek`].
#[derive(Debug, thiserror::Error)]
pub enum WrapError {
    /// The backend does not handle this [`KekProvider`]. Indicates a
    /// deployment misconfiguration (e.g., an org's
    /// `org_master_keys.provider` names a backend the credmgr binary
    /// wasn't built with, or wasn't initialized with).
    #[error("unsupported KEK provider: {0}")]
    UnsupportedProvider(KekProvider),
    /// The org's KEK access was explicitly revoked at its external
    /// provider. Terminal for the org until re-enrollment: callers must
    /// not retry, and upstream this drives org cleanup and sandbox
    /// shutdown.
    #[error("access to the organization's KEK was revoked at the external provider")]
    AccessRevoked,
    /// The KEK backend is temporarily unavailable (provider outage or
    /// deadline). Transient: retriable with backoff; surfaces as gRPC
    /// `UNAVAILABLE` rather than `INTERNAL`.
    #[error("KEK backend temporarily unavailable: {0}")]
    Unavailable(String),
    /// The external provider explicitly rejected the KEK reference. Permanent
    /// for the referenced key; retrying cannot repair it.
    #[error("invalid KEK reference: {0}")]
    InvalidKey(String),
    /// The external broker rejected the caller's authorization evidence.
    /// Permanent for that evidence, but explicitly not a KEK revocation.
    #[error("KEK broker rejected authorization evidence: {0}")]
    AuthorizationRejected(String),
    /// Backend-side failure (for example, an AEAD primitive failure).
    #[error("backend error: {0}")]
    Backend(String),
}

/// Errors returned by [`KeyWrapper::unwrap_dek`].
#[derive(Debug, thiserror::Error)]
pub enum UnwrapError {
    /// Provider not handled by this backend (see [`WrapError::UnsupportedProvider`]).
    #[error("unsupported KEK provider: {0}")]
    UnsupportedProvider(KekProvider),
    /// The backend has no KEK on file for this
    /// `(org, master_key_identifier)`. For the in-memory backend this
    /// means no `generate_and_wrap_dek` has been made under this pair
    /// (e.g., after a credmgr restart). For real backends this means
    /// the identifier names a key the backend cannot resolve.
    #[error("no KEK exists for organization + master_key_identifier")]
    UnknownOrgKek,
    /// The org's KEK access was explicitly revoked at its external
    /// provider. Terminal for the org until re-enrollment: callers must
    /// not retry, and upstream this drives org cleanup and sandbox
    /// shutdown.
    #[error("access to the organization's KEK was revoked at the external provider")]
    AccessRevoked,
    /// The KEK backend is temporarily unavailable (provider outage or
    /// deadline). Transient: retriable with backoff; surfaces as gRPC
    /// `UNAVAILABLE` rather than `INTERNAL`.
    #[error("KEK backend temporarily unavailable: {0}")]
    Unavailable(String),
    /// The external provider explicitly rejected the KEK reference. Permanent
    /// for the referenced key; retrying cannot repair it.
    #[error("invalid KEK reference: {0}")]
    InvalidKey(String),
    /// The external broker rejected the caller's authorization evidence.
    /// Permanent for that evidence, but explicitly not a KEK revocation.
    #[error("KEK broker rejected authorization evidence: {0}")]
    AuthorizationRejected(String),
    /// AEAD authentication failed: AAD mismatch, wrong KEK, wrong org,
    /// or tampering on the ciphertext.
    #[error("authentication failed")]
    AuthFailed,
    /// `wrapped_dek` is structurally invalid (too short to contain a
    /// nonce and tag, or backend-specific framing is malformed).
    #[error("malformed wrapped DEK")]
    Malformed,
    /// Backend-side failure.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Backend that mints, wraps, and unwraps DEKs under per-org KEKs.
///
/// `Send + Sync + 'static` because handlers hold an
/// `Arc<dyn KeyWrapper>` across `.await` points and tonic spawns the
/// request future on the runtime.
#[async_trait]
pub trait KeyWrapper: Send + Sync + 'static {
    /// Generate a fresh DEK and wrap it under the KEK named by
    /// `(org, provider, master_key_identifier)`, binding `aad` into
    /// the ciphertext. Returns both the plaintext (for immediate use
    /// by the caller) and the wrapped form (for persistence).
    ///
    /// Combining "generate" and "wrap" into one call keeps fresh plaintext
    /// DEK handling inside the authority boundary.
    ///
    /// `requesting_sandbox` names the sandbox whose operation triggered the
    /// call, when there is one; the intermediate-key-broker backend presents
    /// that sandbox's own provider grant when it must dial the broker.
    async fn generate_and_wrap_dek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        spec: DekKeySpec,
        aad: &[u8],
    ) -> Result<GenerateDekOutput, WrapError>;

    /// Recover the plaintext DEK previously produced by
    /// `generate_and_wrap_dek`. The backend must verify
    /// (cryptographically, not by trust) that `org`,
    /// `master_key_identifier`, and `aad` all match the values used
    /// at wrap time; mismatch is an [`UnwrapError::AuthFailed`].
    async fn unwrap_dek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        wrapped_dek: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, UnwrapError>;
}

/// Composite `KeyWrapper` that delegates to a per-provider backend.
///
/// Lets a caller configure the external provider and an explicit in-memory
/// development backend while presenting a single
/// `Arc<dyn KeyWrapper>` to the service handlers. Requests are routed
/// by `provider`; an unconfigured provider fails with
/// [`WrapError::UnsupportedProvider`] / [`UnwrapError::UnsupportedProvider`].
///
/// We intentionally hold concrete `Option<Box<dyn KeyWrapper>>` per
/// provider rather than a `HashMap<KekProvider, Arc<dyn KeyWrapper>>`
/// so adding a new provider is a compile-time exhaustiveness check:
/// the `match` in `route_*` won't compile until every `KekProvider`
/// variant is wired up.
pub struct CompositeKeyWrapper {
    in_memory: Option<Box<dyn KeyWrapper>>,
    intermediate_key_broker: Option<Box<dyn KeyWrapper>>,
}

impl CompositeKeyWrapper {
    /// Builder-style construction. All supported backends default to `None`.
    pub fn new() -> Self {
        Self {
            in_memory: None,
            intermediate_key_broker: None,
        }
    }

    /// Install the in-memory backend for an explicit test fixture.
    #[must_use]
    pub fn with_in_memory(mut self, backend: impl KeyWrapper) -> Self {
        self.in_memory = Some(Box::new(backend));
        self
    }

    /// Install the intermediate-key broker backend. Control-plane
    /// authority only — node-local deployments must not hold intermediate keys.
    #[must_use]
    pub fn with_intermediate_key_broker(mut self, backend: impl KeyWrapper) -> Self {
        self.intermediate_key_broker = Some(Box::new(backend));
        self
    }

    /// Pick the backend that owns `provider`, or report
    /// [`WrapError::UnsupportedProvider`] when none is installed.
    fn route_wrap(&self, provider: KekProvider) -> Result<&dyn KeyWrapper, WrapError> {
        let backend = match provider {
            // Kept in the persisted enum so historical rows fail closed with
            // an explicit unsupported-provider error.
            KekProvider::AwsKms => None,
            KekProvider::InMemory => self.in_memory.as_deref(),
            KekProvider::IntermediateKeyBroker => self.intermediate_key_broker.as_deref(),
        };
        backend.ok_or(WrapError::UnsupportedProvider(provider))
    }

    /// Same as [`Self::route_wrap`] but for unwrap.
    fn route_unwrap(&self, provider: KekProvider) -> Result<&dyn KeyWrapper, UnwrapError> {
        let backend = match provider {
            KekProvider::AwsKms => None,
            KekProvider::InMemory => self.in_memory.as_deref(),
            KekProvider::IntermediateKeyBroker => self.intermediate_key_broker.as_deref(),
        };
        backend.ok_or(UnwrapError::UnsupportedProvider(provider))
    }
}

impl Default for CompositeKeyWrapper {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KeyWrapper for CompositeKeyWrapper {
    async fn generate_and_wrap_dek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        spec: DekKeySpec,
        aad: &[u8],
    ) -> Result<GenerateDekOutput, WrapError> {
        self.route_wrap(provider)?
            .generate_and_wrap_dek(
                org,
                requesting_sandbox,
                provider,
                master_key_identifier,
                spec,
                aad,
            )
            .await
    }

    async fn unwrap_dek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        wrapped_dek: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, UnwrapError> {
        self.route_unwrap(provider)?
            .unwrap_dek(
                org,
                requesting_sandbox,
                provider,
                master_key_identifier,
                wrapped_dek,
                aad,
            )
            .await
    }
}

#[cfg(test)]
mod tests;
