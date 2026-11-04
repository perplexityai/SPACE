//! Owner-tuple ingress for trusted federated traffic.
//!
//! A trusted client asserts the resolved upstream owner on each request via
//! headers.
//! The tuple becomes authorization input only after the request authenticated
//! with a trusted federated API key; on every other path the headers are rejected.

use axum::http::HeaderMap;
use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};
use time::OffsetDateTime;

use super::FederatedIdentityConfig;

pub const FEDERATED_ORG_ID_HEADER: &str = "x-space-federated-org-id";
pub const FEDERATED_USER_ID_HEADER: &str = "x-space-federated-user-id";
pub const FEDERATED_SERVICE_GRANT_HEADER: &str = "x-space-federated-service-grant";
/// Absolute expiry (unix seconds) that the credential provider minted the grant with,
/// forwarded verbatim by the client. SPACE cannot open the grant, so this header is
/// the only thing that tells it the grant's real lifetime.
/// The moment ECW minted the grant, stamped by ECW beside the blob and
/// expiry. Optional on the wire: legacy clients omit it, and renewals then
/// fall back to expiry-monotone ordering. Absolute expiry is not an ordering
/// field — the credential provider alone chooses and may tighten grant lifetimes.
pub const FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER: &str =
    "x-space-federated-service-grant-issued-at";
pub const FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER: &str =
    "x-space-federated-service-grant-expires-at";
pub const MAX_FEDERATED_SERVICE_GRANT_BYTES: usize = 65_536;

/// Upstream owner resolved from a trusted federated request.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FederatedOwner {
    pub issuer: UpstreamIssuer,
    pub org_id: UpstreamOrgId,
    pub subject_id: UpstreamSubjectId,
}

/// An opaque credential-provider grant and the expiry it was minted with.
///
/// The blob and its expiry are one artifact and this type keeps them that way:
/// SPACE cannot open the blob, so an expiry it did not receive is an expiry it
/// would have to invent. Making the field mandatory means no construction path
/// — ingress, fork-payload restore, or a future one — can fall back to a
/// locally chosen lifetime.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FederatedServiceGrant {
    bytes: Vec<u8>,
    expires_at: OffsetDateTime,
    /// Producer ordering: when ECW minted this grant. `None` from legacy
    /// clients and persisted payloads that predate the header.
    issued_at: Option<OffsetDateTime>,
}

impl FederatedServiceGrant {
    pub fn new(
        bytes: impl Into<Vec<u8>>,
        expires_at: OffsetDateTime,
    ) -> Result<Self, FederatedServiceGrantError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(FederatedServiceGrantError::Empty);
        }
        if bytes.len() > MAX_FEDERATED_SERVICE_GRANT_BYTES {
            return Err(FederatedServiceGrantError::TooLarge);
        }
        Ok(Self {
            bytes,
            expires_at,
            issued_at: None,
        })
    }

    /// Attach the producer's mint moment as it travels on the wire.
    pub fn with_issued_at_unix(
        mut self,
        issued_at_unix: i64,
    ) -> Result<Self, FederatedServiceGrantError> {
        self.issued_at = Some(
            OffsetDateTime::from_unix_timestamp(issued_at_unix)
                .map_err(|_| FederatedServiceGrantError::InvalidIssuedAt)?,
        );
        Ok(self)
    }

    /// Build from the absolute unix-seconds expiry as it travels on the wire
    /// (the request header, and the persisted fork-operation payload).
    pub fn from_unix_expiry(
        bytes: impl Into<Vec<u8>>,
        expires_at_unix: i64,
    ) -> Result<Self, FederatedServiceGrantError> {
        let expires_at = OffsetDateTime::from_unix_timestamp(expires_at_unix)
            .map_err(|_| FederatedServiceGrantError::InvalidExpiry)?;
        Self::new(bytes, expires_at)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The credential provider's expiry for this grant, stored verbatim. `time`
    /// rather than `jiff` because the only consumer is a `TIMESTAMPTZ` bind
    /// and that is the sqlx type feature this crate enables; no arithmetic
    /// happens on it.
    pub fn expires_at(&self) -> OffsetDateTime {
        self.expires_at
    }

    pub fn expires_at_unix(&self) -> i64 {
        self.expires_at.unix_timestamp()
    }

    pub fn issued_at(&self) -> Option<OffsetDateTime> {
        self.issued_at
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum FederatedIngressError {
    #[error("federated org id header is missing")]
    MissingOrg,
    #[error("federated user id header is missing")]
    MissingSubject,
    #[error("federated org id header is invalid")]
    InvalidOrg,
    #[error("federated user id header is invalid")]
    InvalidSubject,
    #[error("federated user id header is a placeholder subject")]
    PlaceholderSubject,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum FederatedServiceGrantError {
    #[error("federated service grant header is missing")]
    Missing,
    #[error("federated service grant header is empty")]
    Empty,
    #[error("federated service grant header is too large")]
    TooLarge,
    #[error("federated service grant expiry header is missing")]
    MissingExpiry,
    #[error("federated service grant expiry header is not an absolute unix timestamp")]
    InvalidExpiry,
    #[error("federated service grant issued-at header is not an absolute unix timestamp")]
    InvalidIssuedAt,
}

/// Builds the federated owner from request headers.
///
/// Tenantless requests fail closed: a missing org header is an error, never a
/// fallback to the local placeholder org.
pub fn federated_owner_from_headers(
    config: &FederatedIdentityConfig,
    headers: &HeaderMap,
) -> Result<FederatedOwner, FederatedIngressError> {
    let org = header_value(headers, FEDERATED_ORG_ID_HEADER)
        .ok_or(FederatedIngressError::MissingOrg)?
        .ok_or(FederatedIngressError::InvalidOrg)?;
    let subject = header_value(headers, FEDERATED_USER_ID_HEADER)
        .ok_or(FederatedIngressError::MissingSubject)?
        .ok_or(FederatedIngressError::InvalidSubject)?;

    let org_id = UpstreamOrgId::parse(org).map_err(|_| FederatedIngressError::InvalidOrg)?;
    let subject_id =
        UpstreamSubjectId::parse(subject).map_err(|_| FederatedIngressError::InvalidSubject)?;

    if config.policy.rejected_subjects.contains(&subject_id) {
        return Err(FederatedIngressError::PlaceholderSubject);
    }

    Ok(FederatedOwner {
        issuer: config.policy.issuer.clone(),
        org_id,
        subject_id,
    })
}

/// Extracts the grant and the expiry chosen by its credential provider.
///
/// Both headers are required together: a grant whose expiry SPACE did not
/// receive cannot be stored without inventing a lifetime, so it is rejected
/// rather than persisted under a guess.
pub fn federated_service_grant_from_headers(
    headers: &HeaderMap,
) -> Result<FederatedServiceGrant, FederatedServiceGrantError> {
    let value = headers
        .get(FEDERATED_SERVICE_GRANT_HEADER)
        .ok_or(FederatedServiceGrantError::Missing)?;
    let expires_at_unix = grant_expires_at_unix_from_headers(headers)?;
    let grant =
        FederatedServiceGrant::from_unix_expiry(value.as_bytes().to_vec(), expires_at_unix)?;
    // Optional producer ordering: absent means a legacy client (expiry
    // fallback applies at persistence); present-but-malformed is rejected
    // like any other malformed grant header.
    match header_value(headers, FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER) {
        None => Ok(grant),
        Some(value) => {
            let issued_at_unix: i64 = value
                .ok_or(FederatedServiceGrantError::InvalidIssuedAt)?
                .parse()
                .map_err(|_| FederatedServiceGrantError::InvalidIssuedAt)?;
            grant.with_issued_at_unix(issued_at_unix)
        }
    }
}

fn grant_expires_at_unix_from_headers(
    headers: &HeaderMap,
) -> Result<i64, FederatedServiceGrantError> {
    header_value(headers, FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER)
        .ok_or(FederatedServiceGrantError::MissingExpiry)?
        .ok_or(FederatedServiceGrantError::InvalidExpiry)?
        .parse()
        .map_err(|_| FederatedServiceGrantError::InvalidExpiry)
}

/// Returns whether the request carries any owner assertion header. Untrusted
/// Authentication paths use this to reject spoof attempts instead of silently
/// ignoring them.
pub fn owner_headers_present(headers: &HeaderMap) -> bool {
    headers.contains_key(FEDERATED_ORG_ID_HEADER) || headers.contains_key(FEDERATED_USER_ID_HEADER)
}

/// Outer `None`: header absent. Inner `None`: present but not valid UTF-8 or
/// empty.
fn header_value(headers: &HeaderMap, name: &str) -> Option<Option<String>> {
    let value = headers.get(name)?;
    Some(
        value
            .to_str()
            .ok()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
    )
}

#[cfg(test)]
mod tests;
