//! Credential resolution and header injection for inspected HTTP.

use http::{HeaderName, HeaderValue, StatusCode};
use pplx_space_net_common::NetworkCredentialHttpMethodV1;

use super::SanitizedRequest;
use crate::certificates::{CertificateLease, NetworkCredentialAccessError};

pub(super) type CredentialHeaders = Vec<(HeaderName, HeaderValue)>;

impl NetworkCredentialAccessError {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "credential_material_missing",
            Self::TemporarilyUnavailable => "credential_material_unavailable",
            Self::Stale => "lease_invalidated",
            Self::InvalidMaterial => "credential_material_invalid",
        }
    }

    pub(super) const fn status(self) -> StatusCode {
        match self {
            Self::TemporarilyUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Missing | Self::Stale | Self::InvalidMaterial => StatusCode::FORBIDDEN,
        }
    }
}

pub(super) fn credential_headers<B>(
    lease: &CertificateLease,
    request: &SanitizedRequest<B>,
) -> Result<CredentialHeaders, NetworkCredentialAccessError> {
    let Some(method) = NetworkCredentialHttpMethodV1::from_http(request.method()) else {
        return lease
            .is_usable()
            .then(Vec::new)
            .ok_or(NetworkCredentialAccessError::Stale);
    };
    lease.network_credential_headers(request.uri().path(), method)
}

pub(super) fn inject<B>(request: &mut SanitizedRequest<B>, headers: CredentialHeaders) {
    for (name, value) in headers {
        request.set_network_credential(name, value);
    }
}
