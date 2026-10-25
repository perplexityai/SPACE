//! Strict sandbox X.509 profile of the existing token-exchange RPC.

use crate::proto::{
    ExchangeTokenRequest, ExchangeTokenResponse, SandboxTrustAnchor,
    exchange_token_response::Payload,
};
use pplx_space_common_core::types::{SandboxId, SubjectId, SubjectIdParseError};
use std::{fmt, str::FromStr};

#[cfg(test)]
mod tests;

pub const RESOURCE: &str = "urn:space:credential:sandbox-tls-anchor";
const GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const SUBJECT_TYPE: &str = "urn:space:token-type:subject-id";

/// Immutable public KMS identity. Aliases and caller-supplied ARNs are forbidden;
/// authority fixes the account and regions in configuration.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AnchorKeyId(String);

impl AnchorKeyId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AnchorKeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AnchorKeyId {
    type Err = ProfileError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let valid = if let Some(hex) = value.strip_prefix("mrk-") {
            hex.len() == 32
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        } else {
            uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
        };
        valid
            .then(|| Self(value.to_owned()))
            .ok_or(ProfileError::InvalidKey)
    }
}

/// Public-only response with explicit issuer selection for subsequent requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnchorCertificate {
    pub key_id: AnchorKeyId,
    pub certificate_der: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedIssuer {
    pub sandbox_id: SandboxId,
    pub key_id: AnchorKeyId,
}

impl ScopedIssuer {
    pub fn parse(
        request: &crate::proto::IssueIntermediateCertificateRequest,
    ) -> Result<Option<Self>, ProfileError> {
        match (
            request.sandbox_id.as_deref(),
            request.anchor_key_id.as_deref(),
        ) {
            (None, None) => Ok(None),
            (Some(sandbox), Some(key)) => Ok(Some(Self {
                sandbox_id: parse_sandbox_id(sandbox)?,
                key_id: key.parse()?,
            })),
            _ => Err(ProfileError::InvalidProfile),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialKind {
    Certificate,
}

impl CredentialKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Certificate => "urn:space:token-type:x509-certificate",
        }
    }

    pub fn request(self, sandbox_id: SandboxId) -> ExchangeTokenRequest {
        self.request_for_key(sandbox_id, None)
    }

    /// No pin selects the configured active key for a new enrollment. A pin
    /// selects that exact resource, including after rotation.
    pub fn request_for_key(
        self,
        sandbox_id: SandboxId,
        key_id: Option<&AnchorKeyId>,
    ) -> ExchangeTokenRequest {
        ExchangeTokenRequest {
            grant_type: GRANT.to_owned(),
            resource: match key_id {
                Some(key_id) => format!("{RESOURCE}:{key_id}"),
                None => RESOURCE.to_owned(),
            },
            requested_token_type: self.to_string(),
            subject_token: Some(SubjectId::Sandbox(sandbox_id).to_string()),
            subject_token_type: Some(SUBJECT_TYPE.to_owned()),
            audience: None,
            scope: None,
            actor_token: None,
            actor_token_type: None,
            requested_expires_in: None,
            copy_source_resource: None,
        }
    }

    fn response(self, payload: Payload) -> ExchangeTokenResponse {
        ExchangeTokenResponse {
            issued_token_type: self.to_string(),
            token_type: "N_A".to_owned(),
            payload: Some(payload),
            expires_in: None,
            scope: None,
            refresh_token: None,
        }
    }
}

pub fn certificate_response(anchor: AnchorCertificate) -> ExchangeTokenResponse {
    CredentialKind::Certificate.response(Payload::SandboxTrustAnchor(SandboxTrustAnchor {
        certificate_der: anchor.certificate_der,
        key_id: anchor.key_id.to_string(),
    }))
}

impl fmt::Display for CredentialKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CredentialKind {
    type Err = ProfileError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "urn:space:token-type:x509-certificate" => Ok(Self::Certificate),
            _ => Err(ProfileError::Unsupported),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("unsupported X.509 credential type")]
    Unsupported,
    #[error("invalid sandbox X.509 exchange profile")]
    InvalidProfile,
    #[error("invalid X.509 resource subject: {0}")]
    InvalidSubject(#[from] SubjectIdParseError),
    #[error("X.509 exchange requires a sandbox subject")]
    NotSandbox,
    #[error("X.509 exchange requires a canonical UUIDv7 sandbox identity")]
    InvalidSandbox,
    #[error("anchor key must be a canonical immutable KMS key ID")]
    InvalidKey,
}

pub fn parse_sandbox_id(value: &str) -> Result<SandboxId, ProfileError> {
    let id = value
        .parse::<SandboxId>()
        .map_err(|_| ProfileError::InvalidSandbox)?;
    if id.as_uuid().get_version_num() != 7 || id.to_string() != value {
        return Err(ProfileError::InvalidSandbox);
    }
    Ok(id)
}

pub struct AnchorRequest {
    pub kind: CredentialKind,
    pub sandbox_id: SandboxId,
    pub key_id: Option<AnchorKeyId>,
}

impl AnchorRequest {
    pub fn recognizes(request: &ExchangeTokenRequest) -> bool {
        request.resource.starts_with(RESOURCE)
            || request
                .requested_token_type
                .parse::<CredentialKind>()
                .is_ok()
    }

    pub fn parse(request: &ExchangeTokenRequest) -> Result<Self, ProfileError> {
        let kind = request.requested_token_type.parse()?;
        let key_id = if request.resource == RESOURCE {
            None
        } else {
            Some(
                request
                    .resource
                    .strip_prefix(RESOURCE)
                    .and_then(|suffix| suffix.strip_prefix(':'))
                    .ok_or(ProfileError::InvalidProfile)?
                    .parse()?,
            )
        };
        if request.grant_type != GRANT
            || request.subject_token_type.as_deref() != Some(SUBJECT_TYPE)
            || request.audience.is_some()
            || request.scope.is_some()
            || request.actor_token.is_some()
            || request.actor_token_type.is_some()
            || request.requested_expires_in.is_some()
            || request.copy_source_resource.is_some()
        {
            return Err(ProfileError::InvalidProfile);
        }
        let subject = request
            .subject_token
            .as_deref()
            .ok_or(ProfileError::InvalidProfile)?;
        let SubjectId::Sandbox(sandbox_id) = subject.parse()? else {
            return Err(ProfileError::NotSandbox);
        };
        if subject != SubjectId::Sandbox(sandbox_id).to_string()
            || sandbox_id.as_uuid().get_version_num() != 7
        {
            return Err(ProfileError::InvalidSandbox);
        }
        Ok(Self {
            kind,
            sandbox_id,
            key_id,
        })
    }
}
