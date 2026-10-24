//! Strict sandbox X.509 profile of the existing token-exchange RPC.

use crate::proto::{ExchangeTokenRequest, ExchangeTokenResponse, exchange_token_response::Payload};
use pplx_space_common_core::types::{SandboxId, SubjectId, SubjectIdParseError};
use std::{fmt, str::FromStr};

#[cfg(test)]
mod tests;

pub const RESOURCE: &str = "urn:space:credential:sandbox-tls-anchor";
const GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const SUBJECT_TYPE: &str = "urn:space:token-type:subject-id";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialKind {
    Certificate,
    SigningCredentials,
}

impl CredentialKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Certificate => "urn:space:token-type:x509-certificate",
            Self::SigningCredentials => "urn:space:token-type:x509-signing-credentials",
        }
    }

    pub fn request(self, sandbox_id: SandboxId) -> ExchangeTokenRequest {
        ExchangeTokenRequest {
            grant_type: GRANT.to_owned(),
            resource: RESOURCE.to_owned(),
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

pub fn certificate_response(certificate_der: Vec<u8>) -> ExchangeTokenResponse {
    CredentialKind::Certificate.response(Payload::X509CertificateDer(certificate_der))
}

pub fn signing_response(
    credentials: crate::proto::X509SigningCredentials,
) -> ExchangeTokenResponse {
    CredentialKind::SigningCredentials.response(Payload::X509SigningCredentials(credentials))
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
            "urn:space:token-type:x509-signing-credentials" => Ok(Self::SigningCredentials),
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
}

impl AnchorRequest {
    pub fn recognizes(request: &ExchangeTokenRequest) -> bool {
        request.resource == RESOURCE
            || request
                .requested_token_type
                .parse::<CredentialKind>()
                .is_ok()
    }

    pub fn parse(request: &ExchangeTokenRequest) -> Result<Self, ProfileError> {
        let kind = request.requested_token_type.parse()?;
        if request.grant_type != GRANT
            || request.resource != RESOURCE
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
        Ok(Self { kind, sandbox_id })
    }
}
