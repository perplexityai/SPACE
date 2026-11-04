use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use pplx_space_public_api::PageCursor;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::error::PublicApiError;

type HmacSha256 = Hmac<Sha256>;

const CURSOR_PREFIX: &str = "pc1_";
const CURSOR_SECRET_ENV: &str = "SPACE_PUBLIC_CURSOR_SECRET";

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum PublicCursorConfigError {
    #[error("{CURSOR_SECRET_ENV} must be set for public cursor signing")]
    MissingSecret,
}

pub(crate) fn validate_public_cursor_secret() -> Result<(), PublicCursorConfigError> {
    configured_cursor_secret().map(|_| ())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CursorKind {
    OperationEvent,
    Process,
    Sandbox,
    Session,
    Snapshot,
    Template,
}

impl CursorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::OperationEvent => "operation_event",
            Self::Process => "process",
            Self::Sandbox => "sandbox",
            Self::Session => "session",
            Self::Snapshot => "snapshot",
            Self::Template => "template",
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct CursorPayload {
    v: u8,
    k: String,
    p: String,
}

pub(crate) fn encode_cursor(
    kind: CursorKind,
    payload: String,
) -> Result<PageCursor, PublicApiError> {
    let secret = public_cursor_secret().map_err(|error| {
        tracing::error!(error = %error, "public cursor secret is not configured");
        PublicApiError::internal()
    })?;
    encode_cursor_with_secret(kind, payload, secret.as_bytes())
}

fn encode_cursor_with_secret(
    kind: CursorKind,
    payload: String,
    secret: &[u8],
) -> Result<PageCursor, PublicApiError> {
    let payload = CursorPayload {
        v: 1,
        k: kind.as_str().to_string(),
        p: payload,
    };
    let bytes = serde_json::to_vec(&payload).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize public cursor");
        PublicApiError::internal()
    })?;
    let signature = sign_cursor(&bytes, secret)?;
    PageCursor::parse(format!(
        "{CURSOR_PREFIX}{}.{}",
        URL_SAFE_NO_PAD.encode(bytes),
        URL_SAFE_NO_PAD.encode(signature)
    ))
    .map_err(|_| PublicApiError::internal())
}

pub(crate) fn decode_cursor(kind: CursorKind, cursor: &str) -> Result<String, PublicApiError> {
    let secret = public_cursor_secret().map_err(|error| {
        tracing::error!(error = %error, "public cursor secret is not configured");
        PublicApiError::internal()
    })?;
    decode_cursor_with_secret(kind, cursor, secret.as_bytes())
}

fn decode_cursor_with_secret(
    kind: CursorKind,
    cursor: &str,
    secret: &[u8],
) -> Result<String, PublicApiError> {
    let cursor =
        PageCursor::parse(cursor).map_err(|_| PublicApiError::bad_request("invalid cursor"))?;
    let Some(token) = cursor.as_str().strip_prefix(CURSOR_PREFIX) else {
        return Err(PublicApiError::bad_request("invalid cursor"));
    };
    let Some((payload, signature)) = token.split_once('.') else {
        return Err(PublicApiError::bad_request("invalid cursor"));
    };
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))?;
    verify_cursor_signature(&payload, &signature, secret)?;
    let payload = serde_json::from_slice::<CursorPayload>(&payload)
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))?;
    if payload.v != 1 || payload.k != kind.as_str() {
        return Err(PublicApiError::bad_request("invalid cursor"));
    }
    Ok(payload.p)
}

fn sign_cursor(payload: &[u8], secret: &[u8]) -> Result<Vec<u8>, PublicApiError> {
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|error| {
        tracing::error!(error = %error, "failed to initialize public cursor signer");
        PublicApiError::internal()
    })?;
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_cursor_signature(
    payload: &[u8],
    signature: &[u8],
    secret: &[u8],
) -> Result<(), PublicApiError> {
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|error| {
        tracing::error!(error = %error, "failed to initialize public cursor verifier");
        PublicApiError::internal()
    })?;
    mac.update(payload);
    mac.verify_slice(signature)
        .map_err(|_| PublicApiError::bad_request("invalid cursor"))
}

fn public_cursor_secret() -> Result<String, PublicCursorConfigError> {
    #[cfg(test)]
    {
        configured_cursor_secret()
            .or_else(|_| Ok("space-api-unit-test-public-cursor-secret".to_string()))
    }

    #[cfg(not(test))]
    configured_cursor_secret()
}

fn configured_cursor_secret() -> Result<String, PublicCursorConfigError> {
    cursor_secret_from_env(std::env::var(CURSOR_SECRET_ENV).ok())
}

fn cursor_secret_from_env(value: Option<String>) -> Result<String, PublicCursorConfigError> {
    value
        .map(|secret| secret.trim().to_string())
        .filter(|secret| !secret.is_empty())
        .ok_or(PublicCursorConfigError::MissingSecret)
}

#[cfg(test)]
mod tests;
