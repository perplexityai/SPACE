//! ECW credential keys and tokens: loads the current and previous HMAC keys
//! from the environment, mints per-sandbox envd and traffic credentials, and
//! matches a presented credential's SHA-256 digest against them.
//!
//! It is its own crate so a service can mint and check ECW credentials without
//! depending on the rest of `pplx-space-common-svc`.

use std::fmt;
use std::path::PathBuf;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use pplx_space_common_core::types::{EcwCredentialEpoch, SandboxId};

type HmacSha256 = Hmac<Sha256>;

pub const SPACE_ECW_CREDENTIAL_KEY_ENV: &str = "SPACE_ECW_CREDENTIAL_KEY";
pub const SPACE_ECW_CREDENTIAL_KEY_PATH_ENV: &str = "SPACE_ECW_CREDENTIAL_KEY_PATH";
pub const SPACE_ECW_CREDENTIAL_PREVIOUS_KEY_ENV: &str = "SPACE_ECW_CREDENTIAL_PREVIOUS_KEY";
pub const SPACE_ECW_CREDENTIAL_PREVIOUS_KEY_PATH_ENV: &str =
    "SPACE_ECW_CREDENTIAL_PREVIOUS_KEY_PATH";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EcwCredentialKind {
    Envd,
    Traffic,
}

impl EcwCredentialKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Envd => "envd",
            Self::Traffic => "traffic",
        }
    }

    const fn prefix(self) -> &'static str {
        match self {
            Self::Envd => "ecw",
            Self::Traffic => "ecw_traffic",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EcwCredentialMatch {
    V2Current,
    V2Previous,
    NoMatch,
}

#[derive(Clone, Eq, PartialEq)]
pub struct EcwCredentialToken(String);

impl EcwCredentialToken {
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EcwCredentialToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EcwCredentialToken(<redacted>)")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EcwCredentialError {
    #[error("{SPACE_ECW_CREDENTIAL_KEY_ENV} or {SPACE_ECW_CREDENTIAL_KEY_PATH_ENV} is required")]
    MissingCurrentKey,
    #[error("ECW credential key is empty")]
    EmptyKey,
    #[error("ECW credential key file {path} is empty")]
    EmptyKeyFile { path: PathBuf },
    #[error("failed to read ECW credential key file {path}: {source}")]
    ReadKeyFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone)]
pub struct EcwCredentialKeyring {
    current: Option<Vec<u8>>,
    previous: Option<Vec<u8>>,
}

impl EcwCredentialKeyring {
    #[cfg(any(test, feature = "test-support"))]
    pub fn from_current_key_for_test(key: &[u8]) -> Self {
        assert!(!key.is_empty(), "test ECW credential key must not be empty");
        Self {
            current: Some(key.to_vec()),
            previous: None,
        }
    }

    pub fn load_from_env() -> Result<Self, EcwCredentialError> {
        Self::load_from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn load_from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Self, EcwCredentialError> {
        Ok(Self {
            current: load_key(
                env,
                SPACE_ECW_CREDENTIAL_KEY_ENV,
                SPACE_ECW_CREDENTIAL_KEY_PATH_ENV,
            )?,
            previous: load_key(
                env,
                SPACE_ECW_CREDENTIAL_PREVIOUS_KEY_ENV,
                SPACE_ECW_CREDENTIAL_PREVIOUS_KEY_PATH_ENV,
            )?,
        })
    }

    pub fn load_required_from_env() -> Result<Self, EcwCredentialError> {
        Self::load_required_from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn load_required_from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Self, EcwCredentialError> {
        let keyring = Self::load_from_environment(env)?;
        if keyring.current.is_none() {
            return Err(EcwCredentialError::MissingCurrentKey);
        }
        Ok(keyring)
    }

    pub fn mint(
        &self,
        sandbox_id: SandboxId,
        credential_epoch: EcwCredentialEpoch,
        kind: EcwCredentialKind,
    ) -> Result<Option<EcwCredentialToken>, EcwCredentialError> {
        self.current
            .as_deref()
            .map(|key| mint_v2(sandbox_id, credential_epoch, kind, key))
            .transpose()
    }

    pub fn match_sha256(
        &self,
        sandbox_id: SandboxId,
        credential_epoch: EcwCredentialEpoch,
        kind: EcwCredentialKind,
        candidate_sha256: &str,
    ) -> Result<EcwCredentialMatch, EcwCredentialError> {
        let candidate_sha256 = candidate_sha256.trim();
        if candidate_sha256.is_empty() {
            return Ok(EcwCredentialMatch::NoMatch);
        }
        if let Some(key) = self.current.as_deref()
            && token_sha256_matches(
                &mint_v2(sandbox_id, credential_epoch, kind, key)?,
                candidate_sha256,
            )
        {
            return Ok(EcwCredentialMatch::V2Current);
        }
        if let Some(key) = self.previous.as_deref()
            && token_sha256_matches(
                &mint_v2(sandbox_id, credential_epoch, kind, key)?,
                candidate_sha256,
            )
        {
            return Ok(EcwCredentialMatch::V2Previous);
        }
        Ok(EcwCredentialMatch::NoMatch)
    }
}

fn mint_v2(
    sandbox_id: SandboxId,
    credential_epoch: EcwCredentialEpoch,
    kind: EcwCredentialKind,
    key: &[u8],
) -> Result<EcwCredentialToken, EcwCredentialError> {
    let mut mac = <HmacSha256 as hmac::digest::KeyInit>::new_from_slice(key)
        .map_err(|_| EcwCredentialError::EmptyKey)?;
    mac.update(b"ecw-credential-v2\0");
    mac.update(kind.as_str().as_bytes());
    mac.update(b"\0");
    mac.update(sandbox_id.as_uuid().as_bytes());
    mac.update(&credential_epoch.get().to_be_bytes());
    Ok(EcwCredentialToken(format!(
        "{}_v2_{}_{}",
        kind.prefix(),
        credential_epoch.get(),
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )))
}

fn token_sha256_matches(token: &EcwCredentialToken, candidate_sha256: &str) -> bool {
    let expected = lower_hex(&Sha256::digest(token.secret().as_bytes()));
    expected
        .as_bytes()
        .ct_eq(candidate_sha256.as_bytes())
        .unwrap_u8()
        == 1
}

fn lower_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

fn load_key(
    source: &dyn pplx_space_common_core::env::Environment,
    env: &str,
    path_env: &str,
) -> Result<Option<Vec<u8>>, EcwCredentialError> {
    if let Some(path) = source.var_os(path_env) {
        let path = PathBuf::from(path);
        let key = std::fs::read(&path).map_err(|source| EcwCredentialError::ReadKeyFile {
            path: path.clone(),
            source,
        })?;
        if key.is_empty() {
            return Err(EcwCredentialError::EmptyKeyFile { path });
        }
        return Ok(Some(key));
    }
    let Some(key) = source.var_os(env) else {
        return Ok(None);
    };
    let key = key.to_string_lossy().into_owned().into_bytes();
    if key.is_empty() {
        return Err(EcwCredentialError::EmptyKey);
    }
    Ok(Some(key))
}

#[cfg(test)]
mod tests;
