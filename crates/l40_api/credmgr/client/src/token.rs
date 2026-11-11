//! The bearer credential a TCP credmgr client presents.
//!
//! Prefers a projected Kubernetes ServiceAccount token read from a file
//! (`SPACE_CREDMGR_TOKEN_PATH`, default `/var/run/space-auth/credmgr.token`),
//! falling back to the shared static secret in `SPACE_INTERNAL_TOKEN`. The
//! file is re-read per request (with a 1-second mtime-aware cache) so kubelet
//! token rotation is picked up promptly without reconnecting the long-lived
//! gRPC channel — capturing the token once at connect time would let it expire
//! mid-channel.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use tonic::metadata::{Ascii, MetadataValue};

const SPACE_AUTH_DIR: &str = "/var/run/space-auth";
const DEFAULT_TOKEN_FILENAME: &str = "credmgr.token";
const TOKEN_FILE_CACHE_TTL: Duration = Duration::from_secs(1);

/// Path-override env: when set, the file is authoritative and a read failure
/// is an error (no silent fallback to the static env secret).
pub const SPACE_CREDMGR_TOKEN_PATH_ENV: &str = "SPACE_CREDMGR_TOKEN_PATH";
/// Static-secret fallback env (the legacy shared cluster-internal token).
pub const SPACE_INTERNAL_TOKEN_ENV: &str = "SPACE_INTERNAL_TOKEN";

#[derive(Debug, thiserror::Error)]
pub enum BearerTokenError {
    #[error("token file {path} is empty")]
    EmptyFile { path: PathBuf },
    #[error("token file {path} does not exist")]
    MissingFile { path: PathBuf },
    #[error("failed to read token file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{SPACE_INTERNAL_TOKEN_ENV} is empty")]
    EmptyEnv,
    #[error("no projected token file at {default_path} and {SPACE_INTERNAL_TOKEN_ENV} is unset")]
    Missing { default_path: PathBuf },
    #[error("token is not valid gRPC metadata: {0}")]
    InvalidMetadata(#[from] tonic::metadata::errors::InvalidMetadataValue),
}

fn token_path(env: &dyn pplx_space_common_core::env::Environment) -> (PathBuf, bool) {
    match env
        .var(SPACE_CREDMGR_TOKEN_PATH_ENV)
        .ok()
        .filter(|path| !path.trim().is_empty())
    {
        Some(path) => (PathBuf::from(path), true),
        None => (
            Path::new(SPACE_AUTH_DIR).join(DEFAULT_TOKEN_FILENAME),
            false,
        ),
    }
}

/// Resolve the current bearer token: projected token file first, static env
/// secret as fallback (only when the path was not explicitly configured).
pub fn bearer_token() -> Result<String, BearerTokenError> {
    bearer_token_from_environment(&pplx_space_common_core::env::ProcessEnvironment)
}

fn bearer_token_from_environment(
    env: &dyn pplx_space_common_core::env::Environment,
) -> Result<String, BearerTokenError> {
    let (path, explicit_path) = token_path(env);
    match read_token_file(&path) {
        Ok(Some(token)) => return Ok(token),
        Ok(None) if explicit_path => return Err(BearerTokenError::MissingFile { path }),
        Ok(None) => {}
        Err(error) => {
            // An explicitly-configured path that fails to read is fatal: do not
            // fall back to the static secret and silently mask a misconfigured
            // projected token mount.
            if explicit_path {
                return Err(error);
            }
        }
    }

    match env.var(SPACE_INTERNAL_TOKEN_ENV) {
        Ok(token) => {
            let token = token.trim().to_owned();
            if token.is_empty() {
                Err(BearerTokenError::EmptyEnv)
            } else {
                Ok(token)
            }
        }
        Err(_) => Err(BearerTokenError::Missing { default_path: path }),
    }
}

/// The `authorization: Bearer <token>` metadata value for the current token.
pub fn bearer_metadata() -> Result<MetadataValue<Ascii>, BearerTokenError> {
    let token = bearer_token()?;
    Ok(format!("Bearer {token}").parse()?)
}

fn read_token_file(path: &Path) -> Result<Option<String>, BearerTokenError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(BearerTokenError::ReadFile {
                path: path.to_owned(),
                source,
            });
        }
    };
    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let now = Instant::now();

    if let Some(token) = cached_token_file(path, modified, now) {
        return Ok(Some(token));
    }

    match std::fs::read_to_string(path) {
        Ok(token) => {
            let token = token.trim().to_owned();
            if token.is_empty() {
                return Err(BearerTokenError::EmptyFile {
                    path: path.to_owned(),
                });
            }
            cache_token_file(path, modified, now, &token);
            Ok(Some(token))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(source) => Err(BearerTokenError::ReadFile {
            path: path.to_owned(),
            source,
        }),
    }
}

#[derive(Clone)]
struct CachedTokenFile {
    token: String,
    modified: SystemTime,
    checked_at: Instant,
}

fn token_file_cache() -> &'static Mutex<HashMap<PathBuf, CachedTokenFile>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, CachedTokenFile>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached_token_file(path: &Path, modified: SystemTime, now: Instant) -> Option<String> {
    let cache = token_file_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache
        .get(path)
        .filter(|cached| cached.modified == modified)
        .filter(|cached| now.duration_since(cached.checked_at) < TOKEN_FILE_CACHE_TTL)
        .map(|cached| cached.token.clone())
}

fn cache_token_file(path: &Path, modified: SystemTime, checked_at: Instant, token: &str) {
    let mut cache = token_file_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.insert(
        path.to_owned(),
        CachedTokenFile {
            token: token.to_owned(),
            modified,
            checked_at,
        },
    );
}

#[cfg(test)]
mod tests;
