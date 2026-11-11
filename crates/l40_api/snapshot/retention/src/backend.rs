//! Backend-neutral object-store seam for the retention sweeper.
//!
//! The retention sweeper deletes a snapshot's durable blobs given the
//! opaque `storage_connection_string` stored on its
//! `snapshot_storage_locations` row. *How* those bytes are reaped
//! depends on where they live — S3 today, potentially GCS / Azure Blob /
//! a local filesystem later. This module is the abstraction that keeps
//! the reconciler from being S3-centric:
//!
//! - [`StorageBackend`] is a **dyn-compatible** trait (boxed future, so
//!   `Arc<dyn StorageBackend>` works) with one implementation per URI
//!   scheme. Each backend owns its own URI parsing — the connection
//!   string is passed through as an opaque `&str`, because the parsed
//!   shape is backend-specific (`StorageUri` only understands `s3://`).
//! - [`StorageBackendRegistry`] routes each prune to the backend
//!   registered for the connection string's scheme (`s3`, `gs`, …), so
//!   heterogeneous backends can coexist in one cluster. An unregistered
//!   scheme is surfaced as [`DeletePrefixError::UnsupportedScheme`]
//!   rather than silently skipped.
//!
//! The optional `s3` feature provides the only production backend today,
//! `S3Backend`, a thin adapter
//! over the existing, well-tested
//! [`pplx_space_snapshot_storage::s3::S3StorageBackend`] (S3 list +
//! batch DeleteObjects). It parses the `s3://` URI, enforces the
//! cluster's authorized bucket scope, and maps the S3-specific outcome /
//! error types onto the neutral ones here. Adding another backend is a
//! new `impl StorageBackend` plus one `register` call at the spawn site.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[cfg(feature = "s3")]
use pplx_space_common_core::types::StorageUri;
#[cfg(feature = "s3")]
use pplx_space_snapshot_storage::s3::S3StorageBackend;
#[cfg(feature = "s3")]
use pplx_space_snapshot_storage::{
    DeletePrefixError as S3DeletePrefixError, DeletePrefixOutcome as S3DeletePrefixOutcome,
    StorageBackend as S3StorageBackendTrait,
};

/// Outcome of one [`StorageBackend::delete_prefix`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeletePrefixOutcome {
    /// `count` objects were enumerated and deleted under the prefix.
    Deleted { count: u64 },
    /// The prefix had no objects under it — idempotent success.
    AlreadyGone,
}

/// Why a prefix delete didn't reap the snapshot. Each variant maps to a
/// distinct sweeper metric/counter so dashboards can tell a terminal
/// misconfiguration (bad URI, unknown scheme, out-of-scope container)
/// apart from a transient backend failure.
#[derive(Debug, thiserror::Error)]
pub enum DeletePrefixError {
    /// No backend is registered for the connection string's scheme.
    /// Terminal until the backend is added/registered.
    #[error("no storage backend registered for scheme {0:?}")]
    UnsupportedScheme(String),
    /// The connection string didn't parse as a valid URI for its
    /// backend. Terminal until the row is repaired.
    #[error("invalid storage URI")]
    InvalidUri,
    /// The URI targets a container (S3 bucket, …) the backend isn't
    /// authorized to act on. Terminal until cross-scope sweep ships.
    #[error("storage target {container:?} is outside the {backend} backend's authorized scope")]
    OutOfScope {
        backend: &'static str,
        container: String,
    },
    /// Transient or permission-y failure from the object store. Retried
    /// next tick.
    #[error("storage backend delete failed: {0}")]
    Backend(String),
}

/// One object-store backend, keyed by the URI scheme it handles. Boxed
/// future keeps the trait dyn-compatible so backends can be selected at
/// runtime by scheme.
pub trait StorageBackend: Send + Sync + 'static {
    /// The URI scheme this backend handles, e.g. `"s3"`. Used as the
    /// registry key and for the `OutOfScope` backend label.
    fn scheme(&self) -> &'static str;

    /// Parse and scope-check `connection_string` without doing I/O.
    /// Callers use this before creating an irreversible prune claim. It must
    /// agree with [`Self::delete_prefix`].
    fn validate_route(&self, connection_string: &str) -> Result<(), DeletePrefixError>;

    /// List every object under the connection string's prefix and
    /// batch-delete them. The backend parses `connection_string` itself
    /// (the parsed shape is backend-specific). An empty listing is
    /// [`DeletePrefixOutcome::AlreadyGone`] (idempotent success).
    fn delete_prefix<'a>(
        &'a self,
        connection_string: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<DeletePrefixOutcome, DeletePrefixError>> + Send + 'a>>;
}

/// Routes a prune to the backend registered for the connection string's
/// URI scheme. Cheap to clone (`Arc`-backed map).
#[derive(Clone, Default)]
pub struct StorageBackendRegistry {
    by_scheme: HashMap<&'static str, Arc<dyn StorageBackend>>,
}

impl StorageBackendRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a backend under its `scheme()`. A later registration for
    /// the same scheme replaces the earlier one.
    pub fn register(&mut self, backend: Arc<dyn StorageBackend>) {
        self.by_scheme.insert(backend.scheme(), backend);
    }

    pub fn is_empty(&self) -> bool {
        self.by_scheme.is_empty()
    }

    /// Registered schemes, sorted — for startup logging.
    pub fn schemes(&self) -> Vec<&'static str> {
        let mut schemes: Vec<&'static str> = self.by_scheme.keys().copied().collect();
        schemes.sort_unstable();
        schemes
    }

    /// Resolve, parse, and scope-check a connection string without I/O.
    pub fn validate_route(&self, connection_string: &str) -> Result<(), DeletePrefixError> {
        let scheme = scheme_of(connection_string);
        match self.by_scheme.get(scheme) {
            Some(backend) => backend.validate_route(connection_string),
            None => Err(DeletePrefixError::UnsupportedScheme(scheme.to_string())),
        }
    }

    /// Reap the prefix named by `connection_string`, routing to the
    /// backend for its scheme. Returns
    /// [`DeletePrefixError::UnsupportedScheme`] when no backend is
    /// registered for the scheme.
    pub async fn delete_prefix(
        &self,
        connection_string: &str,
    ) -> Result<DeletePrefixOutcome, DeletePrefixError> {
        let scheme = scheme_of(connection_string);
        match self.by_scheme.get(scheme) {
            Some(backend) => backend.delete_prefix(connection_string).await,
            None => Err(DeletePrefixError::UnsupportedScheme(scheme.to_string())),
        }
    }
}

/// Extract the `scheme` from `scheme://rest`. Returns `""` when there is
/// no `://`, which never matches a registered backend (→ unsupported).
fn scheme_of(connection_string: &str) -> &str {
    connection_string
        .split_once("://")
        .map(|(scheme, _)| scheme)
        .unwrap_or("")
}

/// S3 implementation of [`StorageBackend`]. Adapts the existing
/// [`S3StorageBackend`] (list + batch DeleteObjects): parses the `s3://`
/// URI, enforces the authorized bucket scope, and maps the S3 outcome /
/// error types onto the neutral ones.
#[cfg(feature = "s3")]
pub struct S3Backend {
    inner: S3StorageBackend,
    /// Cluster-scoped bucket this backend is authorized to act on; URIs
    /// pointing elsewhere are refused as [`DeletePrefixError::OutOfScope`].
    /// `None` disables the check (any bucket allowed).
    scoped_bucket: Option<String>,
}

#[cfg(feature = "s3")]
impl S3Backend {
    pub fn new(inner: S3StorageBackend, scoped_bucket: Option<String>) -> Self {
        Self {
            inner,
            scoped_bucket,
        }
    }

    /// Parse + scope-check, shared by `validate_route` and
    /// `delete_prefix` so the pre-claim answer cannot drift from the
    /// reap-time one.
    fn route(&self, connection_string: &str) -> Result<StorageUri, DeletePrefixError> {
        let uri =
            StorageUri::parse(connection_string).map_err(|_| DeletePrefixError::InvalidUri)?;
        if uri.key_prefix.is_empty() {
            return Err(DeletePrefixError::InvalidUri);
        }
        if let Some(scoped) = self.scoped_bucket.as_deref()
            && uri.bucket != scoped
        {
            return Err(DeletePrefixError::OutOfScope {
                backend: "s3",
                container: uri.bucket.clone(),
            });
        }
        Ok(uri)
    }
}

#[cfg(feature = "s3")]
impl StorageBackend for S3Backend {
    fn scheme(&self) -> &'static str {
        "s3"
    }

    fn validate_route(&self, connection_string: &str) -> Result<(), DeletePrefixError> {
        self.route(connection_string).map(|_| ())
    }

    fn delete_prefix<'a>(
        &'a self,
        connection_string: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<DeletePrefixOutcome, DeletePrefixError>> + Send + 'a>>
    {
        Box::pin(async move {
            let uri = self.route(connection_string)?;
            match self.inner.delete_prefix(&uri).await {
                Ok(S3DeletePrefixOutcome::Deleted { count }) => {
                    Ok(DeletePrefixOutcome::Deleted { count })
                }
                Ok(S3DeletePrefixOutcome::AlreadyGone) => Ok(DeletePrefixOutcome::AlreadyGone),
                Err(S3DeletePrefixError::Backend(msg)) => Err(DeletePrefixError::Backend(msg)),
            }
        })
    }
}

#[cfg(test)]
mod tests;
