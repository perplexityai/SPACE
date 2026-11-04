use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use pplx_space_common_svc::page_cache;
use pplx_space_object_store::{
    ObjectCredentials, ObjectStore, ObjectStoreError, ObjectTarget, S3ObjectStore,
};
use sha2::{Digest, Sha256};

use crate::blob_storage::{BlobStorageAdmission, BlobStorageReservation};
use crate::object_store::{
    CredentialExchangeError, CredentialSubject, ObjectStoreConfig, ObjectStoreConfigError,
    StorageAccess, object_store_credentials,
};

pub use crate::blob_storage::BlobStorageError;

const BLOB_DIR: &str = "build-manager/blobs/sha256";
const STALE_UPLOAD_TEMP_AGE: Duration = Duration::from_secs(6 * 60 * 60);
const DEFAULT_LOCAL_BLOB_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
static NEXT_UPLOAD_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("blob sha256 must be 64 lowercase hex characters")]
    InvalidSha256,
    #[error("{operation} failed: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    ObjectStoreConfig(#[from] ObjectStoreConfigError),
    #[error(transparent)]
    CredentialExchange(#[from] CredentialExchangeError),
    #[error("{operation} failed for shared blob {sha256}: {source}")]
    ObjectStore {
        operation: &'static str,
        sha256: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("shared blob {sha256} has size {actual}, expected {expected}")]
    SizeMismatch {
        sha256: String,
        expected: u64,
        actual: u64,
    },
    #[error("downloaded shared blob {expected} has sha256 {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("blob {sha256} is not available locally and shared storage is not configured")]
    NotShared { sha256: String },
    #[error("shared blob verification task failed: {0}")]
    VerificationTask(#[source] tokio::task::JoinError),
    #[error("blob read returned {read} bytes for a {capacity}-byte buffer")]
    ReadOverflow { read: usize, capacity: usize },
    #[error(transparent)]
    Storage(#[from] BlobStorageError),
}

impl BlobError {
    /// Whether retrying may recover without changing the build input.
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::CredentialExchange(error) => error.is_retriable(),
            Self::ObjectStore { source, .. } => !matches!(
                source,
                ObjectStoreError::NotFound { .. } | ObjectStoreError::PermissionDenied { .. }
            ),
            Self::Io { source, .. } => matches!(
                source.kind(),
                std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::WouldBlock
            ),
            Self::InvalidSha256
            | Self::ObjectStoreConfig(_)
            | Self::SizeMismatch { .. }
            | Self::DigestMismatch { .. }
            | Self::NotShared { .. }
            | Self::VerificationTask(_)
            | Self::ReadOverflow { .. }
            | Self::Storage(_) => false,
        }
    }
}

pub struct StoredBlob {
    pub sha256: String,
    pub size_bytes: u64,
    path: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrunedLocalBlobs {
    pub files: u64,
    pub bytes: u64,
}

#[derive(Clone)]
pub struct BlobStore {
    remote: Option<RemoteBlobStore>,
    local_dir: PathBuf,
    storage_admission: Option<BlobStorageAdmission>,
    local_retention: Duration,
    local_lifecycle: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct RemoteBlobStore {
    config: ObjectStoreConfig,
    object_store: Arc<dyn ObjectStore>,
    credentials: CredentialSource,
}

#[derive(Clone)]
enum CredentialSource {
    Credmgr,
    #[cfg(test)]
    Static(ObjectCredentials),
}

impl BlobStore {
    pub fn from_env() -> Result<Self, BlobError> {
        let remote = ObjectStoreConfig::from_env_optional()?.map(|config| RemoteBlobStore {
            config,
            object_store: Arc::new(S3ObjectStore::new()),
            credentials: CredentialSource::Credmgr,
        });
        Ok(Self {
            remote,
            local_dir: blob_dir(),
            storage_admission: Some(BlobStorageAdmission::from_env(PathBuf::from(
                pplx_space_common_core::host::linux::paths::FIRECRACKER_CACHE_DIR,
            ))),
            local_retention: DEFAULT_LOCAL_BLOB_RETENTION,
            local_lifecycle: Arc::new(Mutex::new(())),
        })
    }

    pub fn local() -> Self {
        Self::local_in(blob_dir())
    }

    pub fn local_in(local_dir: PathBuf) -> Self {
        Self {
            remote: None,
            local_dir,
            storage_admission: None,
            local_retention: DEFAULT_LOCAL_BLOB_RETENTION,
            local_lifecycle: Arc::new(Mutex::new(())),
        }
    }

    pub fn begin_upload(&self) -> Result<BlobUpload, BlobError> {
        BlobUpload::new_in(
            self.local_dir.clone(),
            None,
            Arc::clone(&self.local_lifecycle),
        )
    }

    pub fn begin_reserved_upload(&self, max_bytes: u64) -> Result<BlobUpload, BlobError> {
        cleanup_stale_blob_temps(&self.local_dir);
        let reservation = self
            .storage_admission
            .as_ref()
            .map(|admission| admission.reserve(max_bytes))
            .transpose()?;
        BlobUpload::new_in(
            self.local_dir.clone(),
            reservation,
            Arc::clone(&self.local_lifecycle),
        )
    }

    #[cfg(test)]
    fn for_test(object_store: Arc<dyn ObjectStore>, local_dir: PathBuf) -> Self {
        Self {
            remote: Some(RemoteBlobStore {
                config: ObjectStoreConfig {
                    bucket: "test-bucket".to_string(),
                    region: "test-region".to_string(),
                    key_prefix: "test-prefix".to_string(),
                    endpoint: None,
                    path_style: false,
                },
                object_store,
                credentials: CredentialSource::Static(ObjectCredentials {
                    access_key_id: "test".to_string(),
                    secret_access_key: "test".to_string(),
                    session_token: None,
                }),
            }),
            local_dir,
            storage_admission: None,
            local_retention: Duration::ZERO,
            local_lifecycle: Arc::new(Mutex::new(())),
        }
    }

    pub async fn finish_upload(&self, upload: BlobUpload) -> Result<StoredBlob, BlobError> {
        let stored = upload.finish()?;
        let Some(remote) = &self.remote else {
            return Ok(stored);
        };
        let key = blob_object_key(&remote.config.key_prefix, &stored.sha256);
        let credentials = remote
            .credentials(&stored.sha256, &key, StorageAccess::ReadWrite)
            .await?;
        let target = remote.target(&key);
        if let Some(meta) = remote
            .object_store
            .head(&credentials, target)
            .await
            .map_err(|source| BlobError::ObjectStore {
                operation: "check",
                sha256: stored.sha256.clone(),
                source,
            })?
            && meta.size == stored.size_bytes
        {
            return Ok(stored);
        }

        let uploaded = remote
            .object_store
            .put_file(&credentials, target, &stored.path)
            .await
            .map_err(|source| BlobError::ObjectStore {
                operation: "upload",
                sha256: stored.sha256.clone(),
                source,
            })?;
        verify_size(&stored.sha256, stored.size_bytes, uploaded)?;
        let remote_size = remote
            .object_store
            .head(&credentials, target)
            .await
            .map_err(|source| BlobError::ObjectStore {
                operation: "verify upload",
                sha256: stored.sha256.clone(),
                source,
            })?
            .ok_or_else(|| BlobError::ObjectStore {
                operation: "verify upload",
                sha256: stored.sha256.clone(),
                source: ObjectStoreError::NotFound { key },
            })?
            .size;
        verify_size(&stored.sha256, stored.size_bytes, remote_size)?;
        Ok(stored)
    }

    pub async fn ensure_local(&self, sha256: &str) -> Result<PathBuf, BlobError> {
        let path = blob_path_in(&self.local_dir, sha256)?;
        {
            let _lifecycle = self
                .local_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if path.is_file() {
                touch_blob(&path)?;
                return Ok(path);
            }
        }
        let remote = self.remote.as_ref().ok_or_else(|| BlobError::NotShared {
            sha256: sha256.to_string(),
        })?;
        std::fs::create_dir_all(&self.local_dir).map_err(|source| BlobError::Io {
            operation: "create build-manager blob dir",
            source,
        })?;
        cleanup_stale_blob_temps(&self.local_dir);
        let key = blob_object_key(&remote.config.key_prefix, sha256);
        let credentials = remote
            .credentials(sha256, &key, StorageAccess::Read)
            .await?;
        let remote_size = remote
            .object_store
            .head(&credentials, remote.target(&key))
            .await
            .map_err(|source| BlobError::ObjectStore {
                operation: "check download",
                sha256: sha256.to_string(),
                source,
            })?
            .ok_or_else(|| BlobError::ObjectStore {
                operation: "check download",
                sha256: sha256.to_string(),
                source: ObjectStoreError::NotFound { key: key.clone() },
            })?
            .size;
        let mut reservation = self
            .storage_admission
            .as_ref()
            .map(|admission| admission.reserve(remote_size))
            .transpose()?;
        let id = NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed);
        let temp_path = self
            .local_dir
            .join(format!(".download-{}-{id}.tmp", std::process::id()));
        let mut download = BlobDownload::new(temp_path);
        let downloaded = remote
            .object_store
            .get_file(&credentials, remote.target(&key), download.path())
            .await
            .map_err(|source| BlobError::ObjectStore {
                operation: "download",
                sha256: sha256.to_string(),
                source,
            })?;
        if let Some(reservation) = &mut reservation {
            reservation.record_written(downloaded)?;
        }
        verify_size(sha256, remote_size, downloaded)?;
        let verify_path = download.path().to_path_buf();
        let actual = tokio::task::spawn_blocking(move || sha256_file(&verify_path))
            .await
            .map_err(BlobError::VerificationTask)??;
        if actual != sha256 {
            return Err(BlobError::DigestMismatch {
                expected: sha256.to_string(),
                actual,
            });
        }
        verify_size(
            sha256,
            downloaded,
            std::fs::metadata(download.path())
                .map_err(|source| BlobError::Io {
                    operation: "stat downloaded build-manager blob",
                    source,
                })?
                .len(),
        )?;

        {
            let _lifecycle = self
                .local_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if path.is_file() {
                touch_blob(&path)?;
                download.remove()?;
            } else {
                std::fs::rename(download.path(), &path).map_err(|source| BlobError::Io {
                    operation: "publish downloaded build-manager blob",
                    source,
                })?;
                download.finished = true;
            }
        }
        page_cache::advise_path_dontneed(&path, "publish downloaded build-manager blob");
        Ok(path)
    }

    pub fn prune_local_blobs(&self) -> Result<PrunedLocalBlobs, BlobError> {
        if self.remote.is_none() {
            return Ok(PrunedLocalBlobs::default());
        }
        let _lifecycle = self
            .local_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entries = match std::fs::read_dir(&self.local_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PrunedLocalBlobs::default());
            }
            Err(source) => {
                return Err(BlobError::Io {
                    operation: "list local build-manager blobs",
                    source,
                });
            }
        };
        let mut pruned = PrunedLocalBlobs::default();
        for entry in entries {
            let entry = entry.map_err(|source| BlobError::Io {
                operation: "read local build-manager blob entry",
                source,
            })?;
            let Some(sha256) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if !is_valid_sha256(&sha256) {
                continue;
            }
            let metadata = entry.metadata().map_err(|source| BlobError::Io {
                operation: "stat local build-manager blob",
                source,
            })?;
            if !metadata.is_file()
                || !metadata
                    .modified()
                    .ok()
                    .and_then(|modified| modified.elapsed().ok())
                    .is_some_and(|age| age >= self.local_retention)
            {
                continue;
            }
            std::fs::remove_file(entry.path()).map_err(|source| BlobError::Io {
                operation: "remove expired local build-manager blob",
                source,
            })?;
            pruned.files += 1;
            pruned.bytes = pruned.bytes.saturating_add(metadata.len());
        }
        Ok(pruned)
    }
}

impl RemoteBlobStore {
    fn target<'a>(&'a self, key: &'a str) -> ObjectTarget<'a> {
        ObjectTarget {
            bucket: &self.config.bucket,
            key,
            region: &self.config.region,
            endpoint: self.config.endpoint.as_deref(),
            path_style: self.config.path_style,
        }
    }

    async fn credentials(
        &self,
        sha256: &str,
        key: &str,
        access: StorageAccess,
    ) -> Result<ObjectCredentials, BlobError> {
        match &self.credentials {
            CredentialSource::Credmgr => Ok(object_store_credentials(
                &self.config.bucket,
                key,
                false,
                CredentialSubject::BuildInput(sha256),
                access,
            )
            .await?),
            #[cfg(test)]
            CredentialSource::Static(credentials) => Ok(credentials.clone()),
        }
    }
}

struct BlobDownload {
    path: PathBuf,
    finished: bool,
}

impl BlobDownload {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            finished: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn remove(&mut self) -> Result<(), BlobError> {
        std::fs::remove_file(&self.path).map_err(|source| BlobError::Io {
            operation: "remove duplicate downloaded build-manager blob",
            source,
        })?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for BlobDownload {
    fn drop(&mut self) {
        if !self.finished
            && let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to remove unfinished shared blob download"
            );
        }
    }
}

fn blob_object_key(key_prefix: &str, sha256: &str) -> String {
    let key = format!("build-inputs/sha256/{sha256}");
    let key_prefix = key_prefix.trim_matches('/');
    if key_prefix.is_empty() {
        key
    } else {
        format!("{key_prefix}/{key}")
    }
}

fn verify_size(sha256: &str, expected: u64, actual: u64) -> Result<(), BlobError> {
    if expected == actual {
        Ok(())
    } else {
        Err(BlobError::SizeMismatch {
            sha256: sha256.to_string(),
            expected,
            actual,
        })
    }
}

fn sha256_file(path: &Path) -> Result<String, BlobError> {
    let mut file = std::fs::File::open(path).map_err(|source| BlobError::Io {
        operation: "open downloaded build-manager blob",
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| BlobError::Io {
            operation: "hash downloaded build-manager blob",
            source,
        })?;
        if read == 0 {
            break;
        }
        let chunk = buffer.get(..read).ok_or(BlobError::ReadOverflow {
            read,
            capacity: buffer.len(),
        })?;
        hasher.update(chunk);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// A build input upload created by [`BlobStore::begin_upload`] and finalized through
/// [`BlobStore::finish_upload`].
pub struct BlobUpload {
    temp_path: PathBuf,
    file: std::fs::File,
    hasher: Sha256,
    size_bytes: u64,
    finished: bool,
    storage_reservation: Option<BlobStorageReservation>,
    local_lifecycle: Arc<Mutex<()>>,
}

impl BlobUpload {
    fn new_in(
        dir: PathBuf,
        storage_reservation: Option<BlobStorageReservation>,
        local_lifecycle: Arc<Mutex<()>>,
    ) -> Result<Self, BlobError> {
        std::fs::create_dir_all(&dir).map_err(|source| BlobError::Io {
            operation: "create build-manager blob dir",
            source,
        })?;
        cleanup_stale_blob_temps(&dir);
        let id = NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed);
        let temp_path = dir.join(format!(".upload-{}-{id}.tmp", std::process::id(),));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|source| BlobError::Io {
                operation: "create build-manager blob upload",
                source,
            })?;
        Ok(Self {
            temp_path,
            file,
            hasher: Sha256::new(),
            size_bytes: 0,
            finished: false,
            storage_reservation,
            local_lifecycle,
        })
    }

    pub fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.file.write_all(bytes).map_err(|source| BlobError::Io {
            operation: "write build-manager blob upload",
            source,
        })?;
        self.hasher.update(bytes);
        self.size_bytes += bytes.len() as u64;
        if let Some(reservation) = &mut self.storage_reservation {
            reservation.record_written(bytes.len() as u64)?;
        }
        Ok(())
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    fn finish(mut self) -> Result<StoredBlob, BlobError> {
        self.file.flush().map_err(|source| BlobError::Io {
            operation: "flush build-manager blob upload",
            source,
        })?;
        let sha256 = hex::encode(std::mem::take(&mut self.hasher).finalize());
        let path = self.temp_path.with_file_name(&sha256);
        let _lifecycle = self
            .local_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if path.exists() {
            touch_blob(&path)?;
            page_cache::advise_dontneed(
                &self.file,
                &self.temp_path,
                "discard duplicate build-manager blob upload",
            );
            std::fs::remove_file(&self.temp_path).map_err(|source| BlobError::Io {
                operation: "remove duplicate build-manager blob upload",
                source,
            })?;
        } else {
            std::fs::rename(&self.temp_path, &path).map_err(|source| BlobError::Io {
                operation: "publish build-manager blob",
                source,
            })?;
            page_cache::advise_path_dontneed(&path, "publish build-manager blob upload");
        }
        self.finished = true;
        Ok(StoredBlob {
            sha256,
            size_bytes: self.size_bytes,
            path,
        })
    }
}

impl Drop for BlobUpload {
    fn drop(&mut self) {
        if !self.finished {
            match std::fs::remove_file(&self.temp_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(
                        path = %self.temp_path.display(),
                        error = %error,
                        "failed to remove unfinished build-manager blob upload"
                    );
                }
            }
        }
    }
}

pub fn blob_path(sha256: &str) -> Result<PathBuf, BlobError> {
    blob_path_in(&blob_dir(), sha256)
}

fn blob_path_in(dir: &Path, sha256: &str) -> Result<PathBuf, BlobError> {
    if !is_valid_sha256(sha256) {
        return Err(BlobError::InvalidSha256);
    }
    Ok(dir.join(sha256))
}

fn blob_dir() -> PathBuf {
    Path::new(pplx_space_common_core::host::linux::paths::FIRECRACKER_CACHE_DIR).join(BLOB_DIR)
}

fn cleanup_stale_blob_temps(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !(name.starts_with(".upload-") || name.starts_with(".download-"))
            || !name.ends_with(".tmp")
        {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        if modified
            .elapsed()
            .is_ok_and(|age| age >= STALE_UPLOAD_TEMP_AGE)
        {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "failed to remove stale build-manager blob temp file"
                    );
                }
            }
        }
    }
}

fn is_valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn touch_blob(path: &Path) -> Result<(), BlobError> {
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.set_modified(SystemTime::now()))
        .map_err(|source| BlobError::Io {
            operation: "refresh local build-manager blob retention",
            source,
        })
}

#[cfg(test)]
mod tests;
