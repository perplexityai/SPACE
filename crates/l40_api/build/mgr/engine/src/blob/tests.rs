use super::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use pplx_space_common_core::host::disk::DiskStatus;
use pplx_space_object_store::{
    ObjectCredentials, ObjectMeta, ObjectStore, ObjectStoreError, ObjectTarget,
};
use rstest::{fixture, rstest};

use crate::storage::StorageProbe;

#[derive(Clone, Default)]
struct MockObjectStore {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    get_count: Arc<AtomicU64>,
    put_count: Arc<AtomicU64>,
}

impl MockObjectStore {
    fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    fn insert(&self, key: String, bytes: Vec<u8>) {
        self.objects.lock().unwrap().insert(key, bytes);
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.objects.lock().unwrap().get(key).cloned()
    }
}

#[async_trait]
impl ObjectStore for MockObjectStore {
    async fn head(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(target.key)
            .map(|bytes| ObjectMeta {
                size: bytes.len() as u64,
                etag: None,
            }))
    }

    async fn put_bytes(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        body: Bytes,
    ) -> Result<(), ObjectStoreError> {
        self.put_count.fetch_add(1, Ordering::Relaxed);
        self.objects
            .lock()
            .unwrap()
            .insert(target.key.to_string(), body.to_vec());
        Ok(())
    }

    async fn get_bytes(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        self.objects
            .lock()
            .unwrap()
            .get(target.key)
            .cloned()
            .ok_or_else(|| ObjectStoreError::NotFound {
                key: target.key.to_string(),
            })
    }

    async fn put_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        src: &Path,
    ) -> Result<u64, ObjectStoreError> {
        let bytes = std::fs::read(src)?;
        let size = bytes.len() as u64;
        self.put_bytes(creds, target, Bytes::from(bytes)).await?;
        Ok(size)
    }

    async fn get_file(
        &self,
        _creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        dst: &Path,
    ) -> Result<u64, ObjectStoreError> {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        let bytes = self
            .objects
            .lock()
            .unwrap()
            .get(target.key)
            .cloned()
            .ok_or_else(|| ObjectStoreError::NotFound {
                key: target.key.to_string(),
            })?;
        std::fs::write(dst, &bytes)?;
        Ok(bytes.len() as u64)
    }
}

struct SharedBlobStores {
    object_store: MockObjectStore,
    uploader_dir: tempfile::TempDir,
    downloader_dir: tempfile::TempDir,
    uploader: BlobStore,
    downloader: BlobStore,
}

impl SharedBlobStores {
    fn upload(&self, bytes: &[u8]) -> BlobUpload {
        let mut upload = self.uploader.begin_upload().unwrap();
        upload.write_chunk(bytes).unwrap();
        upload
    }

    fn insert_shared(&self, sha256: &str, bytes: &[u8]) {
        self.object_store
            .insert(blob_object_key("test-prefix", sha256), bytes.to_vec());
    }
}

#[fixture]
fn shared_blob_stores() -> SharedBlobStores {
    let object_store = MockObjectStore::default();
    let uploader_dir = tempfile::tempdir().unwrap();
    let downloader_dir = tempfile::tempdir().unwrap();
    let uploader = BlobStore::for_test(
        Arc::new(object_store.clone()),
        uploader_dir.path().to_path_buf(),
    );
    let downloader = BlobStore::for_test(
        Arc::new(object_store.clone()),
        downloader_dir.path().to_path_buf(),
    );
    SharedBlobStores {
        object_store,
        uploader_dir,
        downloader_dir,
        uploader,
        downloader,
    }
}

struct StaleTempStorageProbe {
    stale_path: PathBuf,
}

struct DownloadStorageProbe {
    blob_dir: PathBuf,
    available_bytes: u64,
}

impl StorageProbe for DownloadStorageProbe {
    fn status(&self, _root: &Path) -> Option<DiskStatus> {
        let download_exists = std::fs::read_dir(&self.blob_dir)
            .into_iter()
            .flatten()
            .flatten()
            .any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".download-"))
            });
        let available_bytes = if download_exists {
            0
        } else {
            self.available_bytes
        };
        Some(DiskStatus {
            total_bytes: self.available_bytes,
            free_bytes: available_bytes,
            available_bytes,
        })
    }
}

impl StorageProbe for StaleTempStorageProbe {
    fn status(&self, _root: &Path) -> Option<DiskStatus> {
        let available_bytes = if self.stale_path.exists() { 0 } else { 100 };
        Some(DiskStatus {
            total_bytes: 100,
            free_bytes: available_bytes,
            available_bytes,
        })
    }
}

struct StaleUploadFixture {
    _tempdir: tempfile::TempDir,
    stale_path: PathBuf,
    store: BlobStore,
}

#[fixture]
fn stale_upload_fixture() -> StaleUploadFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let stale_path = tempdir.path().join(".upload-123-456.tmp");
    let stale_file = std::fs::File::create(&stale_path).unwrap();
    stale_file
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::SystemTime::now() - STALE_UPLOAD_TEMP_AGE),
        )
        .unwrap();
    let storage_admission = BlobStorageAdmission::for_test(
        tempdir.path().to_path_buf(),
        20,
        Arc::new(StaleTempStorageProbe {
            stale_path: stale_path.clone(),
        }),
    );
    let store = BlobStore {
        remote: None,
        local_dir: tempdir.path().to_path_buf(),
        storage_admission: Some(storage_admission),
        local_retention: DEFAULT_LOCAL_BLOB_RETENTION,
        local_lifecycle: Arc::new(Mutex::new(())),
    };
    StaleUploadFixture {
        _tempdir: tempdir,
        stale_path,
        store,
    }
}

#[rstest]
fn reserved_upload_reclaims_stale_temps_before_checking_capacity(
    stale_upload_fixture: StaleUploadFixture,
) {
    let _upload = stale_upload_fixture
        .store
        .begin_reserved_upload(40)
        .expect("stale temporary upload should be reclaimed before admission");

    assert!(!stale_upload_fixture.stale_path.exists());
}

#[rstest]
#[case("../nope")]
#[case("abc")]
fn rejects_non_hex_blob_ids(#[case] blob_id: &str) {
    assert!(matches!(blob_path(blob_id), Err(BlobError::InvalidSha256)));
}

#[rstest]
#[case::transient_download(ObjectStoreError::Get("connection reset".to_string()), true)]
#[case::missing_blob(ObjectStoreError::NotFound { key: "missing".to_string() }, false)]
#[case::unauthorized_blob(ObjectStoreError::PermissionDenied { message: "denied".to_string() }, false)]
fn classifies_shared_storage_failures_for_job_retries(
    #[case] source: ObjectStoreError,
    #[case] expected_retriable: bool,
) {
    let error = BlobError::ObjectStore {
        operation: "download",
        sha256: "a".repeat(64),
        source,
    };

    assert_eq!(error.is_retriable(), expected_retriable);
}

#[rstest]
#[tokio::test]
async fn shared_blob_is_downloaded_and_verified_on_another_builder(
    shared_blob_stores: SharedBlobStores,
) {
    let bytes = b"shared template build input";
    let sha256 = hex::encode(Sha256::digest(bytes));
    let upload = shared_blob_stores.upload(bytes);

    let stored = shared_blob_stores
        .uploader
        .finish_upload(upload)
        .await
        .unwrap();
    let downloaded = shared_blob_stores
        .downloader
        .ensure_local(&sha256)
        .await
        .unwrap();
    let cached = shared_blob_stores
        .downloader
        .ensure_local(&sha256)
        .await
        .unwrap();

    assert_eq!(stored.sha256, sha256);
    assert_eq!(std::fs::read(downloaded).unwrap(), bytes);
    assert_eq!(std::fs::read(cached).unwrap(), bytes);
    assert_eq!(shared_blob_stores.object_store.put_count(), 1);
    assert_eq!(shared_blob_stores.object_store.get_count(), 1);
}

#[rstest]
#[tokio::test]
async fn shared_blob_download_reserves_capacity_and_rechecks_before_publish(
    mut shared_blob_stores: SharedBlobStores,
) {
    let bytes = b"shared template build input";
    let sha256 = hex::encode(Sha256::digest(bytes));
    shared_blob_stores.insert_shared(&sha256, bytes);
    shared_blob_stores.downloader.storage_admission = Some(BlobStorageAdmission::for_test(
        shared_blob_stores.downloader_dir.path().to_path_buf(),
        20,
        Arc::new(DownloadStorageProbe {
            blob_dir: shared_blob_stores.downloader_dir.path().to_path_buf(),
            available_bytes: 100,
        }),
    ));

    let error = shared_blob_stores
        .downloader
        .ensure_local(&sha256)
        .await
        .unwrap_err();

    assert!(matches!(error, BlobError::Storage(_)));
    assert!(
        !shared_blob_stores
            .downloader_dir
            .path()
            .join(&sha256)
            .exists()
    );
    assert_eq!(shared_blob_stores.object_store.get_count(), 1);
}

#[rstest]
#[tokio::test]
async fn shared_blob_download_rejects_before_transfer_without_headroom(
    mut shared_blob_stores: SharedBlobStores,
) {
    let bytes = vec![b'x'; 81];
    let sha256 = hex::encode(Sha256::digest(&bytes));
    shared_blob_stores.insert_shared(&sha256, &bytes);
    shared_blob_stores.downloader.storage_admission = Some(BlobStorageAdmission::for_test(
        shared_blob_stores.downloader_dir.path().to_path_buf(),
        20,
        Arc::new(DownloadStorageProbe {
            blob_dir: shared_blob_stores.downloader_dir.path().to_path_buf(),
            available_bytes: 100,
        }),
    ));

    let error = shared_blob_stores
        .downloader
        .ensure_local(&sha256)
        .await
        .unwrap_err();

    assert!(matches!(error, BlobError::Storage(_)));
    assert_eq!(shared_blob_stores.object_store.get_count(), 0);
}

#[rstest]
#[tokio::test]
async fn existing_shared_blob_is_not_uploaded_again(shared_blob_stores: SharedBlobStores) {
    let bytes = b"deduplicated template build input";

    for _ in 0..2 {
        let upload = shared_blob_stores.upload(bytes);
        shared_blob_stores
            .uploader
            .finish_upload(upload)
            .await
            .unwrap();
    }

    assert_eq!(shared_blob_stores.object_store.put_count(), 1);
}

#[rstest]
#[tokio::test]
async fn expired_local_blob_is_reaped_after_remote_publish(shared_blob_stores: SharedBlobStores) {
    let bytes = b"unreferenced template build input";
    let sha256 = hex::encode(Sha256::digest(bytes));
    let upload = shared_blob_stores.upload(bytes);
    shared_blob_stores
        .uploader
        .finish_upload(upload)
        .await
        .expect("publish blob");
    let local_path = blob_path_in(shared_blob_stores.uploader_dir.path(), &sha256).unwrap();
    assert!(local_path.is_file());

    let pruned = shared_blob_stores
        .uploader
        .prune_local_blobs()
        .expect("prune local blobs");

    assert_eq!(
        pruned,
        PrunedLocalBlobs {
            files: 1,
            bytes: bytes.len() as u64,
        }
    );
    assert!(!local_path.exists());
    assert_eq!(
        shared_blob_stores
            .object_store
            .get(&blob_object_key("test-prefix", &sha256)),
        Some(bytes.to_vec())
    );
}

#[rstest]
fn upload_is_created_in_its_blob_store(shared_blob_stores: SharedBlobStores) {
    let upload = shared_blob_stores.uploader.begin_upload().unwrap();

    assert_eq!(
        upload.temp_path.parent(),
        Some(shared_blob_stores.uploader_dir.path())
    );
}

#[rstest]
#[tokio::test]
async fn downloaded_shared_blob_must_match_its_digest(shared_blob_stores: SharedBlobStores) {
    let expected_bytes = b"expected template build input";
    let sha256 = hex::encode(Sha256::digest(expected_bytes));
    shared_blob_stores.insert_shared(&sha256, b"tampered template build input");

    let error = shared_blob_stores
        .downloader
        .ensure_local(&sha256)
        .await
        .unwrap_err();

    assert!(matches!(error, BlobError::DigestMismatch { expected, .. } if expected == sha256));
    assert!(
        !blob_path_in(shared_blob_stores.downloader_dir.path(), &sha256)
            .unwrap()
            .exists()
    );
}

#[rstest]
#[tokio::test]
async fn size_mismatched_shared_blob_is_repaired(shared_blob_stores: SharedBlobStores) {
    let bytes = b"expected template build input";
    let sha256 = hex::encode(Sha256::digest(bytes));
    shared_blob_stores.insert_shared(&sha256, b"short");
    let upload = shared_blob_stores.upload(bytes);

    let stored = shared_blob_stores
        .uploader
        .finish_upload(upload)
        .await
        .expect("mismatched shared blob should be replaced");

    assert_eq!(stored.sha256, sha256);
    assert_eq!(shared_blob_stores.object_store.put_count(), 1);
    assert_eq!(
        shared_blob_stores
            .object_store
            .get(&blob_object_key("test-prefix", &sha256)),
        Some(bytes.to_vec())
    );
}

#[rstest]
#[tokio::test]
async fn materializing_blob_removes_stale_download_temp(shared_blob_stores: SharedBlobStores) {
    let bytes = b"shared template build input";
    let sha256 = hex::encode(Sha256::digest(bytes));
    shared_blob_stores.insert_shared(&sha256, bytes);
    let stale_path = shared_blob_stores
        .downloader_dir
        .path()
        .join(".download-123-456.tmp");
    let stale_file = std::fs::File::create(&stale_path).unwrap();
    stale_file
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::SystemTime::now() - STALE_UPLOAD_TEMP_AGE),
        )
        .unwrap();

    shared_blob_stores
        .downloader
        .ensure_local(&sha256)
        .await
        .unwrap();

    assert!(!stale_path.exists());
}
