use super::*;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use pplx_space_object_store::ObjectCredentials;

#[derive(Debug, Default)]
struct RecordingPageCacheOperations {
    starts: Mutex<Vec<(u64, u64)>>,
    waits: Mutex<Vec<(u64, u64)>>,
    advice: Mutex<Vec<(u64, u64)>>,
    fail_start: AtomicBool,
}

impl RecordingPageCacheOperations {
    fn start_count(&self) -> usize {
        self.starts.lock().unwrap().len()
    }

    fn wait_count(&self) -> usize {
        self.waits.lock().unwrap().len()
    }
}

impl StreamPageCacheOperations for RecordingPageCacheOperations {
    fn start(&self, _file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()> {
        if self.fail_start.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("injected writeback start failure"));
        }
        self.starts
            .lock()
            .map_err(|_| std::io::Error::other("start recorder lock poisoned"))?
            .push((offset, length));
        Ok(())
    }

    fn wait(&self, _file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()> {
        self.waits
            .lock()
            .map_err(|_| std::io::Error::other("wait recorder lock poisoned"))?
            .push((offset, length));
        Ok(())
    }

    fn advise_dontneed(
        &self,
        _file: &std::fs::File,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()> {
        self.advice
            .lock()
            .map_err(|_| std::io::Error::other("advice recorder lock poisoned"))?
            .push((offset, length));
        Ok(())
    }
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..100 {
        if predicate() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("condition was not reached before timeout");
}

#[test]
fn common_object_parent_stays_within_one_artifact_directory() {
    let keys = [
        "clusters/a/suspend_snapshots/2a/sandbox/snapshot/memfile",
        "clusters/a/suspend_snapshots/2a/sandbox/snapshot/binfile",
        "clusters/a/suspend_snapshots/2a/sandbox/snapshot/rootfs",
    ];
    assert_eq!(
        common_object_parent(keys.into_iter()),
        Some("clusters/a/suspend_snapshots/2a/sandbox/snapshot".to_string())
    );

    let divergent = [
        "suspend_snapshots/2a/sandbox/snapshot-a/memfile",
        "templates/seed/sha256:abcd/rootfs.ext4.zst",
    ];
    assert_eq!(common_object_parent(divergent.into_iter()), None);
}

#[derive(Default)]
pub(super) struct MockObjectStore {
    heads: Mutex<VecDeque<Option<ObjectMeta>>>,
    body: Vec<u8>,
    head_calls: AtomicUsize,
    get_calls: AtomicUsize,
    put_calls: AtomicUsize,
}

impl MockObjectStore {
    pub(super) fn new(
        heads: impl IntoIterator<Item = Option<ObjectMeta>>,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            heads: Mutex::new(heads.into_iter().collect()),
            body: body.into(),
            head_calls: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
            put_calls: AtomicUsize::new(0),
        }
    }

    fn get_calls(&self) -> usize {
        self.get_calls.load(Ordering::SeqCst)
    }

    fn head_calls(&self) -> usize {
        self.head_calls.load(Ordering::SeqCst)
    }

    pub(super) fn put_calls(&self) -> usize {
        self.put_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ObjectStore for MockObjectStore {
    async fn head(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        self.head_calls.fetch_add(1, Ordering::SeqCst);
        let mut heads = self.heads.lock().expect("heads mutex");
        if heads.len() > 1 {
            return Ok(heads.pop_front().flatten());
        }
        Ok(heads.front().cloned().flatten())
    }

    async fn put_bytes(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
        _body: Bytes,
    ) -> Result<(), ObjectStoreError> {
        panic!("push tests do not use put_bytes")
    }

    async fn get_bytes(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        panic!("pull tests do not use get_bytes")
    }

    async fn put_file(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
        src: &Path,
    ) -> Result<u64, ObjectStoreError> {
        // Report a successful upload of the full source length, as a real
        // store does on the happy path. Whether the *durable* object then
        // matches is governed independently by the queued `head` responses,
        // so a test can simulate a put that "succeeds" yet leaves a
        // truncated object behind.
        self.put_calls.fetch_add(1, Ordering::SeqCst);
        let len = tokio::fs::metadata(src).await?.len();
        Ok(len)
    }

    async fn get_file(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
        dst: &Path,
    ) -> Result<u64, ObjectStoreError> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        tokio::fs::write(dst, &self.body).await?;
        Ok(self.body.len() as u64)
    }
}

/// Mock that exercises the streaming-decode path: feeds `body` through
/// the caller's `wrap_writer` in small chunks, the way the ranged
/// download's sequential writer would.
struct StreamingMockStore {
    inner: MockObjectStore,
}

#[async_trait::async_trait]
impl ObjectStore for StreamingMockStore {
    async fn head(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        self.inner.head(creds, target).await
    }

    async fn put_bytes(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        body: Bytes,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_bytes(creds, target, body).await
    }

    async fn get_bytes(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        self.inner.get_bytes(creds, target).await
    }

    async fn put_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        src: &Path,
    ) -> Result<u64, ObjectStoreError> {
        self.inner.put_file(creds, target, src).await
    }

    async fn get_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        dst: &Path,
    ) -> Result<u64, ObjectStoreError> {
        self.inner.get_file(creds, target, dst).await
    }

    async fn get_file_streamed(
        &self,
        _creds: &ObjectCredentials,
        _target: ObjectTarget<'_>,
        dst: &Path,
        stream: pplx_space_object_store::StreamDecode<'_>,
    ) -> Result<pplx_space_object_store::StreamedGetFile, ObjectStoreError> {
        let size = self.inner.body.len() as u64;
        // Held across the writes and the trailing finish, like the real
        // sequential writer owning the guard.
        let _guard = stream.admission.admit(size).await?;
        let file = std::fs::File::create(dst).map_err(|e| ObjectStoreError::Get(e.to_string()))?;
        let mut sink = (stream.wrap_writer)(file);
        for chunk in self.inner.body.chunks(3) {
            sink.write_all(chunk)
                .map_err(|e| ObjectStoreError::Get(e.to_string()))?;
        }
        let decoded = sink
            .finish()
            .map_err(|e| ObjectStoreError::Get(e.to_string()))?;
        Ok(pplx_space_object_store::StreamedGetFile::Streamed {
            bytes: size,
            decoded,
        })
    }
}

#[tokio::test]
async fn streamed_pull_decodes_inline_and_never_double_decodes() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("rootfs.ext4");
    // Nested-magic payload: the DECODED content itself starts with the
    // zstd magic. A wrong re-sniff after the streamed decode would
    // treat the output as compressed and decode it a second time.
    let mut payload = ZSTD_MAGIC.to_vec();
    payload.extend_from_slice(b"decoded payload that must survive exactly once");
    let body = zstd::stream::encode_all(&payload[..], ZSTD_LEVEL).unwrap();
    let compressed_len = body.len() as u64;

    let meta = ObjectMeta {
        size: compressed_len,
        etag: Some("\"stream\"".to_string()),
    };
    let store = Arc::new(StreamingMockStore {
        inner: MockObjectStore::new([Some(meta)], body),
    });
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            stream_decode_pulls: true,
            codec_concurrency: 2,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store,
    );

    let mut payload_spec = verified_payload(&local_path);
    payload_spec.raw = false;
    let out = executor
        .execute_pull(
            &test_job("pull"),
            &payload_spec,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::LegacySkipPresent,
        )
        .await
        .expect("streamed pull must succeed");

    assert_eq!(
        usize::try_from(out).expect("job size is non-negative"),
        payload.len(),
        "job reports decoded size"
    );
    assert_eq!(
        tokio::fs::read(&local_path).await.unwrap(),
        payload,
        "decoded exactly once despite nested magic"
    );
    assert_eq!(
        executor.codec_permits.available_permits(),
        2,
        "streamed decode permit must be released"
    );
    let residue: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".dec.tmp"))
        .collect();
    assert!(
        residue.is_empty(),
        "no legacy decompress scratch: {residue:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn memory_and_luks_roles_use_sparse_output_regardless_of_persisted_mode() {
    const MEMORY_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let mut expected = vec![0; MEMORY_BYTES];
    expected[31..34].copy_from_slice(b"ram");
    expected[MEMORY_BYTES / 2..MEMORY_BYTES / 2 + 4].copy_from_slice(b"live");
    let body = zstd::stream::encode_all(&expected[..], ZSTD_LEVEL).unwrap();
    for (artifact, stream_decode_pulls, persisted_mode) in [
        SnapshotArtifact::Memory,
        SnapshotArtifact::EncryptedSnapshotVolume,
        SnapshotArtifact::EncryptedRootfsCow,
    ]
    .into_iter()
    .flat_map(|artifact| {
        [true, false].into_iter().flat_map(move |stream| {
            [
                MaterializationMode::Disabled,
                MaterializationMode::Shadow,
                MaterializationMode::Enabled,
            ]
            .into_iter()
            .map(move |mode| (artifact, stream, mode))
        })
    }) {
        let local_path = dir.path().join(format!(
            "opaque-{artifact:?}-{stream_decode_pulls}-{persisted_mode}"
        ));
        let meta = ObjectMeta {
            size: body.len() as u64,
            etag: Some(format!("\"memory-{stream_decode_pulls}-{persisted_mode}\"")),
        };
        let store = Arc::new(StreamingMockStore {
            inner: MockObjectStore::new([Some(meta)], body.clone()),
        });
        let executor = TransferExecutor::new_for_test(
            TransferExecutorConfig {
                compress: false,
                stream_decode_pulls,
                codec_concurrency: 2,
                static_credentials: Some(test_creds()),
                ..Default::default()
            },
            store,
        );
        let payload = TransferPayload {
            subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
            local_path: local_path.to_string_lossy().into_owned(),
            s3_uri: "s3://bucket/snapshots/a/memfile?region=us-east-2".to_string(),
            transfer_scope_id: None,
            snapshot_artifact: Some(artifact),
            materialization_mode: persisted_mode,
            rootfs_materialization_mode: None,
            staged_path: None,
            raw: false,
            file_mode: None,
            existing_file_policy: PullExistingPolicy::LegacySkipPresent,
            encryption: None,
            artifact_id: Some("019e9000-0000-7000-8000-000000000001".to_string()),
            entry_role: Some("memfile".to_string()),
        };

        executor
            .execute_pull(
                &test_job("pull"),
                &payload,
                &test_creds(),
                test_target(),
                None,
                PullExistingPolicy::LegacySkipPresent,
            )
            .await
            .unwrap();

        assert_eq!(std::fs::read(&local_path).unwrap(), expected);
        let logical_bytes = std::fs::metadata(&local_path).unwrap().len();
        let physical_bytes = allocated_bytes(&local_path);
        assert!(
            physical_bytes < logical_bytes / 4,
            "{artifact:?} must use sparse output regardless of persisted mode: stream_decode={stream_decode_pulls} mode={persisted_mode} logical={logical_bytes} physical={physical_bytes}"
        );
    }
}

async fn run_streamed_artifact_pull(
    snapshot_artifact: SnapshotArtifact,
    body: Vec<u8>,
    mode: MaterializationMode,
) -> (tempfile::TempDir, PathBuf, i64) {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("opaque-restore-destination");
    let role = snapshot_artifact.file_name();
    let meta = ObjectMeta {
        size: body.len() as u64,
        etag: Some("\"streamed-artifact\"".to_string()),
    };
    let store = Arc::new(StreamingMockStore {
        inner: MockObjectStore::new([Some(meta.clone()), Some(meta)], body),
    });
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            stream_decode_pulls: true,
            codec_concurrency: 2,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store,
    );
    let mut payload = verified_payload(&local_path);
    payload.raw = false;
    payload.snapshot_artifact = Some(snapshot_artifact);
    payload.entry_role = Some(role.to_string());
    payload.materialization_mode = mode;
    payload.rootfs_materialization_mode =
        (snapshot_artifact == SnapshotArtifact::Rootfs).then_some(mode);
    let output_size = executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .expect("streamed artifact pull must succeed");
    (dir, local_path, output_size)
}

#[cfg(unix)]
#[tokio::test]
async fn plaintext_memory_role_uses_sparse_streaming_at_noncanonical_path() {
    const MEMORY_BYTES: usize = 8 * 1024 * 1024;
    let mut expected = vec![0; MEMORY_BYTES];
    expected[31..34].copy_from_slice(b"ram");
    expected[MEMORY_BYTES / 2..MEMORY_BYTES / 2 + 4].copy_from_slice(b"live");
    let body = zstd::stream::encode_all(&expected[..], ZSTD_LEVEL).unwrap();

    let (_dir, local_path, output_size) =
        run_streamed_artifact_pull(SnapshotArtifact::Memory, body, MaterializationMode::Enabled)
            .await;

    assert_eq!(output_size, MEMORY_BYTES as i64);
    assert_eq!(std::fs::read(&local_path).unwrap(), expected);
    let logical_bytes = std::fs::metadata(&local_path).unwrap().len();
    let physical_bytes = allocated_bytes(&local_path);
    assert!(
        physical_bytes < logical_bytes / 4,
        "memory must use sparse output: logical={logical_bytes} physical={physical_bytes}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn streamed_rootfs_ext4_uses_rootfs_sparse_block_size() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let mut expected = vec![0; IMAGE_BYTES];
    for block in expected[IMAGE_BYTES / 2..].chunks_mut(ROOTFS_SPARSE_BLOCK_BYTES) {
        block[0] = 1;
    }
    let body = zstd::stream::encode_all(expected.as_slice(), ZSTD_LEVEL).unwrap();

    let (_dir, local_path, output_size) =
        run_streamed_artifact_pull(SnapshotArtifact::Rootfs, body, MaterializationMode::Enabled)
            .await;

    assert_eq!(output_size, IMAGE_BYTES as i64);
    assert_eq!(std::fs::read(&local_path).unwrap(), expected);
    let observation = read_materialization_observation(&local_path)
        .expect("streamed rootfs must persist materialization provenance");
    assert_eq!(observation.mode, MaterializationMode::Enabled);
    assert_eq!(observation.layout, MaterializationLayout::Sparse);
    assert_eq!(
        observation.candidate_hole_bytes,
        (IMAGE_BYTES / 2) as u64,
        "64 KiB rootfs classification must retain blocks containing any live byte"
    );
    assert!(
        allocated_bytes(&local_path) < IMAGE_BYTES as u64 * 3 / 4,
        "streamed ext4 output must retain its fully zero half as holes"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn streamed_rootfs_ext4_preserves_non_aligned_tail() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024 + 113;
    let mut expected = vec![0; IMAGE_BYTES];
    expected[IMAGE_BYTES - 113..].copy_from_slice(&[0x5a; 113]);
    let body = zstd::stream::encode_all(expected.as_slice(), ZSTD_LEVEL).unwrap();

    let (_dir, local_path, output_size) =
        run_streamed_artifact_pull(SnapshotArtifact::Rootfs, body, MaterializationMode::Enabled)
            .await;

    assert_eq!(output_size, IMAGE_BYTES as i64);
    assert_eq!(
        std::fs::read(&local_path).unwrap(),
        expected,
        "stream completion must finalize a partial sparse classification block"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn streamed_rootfs_btrfs_send_remains_dense() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let expected = vec![0; IMAGE_BYTES];
    let body = zstd::stream::encode_all(expected.as_slice(), ZSTD_LEVEL).unwrap();

    let (_dir, local_path, _) = run_streamed_artifact_pull(
        SnapshotArtifact::RootfsBtrfsSend,
        body,
        MaterializationMode::Enabled,
    )
    .await;

    assert_eq!(std::fs::read(&local_path).unwrap(), expected);
    assert!(
        allocated_bytes(&local_path) > IMAGE_BYTES as u64 / 2,
        "btrfs send streams must never be sparsified"
    );
    let observation = read_materialization_observation(&local_path)
        .expect("decoded dense output must persist materialization provenance");
    assert_eq!(observation.mode, MaterializationMode::Disabled);
    assert_eq!(observation.layout, MaterializationLayout::Dense);
    assert_eq!(observation.candidate_hole_bytes, 0);
}

#[test]
fn stream_decode_sink_rejects_truncated_frame() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("decoded.bin");
    let encoded = zstd::stream::encode_all(
        &b"payload that will be cut short".repeat(64)[..],
        ZSTD_LEVEL,
    )
    .unwrap();
    let truncated = &encoded[..encoded.len() - 7];

    let file = std::fs::File::create(&out).unwrap();
    let mut sink: Box<dyn pplx_space_object_store::FinishingWrite> =
        Box::new(StreamDecodeSink::new(
            file,
            MaterializationMode::Disabled,
            MEMORY_SPARSE_BLOCK_BYTES,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            StreamWritebackCoordinator::new(DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES, None),
            Arc::new(StreamPullTiming::default()),
            JobPriority::default(),
        ));
    sink.write_all(truncated).unwrap();
    let err = sink
        .finish()
        .expect_err("truncated frame must not finalize");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::UnexpectedEof,
        "truncation surfaces as UnexpectedEof: {err}"
    );
}

#[tokio::test]
async fn streamed_pull_fails_on_truncated_frame() {
    // The two-pass decode rejected truncated frames at the read-back
    // pass; the streamed path must reject them at finish instead of
    // committing partial decoded output as a success.
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("rootfs.ext4");
    let encoded =
        zstd::stream::encode_all(&b"will be truncated".repeat(128)[..], ZSTD_LEVEL).unwrap();
    let truncated = encoded[..encoded.len() - 5].to_vec();
    let meta = ObjectMeta {
        size: truncated.len() as u64,
        etag: Some("\"trunc\"".to_string()),
    };
    let store = Arc::new(StreamingMockStore {
        inner: MockObjectStore::new([Some(meta)], truncated),
    });
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            stream_decode_pulls: true,
            codec_concurrency: 2,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store,
    );

    let mut payload_spec = verified_payload(&local_path);
    payload_spec.raw = false;
    let err = executor
        .execute_pull(
            &test_job("pull"),
            &payload_spec,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::LegacySkipPresent,
        )
        .await
        .expect_err("truncated frame must fail the pull");
    assert!(
        err.message().contains("incomplete frame"),
        "failure names the truncation: {}",
        err.message()
    );
    assert!(
        !local_path.exists(),
        "no artifact may be committed from a truncated frame"
    );
}

#[tokio::test]
async fn raw_pull_bypasses_streaming_and_keeps_wire_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("rootfs.ext4");
    let payload = b"plain payload".to_vec();
    let body = zstd::stream::encode_all(&payload[..], ZSTD_LEVEL).unwrap();
    let meta = ObjectMeta {
        size: body.len() as u64,
        etag: Some("\"raw\"".to_string()),
    };
    let store = Arc::new(StreamingMockStore {
        inner: MockObjectStore::new([Some(meta)], body.clone()),
    });
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            stream_decode_pulls: true,
            codec_concurrency: 2,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store.clone(),
    );

    let mut payload_spec = verified_payload(&local_path);
    payload_spec.raw = true;
    executor
        .execute_pull(
            &test_job("pull"),
            &payload_spec,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::LegacySkipPresent,
        )
        .await
        .expect("raw pull must succeed");

    assert_eq!(
        tokio::fs::read(&local_path).await.unwrap(),
        body,
        "raw pulls must land wire bytes untouched"
    );
    assert_eq!(store.inner.get_calls(), 1, "raw pulls use the plain path");
}

pub(super) fn test_creds() -> ObjectCredentials {
    ObjectCredentials {
        access_key_id: "akid".to_string(),
        secret_access_key: "secret".to_string(),
        session_token: None,
    }
}

#[test]
fn transfer_phase_vocab_includes_shadow_reflink() {
    assert_eq!(TransferPhase::DownloadDecode.as_str(), "download_decode");
    assert_eq!(TransferPhase::Decompress.as_str(), "decompress");
    assert_eq!(TransferPhase::ShadowReflink.as_str(), "shadow_reflink");
}

#[test]
fn object_store_failure_classification_preserves_domain_reason() {
    let cases = [
        (
            ObjectStoreError::NotFound {
                key: "template/rootfs".to_string(),
            },
            JobFailureReason::NotFound,
        ),
        (
            ObjectStoreError::PermissionDenied {
                message: "denied".to_string(),
            },
            JobFailureReason::PermissionDenied,
        ),
        (
            ObjectStoreError::Io(std::io::Error::from(std::io::ErrorKind::NotFound)),
            JobFailureReason::Internal,
        ),
    ];

    for (error, expected_reason) in cases {
        let classified = classify(&error);
        assert_eq!(classified.kind(), JobFailureKind::Permanent);
        assert_eq!(classified.failure_reason(), Some(expected_reason));
    }

    assert_eq!(
        classify(&ObjectStoreError::Get("network reset".to_string())).kind(),
        JobFailureKind::Retriable
    );
}

#[test]
fn pull_failure_boundary_types_unclassified_permanent_errors() {
    let classified = ensure_pull_failure_reason(JobError::Permanent("local I/O".to_string()));
    assert_eq!(classified.kind(), JobFailureKind::Permanent);
    assert_eq!(
        classified.failure_reason(),
        Some(JobFailureReason::Internal)
    );

    let missing = ensure_pull_failure_reason(JobError::permanent_with_reason(
        JobFailureReason::NotFound,
        "missing",
    ));
    assert_eq!(missing.failure_reason(), Some(JobFailureReason::NotFound));

    let retriable = ensure_pull_failure_reason(JobError::Retriable("timeout".to_string()));
    assert_eq!(retriable.kind(), JobFailureKind::Retriable);
    assert_eq!(retriable.failure_reason(), None);
}

#[test]
fn transfer_artifact_tag_classifies_snapshot_restore_paths() {
    // arrange
    let cases = [
        ("/space/cache/sbx/snapshot/memory", "memory"),
        (
            "/space/cache/sbx/snapshot/.memory.restore-019f24f6-c250-7bf1-8c63-6ffdaefad5e0",
            "memory",
        ),
        (
            "/space/cache/sbx/snapshot/rootfs.btrfs-send",
            "rootfs_btrfs_send",
        ),
        (
            "/space/cache/sbx/snapshot/.rootfs.btrfs-send.restore-019f24f8-42d0-71a3-bcf3-79543b2e4d4d",
            "rootfs_btrfs_send",
        ),
        ("/space/cache/sbx/snapshot/snapshot.json", "metadata"),
        ("/space/cache/sbx/snapshot/cow.luks", "encrypted_rootfs_cow"),
        ("/space/cache/other.bin", "other"),
    ];

    for (path, expected) in cases {
        // act
        let actual = transfer_artifact_tag(snapshot_artifact_from_local_path(Path::new(path)));

        // assert
        assert_eq!(actual, expected);
    }
}

#[test]
fn sparse_decompression_policy_matches_memory_and_rootfs_ext4() {
    assert_eq!(
        effective_materialization_mode(
            Some(SnapshotArtifact::Memory),
            MaterializationMode::Enabled,
        ),
        MaterializationMode::Enabled
    );
    assert_eq!(
        effective_materialization_mode(Some(SnapshotArtifact::Memory), MaterializationMode::Shadow,),
        MaterializationMode::Shadow
    );
    assert_eq!(
        effective_materialization_mode(None, MaterializationMode::Enabled),
        MaterializationMode::Disabled
    );
    assert_eq!(
        effective_materialization_mode(Some(SnapshotArtifact::Rootfs), MaterializationMode::Shadow,),
        MaterializationMode::Shadow
    );
    assert_eq!(
        effective_materialization_mode(
            snapshot_artifact_from_local_path(Path::new(
                "/cache/.memory.restore-019f24f6-c250-7bf1-8c63-6ffdaefad5e0"
            )),
            MaterializationMode::Enabled,
        ),
        MaterializationMode::Enabled
    );
    for artifact in [
        None,
        Some(SnapshotArtifact::RootfsBtrfsSend),
        Some(SnapshotArtifact::VmState),
    ] {
        assert_eq!(
            effective_materialization_mode(artifact, MaterializationMode::Enabled),
            MaterializationMode::Disabled
        );
    }
}

#[test]
fn execution_materialization_policy_enables_memory_and_preserves_rootfs_assignment() {
    for persisted_mode in [
        MaterializationMode::Disabled,
        MaterializationMode::Shadow,
        MaterializationMode::Enabled,
    ] {
        assert_eq!(
            execution_materialization_mode(Some(SnapshotArtifact::Memory), persisted_mode),
            MaterializationMode::Enabled,
            "memory must ignore legacy persisted modes"
        );
    }
    assert_eq!(
        execution_materialization_mode(Some(SnapshotArtifact::Rootfs), MaterializationMode::Shadow,),
        MaterializationMode::Shadow
    );
    assert_eq!(
        execution_materialization_mode(
            Some(SnapshotArtifact::RootfsBtrfsSend),
            MaterializationMode::Enabled,
        ),
        MaterializationMode::Disabled,
        "non-ext4 rootfs artifacts must stay dense"
    );
}

#[test]
fn legacy_enabled_rootfs_payload_stays_dense() {
    let payload = TransferPayload::from_json(
        r#"{
            "subject_id":"sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2",
            "local_path":"/space/snap/rootfs",
            "s3_uri":"s3://bucket/snapshots/a/rootfs?region=us-east-2",
            "snapshot_artifact":"rootfs",
            "materialization_mode":"enabled"
        }"#,
    )
    .expect("payload written before rootfs rollout must remain decodable");

    assert_eq!(
        execution_materialization_mode(
            payload.effective_snapshot_artifact(),
            payload.requested_materialization_mode(payload.effective_snapshot_artifact()),
        ),
        MaterializationMode::Disabled,
        "a memory-era assignment must not opt a recovered rootfs into sparse writes"
    );
}

#[test]
fn rootfs_payload_requires_a_scoped_assignment() {
    let mut payload = TransferPayload::from_json(
        r#"{
            "subject_id":"sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2",
            "local_path":"/space/snap/rootfs",
            "s3_uri":"s3://bucket/snapshots/a/rootfs?region=us-east-2",
            "snapshot_artifact":"rootfs",
            "materialization_mode":"enabled",
            "rootfs_materialization_mode":"enabled"
        }"#,
    )
    .expect("new rootfs payload must decode");
    let artifact = payload.effective_snapshot_artifact();
    assert_eq!(
        payload.requested_materialization_mode(artifact),
        MaterializationMode::Enabled
    );

    payload.snapshot_artifact = Some(SnapshotArtifact::Memory);
    payload.rootfs_materialization_mode = None;
    assert_eq!(
        payload.requested_materialization_mode(payload.effective_snapshot_artifact()),
        MaterializationMode::Enabled,
        "memory graduation must continue to use the existing assignment field"
    );
}

pub(super) fn test_target() -> ObjectTarget<'static> {
    ObjectTarget {
        bucket: "bucket",
        key: "snapshots/a/snapshot.luks",
        region: "us-east-2",
        endpoint: None,
        path_style: false,
    }
}

fn test_executor(store: Arc<dyn ObjectStore>) -> TransferExecutor {
    TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            codec_concurrency: 0,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store,
    )
}

pub(super) fn test_job(kind: &str) -> Job {
    Job {
        job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
        kind: kind.to_string(),
        status: "running".to_string(),
        payload: "{}".to_string(),
        subject_id: None,
        dedup_key: None,
        failure: None,
        attempts: 0,
        max_attempts: 1,
        bytes_processed: 0,
        error: None,
        created_at: String::new(),
        started_at: None,
        completed_at: None,
        traceparent: None,
        lease_owner: None,
        priority: Default::default(),
    }
}

#[derive(Clone, Default)]
struct CapturedFields(std::sync::Arc<Mutex<Vec<(String, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedFields {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Visitor<'a>(&'a Mutex<Vec<(String, String)>>);

        impl tracing::field::Visit for Visitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0
                    .lock()
                    .expect("captured fields lock poisoned")
                    .push((field.name().to_string(), format!("{value:?}")));
            }
        }

        event.record(&mut Visitor(&self.0));
    }
}

fn capture_tracing_fields(emit: impl FnOnce()) -> Vec<(String, String)> {
    use tracing_subscriber::layer::SubscriberExt;

    let captured = CapturedFields::default();
    let subscriber = tracing_subscriber::registry().with(captured.clone());
    tracing::subscriber::with_default(subscriber, emit);
    captured
        .0
        .lock()
        .expect("captured fields lock poisoned")
        .clone()
}

#[test]
fn streamed_pull_timing_log_omits_sensitive_paths() {
    let sensitive_uri = "s3://bucket/private/customer-file?region=us-east-1";
    let sensitive_path = "/space/cache/private/customer-file";
    let mut payload = verified_payload(Path::new(sensitive_path));
    payload.s3_uri = sensitive_uri.to_string();

    let fields = capture_tracing_fields(|| {
        log_streamed_pull_pipeline_timing(
            &test_job("pull"),
            &payload,
            &StreamPullTiming::default(),
            true,
            42,
        );
    });

    assert!(
        fields.iter().all(|(_, value)| {
            !value.contains(sensitive_uri) && !value.contains(sensitive_path)
        }),
        "timing log leaked a caller-controlled URI or local path: {fields:?}"
    );
    assert!(
        fields.iter().any(|(name, _)| name == "s3_uri_len")
            && fields.iter().any(|(name, _)| name == "local_path_len"),
        "timing log must retain bounded diagnostic metadata: {fields:?}"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn sparse_shadow_advice_warning_omits_sensitive_path() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let sensitive_path = tempdir.path().join("private-customer-content-id");
    let file = std::fs::File::create(&sensitive_path).expect("create advice input");

    let fields = capture_tracing_fields(|| {
        advise_shadow_read_range(
            &file,
            &sensitive_path,
            u64::MAX,
            4096,
            "dense sparse-shadow comparison input",
        );
    });

    let sensitive_path = sensitive_path.to_string_lossy();
    assert!(
        fields
            .iter()
            .all(|(_, value)| !value.contains(sensitive_path.as_ref())),
        "advice warning leaked a caller-controlled local path: {fields:?}"
    );
    assert!(
        fields.iter().any(|(name, _)| name == "path_len"),
        "advice warning must retain bounded path metadata: {fields:?}"
    );
}

fn verified_payload(local_path: &Path) -> TransferPayload {
    TransferPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        local_path: local_path.to_string_lossy().into_owned(),
        s3_uri: "s3://bucket/snapshots/a/snapshot.luks?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: Some(SnapshotArtifact::EncryptedSnapshotVolume),
        staged_path: None,
        raw: true,
        file_mode: None,
        existing_file_policy: PullExistingPolicy::VerifiedReplica,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    }
}

pub(super) fn push_payload(local_path: &Path) -> TransferPayload {
    TransferPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        local_path: local_path.to_string_lossy().into_owned(),
        s3_uri: "s3://bucket/snapshots/a/memfile?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: Some(SnapshotArtifact::Memory),
        staged_path: None,
        raw: true,
        file_mode: None,
        // Irrelevant on the push path; required by the struct.
        existing_file_policy: PullExistingPolicy::VerifiedReplica,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    }
}

// A `put_file` that returns `Ok` is not proof the object is durable: a
// partial / truncated multipart upload can complete with `Ok` (e.g. a
// completion that races an S3 `RequestTimeout`). The push path must HEAD
// the durable object and refuse to report success when it is short of the
// wire bytes we uploaded — otherwise the snapshot is finalized `ready` and
// the restore marker advances to a corrupt artifact.
#[tokio::test]
async fn push_rejects_truncated_durable_object() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("memfile");
    tokio::fs::write(&local_path, vec![7u8; 4096])
        .await
        .unwrap();

    let payload = push_payload(&local_path);
    // put_file "succeeds", but the durable object comes back short.
    let truncated = ObjectMeta {
        size: 10,
        etag: None,
    };
    let store = Arc::new(MockObjectStore::new([Some(truncated)], Vec::new()));
    let executor = test_executor(store.clone());

    let result = executor
        .execute_push_inner(
            &test_job("push"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            CancellationToken::new(),
        )
        .await;

    let err = result.expect_err("truncated upload must not report success");
    assert!(matches!(err, JobError::Retriable(_)), "got {err:?}");
    assert_eq!(store.put_calls(), 1, "object must have been put");
    assert_eq!(
        store.head_calls(),
        1,
        "durable object must be HEADed after put"
    );
}

#[tokio::test]
async fn push_accepts_fully_durable_object() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("memfile");
    tokio::fs::write(&local_path, vec![7u8; 4096])
        .await
        .unwrap();

    let payload = push_payload(&local_path);
    // Durable size matches the wire bytes — a complete upload.
    let full = ObjectMeta {
        size: 4096,
        etag: None,
    };
    let store = Arc::new(MockObjectStore::new([Some(full)], Vec::new()));
    let executor = test_executor(store.clone());

    let bytes = executor
        .execute_push_inner(
            &test_job("push"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            CancellationToken::new(),
        )
        .await
        .expect("a fully durable object must finalize successfully");

    // Reported bytes are the (uncompressed) source size, unchanged by the
    // durability check.
    assert_eq!(bytes, 4096);
}

async fn write_test_provenance(
    payload: &TransferPayload,
    object: &ObjectMeta,
    local_path: &Path,
    local_size: u64,
) {
    let provenance = FileReplicaProvenance::new(payload, object, local_size, None);
    tokio::fs::write(
        verified_replica_provenance_path(local_path),
        serde_json::to_vec(&provenance).unwrap(),
    )
    .await
    .unwrap();
}

#[test]
fn partial_path_appends_suffix_preserving_extensions() {
    // We deliberately *append* `.partial` rather than replace the
    // extension so a sibling `foo.ext4` and `foo.ext4.zst` can each
    // have a distinct `.partial` file.
    assert_eq!(
        partial_path(Path::new("/space/cache/foo.zst")),
        Path::new("/space/cache/foo.zst.partial"),
    );
    assert_eq!(
        partial_path(Path::new("/space/cache/rootfs.ext4")),
        Path::new("/space/cache/rootfs.ext4.partial"),
    );
    assert_eq!(
        partial_path(Path::new("/space/cache/rootfs.ext4.zst")),
        Path::new("/space/cache/rootfs.ext4.zst.partial"),
    );
    assert_eq!(
        partial_path(Path::new("relative/file")),
        Path::new("relative/file.partial"),
    );
}

#[tokio::test]
async fn cancelled_pull_cleanup_preserves_committed_replica() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("snapshot.luks");
    let partial = partial_path(&local_path);
    let provenance = verified_replica_provenance_path(&local_path);
    let encrypted = encrypted_sibling(&local_path);
    let decrypted = decrypted_sibling(&local_path);

    tokio::fs::write(&local_path, b"committed").await.unwrap();
    tokio::fs::write(&provenance, b"provenance").await.unwrap();
    tokio::fs::write(&partial, b"partial").await.unwrap();
    tokio::fs::write(&encrypted, b"encrypted").await.unwrap();
    tokio::fs::write(&decrypted, b"decrypted").await.unwrap();

    cleanup_cancelled_pull_temp_paths(&local_path)
        .await
        .unwrap();

    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), b"committed");
    assert_eq!(tokio::fs::read(&provenance).await.unwrap(), b"provenance");
    assert!(!partial.exists());
    assert!(!encrypted.exists());
    assert!(!decrypted.exists());
}

#[test]
fn replica_provenance_path_is_hidden_sibling() {
    assert_eq!(
        verified_replica_provenance_path(Path::new("/space/snap/snapshot.luks")),
        Path::new("/space/snap/.snapshot.luks.volmgr-replica.json"),
    );
}

#[test]
fn materialization_provenance_preserves_shared_replica_contract() {
    let payload = verified_payload(Path::new("/space/snap/memory"));
    let object = ObjectMeta {
        size: 101,
        etag: Some("\"etag-a\"".to_string()),
    };
    let materialization: MaterializationObservation = serde_json::from_value(serde_json::json!({
        "mode": "shadow",
        "layout": "dense",
        "logical_bytes": 4096,
        "allocated_bytes": 4096,
        "candidate_hole_bytes": 3072,
    }))
    .unwrap();
    let provenance = FileReplicaProvenance::new(
        &payload,
        &object,
        materialization.logical_bytes,
        Some(materialization.clone()),
    );

    let bytes = serde_json::to_vec(&provenance).unwrap();
    let shared: VerifiedReplicaProvenance = serde_json::from_slice(&bytes).unwrap();
    let persisted: FileReplicaProvenance = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        shared,
        file_replica_provenance(&payload, &object, materialization.logical_bytes)
    );
    assert_eq!(persisted.materialization, Some(materialization));
}

#[test]
fn verified_replica_materialization_compatibility_requires_requested_work() {
    let sparse = MaterializationObservation {
        mode: MaterializationMode::Enabled,
        layout: MaterializationLayout::Sparse,
        logical_bytes: 4096,
        allocated_bytes: 1024,
        candidate_hole_bytes: 3072,
    };
    let shadow = MaterializationObservation {
        mode: MaterializationMode::Shadow,
        layout: MaterializationLayout::Dense,
        logical_bytes: 4096,
        allocated_bytes: 4096,
        candidate_hole_bytes: 3072,
    };

    assert!(materialization_is_compatible(
        MaterializationMode::Disabled,
        None
    ));
    assert!(materialization_is_compatible(
        MaterializationMode::Disabled,
        Some(&sparse)
    ));
    assert!(!materialization_is_compatible(
        MaterializationMode::Enabled,
        None
    ));
    assert!(materialization_is_compatible(
        MaterializationMode::Enabled,
        Some(&sparse)
    ));
    assert!(!materialization_is_compatible(
        MaterializationMode::Enabled,
        Some(&shadow)
    ));
    assert!(!materialization_is_compatible(
        MaterializationMode::Shadow,
        Some(&shadow)
    ));
    assert!(!materialization_is_compatible(
        MaterializationMode::Shadow,
        Some(&sparse)
    ));
}

#[test]
fn file_replica_provenance_matches_only_same_transfer_contract() {
    let base = TransferPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        local_path: "/space/snap/opaque-download".to_string(),
        s3_uri: "s3://bucket/snapshots/a/snapshot.luks?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: Some(SnapshotArtifact::EncryptedSnapshotVolume),
        staged_path: None,
        raw: true,
        file_mode: Some(FileMode::new(0o600).unwrap()),
        existing_file_policy: PullExistingPolicy::VerifiedReplica,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    };
    let object = ObjectMeta {
        size: 17,
        etag: Some("\"etag-a\"".to_string()),
    };
    let provenance = file_replica_provenance(&base, &object, 17);

    assert_eq!(provenance, file_replica_provenance(&base, &object, 17));

    let mut changed_s3 = base.clone();
    changed_s3.s3_uri = "s3://bucket/snapshots/b/snapshot.luks?region=us-east-2".to_string();
    assert_ne!(
        provenance,
        file_replica_provenance(&changed_s3, &object, 17)
    );

    let mut changed_raw = base.clone();
    changed_raw.raw = false;
    assert_ne!(
        provenance,
        file_replica_provenance(&changed_raw, &object, 17)
    );

    let mut changed_mode = base.clone();
    changed_mode.file_mode = Some(FileMode::new(0o644).unwrap());
    assert_ne!(
        provenance,
        file_replica_provenance(&changed_mode, &object, 17)
    );

    let mut changed_artifact = base.clone();
    changed_artifact.snapshot_artifact = Some(SnapshotArtifact::Memory);
    assert_ne!(
        provenance,
        file_replica_provenance(&changed_artifact, &object, 17),
        "changing materialization semantics must invalidate a verified replica"
    );

    let mut unspecified_artifact = base.clone();
    unspecified_artifact.snapshot_artifact = None;
    assert_ne!(
        provenance,
        file_replica_provenance(&unspecified_artifact, &object, 17),
        "legacy provenance must not satisfy a newly explicit artifact request"
    );

    let changed_object = ObjectMeta {
        size: 18,
        etag: object.etag.clone(),
    };
    assert_ne!(
        provenance,
        file_replica_provenance(&base, &changed_object, 17)
    );

    let changed_etag = ObjectMeta {
        size: object.size,
        etag: Some("\"etag-b\"".to_string()),
    };
    assert_ne!(
        provenance,
        file_replica_provenance(&base, &changed_etag, 17)
    );

    assert_ne!(provenance, file_replica_provenance(&base, &object, 18));
}

#[tokio::test]
async fn verified_replica_with_valid_provenance_skips_download() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("snapshot.luks");
    tokio::fs::write(&local_path, b"replica").await.unwrap();

    let payload = verified_payload(&local_path);
    let object = ObjectMeta {
        size: 7,
        etag: Some("\"etag-a\"".to_string()),
    };
    write_test_provenance(&payload, &object, &local_path, 7).await;
    let store = Arc::new(MockObjectStore::new([Some(object)], b"new-replica"));
    let executor = test_executor(store.clone());

    let bytes = executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .unwrap();

    assert_eq!(bytes, 7);
    assert_eq!(store.get_calls(), 0);
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), b"replica");
}

#[tokio::test]
async fn verified_replica_does_not_reuse_unconfirmed_shadow_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("memory");
    let unconfirmed = b"unconfirmed-shadow";
    tokio::fs::write(&local_path, unconfirmed).await.unwrap();

    let mut payload = verified_payload(&local_path);
    payload.snapshot_artifact = Some(SnapshotArtifact::Memory);
    payload.materialization_mode = MaterializationMode::Shadow;
    let body = b"fresh-memory".to_vec();
    let object = ObjectMeta {
        size: body.len() as u64,
        etag: Some("\"memory-etag\"".to_string()),
    };
    let shadow_observation = MaterializationObservation {
        mode: MaterializationMode::Shadow,
        layout: MaterializationLayout::Dense,
        logical_bytes: unconfirmed.len() as u64,
        allocated_bytes: unconfirmed.len() as u64,
        candidate_hole_bytes: 0,
    };
    let provenance = FileReplicaProvenance::new(
        &payload,
        &object,
        unconfirmed.len() as u64,
        Some(shadow_observation),
    );
    tokio::fs::write(
        verified_replica_provenance_path(&local_path),
        serde_json::to_vec(&provenance).unwrap(),
    )
    .await
    .unwrap();

    let store = Arc::new(MockObjectStore::new(
        [Some(object.clone()), Some(object)],
        body.clone(),
    ));
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store.clone(),
    );

    executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .unwrap();

    assert_eq!(
        store.get_calls(),
        1,
        "shadow provenance without a durable successful comparison must be redownloaded"
    );
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), body);
}

#[cfg(unix)]
#[tokio::test]
async fn artifact_pull_reuses_sparse_memory_across_persisted_modes() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("memory");
    let mut expected = vec![0u8; IMAGE_BYTES];
    expected[..4096].fill(0x31);
    expected[IMAGE_BYTES - 4096..].fill(0x79);
    let body = zstd::stream::encode_all(&expected[..], ZSTD_LEVEL).unwrap();
    let object = ObjectMeta {
        size: body.len() as u64,
        etag: Some("\"memory-etag\"".to_string()),
    };
    let store = Arc::new(MockObjectStore::new([Some(object)], body));
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            codec_concurrency: 2,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store.clone(),
    );
    let mut payload = verified_payload(&local_path);
    payload.raw = false;
    payload.snapshot_artifact = Some(SnapshotArtifact::Memory);
    payload.existing_file_policy = artifact_pull_existing_policy();

    executor
        .execute_pull(
            &test_job("pull_artifact"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            artifact_pull_existing_policy(),
        )
        .await
        .unwrap();
    assert_eq!(store.get_calls(), 1);
    assert!(
        allocated_bytes(&local_path) < expected.len() as u64 / 4,
        "persisted disabled mode must still materialize sparse memory"
    );

    payload.materialization_mode = MaterializationMode::Shadow;
    executor
        .execute_pull(
            &test_job("pull_artifact"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            artifact_pull_existing_policy(),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_calls(),
        1,
        "persisted shadow mode must reuse the completed sparse memory file"
    );
    assert!(
        allocated_bytes(&local_path) < expected.len() as u64 / 4,
        "persisted shadow mode must retain sparse memory"
    );

    payload.materialization_mode = MaterializationMode::Enabled;
    executor
        .execute_pull(
            &test_job("pull_artifact"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            artifact_pull_existing_policy(),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_calls(),
        1,
        "persisted enabled mode must reuse the completed sparse memory file"
    );
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), expected);
    assert!(
        allocated_bytes(&local_path) < expected.len() as u64 / 4,
        "persisted enabled mode must retain sparse memory"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn enabled_memory_rematerializes_matching_dense_verified_replica() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("memory");
    let mut expected = vec![0u8; 8 * 1024 * 1024];
    expected[..4096].fill(0x31);
    let tail_start = expected.len() - 4096;
    expected[tail_start..].fill(0x79);
    tokio::fs::write(&local_path, &expected).await.unwrap();

    let mut payload = verified_payload(&local_path);
    payload.raw = false;
    payload.snapshot_artifact = Some(SnapshotArtifact::Memory);
    payload.materialization_mode = MaterializationMode::Enabled;
    let body = zstd::stream::encode_all(&expected[..], ZSTD_LEVEL).unwrap();
    let object = ObjectMeta {
        size: body.len() as u64,
        etag: Some("\"memory-etag\"".to_string()),
    };
    let dense_observation = MaterializationObservation {
        mode: MaterializationMode::Disabled,
        layout: MaterializationLayout::Dense,
        logical_bytes: expected.len() as u64,
        allocated_bytes: expected.len() as u64,
        candidate_hole_bytes: 0,
    };
    let provenance = FileReplicaProvenance::new(
        &payload,
        &object,
        expected.len() as u64,
        Some(dense_observation),
    );
    tokio::fs::write(
        verified_replica_provenance_path(&local_path),
        serde_json::to_vec(&provenance).unwrap(),
    )
    .await
    .unwrap();

    let store = Arc::new(MockObjectStore::new(
        [Some(object.clone()), Some(object)],
        body,
    ));
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            codec_concurrency: 2,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store.clone(),
    );

    executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .unwrap();

    assert_eq!(
        store.get_calls(),
        1,
        "enabled mode must not reuse a dense verified replica"
    );
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), expected);
    assert!(
        allocated_bytes(&local_path) < expected.len() as u64 / 4,
        "enabled mode must rematerialize sparse output"
    );
}

#[tokio::test]
async fn unspecified_canonical_pull_reuses_legacy_verified_replica() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("snapshot.luks");
    tokio::fs::write(&local_path, b"replica").await.unwrap();

    let mut payload = verified_payload(&local_path);
    payload.snapshot_artifact = None;
    let object = ObjectMeta {
        size: 7,
        etag: Some("\"etag-a\"".to_string()),
    };
    let legacy_provenance = VerifiedReplicaProvenance {
        version: VERIFIED_REPLICA_PROVENANCE_VERSION,
        subject_id: payload.subject_id.clone(),
        s3_uri: payload.s3_uri.clone(),
        raw: payload.raw,
        file_mode: None,
        snapshot_artifact: None,
        object_size: object.size,
        object_etag: object.etag.clone(),
        local_size: 7,
    };
    tokio::fs::write(
        verified_replica_provenance_path(&local_path),
        serde_json::to_vec(&legacy_provenance).unwrap(),
    )
    .await
    .unwrap();

    let store = Arc::new(MockObjectStore::new([Some(object)], b"new-replica"));
    let executor = test_executor(store.clone());

    let bytes = executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .unwrap();

    assert_eq!(bytes, 7);
    assert_eq!(store.get_calls(), 0, "legacy replica must be reused");
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), b"replica");
}

#[tokio::test]
async fn verified_replica_with_mismatched_provenance_redownloads() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("snapshot.luks");
    tokio::fs::write(&local_path, b"old").await.unwrap();

    let payload = verified_payload(&local_path);
    let stale_object = ObjectMeta {
        size: 3,
        etag: Some("\"old-etag\"".to_string()),
    };
    write_test_provenance(&payload, &stale_object, &local_path, 3).await;

    let body = b"new-bytes";
    let current_object = ObjectMeta {
        size: body.len() as u64,
        etag: Some("\"new-etag\"".to_string()),
    };
    let store = Arc::new(MockObjectStore::new([Some(current_object.clone())], body));
    let executor = test_executor(store.clone());

    let bytes = executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .unwrap();

    assert_eq!(bytes, body.len() as i64);
    assert_eq!(store.get_calls(), 1);
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), body);

    let persisted = tokio::fs::read(verified_replica_provenance_path(&local_path))
        .await
        .unwrap();
    let provenance: VerifiedReplicaProvenance = serde_json::from_slice(&persisted).unwrap();
    assert_eq!(
        provenance,
        file_replica_provenance(&payload, &current_object, body.len() as u64)
    );
}

#[tokio::test]
async fn explicit_memory_rematerializes_legacy_opaque_verified_replica() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("opaque-download");
    let mut expected = vec![0u8; 8 * 1024 * 1024];
    expected[..4096].fill(0x31);
    let tail_start = expected.len() - 4096;
    expected[tail_start..].fill(0x79);
    tokio::fs::write(&local_path, &expected).await.unwrap();

    let mut payload = verified_payload(&local_path);
    payload.raw = false;
    payload.snapshot_artifact = Some(SnapshotArtifact::Memory);
    payload.materialization_mode = MaterializationMode::Enabled;
    let mut legacy_payload = payload.clone();
    legacy_payload.snapshot_artifact = None;
    let body = zstd::stream::encode_all(&expected[..], ZSTD_LEVEL).unwrap();
    let object = ObjectMeta {
        size: body.len() as u64,
        etag: Some("\"memory-etag\"".to_string()),
    };
    write_test_provenance(&legacy_payload, &object, &local_path, expected.len() as u64).await;

    let store = Arc::new(MockObjectStore::new([Some(object)], body));
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            codec_concurrency: 2,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        store.clone(),
    );

    executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .unwrap();

    assert_eq!(store.get_calls(), 1, "artifact mismatch must redownload");
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), expected);
    let logical_bytes = std::fs::metadata(&local_path).unwrap().len();
    let physical_bytes = allocated_bytes(&local_path);
    assert!(
        physical_bytes < logical_bytes / 4,
        "explicit memory request must replace the dense legacy replica: logical={logical_bytes} physical={physical_bytes}"
    );
}

#[tokio::test]
async fn verified_replica_source_metadata_change_after_download_is_retriable() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("snapshot.luks");
    tokio::fs::write(&local_path, b"stale").await.unwrap();
    let payload = verified_payload(&local_path);
    let stale = ObjectMeta {
        size: 5,
        etag: Some("\"stale\"".to_string()),
    };
    write_test_provenance(&payload, &stale, &local_path, 5).await;
    let before = ObjectMeta {
        size: 5,
        etag: Some("\"before\"".to_string()),
    };
    let after = ObjectMeta {
        size: 5,
        etag: Some("\"after\"".to_string()),
    };
    let store = Arc::new(MockObjectStore::new(
        [Some(before.clone()), Some(before), Some(after)],
        b"bytes",
    ));
    let executor = test_executor(store.clone());

    let err = executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::VerifiedReplica,
        )
        .await
        .unwrap_err();

    assert!(
        matches!(&err, JobError::Retriable(msg) if msg.contains("source object changed")),
        "expected retriable source-change error, got {err:?}"
    );
    assert_eq!(store.get_calls(), 1);
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), b"bytes");
    assert!(
        !verified_replica_provenance_path(&local_path).exists(),
        "stale provenance must be invalidated before replacing local_path"
    );
}

#[tokio::test]
async fn legacy_pull_overwrite_invalidates_stale_verified_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("snapshot.luks");
    let mut half_decompressed = ZSTD_MAGIC.to_vec();
    half_decompressed.extend_from_slice(b"stale");
    tokio::fs::write(&local_path, half_decompressed)
        .await
        .unwrap();

    let mut payload = verified_payload(&local_path);
    payload.raw = false;
    payload.existing_file_policy = PullExistingPolicy::LegacySkipPresent;
    let stale = ObjectMeta {
        size: 9,
        etag: Some("\"stale\"".to_string()),
    };
    write_test_provenance(&payload, &stale, &local_path, 9).await;
    let store = Arc::new(MockObjectStore::new([], b"legacy-new"));
    let executor = test_executor(store.clone());

    let bytes = executor
        .execute_pull(
            &test_job("pull"),
            &payload,
            &test_creds(),
            test_target(),
            None,
            PullExistingPolicy::LegacySkipPresent,
        )
        .await
        .unwrap();

    assert_eq!(bytes, 10);
    assert_eq!(store.get_calls(), 1);
    assert_eq!(tokio::fs::read(&local_path).await.unwrap(), b"legacy-new");
    assert!(!verified_replica_provenance_path(&local_path).exists());
}

#[tokio::test]
async fn same_destination_pull_lock_serializes_legacy_and_verified_paths() {
    let dir = tempfile::tempdir().unwrap();
    let local_path = dir.path().join("snapshot.luks");
    let store = Arc::new(MockObjectStore::new([], b""));
    let executor = test_executor(store);

    let first = executor.lock_pull_destination(&local_path).await;
    let blocked = tokio::time::timeout(
        Duration::from_millis(10),
        executor.lock_pull_destination(&local_path),
    )
    .await;
    assert!(blocked.is_err(), "second pull to same path must wait");

    drop(first);
    let second = tokio::time::timeout(
        Duration::from_secs(1),
        executor.lock_pull_destination(&local_path),
    )
    .await
    .expect("second pull should acquire after first guard drops");
    drop(second);
    assert!(executor.destination_pull_locks.is_empty());
}

#[tokio::test]
async fn unlink_partial_silent_on_missing_file() {
    // ENOENT is the common case (no stale .partial to sweep).
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("never-existed.partial");
    // Should not panic, error, or warn — the test passes if we get here.
    unlink_partial(&missing, "test_missing").await;
}

#[tokio::test]
async fn unlink_partial_removes_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("stale.partial");
    tokio::fs::write(&p, b"stale bytes").await.unwrap();
    assert!(p.exists());
    unlink_partial(&p, "test_existing").await;
    assert!(!p.exists());
}

#[tokio::test]
async fn sparse_decompress_failure_leaves_no_scratch_siblings() {
    // The per-invocation scratch name fixed the cross-job rename
    // race but must not turn decompress failures into unbounded
    // residue: non-restore destinations have no sweeper, so the
    // closure itself owns scratch cleanup on every error path.
    for artifact in ["memory", "rootfs.ext4"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(artifact);
        // Valid zstd magic followed by garbage so the decoder starts
        // writing the scratch and then fails mid-stream.
        let mut bytes = ZSTD_MAGIC.to_vec();
        bytes.extend_from_slice(b"definitely not a zstd frame body");
        std::fs::write(&path, &bytes).unwrap();

        let snapshot_artifact = match artifact {
            "memory" => SnapshotArtifact::Memory,
            "rootfs.ext4" => SnapshotArtifact::Rootfs,
            _ => unreachable!("test artifact set is closed"),
        };
        let result = decompress_in_place(
            &path,
            Some(snapshot_artifact),
            MaterializationMode::Enabled,
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "corrupt {artifact} must fail decompression"
        );
        let siblings: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name != artifact)
            .collect();
        assert!(
            siblings.is_empty(),
            "failed {artifact} decompress must not strand scratch files: {siblings:?}"
        );
        assert!(path.exists(), "the compressed source must be left intact");
    }
}

fn codec_test_executor(codec_concurrency: usize) -> TransferExecutor {
    TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            codec_concurrency,
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        Arc::new(MockObjectStore::default()),
    )
}

fn codec_priority_executor(
    codec_concurrency: usize,
    reserve: usize,
    threshold: i32,
) -> TransferExecutor {
    TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: false,
            codec_concurrency,
            codec_priority_reserve: reserve,
            codec_priority_threshold: Some(JobPriority::new(threshold)),
            static_credentials: Some(test_creds()),
            ..Default::default()
        },
        Arc::new(MockObjectStore::default()),
    )
}

#[test]
fn codec_concurrency_resolves_explicit_and_auto() {
    assert_eq!(resolved_codec_concurrency(7), 7);
    assert!(
        resolved_codec_concurrency(0) >= 1,
        "auto sizing must grant at least one permit"
    );
}

#[test]
fn cpu_request_cores_parse_rejects_junk_and_zero() {
    assert_eq!(parse_cpu_request_cores("6"), Some(6));
    assert_eq!(parse_cpu_request_cores(" 1 "), Some(1));
    // Zero or malformed values must fall through to the next sizing source
    // rather than granting zero codec permits.
    assert_eq!(parse_cpu_request_cores("0"), None);
    assert_eq!(parse_cpu_request_cores(""), None);
    assert_eq!(parse_cpu_request_cores("100m"), None);
    assert_eq!(parse_cpu_request_cores("-1"), None);
}

#[test]
fn cores_from_cfs_quota_rounds_and_floors() {
    assert_eq!(cores_from_cfs_quota(600_000, 100_000), Some(6));
    assert_eq!(
        cores_from_cfs_quota(50_000, 100_000),
        Some(1),
        "0.5 core floors to 1"
    );
    assert_eq!(
        cores_from_cfs_quota(250_000, 100_000),
        Some(3),
        "2.5 rounds to 3"
    );
    assert_eq!(
        cores_from_cfs_quota(100_000, 0),
        None,
        "zero period is unusable"
    );
}

#[tokio::test]
async fn codec_admission_bypasses_small_inputs() {
    let executor = codec_test_executor(1);
    let permit = executor
        .acquire_codec_permit_for_input(CODEC_ADMISSION_MIN_INPUT_BYTES - 1, JobPriority::default())
        .await
        .expect("bypass admission must not fail");
    assert!(permit.is_none(), "small inputs must skip admission");
    assert_eq!(
        executor.codec_permits.available_permits(),
        1,
        "a bypassed input must not consume a permit"
    );
}

#[tokio::test]
async fn codec_decode_admission_bypasses_known_small_frame() {
    // A frame that pledges a small decompressed size bypasses even when
    // it clears the compressed-input threshold. Bulk one-shot encode
    // records the content size in the header (unlike the streaming
    // encoder the push path uses, whose frames report an unknown size).
    let executor = codec_test_executor(1);
    let dir = tempfile::tempdir().unwrap();
    let small_frame = dir.path().join("small.zst");
    let encoded = zstd::bulk::compress(&b"x".repeat(4096)[..], ZSTD_LEVEL).unwrap();
    tokio::fs::write(&small_frame, &encoded).await.unwrap();
    assert_eq!(
        frame_content_size(&small_frame).await,
        Some(4096),
        "bulk-encoded frame should carry its content size"
    );
    assert!(
        executor
            .acquire_codec_permit_for_decode(&small_frame, JobPriority::default())
            .await
            .unwrap()
            .is_none(),
        "a frame pledging a small decode must bypass"
    );
}

#[tokio::test]
async fn codec_decode_admission_takes_permit_on_unknown_size() {
    // The push path streams through zstd, which omits the content-size
    // header; ciphertext never parses as a frame at all. Both report an
    // unknown decode size, which cannot be proven small — so admission
    // must take a permit regardless of the compressed length.
    let executor = codec_test_executor(1);
    let dir = tempfile::tempdir().unwrap();

    let streamed = dir.path().join("streamed.zst");
    let encoded = zstd::stream::encode_all(&b"y".repeat(65536)[..], ZSTD_LEVEL).unwrap();
    tokio::fs::write(&streamed, &encoded).await.unwrap();
    assert_eq!(
        frame_content_size(&streamed).await,
        None,
        "streaming-encoded frames carry no content size"
    );
    let permit = executor
        .acquire_codec_permit_for_decode(&streamed, JobPriority::default())
        .await
        .unwrap();
    assert!(
        permit.is_some(),
        "unknown-size streaming frame must take a permit"
    );
    assert_eq!(
        executor.codec_permits.available_permits(),
        0,
        "the held permit must be consumed while alive"
    );
    drop(permit);

    // Even a tiny non-zstd (ciphertext-like) input takes a permit: we
    // cannot prove the eventual decode is small.
    let executor = codec_test_executor(1);
    let opaque = dir.path().join("opaque.bin");
    tokio::fs::write(&opaque, b"not a zstd frame")
        .await
        .unwrap();
    assert!(
        executor
            .acquire_codec_permit_for_decode(&opaque, JobPriority::default())
            .await
            .unwrap()
            .is_some(),
        "unknown-size decode must take a permit even for a tiny input"
    );
}

#[tokio::test(start_paused = true)]
async fn codec_admission_bounds_large_inputs() {
    let executor = codec_test_executor(2);
    let first = executor
        .acquire_codec_permit_for_input(CODEC_ADMISSION_MIN_INPUT_BYTES, JobPriority::default())
        .await
        .expect("first admission must succeed")
        .expect("large input must take a permit");
    let _second = executor
        .acquire_codec_permit_for_input(CODEC_ADMISSION_MIN_INPUT_BYTES, JobPriority::default())
        .await
        .expect("second admission must succeed")
        .expect("large input must take a permit");

    // Both permits are held: a third large input must queue, not run.
    let blocked = tokio::time::timeout(
        Duration::from_secs(1),
        executor.acquire_codec_permit_for_input(
            CODEC_ADMISSION_MIN_INPUT_BYTES,
            JobPriority::default(),
        ),
    )
    .await;
    assert!(
        blocked.is_err(),
        "a third concurrent codec pass must wait for a permit"
    );

    // Releasing one permit unblocks the queued admission.
    drop(first);
    let third = tokio::time::timeout(
        Duration::from_secs(1),
        executor.acquire_codec_permit_for_input(
            CODEC_ADMISSION_MIN_INPUT_BYTES,
            JobPriority::default(),
        ),
    )
    .await
    .expect("a freed permit must admit the queued codec pass")
    .expect("admission must not fail");
    assert!(third.is_some());
}

#[tokio::test(start_paused = true)]
async fn codec_priority_reserve_protects_high_priority_from_low_backlog() {
    // total 3 = 2 shared + 1 reserved for priority >= 100.
    let executor = codec_priority_executor(3, 1, 100);
    let low = JobPriority::new(0);
    let high = JobPriority::new(100);

    // Low-priority work fills the shared pool (2 permits).
    let _l1 = executor.acquire_codec_permit(low).await.unwrap();
    let _l2 = executor.acquire_codec_permit(low).await.unwrap();

    // A third low-priority job must now wait — the shared pool is empty
    // and low priority cannot touch the reserve.
    let low_blocked =
        tokio::time::timeout(Duration::from_secs(1), executor.acquire_codec_permit(low)).await;
    assert!(
        low_blocked.is_err(),
        "low priority must not touch the reserve"
    );

    // A high-priority job still runs immediately via the reserved permit.
    let high_permit =
        tokio::time::timeout(Duration::from_secs(1), executor.acquire_codec_permit(high))
            .await
            .expect("high priority must get the reserved permit")
            .expect("acquire must not fail");
    drop(high_permit);
}

#[tokio::test]
async fn codec_priority_reserve_zero_shares_one_pool() {
    // reserve 0 disables priority awareness: all three permits are shared
    // and high priority has no dedicated capacity.
    let executor = codec_priority_executor(3, 0, 100);
    assert!(
        executor.codec_priority_threshold.is_none(),
        "zero reserve must disable priority-aware admission"
    );
    assert_eq!(executor.codec_permits.available_permits(), 3);
    assert_eq!(executor.codec_priority_permits.available_permits(), 0);
}
#[cfg(target_os = "linux")]
#[test]
fn dense_stream_charges_and_queues_every_written_byte() {
    // arrange
    const CHUNK_BYTES: usize = 1024 * 1024;
    const OUTPUT_BYTES: u64 = 3 * CHUNK_BYTES as u64;
    let dir = tempfile::tempdir().expect("create streamed-output test directory");
    let path = dir.path().join("streamed-output");
    let file = std::fs::File::create(&path).expect("create streamed output");
    let (sender, receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let timing = Arc::new(StreamPullTiming::default());
    let mut sink = DecryptedFileSink::new(
        file,
        MaterializationMode::Disabled,
        ROOTFS_SPARSE_BLOCK_BYTES,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Some(Arc::clone(&coordinator)),
        Some(Arc::clone(&timing)),
        JobPriority::default(),
    )
    .unwrap();
    let chunk = vec![0xa5; CHUNK_BYTES];

    // act
    for _ in 0..OUTPUT_BYTES / CHUNK_BYTES as u64 {
        sink.write_all(&chunk).expect("write streamed output chunk");
    }
    sink.complete().expect("complete streamed output");

    // assert
    let mut queued_debt = 0;
    let mut queued_ranges = 0;
    while let Ok(command) = receiver.try_recv() {
        match command {
            StreamWritebackCommand::Range(range) => {
                queued_ranges += 1;
                queued_debt += range.debt_bytes;
                assert_eq!(range.length, range.debt_bytes);
            }
            StreamWritebackCommand::ReapToLowWatermark => {
                panic!("completion must not request a global reap")
            }
        }
    }
    assert_eq!(queued_debt, OUTPUT_BYTES);
    assert_eq!(
        queued_ranges, 1,
        "sub-window dense writes must be batched before crossing the coordinator channel"
    );
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        OUTPUT_BYTES
    );
    assert_eq!(
        std::fs::metadata(path).expect("stat streamed output").len(),
        OUTPUT_BYTES
    );
    assert_eq!(
        StreamPullTiming::load(&timing.decoded_output_bytes),
        OUTPUT_BYTES
    );
}

#[cfg(target_os = "linux")]
#[test]
fn scattered_sparse_runs_are_batched_before_coordinator_submission() {
    // arrange
    const OUTPUT_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().expect("create sparse writeback test directory");
    let file =
        std::fs::File::create(dir.path().join("sparse-output")).expect("create sparse output");
    let (sender, receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let mut sink = DecryptedFileSink::new(
        file,
        MaterializationMode::Enabled,
        MEMORY_SPARSE_BLOCK_BYTES,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Some(Arc::clone(&coordinator)),
        Some(Arc::new(StreamPullTiming::default())),
        JobPriority::default(),
    )
    .unwrap();
    let mut input = vec![0_u8; OUTPUT_BYTES];
    for (page_index, page) in input.chunks_mut(MEMORY_SPARSE_BLOCK_BYTES).enumerate() {
        if page_index % 2 == 0 {
            page.fill(0xa5);
        }
    }

    // act
    sink.write_all(&input)
        .expect("write scattered sparse output");
    sink.complete().expect("complete sparse output");

    // assert
    let ranges = receiver
        .try_iter()
        .filter_map(|command| match command {
            StreamWritebackCommand::Range(range) => Some(range),
            StreamWritebackCommand::ReapToLowWatermark => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ranges.len(),
        1,
        "scattered physical runs below one dirty window must cross the channel once"
    );
    assert_eq!(ranges[0].debt_bytes, (OUTPUT_BYTES / 2) as u64);
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        (OUTPUT_BYTES / 2) as u64
    );
}

#[cfg(target_os = "linux")]
#[test]
fn sparse_zero_run_queues_no_hole_only_writeback_range() {
    // arrange
    const CHUNK_BYTES: usize = 1024 * 1024;
    const OUTPUT_BYTES: u64 = 2 * STREAM_WRITEBACK_WINDOW_BYTES;
    let dir = tempfile::tempdir().expect("create sparse writeback test directory");
    let path = dir.path().join("sparse-output");
    let file = std::fs::File::create(&path).expect("create sparse output");
    let (sender, receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let mut sink = DecryptedFileSink::new(
        file,
        MaterializationMode::Enabled,
        ROOTFS_SPARSE_BLOCK_BYTES,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Some(Arc::clone(&coordinator)),
        Some(Arc::new(StreamPullTiming::default())),
        JobPriority::default(),
    )
    .unwrap();
    let zeroes = vec![0; CHUNK_BYTES];

    // act
    for _ in 0..OUTPUT_BYTES / CHUNK_BYTES as u64 {
        sink.write_all(&zeroes)
            .expect("write sparse streamed output chunk");
    }
    sink.complete().expect("complete sparse streamed output");

    // assert
    assert!(
        matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "hole creation must not queue a writeback range"
    );
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        0,
        "hole-only output must release its reservation"
    );
    let metadata = std::fs::metadata(&path).expect("stat sparse streamed output");
    assert_eq!(metadata.len(), OUTPUT_BYTES);
    use std::os::unix::fs::MetadataExt;
    assert!(
        metadata.blocks() * 512 < OUTPUT_BYTES / 16,
        "rolling writeback must not turn classified zeroes into allocated data"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn mixed_sparse_stream_charges_only_written_data_and_preserves_holes() {
    // arrange
    const REGION_BYTES: usize = 4 * 1024 * 1024;
    let dir = tempfile::tempdir().expect("create sparse writeback test directory");
    let path = dir.path().join("mixed-sparse-output");
    let file = std::fs::File::create(&path).expect("create sparse output");
    let (sender, receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let timing = Arc::new(StreamPullTiming::default());
    let mut sink = DecryptedFileSink::new(
        file,
        MaterializationMode::Enabled,
        ROOTFS_SPARSE_BLOCK_BYTES,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Some(Arc::clone(&coordinator)),
        Some(Arc::clone(&timing)),
        JobPriority::default(),
    )
    .unwrap();
    let mut expected = vec![0; 3 * REGION_BYTES];
    expected[REGION_BYTES..2 * REGION_BYTES].fill(0xa5);

    // act
    sink.write_all(&expected)
        .expect("write mixed sparse output");
    sink.complete().expect("complete mixed sparse output");

    // assert
    let range = match receiver.recv().expect("receive data writeback range") {
        StreamWritebackCommand::Range(range) => range,
        StreamWritebackCommand::ReapToLowWatermark => {
            panic!("completion must not request a global reap")
        }
    };
    assert_eq!(range.offset, REGION_BYTES as u64);
    assert_eq!(range.length, REGION_BYTES as u64);
    assert_eq!(range.debt_bytes, REGION_BYTES as u64);
    assert!(matches!(
        receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        REGION_BYTES as u64
    );
    assert_eq!(
        StreamPullTiming::load(&timing.writeback_hole_bytes_released),
        (2 * REGION_BYTES) as u64
    );
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(path).expect("stat sparse output");
    assert_eq!(metadata.len(), (3 * REGION_BYTES) as u64);
    use std::os::unix::fs::MetadataExt;
    assert!(
        metadata.blocks() * 512 < (2 * REGION_BYTES) as u64,
        "zero regions must remain holes"
    );
}

#[test]
fn dirty_debt_below_low_watermark_does_not_start_writeback() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        8 * MIB,
        None,
        operations.clone(),
    );
    let file = Arc::new(tempfile::tempfile().unwrap());
    coordinator
        .reserve(3 * MIB, JobPriority::default())
        .unwrap();

    // act
    coordinator.submit(
        file,
        0,
        3 * MIB,
        3 * MIB,
        Arc::new(StreamPullTiming::default()),
    );
    std::thread::sleep(STREAM_WRITEBACK_IDLE_DRAIN + Duration::from_millis(25));

    // assert
    assert_eq!(
        operations.start_count(),
        0,
        "queued debt below the low watermark must stay lazy across idle cleanup"
    );
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        3 * MIB
    );
}

#[test]
fn complete_writeback_window_starts_before_budget_pressure() {
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        4 * STREAM_WRITEBACK_WINDOW_BYTES,
        None,
        operations.clone(),
    );
    coordinator
        .reserve(STREAM_WRITEBACK_WINDOW_BYTES, JobPriority::default())
        .unwrap();
    coordinator.submit(
        Arc::new(tempfile::tempfile().unwrap()),
        0,
        STREAM_WRITEBACK_WINDOW_BYTES,
        STREAM_WRITEBACK_WINDOW_BYTES,
        Arc::new(StreamPullTiming::default()),
    );
    wait_until(|| operations.start_count() == 1);
    assert_eq!(
        operations.wait_count(),
        0,
        "starting IO must not force a synchronous drain"
    );
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        STREAM_WRITEBACK_WINDOW_BYTES,
        "starting IO must not release the dirty-byte reservation before completion"
    );
}

#[test]
fn crossing_low_watermark_starts_coalesced_writeback() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        8 * MIB,
        None,
        operations.clone(),
    );
    let file = Arc::new(tempfile::tempfile().unwrap());
    let timing = Arc::new(StreamPullTiming::default());

    // act
    for offset in [0, 6 * MIB] {
        coordinator
            .reserve(2 * MIB, JobPriority::default())
            .unwrap();
        coordinator.submit(
            Arc::clone(&file),
            offset,
            2 * MIB,
            2 * MIB,
            Arc::clone(&timing),
        );
    }
    wait_until(|| operations.start_count() == 1);

    // assert
    assert_eq!(*operations.starts.lock().unwrap(), vec![(0, 8 * MIB)]);
    assert_eq!(StreamPullTiming::load(&timing.writeback_start_calls), 1);
    assert_eq!(
        StreamPullTiming::load(&timing.writeback_submitted_logical_bytes),
        8 * MIB
    );
    assert_eq!(
        StreamPullTiming::load(&timing.writeback_submitted_dirty_bytes),
        4 * MIB
    );
    assert_eq!(
        StreamPullTiming::load(&timing.writeback_low_watermark_activations),
        1
    );
}

#[test]
fn coordinator_coalescing_never_skips_a_newer_range_for_the_same_stream() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let file = Arc::new(tempfile::tempfile().unwrap());
    let timing = Arc::new(StreamPullTiming::default());
    let mut pending = std::collections::VecDeque::new();

    // act
    for (offset, debt_bytes) in [(0, 20), (20, 20), (40, 10), (50, 10)] {
        queue_stream_writeback_range(
            &mut pending,
            StreamWritebackRange {
                file: Arc::clone(&file),
                offset: offset * MIB,
                length: debt_bytes * MIB,
                debt_bytes: debt_bytes * MIB,
                started: false,
                timing: Arc::clone(&timing),
            },
        );
    }

    // assert
    let ranges = pending
        .iter()
        .map(|range| (range.offset, range.length, range.debt_bytes))
        .collect::<Vec<_>>();
    assert_eq!(
        ranges,
        vec![
            (0, 20 * MIB, 20 * MIB),
            (20 * MIB, 30 * MIB, 30 * MIB),
            (50 * MIB, 10 * MIB, 10 * MIB),
        ]
    );
    assert!(
        pending
            .iter()
            .zip(pending.iter().skip(1))
            .all(|(older, newer)| older.offset + older.length <= newer.offset),
        "coalesced filesystem spans must not overlap"
    );
    assert!(
        pending
            .iter()
            .map(|range| range.offset + range.length)
            .max()
            .is_some_and(|end| end <= 60 * MIB),
        "submitted spans must stay within the logical file interval"
    );
}

#[test]
fn dense_ranges_keep_rolling_writeback_window_granularity() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        64 * MIB,
        None,
        operations.clone(),
    );
    let file = Arc::new(tempfile::tempfile().unwrap());
    let timing = Arc::new(StreamPullTiming::default());

    // act
    for chunk in 0..64 {
        coordinator.reserve(MIB, JobPriority::default()).unwrap();
        coordinator.submit(
            Arc::clone(&file),
            chunk * MIB,
            MIB,
            MIB,
            Arc::clone(&timing),
        );
    }
    wait_until(|| operations.start_count() == 2);

    // assert
    assert_eq!(
        *operations.starts.lock().unwrap(),
        vec![(0, 32 * MIB), (32 * MIB, 32 * MIB)]
    );
    assert_eq!(StreamPullTiming::load(&timing.writeback_start_calls), 2);
}

#[test]
fn first_pressure_activation_preserves_writeback_window_granularity() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        128 * MIB,
        None,
        operations.clone(),
    );
    let file = Arc::new(tempfile::tempfile().unwrap());
    let timing = Arc::new(StreamPullTiming::default());

    // act
    for chunk in 0..64 {
        coordinator.reserve(MIB, JobPriority::default()).unwrap();
        coordinator.submit(
            Arc::clone(&file),
            chunk * MIB,
            MIB,
            MIB,
            Arc::clone(&timing),
        );
    }
    wait_until(|| operations.start_count() > 0);

    // assert
    assert_eq!(
        *operations.starts.lock().unwrap(),
        vec![(0, 32 * MIB), (32 * MIB, 32 * MIB)],
        "initial low-watermark activation must not submit an oversized range"
    );
}

#[test]
fn range_retirement_releases_dirty_debt_not_filesystem_span() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let operations = RecordingPageCacheOperations::default();
    let budget = StreamWritebackBudget::default();
    *budget.outstanding_bytes.lock().unwrap() = MIB;
    let range = StreamWritebackRange {
        file: Arc::new(tempfile::tempfile().unwrap()),
        offset: 4 * MIB,
        length: 4 * MIB,
        debt_bytes: MIB,
        started: true,
        timing: Arc::new(StreamPullTiming::default()),
    };
    let mut writeback_enabled = true;
    let mut cache_advice_enabled = true;

    // act
    retire_stream_writeback_range(
        range,
        &budget,
        &operations,
        &mut writeback_enabled,
        &mut cache_advice_enabled,
    );

    // assert
    assert_eq!(stream_writeback_outstanding_bytes(&budget), 0);
    assert_eq!(*operations.waits.lock().unwrap(), vec![(4 * MIB, 4 * MIB)]);
    assert_eq!(*operations.advice.lock().unwrap(), vec![(4 * MIB, 4 * MIB)]);
}

#[test]
fn high_watermark_drains_partial_ranges_and_unblocks_admission() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        8 * MIB,
        None,
        operations.clone(),
    );
    for _ in 0..3 {
        coordinator
            .reserve(2 * MIB, JobPriority::default())
            .unwrap();
        coordinator.submit(
            Arc::new(tempfile::tempfile().unwrap()),
            0,
            2 * MIB,
            2 * MIB,
            Arc::new(StreamPullTiming::default()),
        );
    }
    wait_until(|| operations.start_count() == 2);
    let blocked_coordinator = Arc::clone(&coordinator);
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();

    // act
    let waiter = std::thread::spawn(move || {
        blocked_coordinator
            .reserve(4 * MIB, JobPriority::default())
            .unwrap();
        admitted_tx.send(()).unwrap();
    });

    // assert
    admitted_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("high-watermark admission must drain coordinator-owned partial ranges");
    assert!(operations.wait_count() >= 1);
    assert!(
        stream_writeback_outstanding_bytes(&coordinator.budget) <= 8 * MIB,
        "admission must remain within the high watermark"
    );
    coordinator.release(4 * MIB);
    waiter.join().unwrap();
}

#[test]
fn active_high_watermark_drain_starts_new_partial_ranges_before_retiring_them() {
    // arrange
    const MIB: u64 = 1024 * 1024;
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        8 * MIB,
        None,
        operations.clone(),
    );
    let file = Arc::new(tempfile::tempfile().unwrap());
    let timing = Arc::new(StreamPullTiming::default());
    coordinator
        .reserve(8 * MIB, JobPriority::default())
        .unwrap();
    coordinator.submit(Arc::clone(&file), 0, 2 * MIB, 2 * MIB, Arc::clone(&timing));
    let blocked_coordinator = Arc::clone(&coordinator);
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        blocked_coordinator
            .reserve(4 * MIB, JobPriority::default())
            .unwrap();
        admitted_tx.send(()).unwrap();
    });
    wait_until(|| operations.wait_count() == 1);

    // act
    coordinator.submit(file, 2 * MIB, 2 * MIB, 2 * MIB, timing);

    // assert
    admitted_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("new partial range must let high-watermark admission progress");
    assert_eq!(
        *operations.starts.lock().unwrap(),
        vec![(0, 2 * MIB), (2 * MIB, 2 * MIB)],
        "an active drain must start a newly queued partial range before retiring it"
    );
    coordinator.release(4 * MIB);
    coordinator.release(4 * MIB);
    waiter.join().unwrap();
}

#[test]
fn sparse_streams_expose_buffered_data_before_high_watermark_wait() {
    // arrange
    const MIB: usize = 1024 * 1024;
    let operations = Arc::new(RecordingPageCacheOperations::default());
    let coordinator = StreamWritebackCoordinator::new_with_page_cache_operations(
        (8 * MIB) as u64,
        None,
        operations.clone(),
    );
    let make_sink = || {
        DecryptedFileSink::new(
            tempfile::tempfile().unwrap(),
            MaterializationMode::Enabled,
            ROOTFS_SPARSE_BLOCK_BYTES,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Some(Arc::clone(&coordinator)),
            Some(Arc::new(StreamPullTiming::default())),
            JobPriority::default(),
        )
        .unwrap()
    };
    let mut first = make_sink();
    let mut second = make_sink();
    let initial = vec![0xa5; 3 * MIB];
    first.write_all(&initial).unwrap();
    second.write_all(&initial).unwrap();
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        (8 * MIB) as u64,
        "each producer should hold one bounded reusable reservation window"
    );
    let more = vec![0x5a; 2 * MIB];

    // act
    first.write_all(&more).unwrap();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        second.write_all(&more).unwrap();
        completed_tx.send(second).unwrap();
    });

    // assert
    let second = completed_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("sparse producer must expose prior data so writeback can unblock admission");
    assert!(operations.start_count() >= 1);
    assert!(operations.wait_count() >= 1);
    assert!(stream_writeback_outstanding_bytes(&coordinator.budget) <= (8 * MIB) as u64);
    first.complete().unwrap();
    second.complete().unwrap();
    waiter.join().unwrap();
}

#[test]
fn sparse_hole_debt_is_batched_until_additional_admission() {
    // arrange
    const HOLE_BLOCKS: usize = 4;
    let (sender, _receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let mut writeback = StreamWriteback::new(
        Some(Arc::clone(&coordinator)),
        Some(Arc::new(StreamPullTiming::default())),
        JobPriority::default(),
    );
    writeback.reserve(1).expect("reserve sparse stream window");
    let mut sparse = SparseFileWriter::new(
        tempfile::tempfile().expect("create sparse classification fixture"),
        MEMORY_SPARSE_BLOCK_BYTES,
    )
    .expect("create sparse writer");

    // act
    for _ in 0..HOLE_BLOCKS {
        sparse
            .write_all(&vec![0; MEMORY_SPARSE_BLOCK_BYTES])
            .expect("classify one hole block");
        writeback.record_written(MEMORY_SPARSE_BLOCK_BYTES);
        let materialization = sparse.take_materialization();
        writeback
            .classify(sparse.file(), materialization)
            .expect("account for one hole block");
    }

    // assert
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        STREAM_WRITEBACK_WINDOW_BYTES,
        "sub-window holes must remain locally over-accounted instead of locking the coordinator per block"
    );

    writeback
        .try_reserve(STREAM_WRITEBACK_WINDOW_BYTES as usize)
        .expect("request additional admission");
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        STREAM_WRITEBACK_WINDOW_BYTES,
        "deferred holes must be released before requesting additional admission"
    );
}

#[test]
fn sparse_hole_batch_releases_at_the_writeback_window_boundary() {
    // arrange
    let (sender, _receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let timing = Arc::new(StreamPullTiming::default());
    let mut writeback = StreamWriteback::new(
        Some(Arc::clone(&coordinator)),
        Some(Arc::clone(&timing)),
        JobPriority::default(),
    );
    writeback.reserve(1).expect("reserve sparse stream window");
    let mut sparse = SparseFileWriter::new(
        tempfile::tempfile().expect("create sparse classification fixture"),
        MEMORY_SPARSE_BLOCK_BYTES,
    )
    .expect("create sparse writer");
    let leading_holes = STREAM_WRITEBACK_WINDOW_BYTES as usize - MEMORY_SPARSE_BLOCK_BYTES;

    // act
    sparse
        .write_all(&vec![0; leading_holes])
        .expect("classify holes below the batching boundary");
    writeback.record_written(leading_holes);
    let materialization = sparse.take_materialization();
    writeback
        .classify(sparse.file(), materialization)
        .expect("account for holes below the batching boundary");

    // assert
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        STREAM_WRITEBACK_WINDOW_BYTES,
        "a partial hole batch must remain conservatively charged"
    );

    sparse
        .write_all(&vec![0; MEMORY_SPARSE_BLOCK_BYTES])
        .expect("cross the hole batching boundary");
    writeback.record_written(MEMORY_SPARSE_BLOCK_BYTES);
    let materialization = sparse.take_materialization();
    writeback
        .classify(sparse.file(), materialization)
        .expect("release the complete hole batch");
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        0,
        "a complete hole batch must release its writeback debt"
    );
    assert_eq!(
        StreamPullTiming::load(&timing.writeback_hole_bytes_released),
        STREAM_WRITEBACK_WINDOW_BYTES
    );
}

#[test]
fn dropping_a_stream_releases_deferred_holes_and_unused_reservation() {
    // arrange
    let (sender, _receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let mut writeback = StreamWriteback::new(
        Some(Arc::clone(&coordinator)),
        Some(Arc::new(StreamPullTiming::default())),
        JobPriority::default(),
    );
    writeback.reserve(1).expect("reserve sparse stream window");
    let mut sparse = SparseFileWriter::new(
        tempfile::tempfile().expect("create sparse classification fixture"),
        MEMORY_SPARSE_BLOCK_BYTES,
    )
    .expect("create sparse writer");
    sparse
        .write_all(&vec![0; MEMORY_SPARSE_BLOCK_BYTES])
        .expect("classify deferred hole");
    writeback.record_written(MEMORY_SPARSE_BLOCK_BYTES);
    let materialization = sparse.take_materialization();
    writeback
        .classify(sparse.file(), materialization)
        .expect("account for deferred hole");
    assert_ne!(writeback.deferred_hole_bytes, 0);

    // act
    drop(writeback);

    // assert
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        0,
        "drop must release deferred holes and the remaining reusable credit"
    );
}

#[test]
fn deferred_holes_prevent_a_high_watermark_wait() {
    // arrange
    const MIB: usize = 1024 * 1024;
    let (sender, _receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 8 * MIB as u64,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let mut writeback = StreamWriteback::new(
        Some(Arc::clone(&coordinator)),
        Some(Arc::new(StreamPullTiming::default())),
        JobPriority::default(),
    );
    writeback.reserve(1).expect("reserve producer credit");
    let mut sparse = SparseFileWriter::new(
        tempfile::tempfile().expect("create sparse classification fixture"),
        MEMORY_SPARSE_BLOCK_BYTES,
    )
    .expect("create sparse writer");
    sparse
        .write_all(&vec![0; MIB])
        .expect("classify deferred holes");
    writeback.record_written(MIB);
    let materialization = sparse.take_materialization();
    writeback
        .classify(sparse.file(), materialization)
        .expect("account for deferred holes");
    coordinator
        .reserve(4 * MIB as u64, JobPriority::default())
        .expect("fill the high watermark with another producer");
    assert!(
        !coordinator
            .try_reserve(1, JobPriority::default())
            .expect("probe full high watermark"),
        "admission must be full before the producer exposes deferred holes"
    );

    // act
    let admitted = writeback
        .try_reserve(4 * MIB)
        .expect("flush holes before admission");

    // assert
    assert!(admitted, "released holes must prevent a blocking wait");
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        8 * MIB as u64
    );
    coordinator.release(4 * MIB as u64);
}

#[test]
fn outstanding_metric_repeats_unchanged_values_at_the_heartbeat_interval() {
    assert!(should_emit_stream_writeback_outstanding(
        STREAM_WRITEBACK_METRIC_INTERVAL,
        false,
    ));
    assert!(should_emit_stream_writeback_outstanding(
        Duration::ZERO,
        true,
    ));
    assert!(!should_emit_stream_writeback_outstanding(
        STREAM_WRITEBACK_METRIC_INTERVAL / 2,
        false,
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn finishing_one_stream_does_not_reap_another_streams_ranges() {
    // arrange
    let dir = tempfile::tempdir().expect("create streamed-output test directory");
    let file = std::fs::File::create(dir.path().join("output")).expect("create output");
    let (sender, receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let timing = Arc::new(StreamPullTiming::default());
    let mut writeback =
        StreamWriteback::new(Some(coordinator), Some(timing), JobPriority::default());
    writeback.reserve(1024).expect("reserve tail range");
    writeback.record_written(1024);

    // act
    writeback
        .classify_dense(&file, 0, 1024)
        .expect("classify tail range");
    writeback
        .flush_pending(&file)
        .expect("submit final accumulated range");

    // assert
    assert!(matches!(
        receiver.recv().expect("submitted tail range"),
        StreamWritebackCommand::Range(_)
    ));
    assert!(
        matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "finishing one stream must not request a coordinator-global reap"
    );
}

#[test]
fn writeback_budget_reserves_capacity_for_high_priority_streams() {
    // arrange
    let low = JobPriority::new(0);
    let high = JobPriority::new(100);
    let coordinator =
        StreamWritebackCoordinator::new(2 * STREAM_WRITEBACK_WINDOW_BYTES, Some(high));
    coordinator
        .reserve(STREAM_WRITEBACK_WINDOW_BYTES, low)
        .expect("reserve first low-priority range");

    let low_coordinator = Arc::clone(&coordinator);
    let low_waiter = std::thread::spawn(move || {
        low_coordinator
            .reserve(STREAM_WRITEBACK_WINDOW_BYTES, low)
            .expect("reserve second low-priority range");
    });
    std::thread::sleep(Duration::from_millis(20));

    // act
    let high_coordinator = Arc::clone(&coordinator);
    let (high_admitted_tx, high_admitted_rx) = std::sync::mpsc::channel();
    let high_waiter = std::thread::spawn(move || {
        high_coordinator
            .reserve(STREAM_WRITEBACK_WINDOW_BYTES, high)
            .expect("reserve high-priority range");
        high_admitted_tx.send(()).expect("report high admission");
    });

    // assert
    high_admitted_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("high-priority stream must use capacity protected from low-priority backlog");
    assert!(
        stream_writeback_outstanding_bytes(&coordinator.budget)
            <= 2 * STREAM_WRITEBACK_WINDOW_BYTES,
        "aggregate reservations must not exceed the high watermark"
    );
    high_waiter.join().expect("join high-priority waiter");
    coordinator.release(2 * STREAM_WRITEBACK_WINDOW_BYTES);
    low_waiter.join().expect("join low-priority waiter");
    coordinator.release(STREAM_WRITEBACK_WINDOW_BYTES);
}

#[test]
fn dropping_partial_stream_releases_its_writeback_reservation() {
    // arrange
    let (sender, _receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 2 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let mut writeback = StreamWriteback::new(
        Some(Arc::clone(&coordinator)),
        Some(Arc::new(StreamPullTiming::default())),
        JobPriority::default(),
    );
    writeback
        .reserve(1024)
        .expect("reserve partial stream window");
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        STREAM_WRITEBACK_WINDOW_BYTES,
        "the producer should acquire one reusable local reservation window"
    );

    // act
    drop(writeback);

    // assert
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        0,
        "cancellation or drop must release an unsubmitted reservation"
    );
}

#[test]
fn producer_reuses_reserved_credit_for_subwindow_writes() {
    // arrange
    const MIB: usize = 1024 * 1024;
    let (sender, _receiver) = std::sync::mpsc::channel();
    let coordinator = Arc::new(StreamWritebackCoordinator {
        sender,
        budget: Arc::new(StreamWritebackBudget::default()),
        high_watermark_bytes: 4 * STREAM_WRITEBACK_WINDOW_BYTES,
        priority_threshold: None,
        priority_reserve_bytes: 0,
    });
    let file = tempfile::tempfile().expect("create writeback fixture");
    let mut writeback = StreamWriteback::new(
        Some(Arc::clone(&coordinator)),
        Some(Arc::new(StreamPullTiming::default())),
        JobPriority::default(),
    );
    writeback.reserve(MIB).expect("reserve first write");
    writeback.record_written(MIB);
    writeback
        .classify_dense(&file, 0, MIB as u64)
        .expect("classify first write");

    // act
    writeback.reserve(MIB).expect("reserve second write");

    // assert
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        STREAM_WRITEBACK_WINDOW_BYTES,
        "sub-window writes must consume existing producer credit without reacquiring budget"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn writeback_syscall_failure_closes_future_admission() {
    // arrange
    let unsupported = std::fs::File::open("/dev/null").expect("open non-writeback file fixture");
    let priority = JobPriority::default();
    let coordinator = StreamWritebackCoordinator::new(STREAM_WRITEBACK_WINDOW_BYTES, None);
    coordinator
        .reserve(STREAM_WRITEBACK_WINDOW_BYTES, priority)
        .expect("reserve failing range");

    // act
    coordinator.submit(
        Arc::new(unsupported),
        0,
        STREAM_WRITEBACK_WINDOW_BYTES,
        STREAM_WRITEBACK_WINDOW_BYTES,
        Arc::new(StreamPullTiming::default()),
    );
    for _ in 0..100 {
        if coordinator
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // assert
    assert!(
        coordinator
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire),
        "a writeback syscall failure must close admission instead of silently disabling pacing"
    );
    wait_until(|| stream_writeback_outstanding_bytes(&coordinator.budget) == 0);
    assert_eq!(
        stream_writeback_outstanding_bytes(&coordinator.budget),
        0,
        "fail-closed writeback must release every pending reservation"
    );
    let error = coordinator
        .reserve(STREAM_WRITEBACK_WINDOW_BYTES, priority)
        .expect_err("future reservations must fail closed");
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
}

#[cfg(unix)]
fn write_zstd(path: &Path, bytes: &[u8]) {
    let mut encoder =
        zstd::stream::write::Encoder::new(std::fs::File::create(path).unwrap(), ZSTD_LEVEL)
            .unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap();
}

#[cfg(unix)]
fn allocated_bytes(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata(path).unwrap().blocks() * 512
}

#[cfg(unix)]
#[tokio::test]
async fn memory_decompression_preserves_zero_runs_as_holes() {
    // arrange
    const ZERO_RUN_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory");
    let mut expected = b"memory-prefix".to_vec();
    expected.resize(expected.len() + ZERO_RUN_BYTES, 0);
    expected.extend_from_slice(b"memory-suffix");
    write_zstd(&path, &expected);

    // act
    let report = decompress_in_place(
        &path,
        Some(SnapshotArtifact::Memory),
        MaterializationMode::Enabled,
        None,
    )
    .await
    .unwrap();
    assert!(report.candidate_hole_bytes > (ZERO_RUN_BYTES - MEMORY_SPARSE_BLOCK_BYTES * 2) as u64);

    // assert
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert!(
        allocated_bytes < metadata.len() / 4,
        "memory decompression should leave zero runs as holes: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn restore_memory_decompression_preserves_leading_and_trailing_holes() {
    // arrange
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join(".memory.restore-019f24f6-c250-7bf1-8c63-6ffdaefad5e0");
    let mut expected = vec![0; IMAGE_BYTES];
    let data_offset = IMAGE_BYTES / 2;
    expected[data_offset..data_offset + 3].copy_from_slice(b"ram");
    write_zstd(&path, &expected);

    // act
    decompress_in_place(
        &path,
        Some(SnapshotArtifact::Memory),
        MaterializationMode::Enabled,
        None,
    )
    .await
    .unwrap();

    // assert
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert!(
        allocated_bytes < metadata.len() / 4,
        "restore memory should preserve leading and trailing holes: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn restore_rootfs_ext4_decompression_preserves_zero_runs_as_holes() {
    // arrange
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join(".rootfs.ext4.restore-019f24f8-42d0-71a3-bcf3-79543b2e4d4d");
    let mut expected = vec![0; IMAGE_BYTES];
    expected[..6].copy_from_slice(b"rootfs");
    expected[IMAGE_BYTES - 4..].copy_from_slice(b"ext4");
    write_zstd(&path, &expected);

    // act
    decompress_in_place(
        &path,
        Some(SnapshotArtifact::Rootfs),
        MaterializationMode::Enabled,
        None,
    )
    .await
    .unwrap();

    // assert
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert!(
        allocated_bytes < metadata.len() / 4,
        "rootfs.ext4 decompression should leave zero runs as holes: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rootfs_ext4_partial_zero_block_preserves_length_as_a_hole() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rootfs.ext4");
    let expected = vec![0; ROOTFS_SPARSE_BLOCK_BYTES + 173];
    write_zstd(&path, &expected);

    decompress_in_place(
        &path,
        Some(SnapshotArtifact::Rootfs),
        MaterializationMode::Enabled,
        None,
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert_eq!(metadata.len(), (ROOTFS_SPARSE_BLOCK_BYTES + 173) as u64);
    assert!(
        allocated_bytes < metadata.len() / 4,
        "partial trailing zero blocks should remain holes: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rootfs_ext4_preserves_data_around_sparse_block_boundaries() {
    const IMAGE_BYTES: usize = 4 * 1024 * 1024 + 173;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rootfs.ext4");
    let mut expected = vec![0; IMAGE_BYTES];
    for offset in [
        0,
        ROOTFS_SPARSE_BLOCK_BYTES - 1,
        ROOTFS_SPARSE_BLOCK_BYTES,
        ROOTFS_SPARSE_BLOCK_BYTES + 1,
        3 * ROOTFS_SPARSE_BLOCK_BYTES - 1,
        IMAGE_BYTES - 1,
    ] {
        expected[offset] = 0xa5;
    }
    write_zstd(&path, &expected);

    decompress_in_place(
        &path,
        Some(SnapshotArtifact::Rootfs),
        MaterializationMode::Enabled,
        None,
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert!(
        allocated_bytes < metadata.len() / 4,
        "boundary data should not fill surrounding holes: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn rootfs_btrfs_send_decompression_remains_dense() {
    // arrange
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rootfs.btrfs-send");
    let expected = vec![0; IMAGE_BYTES];
    write_zstd(&path, &expected);

    // act
    decompress_in_place(
        &path,
        Some(SnapshotArtifact::RootfsBtrfsSend),
        MaterializationMode::Enabled,
        None,
    )
    .await
    .unwrap();

    // assert
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert!(
        allocated_bytes > metadata.len() / 2,
        "non-memory artifacts should keep the dense writer: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn memory_decompression_remains_dense_when_mode_is_disabled() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory");
    let expected = vec![0; IMAGE_BYTES];
    write_zstd(&path, &expected);

    decompress_in_place(
        &path,
        Some(SnapshotArtifact::Memory),
        MaterializationMode::Disabled,
        None,
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    let metadata = std::fs::metadata(&path).unwrap();
    let allocated_bytes = allocated_bytes(&path);
    assert!(
        allocated_bytes > metadata.len() / 2,
        "disabled mode must keep memory dense: logical={} allocated={allocated_bytes}",
        metadata.len()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn shadow_memory_decompression_defers_comparison_and_stays_dense() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory");
    let mut expected = vec![0; IMAGE_BYTES];
    expected[IMAGE_BYTES / 2..IMAGE_BYTES / 2 + 4].copy_from_slice(b"live");
    write_zstd(&path, &expected);

    let report = decompress_in_place(
        &path,
        Some(SnapshotArtifact::Memory),
        MaterializationMode::Shadow,
        None,
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    assert_eq!(report.mode, MaterializationMode::Shadow);
    assert_eq!(report.candidate_hole_bytes, 0);
    let metadata = std::fs::metadata(&path).unwrap();
    assert!(
        allocated_bytes(&path) > metadata.len() / 2,
        "shadow output must remain physically dense"
    );
    let residue: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .filter(|name| name.to_string_lossy().contains("sparse-shadow"))
        .collect();
    assert!(residue.is_empty(), "shadow comparison residue: {residue:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn shadow_rootfs_decompression_defers_comparison_and_stays_dense() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rootfs.ext4");
    let mut expected = vec![0; IMAGE_BYTES];
    expected[IMAGE_BYTES / 2..IMAGE_BYTES / 2 + 6].copy_from_slice(b"rootfs");
    write_zstd(&path, &expected);

    let report = decompress_in_place(
        &path,
        Some(SnapshotArtifact::Rootfs),
        MaterializationMode::Shadow,
        None,
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    assert_eq!(report.mode, MaterializationMode::Shadow);
    assert_eq!(report.candidate_hole_bytes, 0);
    let metadata = std::fs::metadata(&path).unwrap();
    assert!(
        allocated_bytes(&path) > metadata.len() / 2,
        "shadow rootfs output must remain physically dense"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn background_shadow_comparison_matches_and_releases_admission() {
    const IMAGE_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    std::fs::create_dir(&staging).unwrap();
    let path = dir.path().join("rootfs.ext4");
    let snapshot_path = staging.join("sparse-shadow-source-test.tmp");
    let mut expected = vec![0; IMAGE_BYTES];
    expected[IMAGE_BYTES / 2..IMAGE_BYTES / 2 + 6].copy_from_slice(b"rootfs");
    std::fs::write(&path, &expected).unwrap();
    std::fs::copy(&path, &snapshot_path).unwrap();

    let mut executor = test_executor(Arc::new(MockObjectStore::default()));
    executor.shadow_staging_dir = staging.clone();
    let admission = executor
        .try_acquire_sparse_shadow_admission()
        .expect("idle executor should admit one comparison");
    let handle = executor
        .spawn_sparse_shadow_comparison(
            SparseShadowTask {
                job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
                subject_id: "sandbox:019f725e-0000-7000-8000-000000000001".to_string(),
                local_path: path.clone(),
                snapshot_path,
                block_bytes: ROOTFS_SPARSE_BLOCK_BYTES,
                artifact: "rootfs",
            },
            admission,
        )
        .expect("shadow controller is accepting work");

    assert_eq!(std::fs::read(&path).unwrap(), expected);
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 1);

    let observation = handle
        .await
        .unwrap()
        .expect("background comparison should match");
    assert_eq!(observation.logical_bytes, IMAGE_BYTES as u64);
    assert!(observation.allocated_bytes < observation.logical_bytes / 2);
    assert!(observation.candidate_hole_bytes > (IMAGE_BYTES / 2) as u64);
    assert_eq!(std::fs::read_dir(staging).unwrap().count(), 0);
    assert_eq!(executor.shadow_comparison_permits.available_permits(), 1);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shadow_shutdown_rejects_new_work_before_reflink() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    std::fs::create_dir(&staging).unwrap();
    let path = dir.path().join("memory");
    std::fs::write(&path, b"dense bytes").unwrap();

    let mut executor = test_executor(Arc::new(MockObjectStore::default()));
    executor.shadow_staging_dir = staging.clone();
    let drained = executor
        .shadow_comparison_controller()
        .shutdown(std::time::Duration::from_millis(100))
        .await;
    assert!(drained);
    let handle = executor
        .schedule_sparse_shadow_comparison(
            &JobId::new(),
            "pull",
            "sandbox:019f725e-0000-7000-8000-000000000001",
            &path,
            Some(SnapshotArtifact::Memory),
            MaterializationMode::Shadow,
        )
        .await;
    assert!(handle.is_none());
    assert_eq!(
        std::fs::read_dir(staging).unwrap().count(),
        0,
        "shutdown must reject work before taking a reflink"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shadow_shutdown_interrupts_running_sparse_copy_and_cleans_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dense-shadow-source");
    let source = std::fs::File::create(&path).unwrap();
    source.set_len(512 * 1024 * 1024).unwrap();
    drop(source);
    let shutdown_token = CancellationToken::new();
    let task_path = path.clone();
    let task_token = shutdown_token.clone();
    let writeback_coordinator =
        StreamWritebackCoordinator::new(STREAM_WRITEBACK_WINDOW_BYTES, None);
    let handle = tokio::task::spawn_blocking(move || {
        compare_sparse_shadow(
            &task_path,
            MEMORY_SPARSE_BLOCK_BYTES,
            &task_token,
            writeback_coordinator,
        )
    });

    let shadow_started = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if std::fs::read_dir(dir.path()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains("sparse-shadow")
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        shadow_started.is_ok(),
        "shadow copy must enter its running phase"
    );

    shutdown_token.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .expect("running shadow copy must honor cancellation")
        .unwrap();
    assert!(
        matches!(result, Err(SparseShadowCmpError::Cancelled)),
        "shutdown cancellation is not a comparison result"
    );
    let residue: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name())
        .filter(|name| name.to_string_lossy().contains("sparse-shadow"))
        .collect();
    assert!(residue.is_empty(), "cancelled shadow residue: {residue:?}");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shadow_comparison_skips_before_reflink_when_worker_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    std::fs::create_dir(&staging).unwrap();
    let path = dir.path().join("memory");
    std::fs::write(&path, b"dense bytes").unwrap();

    let mut executor = test_executor(Arc::new(MockObjectStore::default()));
    executor.shadow_staging_dir = staging.clone();
    assert_eq!(
        executor.shadow_comparison_permits.available_permits(),
        1,
        "shadow comparison admission must not retain queued reflinks"
    );
    let held_permit = Arc::clone(&executor.shadow_comparison_permits)
        .acquire_owned()
        .await
        .unwrap();

    let handle = executor
        .schedule_sparse_shadow_comparison(
            &JobId::new(),
            "pull",
            "sandbox:019f725e-0000-7000-8000-000000000001",
            &path,
            Some(SnapshotArtifact::Memory),
            MaterializationMode::Shadow,
        )
        .await;

    assert!(handle.is_none());
    assert_eq!(std::fs::read_dir(staging).unwrap().count(), 0);
    drop(held_permit);
}

#[tokio::test]
async fn shadow_comparison_skips_when_shared_codec_capacity_is_busy() {
    let executor = test_executor(Arc::new(MockObjectStore::default()));
    let shared_capacity = executor.codec_permits.available_permits();
    let _held_codec_permits = Arc::clone(&executor.codec_permits)
        .acquire_many_owned(shared_capacity.try_into().unwrap())
        .await
        .unwrap();

    assert!(
        executor.try_acquire_sparse_shadow_admission().is_none(),
        "shadow verification must not run outside the shared codec budget"
    );
    assert_eq!(
        executor.shadow_comparison_permits.available_permits(),
        1,
        "failed codec admission must release the shadow slot"
    );
}

#[test]
fn shadow_disk_headroom_reserves_worst_case_candidate_and_node_floor() {
    use pplx_space_common_core::host::disk::DiskStatus;

    let status = DiskStatus {
        total_bytes: 800 * 1024 * 1024 * 1024,
        free_bytes: 150 * 1024 * 1024 * 1024,
        available_bytes: 100 * 1024 * 1024 * 1024,
    };
    let floor = sparse_shadow_disk_floor_bytes(status);
    assert_eq!(floor, 50 * 1024 * 1024 * 1024);
    assert!(sparse_shadow_has_disk_headroom(
        status,
        50 * 1024 * 1024 * 1024
    ));
    assert!(!sparse_shadow_has_disk_headroom(
        status,
        50 * 1024 * 1024 * 1024 + 1
    ));
}

#[test]
fn cmp_detects_sparse_shadow_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let dense = dir.path().join("dense");
    let sparse = dir.path().join("sparse");
    let shutdown_token = CancellationToken::new();
    std::fs::write(&dense, b"identical bytes").unwrap();
    std::fs::write(&sparse, b"identical bytes").unwrap();
    compare_completed_files(&dense, &sparse, &shutdown_token).unwrap();

    std::fs::write(&sparse, b"different bytes").unwrap();
    assert!(matches!(
        compare_completed_files(&dense, &sparse, &shutdown_token),
        Err(SparseShadowCmpError::Mismatch)
    ));
}

#[test]
fn legacy_transfer_payload_defaults_materialization_off() {
    let payload: TransferPayload = serde_json::from_str(
        r#"{"subject_id":"sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2","local_path":"/tmp/memory","s3_uri":"s3://b/k?region=us-east-2"}"#,
    )
    .unwrap();
    assert_eq!(payload.materialization_mode, MaterializationMode::Disabled);
}

#[test]
fn transfer_payload_deserializes_legacy_sandbox_id_field() {
    // Volume-manager's SQLite job queue can hold pull/push jobs serialized
    // by a pre-rename binary, with `sandbox_id` as the JSON field name.
    // After an upgrade, BackgroundWorker::recover() re-runs those jobs,
    // and a deserialize failure here would drop the work on the floor.
    let legacy = r#"{"sandbox_id":"019d84bd-280d-7331-99c6-b63377f3a3a2","local_path":"/tmp/x","s3_uri":"s3://b/k?region=us-east-2"}"#;
    let p: TransferPayload =
        serde_json::from_str(legacy).expect("legacy sandbox_id payload must deserialize");
    assert_eq!(p.subject_id, "019d84bd-280d-7331-99c6-b63377f3a3a2");
    assert!(p.staged_path.is_none());
}

#[test]
fn transfer_payload_legacy_subject_id_resolves_as_sandbox() {
    // The legacy field on disk holds a bare sandbox UUID with no kind
    // prefix. The executor must accept it as `SubjectId::Sandbox(...)`
    // so credmgr exchange continues to work for those re-spawned jobs.
    let bare_uuid = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let subject =
        parse_subject_id_with_legacy_fallback(bare_uuid).expect("bare sandbox UUID must resolve");
    match subject {
        SubjectId::Sandbox(_) => {}
        SubjectId::Template(_) => panic!("bare UUID must not resolve as Template"),
    }
}

#[test]
fn transfer_payload_prefixed_subject_id_round_trips() {
    // New shape must still parse via the same fallback helper.
    let s = parse_subject_id_with_legacy_fallback("template:seed").unwrap();
    match s {
        SubjectId::Template(name) => assert_eq!(name.as_str(), "seed"),
        SubjectId::Sandbox(_) => panic!("template:seed must resolve as Template"),
    }
    let s = parse_subject_id_with_legacy_fallback("sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2")
        .unwrap();
    assert!(matches!(s, SubjectId::Sandbox(_)));
}

#[test]
fn transfer_payload_raw_defaults_to_false_for_legacy_compatibility() {
    // A payload that doesn't mention `raw` (legacy on-disk or older
    // callers) must default to the pre-PR behavior: transparent
    // decompress stays on.
    let s = r#"{"subject_id":"sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2","local_path":"/tmp/x","s3_uri":"s3://b/k?region=us-east-2"}"#;
    let p: TransferPayload = serde_json::from_str(s).unwrap();
    assert!(!p.raw, "raw must default to false");
    assert_eq!(p.file_mode, None, "file_mode must default to unchanged");
}

#[test]
fn transfer_payload_raw_round_trips() {
    let file_mode = FileMode::new(0o660).unwrap();
    let p = TransferPayload {
        subject_id: "template:seed".to_string(),
        local_path: "/tmp/x".to_string(),
        s3_uri: "s3://b/k?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: None,
        staged_path: None,
        raw: true,
        file_mode: Some(file_mode),
        existing_file_policy: PullExistingPolicy::LegacySkipPresent,
        materialization_mode: MaterializationMode::Shadow,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    };
    let json = p.to_json();
    let Ok(p2) = TransferPayload::from_json(&json) else {
        panic!("round-trip deserialization failed");
    };
    assert!(p2.raw);
    assert_eq!(p2.file_mode, Some(file_mode));
    assert_eq!(p2.materialization_mode, MaterializationMode::Shadow);
    assert!(p2.encryption.is_none());
}

#[test]
fn transfer_payload_with_encryption_round_trips() {
    // Persisted job payloads must round-trip the wrapped DEK and
    // identifying fields verbatim: the executor re-sends them to
    // credmgr at execute time, and an asymmetric serialize would
    // corrupt the wrap and surface as AuthFailed on the next
    // dequeue (or worse, silently re-encrypt with a different
    // implicit key on push).
    let enc = TransferEncryption {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".parse().unwrap(),
        provider: KekProvider::InMemory,
        master_key_identifier: "test/key-1".parse().unwrap(),
        wrapped_dek: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    };
    let p = TransferPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        local_path: "/tmp/x".to_string(),
        s3_uri: "s3://b/k?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: Some(SnapshotArtifact::Memory),
        staged_path: None,
        raw: false,
        file_mode: None,
        existing_file_policy: PullExistingPolicy::LegacySkipPresent,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: Some(enc.clone()),
        artifact_id: Some("019e9000-0000-7000-8000-000000000001".to_string()),
        entry_role: Some("memfile".to_string()),
    };
    let json = p.to_json();
    let Ok(p2) = TransferPayload::from_json(&json) else {
        panic!("round-trip deserialization failed");
    };
    assert_eq!(p2.encryption, Some(enc));
    assert_eq!(
        p2.artifact_id.as_deref(),
        Some("019e9000-0000-7000-8000-000000000001")
    );
    assert_eq!(p2.entry_role.as_deref(), Some("memfile"));
}

#[test]
fn artifact_payload_round_trips() {
    // The artifact job row is the durable record: subject + entries +
    // wrapped DEK must survive a crash/restart verbatim so the executor
    // (and a recovering retry) reconstruct the same uploads and the same
    // wrap. An asymmetric serialize would strand ciphertext under a key
    // nobody can reproduce.
    let enc = TransferEncryption {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".parse().unwrap(),
        provider: KekProvider::InMemory,
        master_key_identifier: "test/key-1".parse().unwrap(),
        wrapped_dek: vec![9, 8, 7, 6, 5, 4, 3, 2, 1],
    };
    let payload = ArtifactPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        artifact_id: "019e9000-0000-7000-8000-000000000001".to_string(),
        entries: vec![
            ArtifactEntryPayload {
                role: "memfile".to_string(),
                snapshot_artifact: Some(SnapshotArtifact::Memory),
                local_path: "/space/snap/memfile".to_string(),
                s3_uri: "s3://b/snap/memfile?region=us-east-2".to_string(),
                staged_path: Some("/space/staging/memfile".to_string()),
                file_mode: None,
            },
            ArtifactEntryPayload {
                role: "rootfs".to_string(),
                snapshot_artifact: Some(SnapshotArtifact::Rootfs),
                local_path: "/space/snap/rootfs".to_string(),
                s3_uri: "s3://b/snap/rootfs?region=us-east-2".to_string(),
                staged_path: None,
                file_mode: Some(FileMode::new(0o640).unwrap()),
            },
        ],
        materialization_mode: MaterializationMode::Shadow,
        rootfs_materialization_mode: Some(MaterializationMode::Shadow),
        encryption: Some(enc.clone()),
    };
    let p2 = ArtifactPayload::from_json(&payload.to_json()).expect("round-trip");
    assert_eq!(p2.subject_id, payload.subject_id);
    assert_eq!(p2.artifact_id, payload.artifact_id);
    assert_eq!(p2.encryption, Some(enc));
    assert_eq!(p2.materialization_mode, MaterializationMode::Shadow);
    assert_eq!(
        p2.rootfs_materialization_mode,
        Some(MaterializationMode::Shadow)
    );
    assert_eq!(p2.entries.len(), 2);
    assert_eq!(p2.entries[0].role, "memfile");
    assert_eq!(
        p2.entries[0].snapshot_artifact,
        Some(SnapshotArtifact::Memory)
    );
    assert_eq!(
        p2.entries[0].staged_path.as_deref(),
        Some("/space/staging/memfile")
    );
    assert_eq!(p2.entries[0].file_mode, None);
    assert_eq!(p2.entries[1].role, "rootfs");
    assert_eq!(
        p2.entries[1].snapshot_artifact,
        Some(SnapshotArtifact::Rootfs)
    );
    assert_eq!(p2.entries[1].staged_path, None);
    assert_eq!(p2.entries[1].file_mode, Some(FileMode::new(0o640).unwrap()));
}

#[test]
fn artifact_payload_omits_optional_fields_when_plaintext() {
    // Plaintext artifact: no encryption block, and entry staging /
    // file_mode are skipped when absent so the on-disk JSON stays lean
    // and a plaintext row never carries a stray `encryption: null`.
    let payload = ArtifactPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        artifact_id: "019e9000-0000-7000-8000-000000000001".to_string(),
        entries: vec![ArtifactEntryPayload {
            role: "metadata.json".to_string(),
            snapshot_artifact: Some(SnapshotArtifact::Metadata),
            local_path: "/space/snap/metadata.json".to_string(),
            s3_uri: "s3://b/snap/metadata.json?region=us-east-2".to_string(),
            staged_path: None,
            file_mode: None,
        }],
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
    };
    let json = payload.to_json();
    assert!(!json.contains("encryption"), "json: {json}");
    assert!(!json.contains("staged_path"), "json: {json}");
    assert!(!json.contains("file_mode"), "json: {json}");
}

#[test]
fn transfer_payload_encryption_wire_shape_is_plain_strings() {
    // Pin the on-disk JSON shape: domain newtypes serialize as
    // plain strings (via `serde(try_from / into)`), so a hand-written
    // payload from another release of vol-mgr is interpreted the
    // same way. A regression here (e.g. someone changes OrgId to
    // a tuple representation) would corrupt every persisted job
    // payload across an upgrade.
    let json = r#"{
        "subject_id": "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2",
        "local_path": "/tmp/x",
        "s3_uri": "s3://b/k?region=us-east-2",
        "raw": false,
        "encryption": {
            "organization_id": "019e31d9-79a6-7132-b90f-007f1c62ddea",
            "provider": "in-memory",
            "master_key_identifier": "test/key-1",
            "wrapped_dek": [1, 2, 3, 4]
        }
    }"#;
    let p: TransferPayload = serde_json::from_str(json).expect("plain-string shape must parse");
    let enc = p.encryption.expect("encryption present");
    assert_eq!(
        enc.organization_id.to_string(),
        "019e31d9-79a6-7132-b90f-007f1c62ddea"
    );
    assert_eq!(enc.provider, KekProvider::InMemory);
    assert_eq!(enc.master_key_identifier.as_str(), "test/key-1");
    assert_eq!(enc.wrapped_dek, vec![1, 2, 3, 4]);
}

#[test]
fn transfer_payload_legacy_without_encryption_field_deserializes() {
    // A payload serialized by the pre-encryption binary has no
    // `encryption` field. The default must be `None` so existing
    // queued jobs continue to run unencrypted after the upgrade.
    let legacy = r#"{"subject_id":"sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2","local_path":"/tmp/x","s3_uri":"s3://b/k?region=us-east-2","raw":false}"#;
    let p: TransferPayload = serde_json::from_str(legacy).expect("legacy payload must deserialize");
    assert!(p.encryption.is_none());
}

#[test]
fn file_mode_validates_rwx_bits_and_allows_zero_mode() {
    assert_eq!(FileMode::new(0o000).unwrap().bits(), 0o000);
    assert_eq!(FileMode::new(0o777).unwrap().bits(), 0o777);
    FileMode::new(0o1000).unwrap_err();
}

#[test]
fn chmod_permission_denied_is_ignored_for_safe_superset_modes() {
    assert!(should_ignore_chmod_permission_denied(
        FileModeApplyPolicy::Required,
        0o664,
        0o660,
    ));
}

#[test]
fn chmod_permission_denied_is_ignored_for_existing_readable_files() {
    assert!(should_ignore_chmod_permission_denied(
        FileModeApplyPolicy::ExistingFile,
        0o644,
        0o660,
    ));
}

#[test]
fn chmod_permission_denied_remains_strict_for_unreadable_existing_files() {
    assert!(!should_ignore_chmod_permission_denied(
        FileModeApplyPolicy::ExistingFile,
        0o600,
        0o660,
    ));
}

#[test]
fn dedup_key_distinguishes_raw_from_decompressing_pulls() {
    // Pull jobs with the same (subject, path, uri) but different raw
    // flags MUST NOT collapse onto each other — a raw=true caller
    // would silently receive the earlier raw=false caller's
    // transparently-decompressed result (or vice versa). The dedup
    // key has to carry `raw` so the worker keeps them as distinct
    // jobs.
    let base = ("pull", "sandbox:abc", "/tmp/x", "s3://b/k?region=us");
    let raw_true = dedup_key(base.0, base.1, base.2, base.3, true, Default::default());
    let raw_false = dedup_key(base.0, base.1, base.2, base.3, false, Default::default());
    assert_eq!(
        raw_false,
        "pull\0sandbox:abc\0/tmp/x\0s3://b/k?region=us\0false\0-\0legacy_skip_present\0-",
        "an unspecified artifact must retain the pre-upgrade key shape"
    );
    assert_ne!(
        raw_true, raw_false,
        "raw=true and raw=false on the same target must produce distinct dedup keys"
    );
    let mode_644 = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        true,
        TransferDedupOptions {
            file_mode: Some(FileMode::new(0o644).unwrap()),
            ..Default::default()
        },
    );
    let mode_660 = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        true,
        TransferDedupOptions {
            file_mode: Some(FileMode::new(0o660).unwrap()),
            ..Default::default()
        },
    );
    assert_ne!(
        mode_644, mode_660,
        "different requested modes must not collapse onto the same pull job"
    );
    let verified = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        true,
        TransferDedupOptions {
            file_mode: Some(FileMode::new(0o644).unwrap()),
            existing_file_policy: PullExistingPolicy::VerifiedReplica,
            snapshot_artifact: None,
            materialization_mode: MaterializationMode::Disabled,
            encryption: None,
        },
    );
    assert_ne!(
        mode_644, verified,
        "legacy and verified existing-file policies must not collapse"
    );
    let memory = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        true,
        TransferDedupOptions {
            snapshot_artifact: Some(SnapshotArtifact::Memory),
            ..Default::default()
        },
    );
    let rootfs = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        true,
        TransferDedupOptions {
            snapshot_artifact: Some(SnapshotArtifact::Rootfs),
            ..Default::default()
        },
    );
    assert_ne!(
        memory, rootfs,
        "different snapshot materialization policies must not collapse"
    );
    assert_ne!(
        raw_true, memory,
        "an explicit snapshot identity must not use the legacy key"
    );
    let key_for_materialization = |materialization_mode| {
        dedup_key(
            base.0,
            base.1,
            base.2,
            base.3,
            true,
            TransferDedupOptions {
                snapshot_artifact: Some(SnapshotArtifact::Memory),
                materialization_mode,
                ..Default::default()
            },
        )
    };
    let shadow = key_for_materialization(MaterializationMode::Shadow);
    let enabled = key_for_materialization(MaterializationMode::Enabled);
    assert_ne!(memory, shadow, "dense and shadow pulls must not collapse");
    assert_ne!(
        shadow, enabled,
        "shadow and enabled pulls must not collapse"
    );
}

#[test]
fn dedup_key_distinguishes_different_encryption_keys() {
    // Two callers asking to encrypt the same (subject, path, uri)
    // with different DEKs produce different ciphertext on S3, so
    // they MUST NOT collapse onto a single in-flight job — the
    // second submitter would otherwise silently inherit the first
    // request's wrap and the result would fail to decrypt on its
    // pull. The encryption fingerprint in the key is what prevents
    // that collapse.
    let base = ("push", "sandbox:abc", "/tmp/x", "s3://b/k?region=us");
    let mk_enc = |wrap: &[u8]| TransferEncryption {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".parse().unwrap(),
        provider: KekProvider::InMemory,
        master_key_identifier: "test/key-1".parse().unwrap(),
        wrapped_dek: wrap.to_vec(),
    };
    let enc_a = mk_enc(b"wrap-bytes-A");
    let enc_b = mk_enc(b"wrap-bytes-B");

    let none = dedup_key(base.0, base.1, base.2, base.3, false, Default::default());
    let key_a = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        false,
        TransferDedupOptions {
            encryption: Some(&enc_a),
            ..Default::default()
        },
    );
    let key_b = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        false,
        TransferDedupOptions {
            encryption: Some(&enc_b),
            ..Default::default()
        },
    );

    assert_ne!(none, key_a, "encrypted-vs-plain must not collapse");
    assert_ne!(key_a, key_b, "different wrapped DEKs must not collapse");

    // Same encryption fingerprint must produce the same dedup key —
    // otherwise legitimate dedup of an idempotent re-submission breaks.
    let key_a_again = dedup_key(
        base.0,
        base.1,
        base.2,
        base.3,
        false,
        TransferDedupOptions {
            encryption: Some(&enc_a),
            ..Default::default()
        },
    );
    assert_eq!(
        key_a, key_a_again,
        "identical encryption fingerprints must produce the same dedup key"
    );
}

#[test]
fn transfer_payload_raw_suppresses_transparent_decompress() {
    // Template-manager pulls a zstd rootfs and runs its own
    // SHA-validating decoder over the wire bytes, so it needs the object
    // verbatim. `raw = true` must suppress vol-mgr's transparent in-place
    // decompress; otherwise the caller gets a plain ext4 image where it
    // expected zstd and its decoder dead-ends. `raw = false` (the
    // default) keeps the symmetric decompress-on-zstd behavior.
    let plain = TransferPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        local_path: "/tmp/x".to_string(),
        s3_uri: "s3://b/k?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: None,
        staged_path: None,
        raw: false,
        file_mode: None,
        existing_file_policy: PullExistingPolicy::LegacySkipPresent,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: None,
        entry_role: None,
    };
    let raw = TransferPayload {
        raw: true,
        ..plain.clone()
    };
    // Default (non-raw) path: transparent decompress of zstd objects.
    assert!(plain.should_transparent_decompress(true));
    assert!(!plain.should_transparent_decompress(false));
    // Template-manager path: raw=true MUST suppress decompress regardless
    // of zstd magic.
    assert!(!raw.should_transparent_decompress(true));
    assert!(!raw.should_transparent_decompress(false));
}

#[tokio::test]
async fn stage_encrypted_temp_returns_partial_enc_path_on_aad_derivation_error() {
    // On any failure in the encryption stage, `stage_encrypted_temp`
    // must return the `.enc.tmp` path alongside the error so the caller
    // routes it through the same cleanup loop the success path uses. A
    // partial or zero-byte temp left under /space/cache would otherwise
    // leak until the next process-restart sweep.
    //
    // Drive the failure via `derive_artifact_aads`: a `template:*`
    // subject_id is a permanent client error there (encrypted transfers
    // are sandbox-scoped), which short-circuits before any I/O. The
    // returned partial path is the deterministic
    // `encrypted_sibling(post_compress)` — proving the caller has the
    // path it needs to clean up even on the earliest stage failure.
    let dir = tempfile::tempdir().unwrap();
    let post_compress = dir.path().join("artifact.zst");
    tokio::fs::write(&post_compress, b"fake compressed bytes")
        .await
        .unwrap();
    let expected_enc_path = encrypted_sibling(&post_compress);

    let job = Job {
        job_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".parse().unwrap(),
        kind: "push".to_string(),
        status: "running".to_string(),
        payload: "{}".to_string(),
        subject_id: None,
        dedup_key: None,
        failure: None,
        attempts: 0,
        max_attempts: 1,
        bytes_processed: 0,
        error: None,
        created_at: String::new(),
        started_at: None,
        completed_at: None,
        traceparent: None,
        lease_owner: None,
        priority: Default::default(),
    };
    let p = TransferPayload {
        // Template subject is unconditionally rejected by
        // `derive_artifact_aads` (before the artifact_id/role checks) —
        // gives us a deterministic Err.
        subject_id: "template:seed".to_string(),
        local_path: post_compress.to_string_lossy().into_owned(),
        s3_uri: "s3://b/k?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: Some(SnapshotArtifact::Memory),
        staged_path: None,
        raw: false,
        file_mode: None,
        existing_file_policy: PullExistingPolicy::LegacySkipPresent,
        materialization_mode: MaterializationMode::Disabled,
        rootfs_materialization_mode: None,
        encryption: None,
        artifact_id: Some("019e9000-0000-7000-8000-000000000001".to_string()),
        entry_role: Some("memfile".to_string()),
    };
    let enc = TransferEncryption {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".parse().unwrap(),
        provider: KekProvider::InMemory,
        master_key_identifier: "test/key-1".parse().unwrap(),
        wrapped_dek: vec![0u8; 16],
    };

    // `compress = false` so the test exercises the AAD-derivation
    // failure before any zstd init; the early-Err path is the same
    // regardless of the compress branch.
    let result = stage_encrypted_temp(&job, &p, &enc, &post_compress, false, None, None).await;
    let (partial_enc_path, err) =
        result.expect_err("template subject must fail derive_artifact_aads");
    assert_eq!(
        partial_enc_path, expected_enc_path,
        "Err arm must return the same path encrypted_sibling would, so the caller can include it in cleanup",
    );
    assert!(
        matches!(err, JobError::Permanent(_)),
        "template subject is a permanent client error, not retriable",
    );
}

// ── MaybeZstdDecodingWriter (pull-side chained decode) ─────────────

fn zstd_compress(plaintext: &[u8]) -> Vec<u8> {
    zstd::encode_all(plaintext, 3).unwrap()
}

#[test]
fn maybe_zstd_writer_decompresses_when_magic_matches_and_requested() {
    // Common case for an encrypted-and-compressed BYOK pull: the
    // decrypt stage emits zstd-magic bytes followed by the rest
    // of the compressed frame; the writer pipes everything
    // through the decoder and the destination ends up holding
    // the original plaintext.
    let plaintext: Vec<u8> = (0u32..200_000).map(|i| (i % 251) as u8).collect();
    let compressed = zstd_compress(&plaintext);
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = MaybeZstdDecodingWriter::new(&mut buf, /* decompress_if_zstd */ true);
    // Write in two odd-sized chunks straddling the magic boundary
    // so the peek-then-decide branch exercises both "fewer than 4
    // bytes pending" and "decision made mid-write".
    let (first, second) = compressed.split_at(2);
    writer.write_all(first).unwrap();
    writer.write_all(second).unwrap();
    let decompressed = writer.finish().unwrap();
    assert!(
        decompressed,
        "magic matched + opt-in → must have decompressed"
    );
    assert_eq!(buf, plaintext);
}

#[test]
fn maybe_zstd_writer_preserves_raw_bytes_when_opt_out() {
    // Template-manager pulls set `raw = true` so its own
    // SHA-validating decoder sees the same wire bytes vol-mgr
    // fetched. The chained writer with `decompress_if_zstd =
    // false` must therefore NOT decompress even when the bytes
    // start with zstd magic.
    let plaintext = b"any bytes here";
    let compressed = zstd_compress(plaintext);
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = MaybeZstdDecodingWriter::new(&mut buf, /* decompress_if_zstd */ false);
    writer.write_all(&compressed).unwrap();
    let decompressed = writer.finish().unwrap();
    assert!(!decompressed, "raw mode must keep bytes compressed");
    assert_eq!(buf, compressed);
}

#[test]
fn maybe_zstd_writer_passes_non_magic_bytes_through() {
    // Plaintext that doesn't start with the zstd magic — even
    // when the caller asks for decompression — must land
    // unchanged. Guards against a misfire where the writer
    // would route arbitrary plaintext through the zstd decoder.
    let plaintext = b"hello, plaintext stream";
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = MaybeZstdDecodingWriter::new(&mut buf, true);
    writer.write_all(plaintext).unwrap();
    let decompressed = writer.finish().unwrap();
    assert!(!decompressed, "non-magic input must not be decompressed");
    assert_eq!(buf, plaintext);
}

#[test]
fn maybe_zstd_writer_handles_sub_magic_input() {
    // An adversarial edge case: plaintext shorter than the
    // 4-byte magic. The writer must still flush whatever it
    // saw on `finish()` rather than swallow the bytes.
    let plaintext = b"hi";
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = MaybeZstdDecodingWriter::new(&mut buf, true);
    writer.write_all(plaintext).unwrap();
    let decompressed = writer.finish().unwrap();
    assert!(!decompressed);
    assert_eq!(buf, plaintext);
}

#[test]
fn encrypt_then_decrypt_via_maybe_zstd_writer_round_trips() {
    // End-to-end of the chained pull path against the chained push
    // path: compress (via zstd::stream::read::Encoder) + AEAD
    // encrypt → AEAD decrypt + maybe-decompress. Mirrors what the
    // sandbox-manager BYOK path drives in production: an encrypted
    // and compressed S3 object pulled back as the original
    // plaintext, no `.plaintext.tmp` intermediate.
    use crate::crypto::{decrypt_stream_blocking, encrypt_stream_blocking};
    let plaintext: Vec<u8> = (0u32..3 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
    let key: Vec<u8> = (0..32u8).collect();
    let aad = b"space-test/v1";

    // Push: source → zstd → AEAD → ciphertext_buf
    let mut ciphertext_buf: Vec<u8> = Vec::new();
    let mut zstd_reader = zstd::stream::read::Encoder::new(plaintext.as_slice(), 3).unwrap();
    encrypt_stream_blocking(&mut zstd_reader, &mut ciphertext_buf, &key, aad).unwrap();
    let ciphertext_head = ciphertext_buf
        .get(crate::crypto::HEADER_LEN..)
        .and_then(|body| body.get(..16))
        .expect("test ciphertext contains header plus at least one encrypted block");
    let plaintext_head = plaintext
        .get(..16)
        .expect("test plaintext contains at least 16 bytes");
    assert_ne!(
        ciphertext_head, plaintext_head,
        "ciphertext head must differ from plaintext head"
    );

    // Pull: ciphertext_buf → AEAD decrypt → MaybeZstdDecodingWriter
    // → recovered plaintext
    let mut recovered: Vec<u8> = Vec::new();
    let mut writer = MaybeZstdDecodingWriter::new(&mut recovered, true);
    let mut ciphertext_reader = std::io::Cursor::new(&ciphertext_buf);
    decrypt_stream_blocking(&mut ciphertext_reader, &mut writer, &key, aad).unwrap();
    let decompressed = writer.finish().unwrap();
    assert!(
        decompressed,
        "BYOK + compressed must round-trip via decoder"
    );
    assert_eq!(
        recovered, plaintext,
        "decrypt + decompress must recover original bytes"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn encrypted_compressed_memory_restore_is_sparse() {
    use crate::crypto::{encrypt_stream_blocking, encrypted_sibling};

    const MEMORY_BYTES: usize = 8 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    // PullFilesAsync permits callers to choose an arbitrary destination
    // name. The persisted memfile role, not this basename, must select
    // sparse materialization.
    let local_path = dir.path().join("opaque-restore-destination");
    let ciphertext_path = encrypted_sibling(&local_path);
    let mut expected = vec![0; MEMORY_BYTES];
    expected[17..20].copy_from_slice(b"ram");
    expected[MEMORY_BYTES / 2..MEMORY_BYTES / 2 + 4].copy_from_slice(b"live");

    let encryption = TransferEncryption {
        organization_id: "019e31d9-79a6-7132-b90f-007f1c62ddea".parse().unwrap(),
        provider: KekProvider::InMemory,
        master_key_identifier: "test/key-1".parse().unwrap(),
        wrapped_dek: vec![0; 16],
    };
    let payload = TransferPayload {
        subject_id: "sandbox:019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        local_path: local_path.to_string_lossy().into_owned(),
        s3_uri: "s3://bucket/snapshots/a/memfile?region=us-east-2".to_string(),
        transfer_scope_id: None,
        snapshot_artifact: Some(SnapshotArtifact::Memory),
        staged_path: None,
        raw: false,
        file_mode: None,
        existing_file_policy: PullExistingPolicy::LegacySkipPresent,
        materialization_mode: MaterializationMode::Enabled,
        rootfs_materialization_mode: None,
        encryption: Some(encryption.clone()),
        artifact_id: Some("019e9000-0000-7000-8000-000000000001".to_string()),
        entry_role: Some("memfile".to_string()),
    };
    let (_, entry_aad) = derive_artifact_aads(&payload, encryption.organization_id).unwrap();
    let key: Vec<u8> = (0..32u8).collect();
    let compressed = zstd_compress(&expected);
    let mut ciphertext = Vec::new();
    encrypt_stream_blocking(
        &mut std::io::Cursor::new(compressed),
        &mut ciphertext,
        &key,
        &entry_aad,
    )
    .unwrap();
    std::fs::write(&ciphertext_path, ciphertext).unwrap();

    let (plaintext_path, was_decompressed, report) = stage_decrypted_temp(
        &test_job("pull"),
        &payload,
        &encryption,
        &ciphertext_path,
        DecryptedPullOptions {
            decompress_if_zstd: true,
            materialization_mode: MaterializationMode::Enabled,
        },
        Some(&key),
        None,
    )
    .await
    .unwrap();

    assert!(was_decompressed);
    assert_eq!(report.mode, MaterializationMode::Enabled);
    assert!(report.candidate_hole_bytes > (MEMORY_BYTES / 2) as u64);
    assert_eq!(std::fs::read(&plaintext_path).unwrap(), expected);
    let logical_bytes = std::fs::metadata(&plaintext_path).unwrap().len();
    let physical_bytes = allocated_bytes(&plaintext_path);
    assert!(
        physical_bytes < logical_bytes / 4,
        "encrypted compressed memory must use sparse output: logical={logical_bytes} physical={physical_bytes}"
    );

    std::fs::remove_file(&plaintext_path).unwrap();
    let (plaintext_path, was_decompressed, report) = stage_decrypted_temp(
        &test_job("pull"),
        &payload,
        &encryption,
        &ciphertext_path,
        DecryptedPullOptions {
            decompress_if_zstd: true,
            materialization_mode: MaterializationMode::Shadow,
        },
        Some(&key),
        None,
    )
    .await
    .unwrap();
    assert!(was_decompressed);
    assert_eq!(report.mode, MaterializationMode::Shadow);
    assert_eq!(report.candidate_hole_bytes, 0);
    assert_eq!(std::fs::read(&plaintext_path).unwrap(), expected);
    let logical_bytes = std::fs::metadata(&plaintext_path).unwrap().len();
    let physical_bytes = allocated_bytes(&plaintext_path);
    assert!(
        physical_bytes >= logical_bytes,
        "encrypted shadow output must remain dense: logical={logical_bytes} physical={physical_bytes}"
    );
    assert!(
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .contains("sparse-shadow")),
        "encrypted shadow decompression must not create a sparse candidate inline"
    );

    std::fs::remove_file(&plaintext_path).unwrap();
    let (plaintext_path, was_decompressed, report) = stage_decrypted_temp(
        &test_job("pull"),
        &payload,
        &encryption,
        &ciphertext_path,
        DecryptedPullOptions {
            decompress_if_zstd: true,
            materialization_mode: MaterializationMode::Disabled,
        },
        Some(&key),
        None,
    )
    .await
    .unwrap();
    assert!(was_decompressed);
    assert_eq!(report, DecompressionReport::default());
    assert_eq!(std::fs::read(&plaintext_path).unwrap(), expected);
    let logical_bytes = std::fs::metadata(&plaintext_path).unwrap().len();
    let physical_bytes = allocated_bytes(&plaintext_path);
    assert!(
        physical_bytes >= logical_bytes,
        "disabled sparse decompression must preserve dense allocation: logical={logical_bytes} physical={physical_bytes}"
    );

    std::fs::remove_file(&plaintext_path).unwrap();
    let mut ciphertext = Vec::new();
    encrypt_stream_blocking(
        &mut std::io::Cursor::new(&expected),
        &mut ciphertext,
        &key,
        &entry_aad,
    )
    .unwrap();
    std::fs::write(&ciphertext_path, ciphertext).unwrap();
    let (plaintext_path, was_decompressed, report) = stage_decrypted_temp(
        &test_job("pull"),
        &payload,
        &encryption,
        &ciphertext_path,
        DecryptedPullOptions {
            decompress_if_zstd: true,
            materialization_mode: MaterializationMode::Enabled,
        },
        Some(&key),
        None,
    )
    .await
    .unwrap();
    assert!(!was_decompressed);
    assert_eq!(report, DecompressionReport::default());
    assert_eq!(std::fs::read(&plaintext_path).unwrap(), expected);
    let logical_bytes = std::fs::metadata(&plaintext_path).unwrap().len();
    let physical_bytes = allocated_bytes(&plaintext_path);
    assert!(
        physical_bytes >= logical_bytes,
        "uncompressed encrypted memory must remain dense: logical={logical_bytes} physical={physical_bytes}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn benchmark_production_shaped_sparse_accounting_with_metrics() {
    if std::env::var_os("SPACE_RUN_SPARSE_ACCOUNTING_BENCHMARK").is_none() {
        return;
    }

    use std::net::UdpSocket;
    use std::os::unix::fs::MetadataExt;

    const LOGICAL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
    const CHUNK_BYTES: usize = STREAM_WRITEBACK_WINDOW_BYTES as usize;
    let dogstatsd_port = std::env::var("DD_DOGSTATSD_PORT")
        .expect("benchmark requires DD_DOGSTATSD_PORT")
        .parse::<u16>()
        .expect("DD_DOGSTATSD_PORT must be a port");
    let metric_socket =
        UdpSocket::bind(("127.0.0.1", dogstatsd_port)).expect("bind benchmark DogStatsD receiver");
    metric_socket
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("configure benchmark DogStatsD receiver");
    let stop_metrics = Arc::new(AtomicBool::new(false));
    let outstanding_metric_calls = Arc::new(AtomicUsize::new(0));
    let receiver_stop = Arc::clone(&stop_metrics);
    let receiver_count = Arc::clone(&outstanding_metric_calls);
    let receiver = std::thread::spawn(move || {
        let mut payload = [0_u8; 2048];
        while !receiver_stop.load(Ordering::Relaxed) {
            match metric_socket.recv(&mut payload) {
                Ok(length)
                    if std::str::from_utf8(&payload[..length]).is_ok_and(|payload| {
                        payload.contains("space.volume_manager.stream_writeback.outstanding_bytes:")
                    }) =>
                {
                    receiver_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => panic!("benchmark DogStatsD receive failed: {error}"),
            }
        }
    });
    let _telemetry = pplx_space_common_svc::init_service_subprocess(
        "space-volume-manager-sparse-accounting-benchmark",
    )
    .expect("initialize benchmark telemetry");

    let dir = tempfile::tempdir().expect("create benchmark directory");
    let path = dir.path().join("memory.snap");
    let timing = Arc::new(StreamPullTiming::default());
    let coordinator = StreamWritebackCoordinator::new(DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES, None);
    let mut sink = DecryptedFileSink::new(
        std::fs::File::create(&path).expect("create benchmark output"),
        MaterializationMode::Enabled,
        MEMORY_SPARSE_BLOCK_BYTES,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        Some(Arc::clone(&coordinator)),
        Some(Arc::clone(&timing)),
        JobPriority::default(),
    )
    .expect("create benchmark sink");
    let mut chunk = vec![0_u8; CHUNK_BYTES];
    for page in chunk.chunks_mut(MEMORY_SPARSE_BLOCK_BYTES).step_by(10) {
        page.fill(0xa5);
    }

    let wall_started_at = Instant::now();
    let cpu_started_at = current_thread_cpu_us();
    for _ in 0..LOGICAL_BYTES / CHUNK_BYTES as u64 {
        sink.write_all(&chunk).expect("write benchmark chunk");
    }
    sink.complete().expect("complete benchmark output");
    let wall_ms = wall_started_at.elapsed().as_millis();
    let total_cpu_us = current_thread_cpu_us().saturating_sub(cpu_started_at);
    let output_us = StreamPullTiming::load(&timing.output_writer_us);
    let output_cpu_us = StreamPullTiming::load(&timing.output_writer_cpu_us);
    let reserve_cpu_us = StreamPullTiming::load(&timing.writeback_reserve_cpu_us);
    let accounting_cpu_us = total_cpu_us
        .saturating_sub(output_cpu_us)
        .saturating_sub(reserve_cpu_us);
    let metadata = std::fs::metadata(&path).expect("stat benchmark output");
    let physical_bytes = metadata.blocks() * 512;
    assert_eq!(metadata.len(), LOGICAL_BYTES);
    assert!(
        (8..=12).contains(&(physical_bytes.saturating_mul(100) / LOGICAL_BYTES)),
        "benchmark allocation must remain near 10%: logical={LOGICAL_BYTES} physical={physical_bytes}"
    );

    std::thread::sleep(Duration::from_millis(1_100));
    stop_metrics.store(true, Ordering::Relaxed);
    receiver.join().expect("join benchmark metric receiver");
    println!(
        "SPARSE_ACCOUNTING_BENCHMARK logical_bytes={LOGICAL_BYTES} physical_bytes={physical_bytes} wall_ms={wall_ms} output_ms={} output_cpu_ms={} accounting_cpu_ms={} metric_calls={}",
        output_us / 1_000,
        output_cpu_us / 1_000,
        accounting_cpu_us / 1_000,
        outstanding_metric_calls.load(Ordering::Relaxed),
    );
}
