use super::*;
use bytes::Bytes;
use pplx_space_common_core::types::SandboxId;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct CopyStore {
    source: Option<Vec<u8>>,
    destination: Mutex<Option<Vec<u8>>>,
    corrupt: bool,
    copies: AtomicUsize,
}

#[async_trait::async_trait]
impl ObjectStore for CopyStore {
    async fn head(
        &self,
        _: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<Option<ObjectMeta>, ObjectStoreError> {
        let size = if target.key == "source" {
            self.source.as_ref().map(Vec::len)
        } else {
            self.destination.lock().unwrap().as_ref().map(Vec::len)
        };
        Ok(size.map(|size| ObjectMeta {
            size: size as u64,
            etag: None,
        }))
    }
    async fn copy_object(
        &self,
        _: &pplx_space_object_store::ObjectCredentials,
        source: ObjectTarget<'_>,
        destination: ObjectTarget<'_>,
        size: u64,
    ) -> Result<(), ObjectStoreError> {
        assert_eq!(source.key, "source");
        assert_eq!(destination.key, "destination");
        let mut bytes = self.source.clone().expect("source exists");
        assert_eq!(size, bytes.len() as u64);
        self.copies.fetch_add(1, Ordering::SeqCst);
        if self.corrupt {
            bytes.pop();
        }
        *self.destination.lock().unwrap() = Some(bytes);
        Ok(())
    }
    async fn put_bytes(
        &self,
        _: &pplx_space_object_store::ObjectCredentials,
        _: ObjectTarget<'_>,
        _: Bytes,
    ) -> Result<(), ObjectStoreError> {
        panic!("server-side copies must not upload payload bytes")
    }
    async fn get_bytes(
        &self,
        _: &pplx_space_object_store::ObjectCredentials,
        _: ObjectTarget<'_>,
    ) -> Result<Vec<u8>, ObjectStoreError> {
        panic!("server-side copies must not download payload bytes")
    }
    async fn put_file(
        &self,
        _: &pplx_space_object_store::ObjectCredentials,
        _: ObjectTarget<'_>,
        _: &Path,
    ) -> Result<u64, ObjectStoreError> {
        panic!("server-side copies must not stage an upload")
    }
    async fn get_file(
        &self,
        _: &pplx_space_object_store::ObjectCredentials,
        _: ObjectTarget<'_>,
        _: &Path,
    ) -> Result<u64, ObjectStoreError> {
        panic!("server-side copies must not stage a download")
    }
}

fn request() -> CopyObjectRequest {
    CopyObjectRequest {
        subject_id: format!("sandbox:{}", SandboxId::new()),
        source_s3_uri: "s3://example-artifacts/source?region=us-east-1".into(),
        destination_s3_uri: "s3://example-artifacts/destination?region=us-east-1".into(),
    }
}

#[test]
fn durable_copy_payload_retains_exact_source_and_destination_uris() {
    let request = CopyObjectRequest {
        subject_id: "sandbox:019f8000-0000-7000-8000-000000000002".into(),
        source_s3_uri: "s3://example-artifacts/source?endpoint=http://object-store.example:9000&region=us-east-1".into(),
        destination_s3_uri: "s3://example-artifacts/destination?region=us-east-1&endpoint=http://object-store.example:9000".into(),
    };
    let payload = CopyPayload::try_from(request.clone()).unwrap();
    insta::assert_snapshot!(
        "durable_copy_payload",
        serde_json::to_string_pretty(&payload).unwrap()
    );
    let recovered: CopyPayload =
        serde_json::from_str(&serde_json::to_string(&payload).unwrap()).unwrap();
    assert_eq!(CopyObjectRequest::from(recovered), request);
}

fn fixture(source: Option<Vec<u8>>, corrupt: bool) -> (Arc<CopyStore>, TransferExecutor, Job) {
    let store = Arc::new(CopyStore {
        source,
        corrupt,
        destination: Mutex::new(None),
        copies: AtomicUsize::new(0),
    });
    let executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            static_credentials: Some(pplx_space_object_store::ObjectCredentials {
                access_key_id: "fixture".into(),
                secret_access_key: "fixture".into(),
                session_token: None,
            }),
            ..Default::default()
        },
        store.clone(),
    );
    let payload = CopyPayload::try_from(request()).unwrap();
    let mut job = super::super::tests::test_job(KIND);
    job.payload = serde_json::to_string(&payload).unwrap();
    (store, executor, job)
}

#[tokio::test]
async fn copy_preserves_encoded_bytes_without_local_payload_io() {
    for bytes in [vec![], vec![0x28, 0xb5, 0x2f, 0xfd, 0, 255, 42, 13]] {
        let (store, executor, job) = fixture(Some(bytes.clone()), false);
        assert_eq!(
            executor
                .execute(&job, CancellationToken::new())
                .await
                .unwrap(),
            bytes.len() as i64
        );
        assert_eq!(store.destination.lock().unwrap().as_ref(), Some(&bytes));
        assert_eq!(store.copies.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn copy_refuses_missing_sources_and_unverified_destinations() {
    let (store, executor, job) = fixture(None, false);
    let error = executor
        .execute(&job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.failure_reason(), Some(JobFailureReason::NotFound));
    assert!(store.destination.lock().unwrap().is_none());
    assert_eq!(store.copies.load(Ordering::SeqCst), 0);

    let (_, executor, job) = fixture(Some(vec![1, 2, 3]), true);
    assert!(matches!(
        executor
            .execute(&job, CancellationToken::new())
            .await
            .unwrap_err(),
        JobError::Retriable(_)
    ));
}

#[tokio::test]
async fn cancellation_and_invalid_recovered_payloads_cannot_start_a_copy() {
    let (store, executor, mut job) = fixture(Some(vec![1, 2, 3]), false);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        executor.execute(&job, cancellation).await.unwrap_err(),
        JobError::Retriable(_)
    ));
    let mut wire: serde_json::Value = serde_json::from_str(&job.payload).unwrap();
    wire["source_s3_uri"] = "s3://example-artifacts/source?region=us-west-2".into();
    job.payload = serde_json::to_string(&wire).unwrap();
    let error = executor
        .execute(&job, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.failure_reason(), Some(JobFailureReason::InvalidInput));
    assert_eq!(store.copies.load(Ordering::SeqCst), 0);
    assert!(store.destination.lock().unwrap().is_none());
}

#[test]
fn storage_credential_rejections_are_terminal_but_transient_failures_retry() {
    let directory = tempfile::tempdir().unwrap();
    let endpoint = format!(
        "unix://{}/credential-manager.sock",
        directory.path().display()
    );
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_CREDMGR_ENDPOINT",
        Some(std::ffi::OsStr::new(&endpoint)),
    )]) {
        return;
    }
    let endpoint = std::env::var("SPACE_CREDMGR_ENDPOINT").unwrap();
    let socket = endpoint.strip_prefix("unix://").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        let reply = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let response_code = Arc::clone(&reply);
        let router = axum::Router::new().fallback(move || {
            let code = tonic::Code::from_i32(response_code.load(Ordering::SeqCst));
            async move {
                tonic::Status::new(code, "synthetic credential exchange failure")
                    .into_http::<axum::body::Body>()
            }
        });
        let shutdown = CancellationToken::new();
        let stopped = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(stopped.cancelled_owned())
                .await
                .unwrap();
        });
        for (code, permanent) in [
            (tonic::Code::InvalidArgument, true),
            (tonic::Code::NotFound, true),
            (tonic::Code::PermissionDenied, true),
            (tonic::Code::Unauthenticated, true),
            (tonic::Code::FailedPrecondition, true),
            (tonic::Code::Unimplemented, true),
            (tonic::Code::Unavailable, false),
            (tonic::Code::DeadlineExceeded, false),
            (tonic::Code::ResourceExhausted, false),
            (tonic::Code::Aborted, false),
            (tonic::Code::Internal, false),
        ] {
            reply.store(code as i32, Ordering::SeqCst);
            let (store, mut executor, job) = fixture(Some(vec![1, 2, 3]), false);
            executor.config.static_credentials = None;
            let error = executor
                .execute(&job, CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(
                matches!(
                    error,
                    JobError::Permanent(_) | JobError::PermanentWithReason { .. }
                ),
                permanent,
                "{code:?}: permanent credential rejections must allow prompt copy fallback; {error}"
            );
            assert_eq!(store.copies.load(Ordering::SeqCst), 0);
            assert!(store.destination.lock().unwrap().is_none());

            let local_path = directory.path().join("snapshot.luks");
            let mut pull = super::super::tests::test_job("pull");
            pull.payload =
                serde_json::to_string(&super::super::tests::verified_payload(&local_path)).unwrap();
            let error = executor
                .execute(&pull, CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(
                matches!(
                    error,
                    JobError::Permanent(_) | JobError::PermanentWithReason { .. }
                ),
                permanent,
                "{code:?}: a pull for a deleted subject must release its restore promptly; {error}"
            );
        }
        shutdown.cancel();
        server.await.unwrap();
    });
}
