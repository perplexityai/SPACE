use std::collections::BTreeMap;
use std::io::Seek;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use tokio::sync::{Notify, Semaphore};

use super::super::tests::{MockObjectStore, push_payload, test_creds, test_job};
use super::*;

#[derive(Default)]
struct Recorded {
    parts: BTreeMap<(String, usize), Bytes>,
    object: Vec<u8>,
    creates: usize,
    completes: usize,
    aborts: usize,
    puts: usize,
}

struct Server {
    recorded: Mutex<Recorded>,
    short_head: AtomicBool,
    hold_parts: AtomicBool,
    retry_part: AtomicBool,
    part_attempts: AtomicUsize,
    release: Semaphore,
    changed: Notify,
    endpoint: String,
    shutdown: CancellationToken,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Server {
    async fn start() -> Arc<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = Arc::new(Self {
            recorded: Mutex::new(Recorded::default()),
            short_head: AtomicBool::new(false),
            hold_parts: AtomicBool::new(false),
            retry_part: AtomicBool::new(false),
            part_attempts: AtomicUsize::new(0),
            release: Semaphore::new(0),
            changed: Notify::new(),
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            shutdown: CancellationToken::new(),
        });
        let router = Router::new()
            .fallback(handle)
            .layer(DefaultBodyLimit::disable())
            .with_state(Arc::downgrade(&server));
        let shutdown = server.shutdown.clone();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        server
    }

    fn target(&self) -> ObjectTarget<'_> {
        ObjectTarget {
            bucket: "bucket",
            key: "snapshot/memfile",
            region: "us-east-2",
            endpoint: Some(&self.endpoint),
            path_style: true,
        }
    }

    async fn wait_for(&self, check: impl Fn(&Recorded) -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let changed = self.changed.notified();
                if check(&self.recorded.lock().unwrap()) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("server observation deadline");
    }
}

fn query<'a>(uri: &'a Uri, name: &str) -> Option<&'a str> {
    uri.query()?.split('&').find_map(|s| {
        let (key, value) = s.split_once('=').unwrap_or((s, ""));
        (key == name).then_some(value)
    })
}

async fn handle(
    State(server): State<std::sync::Weak<Server>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let Some(server) = server.upgrade() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let xml = |body: String| ([("content-type", "application/xml")], body).into_response();
    match method {
        Method::POST if query(&uri, "uploads").is_some() => {
            let mut state = server.recorded.lock().unwrap();
            state.creates += 1;
            xml(format!(
                "<InitiateMultipartUploadResult><UploadId>u{}</UploadId></InitiateMultipartUploadResult>",
                state.creates
            ))
        }
        Method::PUT => {
            let id = query(&uri, "uploadId").unwrap_or("single").to_owned();
            let number = query(&uri, "partNumber")
                .map(|s| s.parse().unwrap())
                .unwrap_or(0);
            {
                let mut state = server.recorded.lock().unwrap();
                state.puts += 1;
                if let Some(previous) = state.parts.insert((id, number), body.clone()) {
                    assert_eq!(previous, body, "a retried part must replay exact bytes");
                }
                if number == 0 {
                    state.object = body.to_vec();
                }
            }
            server.changed.notify_waiters();
            let attempt = server.part_attempts.fetch_add(1, Ordering::SeqCst);
            if server.retry_part.load(Ordering::SeqCst) && attempt == 0 {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "<Error><Code>SlowDown</Code></Error>",
                )
                    .into_response();
            }
            if server.hold_parts.load(Ordering::SeqCst) {
                tokio::select! {
                    permit = server.release.acquire() => permit.unwrap().forget(),
                    _ = server.shutdown.cancelled() => {},
                }
            }
            (StatusCode::OK, [("etag", format!("\"p{number}\""))]).into_response()
        }
        Method::POST => {
            let id = query(&uri, "uploadId").unwrap();
            let document = std::str::from_utf8(&body).unwrap();
            let mut state = server.recorded.lock().unwrap();
            state.object = document
                .split("<PartNumber>")
                .skip(1)
                .flat_map(|part| {
                    let number: usize = part.split('<').next().unwrap().parse().unwrap();
                    state.parts[&(id.to_owned(), number)].to_vec()
                })
                .collect();
            state.completes += 1;
            xml("<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>".into())
        }
        Method::HEAD => {
            let size = server.recorded.lock().unwrap().object.len();
            let size = if server.short_head.load(Ordering::SeqCst) {
                size.saturating_sub(1)
            } else {
                size
            };
            (
                StatusCode::OK,
                [
                    ("content-length", size.to_string()),
                    ("etag", "\"complete\"".to_owned()),
                ],
            )
                .into_response()
        }
        Method::DELETE => {
            server.recorded.lock().unwrap().aborts += 1;
            server.changed.notify_waiters();
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::NOT_IMPLEMENTED.into_response(),
    }
}

fn executor() -> TransferExecutor {
    let mut executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: true,
            snapshot_upload_mode: SnapshotUploadMode::from_buffer_budget_bytes(64 * MIB).unwrap(),
            codec_concurrency: 2,
            codec_priority_reserve: 1,
            codec_priority_threshold: Some(JobPriority::new(100)),
            ..Default::default()
        },
        Arc::new(S3ObjectStore::new()),
    );
    // One part globally forces real backpressure across a multipart source.
    executor.stream_push = Some(StreamPushConfig {
        budget: UploadBufferBudget::new(5 * MIB).unwrap(),
        producers: Arc::new(Semaphore::new(2)),
        part_bytes: 5 * MIB,
        min_source_bytes: 5 * MIB as u64,
    });
    executor
}

fn synthetic_bytes(size: usize) -> Vec<u8> {
    let mut state = 1u64;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn staged(dir: &Path, artifact: SnapshotArtifact, contents: &[u8]) -> TransferPayload {
    // Local source names differ from S3 artifact keys (memory vs memfile).
    let original = dir.join(match artifact {
        SnapshotArtifact::Memory => "memory",
        SnapshotArtifact::Rootfs => "rootfs.ext4",
        _ => artifact.file_name(),
    });
    let private = dir.join("staged-source");
    std::fs::write(&original, b"caller path is no longer the snapshot").unwrap();
    std::fs::write(&private, contents).unwrap();
    let mut payload = push_payload(&original);
    // Also cover queued payloads that rely on local-name inference.
    payload.snapshot_artifact = None;
    payload.staged_path = Some(private.to_str().unwrap().to_owned());
    payload
}

async fn push(
    executor: &TransferExecutor,
    server: &Server,
    payload: &TransferPayload,
    cancellation: CancellationToken,
) -> Result<i64, JobError> {
    executor
        .execute_push(
            &test_job("push"),
            payload,
            &test_creds(),
            server.target(),
            PushExecutionOptions {
                cleanup_staged: true,
                resolved_dek: None,
                cancellation,
            },
        )
        .await
}

#[tokio::test]
async fn ordinary_and_luks_retry_from_staging_and_verify_actual_wire_size() {
    let server = Server::start().await;
    for artifact in [
        SnapshotArtifact::Memory,
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::RootfsBtrfsSend,
        SnapshotArtifact::EncryptedSnapshotVolume,
        SnapshotArtifact::EncryptedRootfsCow,
    ] {
        let dir = tempfile::tempdir().unwrap();
        // Ciphertext-like allocated data surrounded by zero/sparse space. No
        // crypto transform is selected by volume-manager for a LUKS artifact.
        // Cross two consumed-input eviction windows, with allocated data
        // straddling the first boundary and a final partial window of holes.
        let mut bytes = vec![0; 130 * MIB];
        bytes[60 * MIB..68 * MIB].copy_from_slice(&synthetic_bytes(8 * MIB));
        let payload = staged(dir.path(), artifact, &bytes);
        // Exercise actual holes as well as allocated ciphertext-like bytes.
        let mut source = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(payload.upload_source())
            .unwrap();
        source.set_len(bytes.len() as u64).unwrap();
        source
            .seek(std::io::SeekFrom::Start(60 * MIB as u64))
            .unwrap();
        source.write_all(&bytes[60 * MIB..68 * MIB]).unwrap();
        drop(source);
        let (temp, logical_size) = compress_to_temp(Path::new(payload.upload_source()), None)
            .await
            .unwrap();
        assert_eq!(logical_size, bytes.len() as u64);
        let file_encoded = tokio::fs::read(&temp).await.unwrap();
        tokio::fs::remove_file(temp).await.unwrap();
        let persisted = payload.to_json();
        let creates_before = server.recorded.lock().unwrap().creates;
        server.short_head.store(true, Ordering::SeqCst);
        server.retry_part.store(true, Ordering::SeqCst);
        let first = push(&executor(), &server, &payload, CancellationToken::new()).await;
        assert!(matches!(first, Err(JobError::Retriable(_))));
        assert!(
            Path::new(payload.upload_source()).exists(),
            "verification failure retains retry source"
        );
        assert!(
            !Path::new(payload.upload_source())
                .with_extension("zst.tmp")
                .exists()
        );
        // A new executor and persisted payload model the restart boundary: no
        // in-memory multipart/codec state is required to retry the whole object.
        server.short_head.store(false, Ordering::SeqCst);
        let restored = TransferPayload::from_json(&persisted).unwrap();
        assert_eq!(
            push(&executor(), &server, &restored, CancellationToken::new())
                .await
                .unwrap(),
            bytes.len() as i64
        );
        assert!(
            !Path::new(payload.upload_source()).exists(),
            "only verified terminal success cleans staging"
        );
        let state = server.recorded.lock().unwrap();
        assert_eq!(
            state.creates,
            creates_before + 2,
            "each attempt starts a fresh multipart upload"
        );
        assert!(state.parts.values().all(|part| part.len() <= 5 * MIB));
        let object = state.object.clone();
        drop(state);
        assert!(object.len() < bytes.len());
        assert_eq!(
            object, file_encoded,
            "streaming preserves the file encoder output for {artifact:?}"
        );
        assert_eq!(zstd::decode_all(object.as_slice()).unwrap(), bytes);
        assert_eq!(
            std::fs::read(&payload.local_path).unwrap(),
            b"caller path is no longer the snapshot"
        );
    }
}

#[tokio::test]
async fn deterministic_stream_failures_are_permanent_and_join_the_producer() {
    for invalid_budget in [false, true] {
        let server = Server::start().await;
        let dir = tempfile::tempdir().unwrap();
        let bytes = synthetic_bytes(6 * MIB);
        let payload = staged(dir.path(), SnapshotArtifact::Memory, &bytes);
        let mut executor = executor();
        if invalid_budget {
            executor.stream_push.as_mut().unwrap().budget = UploadBufferBudget::new(MIB).unwrap();
        }
        let max_bytes = if invalid_budget { 12 * MIB } else { MIB };
        let error = executor
            .execute_stream_push(
                &test_job("push"),
                &payload,
                &test_creds(),
                server.target(),
                StreamPushAttempt {
                    uploader: executor.store.streaming_upload().unwrap(),
                    config: executor.stream_push.as_ref().unwrap(),
                    options: StreamingUploadOptions::new(5 * MIB, 2, max_bytes as u64).unwrap(),
                    original_size: bytes.len() as u64,
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, JobError::Permanent(_)), "{error:?}");
        assert_eq!(std::fs::read(payload.upload_source()).unwrap(), bytes);
        assert_eq!(server.recorded.lock().unwrap().completes, 0);
        assert_eq!(executor.codec.shared_available(), 1);
        assert_eq!(
            executor
                .stream_push
                .as_ref()
                .unwrap()
                .producers
                .available_permits(),
            2
        );
    }
}

#[tokio::test]
async fn cancel_and_drop_under_backpressure_release_codec_and_abort_without_completion() {
    for drop_future in [false, true] {
        let server = Server::start().await;
        server.hold_parts.store(true, Ordering::SeqCst);
        let dir = tempfile::tempdir().unwrap();
        let payload = staged(
            dir.path(),
            SnapshotArtifact::Memory,
            &synthetic_bytes(20 * MIB),
        );
        let executor = Arc::new(executor());
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let executor = executor.clone();
            let server = server.clone();
            let payload = payload.clone();
            let cancellation = cancellation.clone();
            async move { push(&executor, &server, &payload, cancellation).await }
        });
        server.wait_for(|s| s.puts > 0).await;
        assert!(
            !Path::new(payload.upload_source())
                .with_extension("zst.tmp")
                .exists(),
            "streaming must never create a compressed temporary file"
        );
        let cancelled_waiter = CancellationToken::new();
        cancelled_waiter.cancel();
        tokio::time::timeout(
            Duration::from_secs(1),
            push(&executor, &server, &payload, cancelled_waiter),
        )
        .await
        .expect("cancelled admission exits promptly")
        .expect_err("cancelled admission cannot upload");
        assert_eq!(
            server.recorded.lock().unwrap().creates,
            1,
            "cancelled admission must not open an upload"
        );
        assert_eq!(
            executor
                .stream_push
                .as_ref()
                .unwrap()
                .producers
                .available_permits(),
            1,
            "the live producer remains bounded while its codec slot is lent"
        );
        let borrowed_codec = tokio::time::timeout(
            Duration::from_secs(5),
            executor.codec.acquire(CodecPriority::Normal),
        )
        .await
        .expect("a backpressured encoder must lend its idle codec slot")
        .unwrap();
        assert_eq!(
            executor.codec.reserve_available(),
            1,
            "protected restore admission is untouched"
        );
        if drop_future {
            task.abort();
        } else {
            cancellation.cancel();
        }
        let outcome = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap();
        assert!(outcome.is_err() || outcome.unwrap().is_err());
        drop(borrowed_codec);
        let permit = tokio::time::timeout(
            Duration::from_secs(10),
            executor.codec.acquire(CodecPriority::Normal),
        )
        .await
        .unwrap()
        .unwrap();
        drop(permit);
        server.wait_for(|s| s.aborts > 0).await;
        assert_eq!(server.recorded.lock().unwrap().completes, 0);
        assert!(Path::new(payload.upload_source()).exists());
        server.shutdown.cancel();
    }
}

#[tokio::test]
async fn cancellation_while_reacquiring_codec_keeps_producer_bounded() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let executor = executor();
    let config = executor.stream_push.as_ref().unwrap();
    let (mut reader, mut pipe) = tokio::io::duplex(1);
    pipe.write_all(&[0]).await.unwrap();
    let cancellation = CancellationToken::new();
    let mut output = PipeWriter {
        writer: pipe,
        runtime: tokio::runtime::Handle::current(),
        cancellation: cancellation.clone(),
        admission: ProducerAdmission {
            codec: executor
                .codec
                .acquire_slot(CodecPriority::Normal)
                .await
                .unwrap(),
            _producer: config.producers.clone().acquire_owned().await.unwrap(),
        },
        waited: Duration::ZERO,
    };
    let task = tokio::task::spawn_blocking(move || output.write(&[1]));
    let borrowed = tokio::time::timeout(
        Duration::from_secs(5),
        executor.codec.acquire(CodecPriority::Normal),
    )
    .await
    .expect("blocked writer must lend its codec slot")
    .unwrap();
    assert_eq!(config.producers.available_permits(), 1);
    assert_eq!(reader.read_u8().await.unwrap(), 0);
    assert_eq!(reader.read_u8().await.unwrap(), 1);
    // The pipe write succeeded, but Write cannot return to the encoder while
    // another codec owns the slot. Cancellation must interrupt that reacquire.
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .expect_err("encoder must not resume without codec admission");
    assert_eq!(config.producers.available_permits(), 2);
    drop(borrowed);
    assert_eq!(executor.codec.shared_available(), 1);
}

#[tokio::test]
async fn producer_error_after_eof_never_completes_an_object() {
    let server = Server::start().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    std::fs::write(&path, synthetic_bytes(6 * MIB)).unwrap();
    let executor = executor();
    let source = compression_source(
        std::fs::File::open(path).unwrap(),
        7 * MIB as u64,
        ProducerAdmission {
            codec: executor
                .codec
                .acquire_slot(CodecPriority::Normal)
                .await
                .unwrap(),
            _producer: executor
                .stream_push
                .as_ref()
                .unwrap()
                .producers
                .clone()
                .acquire_owned()
                .await
                .unwrap(),
        },
        CancellationToken::new(),
        "memory",
    );
    let store = S3ObjectStore::new();
    let error = store
        .put_stream(
            &test_creds(),
            server.target(),
            source,
            UploadBufferBudget::new(5 * MIB).unwrap(),
            StreamingUploadOptions::new(5 * MIB, 2, 12 * MIB as u64).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamingUploadError::Source(_)));
    assert_eq!(executor.codec.shared_available(), 1);
    let state = server.recorded.lock().unwrap();
    assert_eq!(state.completes, 0);
    assert_eq!(state.aborts, 1);
}

#[tokio::test]
async fn compatibility_paths_keep_file_uploads_and_small_sources_bypass_codec_queue() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = vec![7; 6 * MIB];
    let payload = staged(dir.path(), SnapshotArtifact::Memory, &bytes);
    let mut encoded = Vec::new();
    zstd::stream::copy_encode(bytes.as_slice(), &mut encoded, ZSTD_LEVEL).unwrap();
    let store = Arc::new(MockObjectStore::new(
        [Some(ObjectMeta {
            size: encoded.len() as u64,
            etag: None,
        })],
        Vec::new(),
    ));
    let mut executor = TransferExecutor::new_for_test(
        TransferExecutorConfig {
            compress: true,
            snapshot_upload_mode: SnapshotUploadMode::from_buffer_budget_bytes(64 * MIB).unwrap(),
            ..Default::default()
        },
        store.clone(),
    );
    executor.stream_push = Some(StreamPushConfig {
        budget: UploadBufferBudget::new(5 * MIB).unwrap(),
        producers: Arc::new(Semaphore::new(2)),
        part_bytes: 5 * MIB,
        min_source_bytes: 5 * MIB as u64,
    });
    assert!(
        executor
            .stream_push
            .as_ref()
            .unwrap()
            .options(&payload, bytes.len() as u64)
            .is_some()
    );
    assert_eq!(
        executor
            .execute_push_inner(
                &test_job("push"),
                &payload,
                &test_creds(),
                super::super::tests::test_target(),
                None,
                CancellationToken::new()
            )
            .await
            .unwrap(),
        bytes.len() as i64
    );
    assert_eq!(store.put_calls(), 1);
    assert!(
        !Path::new(payload.upload_source())
            .with_extension("zst.tmp")
            .exists()
    );

    // Small inputs must retain their file-path admission bypass even when
    // streaming is enabled and a large producer occupies the shared pool.
    let executor = self::executor();
    let _busy = executor.codec.acquire(CodecPriority::Normal).await.unwrap();
    let server = Server::start().await;
    let dir = tempfile::tempdir().unwrap();
    let bytes = vec![7; 2 * MIB];
    let payload = staged(dir.path(), SnapshotArtifact::Memory, &bytes);
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            push(&executor, &server, &payload, CancellationToken::new()),
        )
        .await
        .expect("small source must not queue for codec admission")
        .unwrap(),
        bytes.len() as i64
    );
    let state = server.recorded.lock().unwrap();
    assert_eq!(state.creates, 0);
    assert_eq!(zstd::decode_all(state.object.as_slice()).unwrap(), bytes);
}
