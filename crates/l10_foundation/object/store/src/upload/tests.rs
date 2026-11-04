use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use md5::{Digest as _, Md5};
use tokio::io::ReadBuf;
use tokio::sync::Notify;

use super::*;
use crate::test_support::{self, Recorder, credentials, every_class, target};

const PART: usize = 5 * MIB;

#[derive(Clone, Copy, Default)]
enum Behavior {
    #[default]
    Normal,
    RetryFirst,
    RejectDigest,
    FailCompletion,
    OutOfOrder,
    HoldParts,
}

#[derive(Default)]
struct Recorded {
    requests: Vec<(String, i32, Bytes)>,
    completed: Vec<Vec<i32>>,
    objects: Vec<Vec<u8>>,
    creates: usize,
    aborts: usize,
}

struct Server {
    behavior: Behavior,
    recorded: Mutex<Recorded>,
    changed: Notify,
    release: Semaphore,
}

impl Server {
    async fn start(behavior: Behavior) -> (Arc<Self>, String) {
        let server = Arc::new(Self {
            behavior,
            recorded: Mutex::new(Recorded::default()),
            changed: Notify::new(),
            release: Semaphore::new(0),
        });
        let endpoint = test_support::serve(
            Router::new()
                .fallback(handle)
                .layer(DefaultBodyLimit::disable())
                .with_state(server.clone()),
        )
        .await;
        (server, endpoint)
    }

    async fn wait_for(&self, predicate: impl Fn(&Recorded) -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let notified = self.changed.notified();
                if predicate(&self.recorded.lock().unwrap()) {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("server observation deadline");
    }
}

fn query<'a>(uri: &'a Uri, name: &str) -> Option<&'a str> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then_some(value)
    })
}

fn xml(status: StatusCode, body: impl Into<String>) -> Response {
    (status, [("content-type", "application/xml")], body.into()).into_response()
}

async fn handle(
    State(server): State<Arc<Server>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match method {
        Method::POST if query(&uri, "uploads").is_some() => {
            let id = {
                let mut state = server.recorded.lock().unwrap();
                state.creates += 1;
                state.creates
            };
            xml(
                StatusCode::OK,
                format!(
                    "<InitiateMultipartUploadResult><UploadId>upload-{id}</UploadId></InitiateMultipartUploadResult>"
                ),
            )
        }
        Method::PUT => {
            let number = query(&uri, "partNumber").map(|s| s.parse::<i32>().unwrap());
            if !body.is_empty()
                && headers
                    .get("x-amz-content-sha256")
                    .and_then(|v| v.to_str().ok())
                    != Some("UNSIGNED-PAYLOAD")
            {
                return xml(
                    StatusCode::BAD_REQUEST,
                    "<Error><Code>InvalidRequest</Code><Message>part signing must not rehash the payload on the runtime</Message></Error>",
                );
            }
            let id = query(&uri, "uploadId").unwrap_or("single").to_string();
            let expected = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&body));
            let valid = headers.get("content-md5").and_then(|v| v.to_str().ok()) == Some(&expected);
            let attempt = {
                let mut state = server.recorded.lock().unwrap();
                state
                    .requests
                    .push((id.clone(), number.unwrap_or(0), body.clone()));
                state
                    .requests
                    .iter()
                    .filter(|(upload, part, _)| upload == &id && *part == number.unwrap_or(0))
                    .count()
            };
            server.changed.notify_one();
            if !valid || matches!(server.behavior, Behavior::RejectDigest) {
                return xml(
                    StatusCode::BAD_REQUEST,
                    "<Error><Code>BadDigest</Code></Error>",
                );
            }
            if number.unwrap_or(1) == 1
                && attempt == 1
                && matches!(server.behavior, Behavior::RetryFirst)
            {
                return xml(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "<Error><Code>SlowDown</Code></Error>",
                );
            }
            match server.behavior {
                Behavior::HoldParts => server.release.acquire().await.unwrap().forget(),
                Behavior::OutOfOrder if number == Some(1) => {
                    server.release.acquire().await.unwrap().forget()
                }
                Behavior::OutOfOrder if number == Some(2) => server.release.add_permits(1),
                Behavior::Normal
                | Behavior::RetryFirst
                | Behavior::RejectDigest
                | Behavior::FailCompletion
                | Behavior::OutOfOrder => {}
            }
            if number.is_none() {
                server.recorded.lock().unwrap().objects.push(body.to_vec());
            }
            (
                StatusCode::OK,
                [("etag", format!("\"part-{}\"", number.unwrap_or(0)))],
            )
                .into_response()
        }
        Method::POST => {
            if matches!(server.behavior, Behavior::FailCompletion) {
                return xml(
                    StatusCode::BAD_REQUEST,
                    "<Error><Code>InvalidPart</Code></Error>",
                );
            }
            let document = std::str::from_utf8(&body).unwrap();
            let parts: Vec<i32> = document
                .split("<PartNumber>")
                .skip(1)
                .map(|part| part.split('<').next().unwrap().parse().unwrap())
                .collect();
            let id = query(&uri, "uploadId").unwrap();
            let mut state = server.recorded.lock().unwrap();
            let bodies: BTreeMap<_, _> = state
                .requests
                .iter()
                .filter(|(upload, _, _)| upload == id)
                .map(|(_, number, body)| (*number, body.clone()))
                .collect();
            let object = parts
                .iter()
                .flat_map(|part| bodies[part].iter().copied())
                .collect();
            state.completed.push(parts);
            state.objects.push(object);
            xml(
                StatusCode::OK,
                "<CompleteMultipartUploadResult><ETag>\"complete\"</ETag></CompleteMultipartUploadResult>",
            )
        }
        Method::DELETE => {
            assert!(
                query(&uri, "uploadId").is_some(),
                "never delete a completed object"
            );
            server.recorded.lock().unwrap().aborts += 1;
            server.changed.notify_one();
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn store() -> S3ObjectStore {
    test_support::store(
        every_class(Duration::from_secs(15), Duration::from_secs(30)),
        Duration::from_secs(10),
        &Arc::new(Recorder::default()),
    )
}

fn options(max_bytes: u64) -> StreamingUploadOptions {
    StreamingUploadOptions::new(PART, 2, max_bytes).unwrap()
}

fn payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 31 + 7) % 251) as u8).collect()
}

fn source(bytes: Vec<u8>) -> UploadSource {
    UploadSource::new(
        io::Cursor::new(bytes),
        async { Ok(()) },
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn reconstructs_empty_short_exact_and_multipart_objects_and_replays_parts() {
    for (size, behavior) in [
        (0, Behavior::Normal),
        (37, Behavior::RetryFirst),
        (PART, Behavior::Normal),
        (PART + 19, Behavior::OutOfOrder),
        (2 * PART + 1, Behavior::RetryFirst),
    ] {
        let (server, endpoint) = Server::start(behavior).await;
        let store = store();
        let expected = payload(size);
        let budget = UploadBufferBudget::new(2 * PART).unwrap();
        let result = store
            .streaming_upload()
            .unwrap()
            .put_stream(
                &credentials(),
                target(&endpoint),
                source(expected.clone()),
                budget.clone(),
                options(size as u64),
            )
            .await
            .unwrap();
        assert_eq!(result, size as u64);
        let state = server.recorded.lock().unwrap();
        assert_eq!(state.objects, vec![expected]);
        assert_eq!(state.aborts, 0);
        for completed in &state.completed {
            assert_eq!(
                *completed,
                (1..=size.div_ceil(PART) as i32).collect::<Vec<_>>()
            );
        }
        if matches!(behavior, Behavior::RetryFirst) {
            let first: Vec<_> = state
                .requests
                .iter()
                .filter(|(_, n, _)| *n == if size < PART { 0 } else { 1 })
                .collect();
            assert_eq!(first.len(), 2);
            assert_eq!(
                first[0].2, first[1].2,
                "retry must replay the exact retained bytes"
            );
        }
        assert_eq!(budget.permits.available_permits(), 2 * PART / MIB);
    }
}

struct ReadFailure {
    remaining: usize,
}

impl AsyncRead for ReadFailure {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.remaining == 0 {
            return Poll::Ready(Err(io::Error::other("synthetic source read failure")));
        }
        let count = self.remaining.min(buf.remaining());
        buf.initialize_unfilled_to(count);
        buf.advance(count);
        self.remaining -= count;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn producer_failure_or_rejected_upload_never_publishes_and_awaits_abort() {
    for (size, behavior, producer_fails, read_fails) in [
        (0, Behavior::Normal, true, false),
        (37, Behavior::Normal, true, false),
        (37, Behavior::RejectDigest, false, false),
        (PART + 23, Behavior::Normal, true, false),
        (PART + 23, Behavior::Normal, false, true),
        (PART + 23, Behavior::RejectDigest, false, false),
        (PART + 23, Behavior::FailCompletion, false, false),
    ] {
        let (server, endpoint) = Server::start(behavior).await;
        let joined = Arc::new(AtomicBool::new(false));
        let completed = joined.clone();
        let reader: Box<dyn AsyncRead + Unpin + Send> = if read_fails {
            Box::new(ReadFailure { remaining: size })
        } else {
            Box::new(io::Cursor::new(payload(size)))
        };
        let source = UploadSource::new(
            reader,
            async move {
                completed.store(true, Ordering::SeqCst);
                if producer_fails {
                    Err(io::Error::other("synthetic encoder finalization failure"))
                } else {
                    Ok(())
                }
            },
            CancellationToken::new(),
        );
        let result = store()
            .put_stream(
                &credentials(),
                target(&endpoint),
                source,
                UploadBufferBudget::new(PART).unwrap(),
                options(size as u64),
            )
            .await;
        result.unwrap_err();
        assert!(joined.load(Ordering::SeqCst));
        let state = server.recorded.lock().unwrap();
        assert!(state.objects.is_empty());
        assert!(state.completed.is_empty());
        assert_eq!(state.aborts, usize::from(size >= PART));
    }
}

#[tokio::test]
async fn producer_success_is_required_after_eof_before_completion() {
    let (server, endpoint) = Server::start(Behavior::Normal).await;
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel();
    let (reader, mut writer) = tokio::io::duplex(8);
    let producer = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        writer.write_all(&[42; 17]).await?;
        drop(writer);
        finish_rx.await.map_err(io::Error::other)
    });
    let source = UploadSource::new(
        reader,
        async move {
            waiting_tx.send(()).unwrap();
            producer.await.map_err(io::Error::other)?
        },
        CancellationToken::new(),
    );
    let task = tokio::spawn(async move {
        store()
            .put_stream(
                &credentials(),
                target(&endpoint),
                source,
                UploadBufferBudget::new(PART).unwrap(),
                options(17),
            )
            .await
    });
    waiting_rx.await.unwrap();
    assert!(
        server.recorded.lock().unwrap().requests.is_empty(),
        "single PUT must wait for producer success"
    );
    finish_tx.send(()).unwrap();
    assert_eq!(task.await.unwrap().unwrap(), 17);
    assert_eq!(server.recorded.lock().unwrap().objects, vec![vec![42; 17]]);
}

#[tokio::test]
async fn rejects_oversized_sources_and_impossible_limits_without_publication() {
    StreamingUploadOptions::new(PART, 1, PART as u64 * MAX_PARTS + 1).unwrap_err();
    for (size, max) in [(1, 0), (PART + 1, PART as u64)] {
        let (server, endpoint) = Server::start(Behavior::Normal).await;
        let result = store()
            .put_stream(
                &credentials(),
                target(&endpoint),
                source(payload(size)),
                UploadBufferBudget::new(PART).unwrap(),
                options(max),
            )
            .await;
        assert!(matches!(
            result,
            Err(StreamingUploadError::SizeLimit { .. })
        ));
        let state = server.recorded.lock().unwrap();
        assert!(state.objects.is_empty());
        assert_eq!(state.aborts, usize::from(size > PART));
    }
    let (server, endpoint) = Server::start(Behavior::Normal).await;
    let result = store()
        .put_stream(
            &credentials(),
            target(&endpoint),
            source(vec![1]),
            UploadBufferBudget::new(MIB).unwrap(),
            options(1),
        )
        .await;
    assert!(matches!(
        result,
        Err(StreamingUploadError::InvalidConfiguration(_))
    ));
    assert_eq!(server.recorded.lock().unwrap().creates, 0);
}

struct CountingReader {
    cursor: io::Cursor<Vec<u8>>,
    read: Arc<AtomicUsize>,
}

impl AsyncRead for CountingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.cursor).poll_read(cx, buf);
        self.read
            .fetch_add(buf.filled().len() - before, Ordering::SeqCst);
        result
    }
}

#[tokio::test]
async fn shared_budget_backpressures_independent_stores_without_deadlocking_uploads() {
    let (server, endpoint) = Server::start(Behavior::HoldParts).await;
    let budget = UploadBufferBudget::new(2 * PART).unwrap();
    let read = Arc::new(AtomicUsize::new(0));
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let budget = budget.clone();
        let reader = CountingReader {
            cursor: io::Cursor::new(payload(PART * 3)),
            read: read.clone(),
        };
        let endpoint = endpoint.clone();
        jobs.spawn(async move {
            store()
                .put_stream(
                    &credentials(),
                    target(&endpoint),
                    UploadSource::new(reader, async { Ok(()) }, CancellationToken::new()),
                    budget,
                    options((PART * 3) as u64),
                )
                .await
        });
    }
    server.wait_for(|s| s.requests.len() == 2).await;
    assert_eq!(
        read.load(Ordering::SeqCst),
        2 * PART,
        "no producer may read ahead outside the shared budget"
    );
    assert_eq!(budget.permits.available_permits(), 0);
    server.release.add_permits(6);
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = jobs.join_next().await {
            assert_eq!(result.unwrap().unwrap(), (PART * 3) as u64);
        }
    })
    .await
    .expect("budget waiters must not block part completion");
    assert_eq!(server.recorded.lock().unwrap().objects.len(), 2);
    assert_eq!(budget.permits.available_permits(), 2 * PART / MIB);
}

#[tokio::test]
async fn prefetch_keeps_producer_running_without_exceeding_upload_concurrency() {
    let (server, endpoint) = Server::start(Behavior::HoldParts).await;
    let budget = UploadBufferBudget::new(2 * PART).unwrap();
    let read = Arc::new(AtomicUsize::new(0));
    let reader = CountingReader {
        cursor: io::Cursor::new(payload(3 * PART)),
        read: read.clone(),
    };
    let task_budget = budget.clone();
    let job = tokio::spawn(async move {
        store()
            .put_stream(
                &credentials(),
                target(&endpoint),
                UploadSource::new(reader, async { Ok(()) }, CancellationToken::new()),
                task_budget,
                StreamingUploadOptions::new(PART, 1, (3 * PART) as u64).unwrap(),
            )
            .await
    });
    server.wait_for(|s| s.requests.len() == 1).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while read.load(Ordering::SeqCst) < 2 * PART {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the next part must fill while the first request is blocked");
    assert_eq!(
        server.recorded.lock().unwrap().requests.len(),
        1,
        "prefetch must not increase request concurrency"
    );
    assert_eq!(read.load(Ordering::SeqCst), 2 * PART);
    assert_eq!(budget.permits.available_permits(), 0);
    server.release.add_permits(3);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        (3 * PART) as u64
    );
    assert_eq!(budget.permits.available_permits(), 2 * PART / MIB);
}

#[tokio::test]
async fn cancellation_and_future_drop_cancel_producer_and_abort_without_completing() {
    for drop_future in [false, true] {
        let (server, endpoint) = Server::start(Behavior::HoldParts).await;
        let cancel = CancellationToken::new();
        let producer_token = cancel.clone();
        let joined = Arc::new(AtomicBool::new(false));
        let producer_joined = joined.clone();
        let source = UploadSource::new(
            io::Cursor::new(payload(PART * 2)),
            async move {
                producer_token.cancelled().await;
                producer_joined.store(true, Ordering::SeqCst);
                Ok(())
            },
            cancel.clone(),
        );
        let budget = UploadBufferBudget::new(PART).unwrap();
        let job_budget = budget.clone();
        let job = tokio::spawn(async move {
            store()
                .put_stream(
                    &credentials(),
                    target(&endpoint),
                    source,
                    job_budget,
                    options((PART * 2) as u64),
                )
                .await
        });
        server.wait_for(|s| s.requests.len() == 1).await;
        if drop_future {
            job.abort();
            assert!(job.await.unwrap_err().is_cancelled());
        } else {
            cancel.cancel();
            assert!(matches!(
                job.await.unwrap(),
                Err(StreamingUploadError::Cancelled)
            ));
            assert!(
                joined.load(Ordering::SeqCst),
                "ordinary cancellation joins the producer"
            );
        }
        server.wait_for(|s| s.aborts == 1).await;
        assert!(cancel.is_cancelled());
        assert!(server.recorded.lock().unwrap().completed.is_empty());
        assert_eq!(budget.permits.available_permits(), PART / MIB);
        server.release.add_permits(1);
    }
}

#[tokio::test]
async fn budget_charge_follows_body_clones_until_the_last_owner_drops() {
    let budget = UploadBufferBudget::new(PART).unwrap();
    let mut buffer = budget.allocate(PART).await.unwrap();
    buffer.len = 1;
    let body = Bytes::from_owner(buffer);
    let retry_body = body.clone();
    drop(body);
    assert_eq!(
        budget.permits.available_permits(),
        0,
        "short tails retain their full allocation charge"
    );
    drop(retry_body);
    assert_eq!(budget.permits.available_permits(), PART / MIB);
}

#[tokio::test]
async fn cancellation_while_waiting_for_budget_stops_without_reading_or_creating_upload() {
    let (server, endpoint) = Server::start(Behavior::Normal).await;
    let budget = UploadBufferBudget::new(PART).unwrap();
    let reserved = budget.allocate(PART).await.unwrap();
    let cancel = CancellationToken::new();
    let read = Arc::new(AtomicUsize::new(0));
    let reader = CountingReader {
        cursor: io::Cursor::new(vec![1]),
        read: read.clone(),
    };
    let source = UploadSource::new(reader, async { Ok(()) }, cancel.clone());
    let store = store();
    let creds = credentials();
    let upload = store.put_stream(
        &creds,
        target(&endpoint),
        source,
        budget.clone(),
        options(1),
    );
    tokio::pin!(upload);
    assert!(futures_util::poll!(&mut upload).is_pending());
    cancel.cancel();
    assert!(matches!(upload.await, Err(StreamingUploadError::Cancelled)));
    assert_eq!(read.load(Ordering::SeqCst), 0);
    assert_eq!(server.recorded.lock().unwrap().creates, 0);
    drop(reserved);
    assert_eq!(budget.permits.available_permits(), PART / MIB);
}
