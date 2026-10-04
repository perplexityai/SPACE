use super::*;
use pplx_space_common_core::types::IdempotencyKey;
use pplx_space_spaced_common::proto::bridge_guest::{
    WriteBatchEntry, filesystem_client::FilesystemClient,
};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tonic::transport::{Channel, Server};

fn router() -> FilesystemRouter {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("user lookup")
        .expect("current user");
    FilesystemRouter::new(
        FilesystemExecutionMode::Direct,
        GuestUserName::parse(user.name).expect("username"),
        CancellationToken::new(),
        super::DEFAULT_CANCELLATION_GRACE,
    )
}

/// A router whose file operations are rejected at once when every slot is
/// busy, for tests about rejection rather than the queue.
#[cfg_attr(
    target_os = "linux",
    allow(clippy::panic, reason = "the test router is always direct")
)]
fn router_without_file_operation_queue() -> FilesystemRouter {
    let mut router = router();
    #[cfg(not(target_os = "linux"))]
    let Backend::Direct(service) = &mut router.backend;
    #[cfg(target_os = "linux")]
    let Backend::Direct(service) = &mut router.backend else {
        panic!("direct test executor")
    };
    service.operation_admission = pplx_space_common_grpc::QueuedSlots::new(Arc::new(
        tokio::sync::Semaphore::new(super::super::MAX_CONCURRENT_FILE_OPERATIONS),
    ));
    router
}

#[tokio::test]
async fn parent_cancellation_stops_direct_watches_before_drain() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().display().to_string();
    let router = router();
    let mut stream = router
        .watch(Request::new(WatchRequest {
            path: path.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    router.shutdown.cancel();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("parent cancellation closes the watch stream")
            .is_none()
    );
    assert_eq!(
        router
            .create_watcher(Request::new(CreateWatcherRequest {
                root: path,
                ..Default::default()
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    router.drain().await;
}

async fn connect(
    router: &FilesystemRouter,
) -> (FilesystemClient<Channel>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let service = router.grpc_service();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("server");
    });
    let client = FilesystemClient::connect(format!("http://{address}"))
        .await
        .expect("client");
    (client, server)
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition reached");
}

#[tokio::test]
async fn watch_setup_shares_file_admission_but_live_watches_release_it() {
    let directory = tempfile::tempdir().expect("directory");
    let router = router_without_file_operation_queue();
    #[cfg(not(target_os = "linux"))]
    let Backend::Direct(service) = &router.backend;
    #[cfg(target_os = "linux")]
    let Backend::Direct(service) = &router.backend else {
        panic!("direct test executor")
    };
    let slots = service.operation_admission.acquire(8).await.unwrap();
    let path = directory.path().display().to_string();
    let results = [
        service
            .watch(Request::new(WatchRequest {
                path: path.clone(),
                ..Default::default()
            }))
            .await
            .map(|_| ()),
        service
            .create_watcher(Request::new(CreateWatcherRequest {
                root: path.clone(),
                ..Default::default()
            }))
            .await
            .map(|_| ()),
        service
            .enable_file_changes(Request::new(EnableFileChangesRequest {
                roots: vec![path.clone()],
                ..Default::default()
            }))
            .await
            .map(|_| ()),
    ];
    for result in results {
        assert_eq!(
            result.expect_err("setup admission").code(),
            tonic::Code::ResourceExhausted
        );
    }
    drop(slots);
    let _stream = service
        .watch(Request::new(WatchRequest {
            path,
            ..Default::default()
        }))
        .await
        .expect("capacity recovered");
    assert!(
        matches!(
            service.operation_admission.request(8),
            pplx_space_common_grpc::SlotRequest::Admitted(_)
        ),
        "stream lifetime must not retain setup admission"
    );
    router.drain().await;
}

#[tokio::test]
async fn worker_admission_rejection_allows_same_key_retry_after_capacity_recovers() {
    use pplx_space_spaced_common::proto::bridge_guest::{WriteOpen, write_request::Message};

    let directory = tempfile::tempdir().expect("directory");
    let router = router_without_file_operation_queue();
    #[cfg(not(target_os = "linux"))]
    let Backend::Direct(service) = &router.backend;
    #[cfg(target_os = "linux")]
    let Backend::Direct(service) = &router.backend else {
        panic!("direct test executor")
    };
    let (mut client, server) = connect(&router).await;
    for kind in [Kind::Write, Kind::Batch, Kind::Rename] {
        let path = directory.path().join(format!("{kind:?}"));
        let destination = path.with_extension("renamed");
        tokio::fs::write(&path, b"before").await.expect("seed");
        let mut slots = Some(
            service
                .operation_admission
                .acquire(8)
                .await
                .expect("occupy file slots"),
        );
        for overloaded in [true, false] {
            let result = match kind {
                Kind::Write => client
                    .write(tokio_stream::iter([
                        WriteRequest {
                            message: Some(Message::Open(WriteOpen {
                                path: path.display().to_string(),
                                append: true,
                                idempotency_key: Some(format!("retry-{kind:?}")),
                                user: None,
                            })),
                        },
                        WriteRequest {
                            message: Some(Message::Data(b"after".to_vec())),
                        },
                    ]))
                    .await
                    .map(|_| ()),
                Kind::Batch => client
                    .write_batch(WriteBatchRequest {
                        entries: vec![WriteBatchEntry {
                            path: path.display().to_string(),
                            data: b"after".to_vec(),
                            append: true,
                        }],
                        idempotency_key: Some(format!("retry-{kind:?}")),
                        user: None,
                    })
                    .await
                    .map(|_| ()),
                Kind::Rename => client
                    .rename(RenameRequest {
                        old_path: path.display().to_string(),
                        new_path: destination.display().to_string(),
                        idempotency_key: Some(format!("retry-{kind:?}")),
                        user: None,
                    })
                    .await
                    .map(|_| ()),
            };
            if overloaded {
                assert_eq!(
                    result.expect_err("worker admission").code(),
                    tonic::Code::ResourceExhausted
                );
                assert_eq!(tokio::fs::read(&path).await.expect("unchanged"), b"before");
                // Release after the rejected RPC, before retrying its exact key.
                drop(slots.take());
            } else {
                result.expect("retry executes without waiting for replay retention");
            }
        }
        let (written, expected): (_, &[u8]) = match kind {
            Kind::Rename => (destination, b"before"),
            Kind::Write | Kind::Batch => (path, b"beforeafter"),
        };
        assert_eq!(tokio::fs::read(written).await.expect("effect"), expected);
    }
    router.drain().await;
    server.abort();
    assert!(server.await.expect_err("server stopped").is_cancelled());
}

#[tokio::test]
async fn large_duplicate_batches_share_ingress_budget_across_listeners_and_recover() {
    let router = router();
    let (client, server) = connect(&router).await;
    let (mut other, other_server) = connect(&router).await;
    // Exercise repeated pressure/recovery, not just one full semaphore. The
    // reservation represents an active original; duplicates must never write.
    for round in 0..4 {
        let key = format!("large-duplicate-{round}");
        let Lookup::New(reservation) = router
            .operations
            .reserve(
                router.default_user.clone(),
                Some(IdempotencyKey::parse(key.clone()).expect("key")),
                Kind::Batch,
            )
            .await
            .expect("original reservation")
        else {
            panic!("new original")
        };
        let execution = reservation.dispatch();
        let mut requests = tokio::task::JoinSet::new();
        for index in
            0..filesystem_limits::PAYLOAD_SLOTS / filesystem_limits::BATCH_REQUEST_WEIGHT as usize
        {
            let mut client = if index % 2 == 0 {
                client.clone()
            } else {
                other.clone()
            };
            let key = key.clone();
            requests.spawn(async move {
                client
                    .write_batch(WriteBatchRequest {
                        entries: vec![WriteBatchEntry {
                            path: "/must-not-execute-duplicate".into(),
                            data: vec![42; filesystem_limits::MAX_MESSAGE_BYTES - 1024],
                            append: true,
                        }],
                        idempotency_key: Some(key),
                        user: None,
                    })
                    .await
            });
        }
        wait_for(|| router.payload_slots.available_permits() == 0).await;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            other.write(futures_util::stream::pending::<WriteRequest>()),
        )
        .await
        .expect("reject without waiting for an open frame")
        .expect_err("overload");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        // Metadata remains usable while payload requests occupy the budget.
        other
            .stat_info(StatRequest {
                path: "/".into(),
                user: None,
            })
            .await
            .expect("stat unaffected");
        execution.complete(Ok(operations::outcome(Ok(
            mutation_result::Outcome::Batch(WriteBatchResponse { files_written: 17 }),
        ))));
        while let Some(result) = requests.join_next().await {
            assert_eq!(
                result
                    .expect("join")
                    .expect("replayed batch")
                    .into_inner()
                    .files_written,
                17
            );
        }
        wait_for(|| router.payload_slots.available_permits() == filesystem_limits::PAYLOAD_SLOTS)
            .await;
    }
    router.drain().await;
    server.abort();
    other_server.abort();
    assert!(server.await.expect_err("server stopped").is_cancelled());
    assert!(
        other_server
            .await
            .expect_err("server stopped")
            .is_cancelled()
    );
}

#[tokio::test]
async fn cancelled_batch_keeps_payload_admission_until_blocking_io_settles() {
    let directory = tempfile::tempdir().expect("directory");
    let fifo = directory.path().join("blocked");
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .expect("fifo");
    let marker = directory.path().join("started");
    let mut router = router();
    router.payload_slots = Arc::new(Semaphore::new(
        filesystem_limits::BATCH_REQUEST_WEIGHT as usize,
    ));
    let (mut client, server) = connect(&router).await;
    let mut writing = client.clone();
    let request = WriteBatchRequest {
        entries: vec![
            WriteBatchEntry {
                path: marker.display().to_string(),
                data: b"started".to_vec(),
                append: false,
            },
            WriteBatchEntry {
                path: fifo.display().to_string(),
                data: b"blocked".to_vec(),
                append: true,
            },
        ],
        idempotency_key: Some("cancelled-batch".into()),
        user: None,
    };
    let upload = tokio::spawn(async move { writing.write_batch(request).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while tokio::fs::read(&marker).await.ok().as_deref() != Some(b"started") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first effect visible");
    upload.abort();
    assert!(upload.await.expect_err("caller cancelled").is_cancelled());
    // Give the transport cancellation time to reach the server. Admission must
    // remain unavailable throughout this window, not recover with the caller.
    let mut outcomes = Vec::new();
    for _ in 0..10 {
        outcomes.push(
            client
                .write_batch(WriteBatchRequest::default())
                .await
                .map(|_| ())
                .map_err(|error| error.code()),
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let reader = tokio::fs::File::open(&fifo).await.expect("unblock writer");
    wait_for(|| {
        router.payload_slots.available_permits() == filesystem_limits::BATCH_REQUEST_WEIGHT as usize
    })
    .await;
    client
        .write_batch(WriteBatchRequest::default())
        .await
        .expect("admission recovered");
    drop(reader);
    router.drain().await;
    server.abort();
    assert!(server.await.expect_err("server stopped").is_cancelled());
    assert!(
        outcomes
            .iter()
            .all(|result| *result == Err(tonic::Code::ResourceExhausted)),
        "cleanup must retain admission: {outcomes:?}"
    );
}
