use super::*;
use pplx_space_common_core::types::IdempotencyKey;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    WriteBatchEntry, filesystem_client::FilesystemClient,
};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tonic::transport::{Channel, Server};

fn router() -> FilesystemHandle {
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("user lookup")
        .expect("current user");
    FilesystemHandle::new(
        FilesystemExecutionMode::Direct,
        None,
        crate::IdentityPolicy::Guest,
        GuestUserName::parse(user.name).expect("username"),
        CancellationToken::new(),
        super::DEFAULT_CANCELLATION_GRACE,
        None,
    )
    .expect("direct router")
}

/// A router whose file operations are rejected at once when every slot is
/// busy, for tests about rejection rather than the queue.
#[cfg_attr(
    target_os = "linux",
    allow(clippy::panic, reason = "the test router is always direct")
)]
fn router_without_file_operation_queue() -> FilesystemHandle {
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
    router: &FilesystemHandle,
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
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        WriteOpen, write_request::Message,
    };

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
async fn large_duplicate_batches_share_native_and_transport_budget_and_recover() {
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
            let native = router.clone();
            requests.spawn(async move {
                let request = WriteBatchRequest {
                    entries: vec![WriteBatchEntry {
                        path: "/must-not-execute-duplicate".into(),
                        data: vec![42; filesystem_limits::MAX_MESSAGE_BYTES - 1024],
                        append: true,
                    }],
                    idempotency_key: Some(key),
                    user: None,
                };
                if index % 2 == round % 2 {
                    native.write_batch(Request::new(request)).await
                } else {
                    client.write_batch(request).await
                }
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
    for native in [false, true] {
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
        let service = router.clone();
        let upload = tokio::spawn(async move {
            if native {
                service.write_batch(Request::new(request)).await
            } else {
                writing.write_batch(request).await
            }
        });
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
            router.payload_slots.available_permits()
                == filesystem_limits::BATCH_REQUEST_WEIGHT as usize
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
}

#[tokio::test]
async fn native_mutations_share_the_transport_payload_budget() {
    let router = router();
    let occupied = router
        .payload_slots
        .clone()
        .acquire_many_owned(filesystem_limits::PAYLOAD_SLOTS as u32)
        .await
        .unwrap();
    let error = router
        .write_batch(Request::new(WriteBatchRequest::default()))
        .await
        .expect_err("native batch requires payload capacity");
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));
    let mut polled = false;
    let input = futures_util::stream::poll_fn(|_| {
        polled = true;
        std::task::Poll::Ready(None)
    });
    let error = router
        .write_from_stream(Request::new(input))
        .await
        .expect_err("native upload requires payload capacity");
    assert!(!polled, "an upload without capacity must not poll input");
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));
    drop(occupied);
    let mut upload =
        Box::pin(router.write_from_stream(Request::new(futures_util::stream::pending())));
    assert!(futures_util::poll!(&mut upload).is_pending());
    assert_eq!(
        router.payload_slots.available_permits(),
        filesystem_limits::PAYLOAD_SLOTS - 1,
        "waiting for Open retains capacity"
    );
    drop(upload);
    assert_eq!(
        router.payload_slots.available_permits(),
        filesystem_limits::PAYLOAD_SLOTS
    );
    router
        .write_batch(Request::new(WriteBatchRequest::default()))
        .await
        .expect("recovered");
    router.drain().await;
}

#[tokio::test]
async fn native_messages_allow_exactly_the_size_limit() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        WriteOpen, write_request::Message as WriteMessage,
    };
    use prost::Message;
    let router = router();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bounded");
    tokio::fs::write(&path, b"before").await.unwrap();
    let mut request = WriteBatchRequest {
        entries: vec![WriteBatchEntry {
            path: path.display().to_string(),
            data: vec![42; filesystem_limits::MAX_MESSAGE_BYTES],
            append: false,
        }],
        ..Default::default()
    };
    let overhead = request.encoded_len() - request.entries[0].data.len();
    let error = router
        .write_batch(Request::new(request.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::OutOfRange);
    assert!(!pplx_space_common_grpc::is_refused_before_start(&error));
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"before");
    request.entries[0]
        .data
        .truncate(filesystem_limits::MAX_MESSAGE_BYTES - overhead);
    assert_eq!(request.encoded_len(), filesystem_limits::MAX_MESSAGE_BYTES);
    router
        .write_batch(Request::new(request))
        .await
        .expect("exact limit is allowed");
    let error = router
        .stat_info(Request::new(StatRequest {
            path: "x".repeat(filesystem_limits::MAX_MESSAGE_BYTES),
            user: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::OutOfRange);
    assert!(!pplx_space_common_grpc::is_refused_before_start(&error));
    let oversized_open = WriteRequest {
        message: Some(WriteMessage::Open(WriteOpen {
            path: "x".repeat(filesystem_limits::MAX_MESSAGE_BYTES),
            ..Default::default()
        })),
    };
    let error = router
        .write_from_stream(Request::new(futures_util::stream::iter([Ok(
            oversized_open,
        )])))
        .await
        .expect_err("oversized Open");
    assert_eq!(error.code(), tonic::Code::OutOfRange);
    let mut committed = Vec::new();
    for excess in [0, 1] {
        let open = WriteRequest {
            message: Some(WriteMessage::Open(WriteOpen {
                path: path.display().to_string(),
                ..Default::default()
            })),
        };
        let mut data = WriteRequest {
            message: Some(WriteMessage::Data(vec![
                42;
                filesystem_limits::MAX_MESSAGE_BYTES
            ])),
        };
        let overhead = data.encoded_len() - filesystem_limits::MAX_MESSAGE_BYTES;
        let Some(WriteMessage::Data(bytes)) = &mut data.message else {
            unreachable!()
        };
        bytes.truncate(filesystem_limits::MAX_MESSAGE_BYTES - overhead + excess);
        let expected_bytes = bytes.len();
        assert_eq!(
            data.encoded_len(),
            filesystem_limits::MAX_MESSAGE_BYTES + excess
        );
        let mut messages = vec![Ok(open)];
        if excess != 0 {
            messages.push(Ok(WriteRequest {
                message: Some(WriteMessage::Data(b"must not commit".to_vec())),
            }));
        }
        messages.push(Ok(data));
        let result = router
            .write_from_stream(Request::new(futures_util::stream::iter(messages)))
            .await;
        if excess == 0 {
            result.expect("exact upload message limit is allowed");
            committed = vec![42; expected_bytes];
            assert_eq!(tokio::fs::read(&path).await.unwrap(), committed);
        } else {
            let error = result.expect_err("oversized Data");
            assert_eq!(error.code(), tonic::Code::OutOfRange);
            assert!(!pplx_space_common_grpc::is_refused_before_start(&error));
        }
    }
    router.drain().await;
    assert_eq!(
        tokio::fs::read(&path).await.unwrap(),
        committed,
        "rejected replacement must preserve the exact-limit upload after cleanup"
    );
}

#[tokio::test(start_paused = true)]
async fn native_upload_idle_timeout_cancels_before_open_and_between_data() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        WriteOpen, write_request::Message,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    for after_open in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unchanged");
        std::fs::write(&path, b"original").unwrap();
        let service = crate::ExecutionService::with_config(
            CancellationToken::new(),
            crate::ExecutionServiceConfig {
                filesystem_execution: FilesystemExecutionMode::Direct,
                identity_policy: crate::IdentityPolicy::Guest,
                default_operation_user: router().default_user,
                native_upload_idle_timeout: std::time::Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
        let router: crate::FilesystemHandle = service.filesystem_service();
        let waiting = Arc::new(AtomicBool::new(false));
        let observed = waiting.clone();
        let stalled = futures_util::stream::poll_fn(move |_| {
            observed.store(true, Ordering::SeqCst);
            std::task::Poll::Pending
        });
        let messages = if after_open {
            vec![
                Ok(WriteRequest {
                    message: Some(Message::Open(WriteOpen {
                        path: path.display().to_string(),
                        idempotency_key: Some("idle-upload".into()),
                        ..Default::default()
                    })),
                }),
                Ok(WriteRequest {
                    message: Some(Message::Data(b"partial".to_vec())),
                }),
            ]
        } else {
            vec![]
        };
        let mut upload = Box::pin(router.write_file(Request::new(
            futures_util::stream::iter(messages).chain(stalled),
        )));
        while !waiting.load(Ordering::SeqCst) {
            assert!(futures_util::poll!(&mut upload).is_pending());
            tokio::task::yield_now().await;
        }
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), &mut upload)
            .await
            .expect("native input idle time is bounded")
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert!(!pplx_space_common_grpc::is_refused_before_start(&error));
        drop(upload);
        tokio::time::resume();
        router.drain().await;
        assert_eq!(
            router.payload_slots.available_permits(),
            filesystem_limits::PAYLOAD_SLOTS
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
        tokio::time::pause();
    }
}

#[tokio::test(start_paused = true)]
async fn native_upload_idle_budget_resets_and_transport_path_has_no_new_timeout() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        WriteOpen, write_request::Message,
    };
    for native in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("upload");
        let router = router().with_native_upload_idle_timeout(std::time::Duration::from_secs(3));
        let delay = std::time::Duration::from_secs(if native { 2 } else { 4 });
        let input = futures_util::stream::iter([
            WriteRequest {
                message: Some(Message::Open(WriteOpen {
                    path: path.display().to_string(),
                    ..Default::default()
                })),
            },
            WriteRequest {
                message: Some(Message::Data(b"first".to_vec())),
            },
            WriteRequest {
                message: Some(Message::Data(b"second".to_vec())),
            },
        ])
        .then(|message| async move {
            tokio::time::sleep(delay).await;
            Ok(message)
        });
        if native {
            router.write_file(Request::new(input)).await.unwrap();
        } else {
            // The generated Filesystem::write adapter delegates here, without
            // the native-only idle policy.
            router.write_from_stream(Request::new(input)).await.unwrap();
        }
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"firstsecond");
        router.drain().await;
    }
}

#[tokio::test]
async fn cancelled_router_refuses_new_filesystem_work() {
    let router = router();
    let (mut client, server) = connect(&router).await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("after-close");
    router.shutdown.cancel();
    let error = router
        .mkdir(Request::new(MkdirRequest {
            path: path.display().to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("closed router must reject mkdir");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));
    let error = client
        .mkdir(MkdirRequest {
            path: path.display().to_string(),
            ..Default::default()
        })
        .await
        .expect_err("gRPC must share admission closure");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));
    assert!(!path.exists(), "rejected mkdir must have no effect");
    router.drain().await;
    server.abort();
    assert!(server.await.expect_err("server stopped").is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_upload_before_open() {
    let router = router();
    let mut upload =
        Box::pin(router.write_from_stream(Request::new(futures_util::stream::pending())));
    assert!(futures_util::poll!(&mut upload).is_pending());
    router.shutdown.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), upload)
        .await
        .expect("shutdown must wake an upload waiting for Open")
        .expect_err("unstarted upload is refused");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));
    assert_eq!(
        router.payload_slots.available_permits(),
        filesystem_limits::PAYLOAD_SLOTS
    );
    router.drain().await;
}
