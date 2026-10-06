use std::io;
use std::sync::Arc;

use hyper_util::rt::TokioIo;
use pplx_space_spaced_guest_protocol::proto::worker::worker_filesystem_server::WorkerFilesystemServer as FilesystemServer;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint, Server};

use super::*;

fn current_user() -> GuestUserName {
    GuestUserName::parse(
        nix::unistd::User::from_uid(nix::unistd::Uid::current())
            .expect("current user lookup")
            .expect("current user exists")
            .name,
    )
    .expect("current user is valid")
}

fn identity_router() -> FilesystemRouter {
    FilesystemRouter::new(
        FilesystemExecutionMode::IdentityWorkers,
        current_user(),
        CancellationToken::new(),
        super::DEFAULT_CANCELLATION_GRACE,
        None,
    )
}

async fn connected_worker(
    router: &FilesystemRouter,
) -> (
    Channel,
    tokio::task::JoinHandle<io::Result<(u64, u64)>>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (channel, proxy, server, _relayed) = connected_counted_worker(router).await;
    (channel, proxy, server)
}

/// A worker behind a proxy that counts the bytes the worker sends.
async fn connected_counted_worker(
    router: &FilesystemRouter,
) -> (
    Channel,
    tokio::task::JoinHandle<io::Result<(u64, u64)>>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    Arc<std::sync::atomic::AtomicU64>,
) {
    let (client_stream, proxy_from_client) =
        std::os::unix::net::UnixStream::pair().expect("client socketpair");
    let (proxy_to_server, server_stream) =
        std::os::unix::net::UnixStream::pair().expect("server socketpair");
    for stream in [
        &client_stream,
        &proxy_from_client,
        &proxy_to_server,
        &server_stream,
    ] {
        stream.set_nonblocking(true).expect("nonblocking stream");
    }
    let client_stream =
        tokio::net::UnixStream::from_std(client_stream).expect("tokio client stream");
    let mut proxy_from_client =
        tokio::net::UnixStream::from_std(proxy_from_client).expect("tokio proxy client stream");
    let mut proxy_to_server =
        tokio::net::UnixStream::from_std(proxy_to_server).expect("tokio proxy server stream");
    let server_stream =
        tokio::net::UnixStream::from_std(server_stream).expect("tokio server stream");
    let relayed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counted = Arc::clone(&relayed);
    let proxy = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut from_client, mut to_client) = proxy_from_client.split();
        let (mut from_server, mut to_server) = proxy_to_server.split();
        let upstream = tokio::io::copy(&mut from_client, &mut to_server);
        let downstream = async {
            let mut buffer = vec![0_u8; 64 * 1024];
            let mut total = 0_u64;
            loop {
                let read = from_server.read(&mut buffer).await?;
                if read == 0 {
                    return Ok::<_, io::Error>(total);
                }
                counted.fetch_add(read as u64, std::sync::atomic::Ordering::Relaxed);
                total += read as u64;
                to_client
                    .write_all(buffer.get(..read).unwrap_or_default())
                    .await?;
            }
        };
        tokio::try_join!(upstream, downstream)
    });
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FilesystemServer::new(FilesystemWorkerService::new(
                current_user(),
                CancellationToken::new(),
            )))
            .serve_with_incoming(tokio_stream::once(Ok::<_, io::Error>(server_stream)))
            .await
    });

    let stream = Arc::new(Mutex::new(Some(client_stream)));
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("worker endpoint")
        .connect_with_connector(tower::service_fn(move |_| {
            let stream = Arc::clone(&stream);
            async move {
                let stream = stream
                    .lock()
                    .await
                    .take()
                    .expect("worker connection is made once");
                Ok::<_, io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .expect("worker channel");
    router
        .worker_pool()
        .expect("worker backend")
        .insert_test_client(current_user(), channel.clone())
        .await
        .expect("insert worker");
    (channel, proxy, server, relayed)
}

async fn stop_worker(
    proxy: tokio::task::JoinHandle<io::Result<(u64, u64)>>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    proxy.abort();
    server.abort();
    let _ = tokio::join!(proxy, server);
}

// A read goes through a descriptor the worker opened as its user and the
// router duplicated: the file's bytes never cross the worker's connection,
// as relaying every chunk through the worker would make them.
#[tokio::test]
async fn a_read_streams_from_the_workers_descriptor_without_relaying_through_it() {
    use futures_util::StreamExt;
    let router = identity_router();
    let (channel, _proxy, _server, relayed) = connected_counted_worker(&router).await;
    router
        .worker_pool()
        .expect("worker backend")
        .insert_test_client_in_this_process(current_user(), channel)
        .await
        .expect("worker with a reachable descriptor table");
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("file");
    let data: Vec<u8> = (0..=250_u8).cycle().take(1 << 20).collect();
    std::fs::write(&path, &data).expect("file");
    let before = relayed.load(std::sync::atomic::Ordering::Relaxed);

    let mut stream = read_file(&router, &path).await;
    let mut read = Vec::new();
    while let Some(chunk) = stream.next().await {
        read.extend_from_slice(&chunk.expect("chunk").data);
    }

    assert_eq!(read, data);
    let sent = relayed.load(std::sync::atomic::Ordering::Relaxed) - before;
    assert!(
        sent < 64 * 1024,
        "the worker sent {sent} bytes for a 1 MiB read; only its descriptor should cross"
    );
}

// The read counts against its worker's admission, which ends with the worker,
// so the read ends too instead of outliving the limit a replacement starts
// afresh, and the dead worker is retired as a relayed stream's failure would
// retire it, so the user's next operation does not reach it.
#[tokio::test]
async fn a_read_through_a_workers_descriptor_ends_with_the_worker() {
    use futures_util::StreamExt;
    let router = identity_router();
    let (channel, proxy, server) = connected_worker(&router).await;
    router
        .worker_pool()
        .expect("worker backend")
        .insert_test_client_in_this_process(current_user(), channel)
        .await
        .expect("worker with a reachable descriptor table");
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("file");
    let data: Vec<u8> = (0..=250_u8).cycle().take(1 << 20).collect();
    std::fs::write(&path, &data).expect("file");

    let pool = router.worker_pool().expect("worker backend");
    let worker = router.worker(None).await.expect("cached worker");
    let mut stream = read_file(&router, &path).await;
    let mut read = stream
        .next()
        .await
        .expect("first chunk")
        .expect("chunk")
        .data
        .to_vec();
    stop_worker(proxy, server).await;
    // The read learns of the worker's end asynchronously, through its lease
    // stream failing. Drain only once the dead worker is retired: by then the
    // read has observed the end, so it cannot race the rest of the file
    // through its few buffered chunks first (which a fast, busy host did).
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pool.contains(&worker).await {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the dead worker is retired");
    let mut ended = None;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => read.extend_from_slice(&chunk.data),
            Err(status) => ended = Some(status),
        }
    }

    let status = ended.expect("the read fails once its worker is gone");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(
        read.len() < data.len(),
        "the read stopped before the end of the file"
    );
    assert_eq!(read[..], data[..read.len()]);
}

async fn read_file(router: &FilesystemRouter, path: &std::path::Path) -> GuestFileChunkStream {
    Filesystem::read(
        router,
        Request::new(ReadRequest {
            path: path.to_string_lossy().into_owned(),
            offset: 0,
            length: None,
            user: None,
        }),
    )
    .await
    .expect("read")
    .into_inner()
}

#[tokio::test]
async fn reset_routes_release_admission_on_removal_or_expiry() {
    let directory = tempfile::tempdir().unwrap();
    let user = current_user();
    let mut router = identity_router();
    router.watcher_slots = Arc::new(tokio::sync::Semaphore::new(2));
    let (channel, proxy, server) = connected_worker(&router).await;
    let pool = router.worker_pool().expect("pool");
    let mut watcher_ids = Vec::new();
    for _ in 0..2 {
        pool.insert_test_client(user.clone(), channel.clone())
            .await
            .unwrap();
        let watcher = router
            .create_watcher(Request::new(CreateWatcherRequest {
                root: directory.path().display().to_string(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        watcher_ids.push(watcher.watcher_id);
        let worker = router.worker(None).await.unwrap();
        router.record_worker_failure(&worker).await;
    }
    let request = || {
        Request::new(CreateWatcherRequest {
            root: directory.path().display().to_string(),
            ..Default::default()
        })
    };
    assert_eq!(
        router.create_watcher(request()).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        router
            .get_watcher_events(Request::new(GetWatcherEventsRequest {
                watcher_id: watcher_ids[0].clone(),
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    router
        .remove_watcher(Request::new(RemoveWatcherRequest {
            watcher_id: watcher_ids[0].clone(),
        }))
        .await
        .expect("reset watcher removal succeeds without a live worker");
    assert_eq!(
        router
            .remove_watcher(Request::new(RemoveWatcherRequest {
                watcher_id: watcher_ids[0].clone(),
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    pool.insert_test_client(user, channel).await.unwrap();
    let live = router
        .create_watcher(request())
        .await
        .expect("explicit removal releases capacity before expiry")
        .into_inner();
    assert_eq!(
        router.create_watcher(request()).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    {
        let mut routes = router.watcher_routes.lock().await;
        assert_eq!(routes.len(), 2);
        for route in routes.values_mut() {
            if let WatcherRoute::Reset { expires_at, .. } = route {
                *expires_at = Instant::now() - Duration::from_secs(1);
            }
        }
    }
    router
        .create_watcher(request())
        .await
        .expect("expiry releases route capacity");
    router
        .remove_watcher(Request::new(RemoveWatcherRequest {
            watcher_id: live.watcher_id,
        }))
        .await
        .expect("live watcher removal still reaches the worker");
    router
        .create_watcher(request())
        .await
        .expect("live watcher removal releases capacity");
    stop_worker(proxy, server).await;
}

#[test]
fn dropping_watch_stream_outside_runtime_does_not_panic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let (stream, pool, user) = runtime.block_on(async {
        let user = current_user();
        let router = identity_router();
        let channel = Endpoint::from_static("http://[::]:50051").connect_lazy();
        router
            .worker_pool()
            .expect("worker backend")
            .insert_test_client(user.clone(), channel)
            .await
            .expect("test worker inserted");
        let worker = router
            .worker(Some(user.as_str().to_owned()))
            .await
            .expect("worker routed");
        (
            FailureAwareStream {
                inner: Box::pin(tokio_stream::empty::<Result<(), Status>>()),
                router: router.clone(),
                worker,
                runtime_handle: tokio::runtime::Handle::current(),
                failure_recorded: false,
            },
            router.worker_pool().expect("worker backend").clone(),
            user,
        )
    });

    assert!(
        !runtime.block_on(pool.is_idle_test(&user)),
        "stream retains its worker"
    );
    drop(stream);
    runtime.block_on(async {
        assert!(
            pool.is_idle_test(&user).await,
            "dropping a watch stream releases worker accounting"
        );
    });
}

#[tokio::test]
async fn worker_loss_remains_visible_when_the_observer_is_cancelled() {
    for cancel_observer in [false, true] {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let user = current_user();
        let router = identity_router();
        let (_channel, proxy, server) = connected_worker(&router).await;

        router
            .enable_file_changes(Request::new(EnableFileChangesRequest {
                roots: vec![temp_dir.path().display().to_string()],
                include_globs: Vec::new(),
                exclude_globs: Vec::new(),
                use_default_excludes: None,
                user: Some(user.as_str().to_owned()),
            }))
            .await
            .expect("ledger enabled");
        if cancel_observer {
            let worker = router.worker(None).await.expect("worker");
            let accounting = router.watcher_routes.lock().await;
            let observer_router = router.clone();
            let observer_worker = worker.clone();
            let observer = tokio::spawn(async move {
                observer_router
                    .record_worker_failure(&observer_worker)
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while router.worker_pool().expect("pool").contains(&worker).await {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("resident removed before route reset");
            observer.abort();
            observer.await.expect_err("observer cancelled");
            drop(accounting);
            tokio::time::timeout(Duration::from_secs(5), async {
                while !router.tasks.is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("loss owner settles");
            assert!(router.ledger_evidence.lock().await.get(&user) == Some(&LedgerEvidence::Lost));
        }
        proxy.abort();
        let _ = proxy.await;

        let response = router
            .get_modified_files(Request::new(GetModifiedFilesRequest {
                window: None,
                since: None,
                cursor: None,
                user: Some(user.as_str().to_owned()),
            }))
            .await
            .expect("worker loss is represented as ledger overflow")
            .into_inner();

        assert!(!response.monitoring_enabled);
        assert!(response.overflowed);
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn ledger_update_outlives_caller_and_preserves_worker_loss() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let user = current_user();
    let router = identity_router();
    let (channel, proxy, server) = connected_worker(&router).await;
    let pool = router.worker_pool().expect("pool");
    let worker = router
        .worker(Some(user.as_str().to_owned()))
        .await
        .expect("worker routed");
    let mut client = worker.client.clone();
    let caller_router = router.clone();
    let caller_worker = worker.clone();
    let path = temp_dir.path().display().to_string();
    let (enabled_tx, enabled_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(async move {
        caller_router
            .update_ledger(caller_worker, true, async move {
                let response = client
                    .enable_file_changes(EnableFileChangesRequest {
                        roots: vec![path],
                        ..Default::default()
                    })
                    .await;
                enabled_tx.send(()).expect("enabled signal");
                release_rx.await.expect("accounting release");
                response
            })
            .await
    });
    enabled_rx.await.expect("worker enabled ledger");
    let accounting = router.ledger_evidence.lock().await;
    release_tx.send(()).expect("release worker reply");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let error = router
        .disable_file_changes(Request::new(DisableFileChangesRequest::default()))
        .await
        .expect_err("cannot reorder a disable ahead of pending enable accounting");
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    drop(accounting);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !router.tasks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("service-owned update settles");
    drop(worker);
    assert!(
        !pool.is_idle_test(&user).await,
        "caller cancellation must not make the enabled ledger evictable"
    );
    let worker = router.worker(None).await.unwrap();

    router.record_worker_failure(&worker).await;
    router.finish_enable_file_changes(&worker, &user).await;

    assert!(
        router.ledger_evidence.lock().await.get(&user) == Some(&LedgerEvidence::Lost),
        "successful enable whose worker is lost before router accounting must preserve overflow"
    );
    let response = router
        .get_modified_files(Request::new(GetModifiedFilesRequest {
            window: None,
            since: None,
            cursor: None,
            user: Some(user.as_str().to_owned()),
        }))
        .await
        .expect("lost acknowledged ledger is exposed through modified files")
        .into_inner();
    assert!(!response.monitoring_enabled);
    assert!(response.overflowed);
    pool.insert_test_client(user.clone(), channel)
        .await
        .unwrap();
    router
        .enable_file_changes(Request::new(EnableFileChangesRequest {
            roots: vec![temp_dir.path().display().to_string()],
            ..Default::default()
        }))
        .await
        .expect("replacement ledger enabled");
    router.finish_enable_file_changes(&worker, &user).await;
    router.record_worker_failure(&worker).await;
    assert!(
        router.ledger_evidence.lock().await.get(&user) != Some(&LedgerEvidence::Lost),
        "late old-worker accounting cannot overwrite replacement acknowledgement"
    );

    // A stalled accepted update has a deadline.
    let replacement = router.worker(None).await.unwrap();
    tokio::time::pause();
    let mut stalled = Box::pin(router.update_ledger(
        replacement.clone(),
        false,
        std::future::pending::<Result<Response<DisableFileChangesResponse>, Status>>(),
    ));
    assert!(futures_util::poll!(&mut stalled).is_pending());
    tokio::task::yield_now().await;
    tokio::time::advance(
        pplx_space_common_core::operation_budget::RuntimeOperationBudget::FilesystemMetadata
            .timeout(),
    )
    .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stalled)
            .await
            .expect("stalled update must settle after its RPC deadline")
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    assert!(!pool.contains(&replacement).await);
    assert!(router.ledger_evidence.lock().await.get(&user) == Some(&LedgerEvidence::Lost));
    tokio::time::resume();
    stop_worker(proxy, server).await;
}

#[tokio::test]
async fn ordinary_filesystem_work_releases_expired_watcher_leases() {
    enum MetadataCall {
        Stat,
        Query,
        Enable,
    }
    for metadata_call in [
        MetadataCall::Stat,
        MetadataCall::Query,
        MetadataCall::Enable,
    ] {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let user = current_user();
        let router = identity_router();
        let (_channel, proxy, server) = connected_worker(&router).await;

        let public_watcher_id = router
            .create_watcher(Request::new(CreateWatcherRequest {
                root: temp_dir.path().display().to_string(),
                recursive: true,
                include_metadata_events: false,
                include_globs: Vec::new(),
                exclude_globs: Vec::new(),
                use_default_excludes: None,
                user: Some(user.as_str().to_owned()),
            }))
            .await
            .expect("watcher created")
            .into_inner()
            .watcher_id;
        {
            let mut routes = router.watcher_routes.lock().await;
            let WatcherRoute::Live(route) = routes
                .get_mut(&public_watcher_id)
                .expect("public watcher route retained")
            else {
                panic!("created watcher route is live");
            };
            route.last_access = Instant::now() - WATCHER_ROUTE_TTL - Duration::from_secs(1);
        };

        assert!(!router.worker_pool().unwrap().is_idle_test(&user).await);
        match metadata_call {
            MetadataCall::Stat => {
                router
                    .stat_info(Request::new(StatRequest {
                        path: temp_dir.path().display().to_string(),
                        user: None,
                    }))
                    .await
                    .expect("stat");
            }
            MetadataCall::Query => {
                router
                    .get_modified_files(Request::new(GetModifiedFilesRequest::default()))
                    .await
                    .expect("query");
            }
            MetadataCall::Enable => {
                router
                    .enable_file_changes(Request::new(EnableFileChangesRequest {
                        roots: vec![temp_dir.path().display().to_string()],
                        ..Default::default()
                    }))
                    .await
                    .expect("enable");
                // Inspect eviction eligibility before a second handler can prune routes.
                assert!(
                    router.watcher_routes.lock().await.is_empty(),
                    "enable must prune expired leases"
                );
                router
                    .disable_file_changes(Request::new(DisableFileChangesRequest::default()))
                    .await
                    .expect("disable");
            }
        }
        assert!(
            router.worker_pool().unwrap().is_idle_test(&user).await,
            "expired routes must not keep identity workers active after ordinary work"
        );

        let error = router
            .get_watcher_events(Request::new(GetWatcherEventsRequest {
                watcher_id: public_watcher_id,
            }))
            .await
            .expect_err("expired public watcher is removed");
        assert_eq!(error.code(), tonic::Code::NotFound);

        stop_worker(proxy, server).await;
    }
}

#[tokio::test]
async fn write_input_failure_preserves_the_routed_worker() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let path = temp_dir.path().join("partial.txt");
    let user = current_user();
    let router = identity_router();
    let (_channel, worker_proxy, worker_server) = connected_worker(&router).await;
    let worker = router
        .worker(Some(user.as_str().to_owned()))
        .await
        .expect("test worker routed");
    let first = WriteRequest {
        message: Some(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::write_request::Message::Open(
                pplx_space_spaced_guest_protocol::proto::bridge_guest::WriteOpen {
                    path: path.display().to_string(),
                    append: false,
                    idempotency_key: Some("partial-write-key".to_string()),
                    user: None,
                },
            ),
        ),
    };
    let error = router
        .write_stream(
            first,
            Box::pin(tokio_stream::iter(vec![
                Ok(WriteRequest {
                    message: Some(
                        pplx_space_spaced_guest_protocol::proto::bridge_guest::write_request::Message::Data(
                            b"partial".to_vec(),
                        ),
                    ),
                }),
                Err(Status::aborted("host write stream aborted")),
            ])),
            None,
        )
        .await
        .expect_err("input stream error is returned");
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert!(
        router
            .worker_pool()
            .expect("worker backend")
            .contains(&worker)
            .await,
        "input stream failure must preserve a healthy identity worker"
    );
    stop_worker(worker_proxy, worker_server).await;
}

#[tokio::test]
async fn dropped_write_relay_releases_input() {
    let directory = tempfile::tempdir().expect("directory");
    let router = identity_router();
    let (_channel, proxy, server) = connected_worker(&router).await;
    let (dropped, observed) = tokio::sync::oneshot::channel::<()>();
    let input = futures_util::stream::pending().map(move |message| {
        let _keep_until_stream_drop = &dropped;
        message
    });
    let mut relay = Box::pin(router.write_stream(
        WriteRequest {
            message: Some(
                pplx_space_spaced_guest_protocol::proto::operations::write_request::Message::Open(
                    pplx_space_spaced_guest_protocol::proto::operations::WriteOpen {
                        path: directory.path().join("append").display().to_string(),
                        append: true,
                        idempotency_key: Some("drop-relay".into()),
                        user: None,
                    },
                ),
            ),
        },
        Box::pin(input),
        None,
    ));
    assert!(futures_util::poll!(&mut relay).is_pending());
    drop(relay);
    let released = tokio::time::timeout(Duration::from_secs(1), observed)
        .await
        .is_ok();
    router.drain().await;
    stop_worker(proxy, server).await;
    assert!(
        released,
        "dropping the handler must promptly drop its input stream"
    );
}

#[tokio::test]
async fn partial_append_retry_survives_worker_replacement() {
    use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
        WriteOpen, write_request::Message,
    };
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("append");
    let router = identity_router();
    let (_channel, proxy, server) = connected_worker(&router).await;
    let worker = router.worker(None).await.expect("worker");
    let open = WriteRequest {
        message: Some(Message::Open(WriteOpen {
            path: path.display().to_string(),
            append: true,
            idempotency_key: Some("replacement-append".into()),
            user: None,
        })),
    };
    let data = WriteRequest {
        message: Some(Message::Data(b"once".to_vec())),
    };
    let watched_path = path.clone();
    let input = futures_util::stream::iter([Ok(data.clone())]).chain(futures_util::stream::once(
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if tokio::fs::read(&watched_path)
                        .await
                        .is_ok_and(|bytes| bytes == b"once")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("append must be visible before interruption");
            Err(Status::aborted("upload interrupted"))
        },
    ));
    router
        .write_stream(open.clone(), Box::pin(input), None)
        .await
        .expect_err("interrupted upload");
    router.record_worker_failure(&worker).await;
    stop_worker(proxy, server).await;
    let (_channel, proxy, server) = connected_worker(&router).await;
    router.worker(None).await.expect("replacement worker");
    let retry = router
        .write_stream(open, Box::pin(futures_util::stream::iter([Ok(data)])), None)
        .await;
    let bytes = tokio::fs::read(path).await.expect("file");
    stop_worker(proxy, server).await;
    assert!(
        retry.is_err(),
        "same-key retry must not execute after replacement"
    );
    assert_eq!(bytes, b"once");
}

#[tokio::test]
async fn saturated_pool_recovers_lost_ledger_residency_after_acquirer_cancellation() {
    let router = identity_router();
    let (channel, proxy, server) = connected_worker(&router).await;
    let pool = router.worker_pool().expect("workers");
    let initial = pool
        .client_for(current_user())
        .await
        .expect("initial worker");
    pool.invalidate(&initial).await;
    drop(initial);
    let mut identities = Vec::new();
    let mut update_slots = Vec::new();
    for index in 0..16 {
        let user = GuestUserName::parse(format!("ledger-{index}")).expect("identity");
        pool.insert_test_client(user.clone(), channel.clone())
            .await
            .expect("insert");
        let worker = pool.client_for(user.clone()).await.expect("resident");
        pool.set_ledger_enabled(&worker, true).await;
        update_slots.push(worker.ledger_update_slot.clone());
        router
            .ledger_evidence
            .lock()
            .await
            .insert(user.clone(), LedgerEvidence::Tracking);
        identities.push(user);
    }
    // The real worker has no active ledger, but stale parent flags pin all16.
    // Hold publication so cancellation occurs after the recovery owner starts.
    let accounting = router.ledger_evidence.lock().await;
    let acquiring_router = router.clone();
    let caller = tokio::spawn(async move { acquiring_router.acquire_worker(current_user()).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while update_slots
            .iter()
            .all(|slot| slot.available_permits() == 1)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("capacity recovery starts without ledger polling");
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    drop(accounting);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !router.tasks.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovery owner publishes after caller cancellation");
    let mut recovered = 0;
    for user in identities {
        if !pool.ledger_enabled(&user).await {
            recovered += 1;
        }
    }
    assert!(
        recovered > 0,
        "stale monitoring state must release resident capacity"
    );
    stop_worker(proxy, server).await;
}

#[tokio::test]
async fn failed_initial_enable_cannot_erase_replacement_reservation() {
    let router = identity_router();
    let (channel, proxy, server) = connected_worker(&router).await;
    let pool = router.worker_pool().expect("pool");
    let worker = router.worker(None).await.expect("worker");
    let (release, released) = tokio::sync::oneshot::channel();
    let mut old = Box::pin(router.update_ledger(worker.clone(), true, async move {
        released.await.expect("release old reply");
        Err::<Response<EnableFileChangesResponse>, _>(Status::invalid_argument(
            "old request failed",
        ))
    }));
    assert!(futures_util::poll!(&mut old).is_pending());
    pool.insert_test_client(worker.user.clone(), channel)
        .await
        .expect("replacement");
    let replacement = router.worker(None).await.expect("replacement");
    let (ack, acknowledged) = tokio::sync::oneshot::channel();
    let mut new = Box::pin(router.update_ledger(replacement.clone(), true, async move {
        acknowledged.await.expect("release new reply");
        Ok(Response::new(EnableFileChangesResponse::default()))
    }));
    assert!(futures_util::poll!(&mut new).is_pending());
    release.send(()).expect("release old request");
    assert_eq!(
        old.await.expect_err("old failure").code(),
        tonic::Code::InvalidArgument
    );
    ack.send(()).expect("acknowledge replacement");
    new.await.expect("replacement enabled");
    assert!(
        pool.ledger_enabled(&worker.user).await,
        "acknowledged replacement must remain pinned"
    );
    assert!(
        router.ledger_evidence.lock().await.get(&worker.user) == Some(&LedgerEvidence::Tracking)
    );
    router
        .update_ledger(replacement.clone(), false, async {
            Ok(Response::new(DisableFileChangesResponse {}))
        })
        .await
        .expect("disable replacement");
    let (release, released) = tokio::sync::oneshot::channel();
    let mut unacknowledged =
        Box::pin(router.update_ledger(replacement.clone(), true, async move {
            released.await.expect("release successful reply");
            Ok(Response::new(EnableFileChangesResponse::default()))
        }));
    assert!(futures_util::poll!(&mut unacknowledged).is_pending());
    router.record_worker_failure(&replacement).await;
    release.send(()).expect("release reply after loss");
    assert_eq!(
        unacknowledged
            .await
            .expect_err("retired generation cannot acknowledge an untracked ledger")
            .code(),
        tonic::Code::Unavailable
    );
    assert!(
        !router
            .ledger_evidence
            .lock()
            .await
            .contains_key(&worker.user)
    );
    let result = router
        .update_ledger(replacement, true, async {
            Ok(Response::new(EnableFileChangesResponse::default()))
        })
        .await;
    assert_eq!(
        result
            .expect_err("retired generation cannot reserve new evidence")
            .code(),
        tonic::Code::Unavailable
    );
    stop_worker(proxy, server).await;
}

#[tokio::test]
async fn ledger_identity_capacity_preserves_loss_and_heals_through_reenable_disable() {
    let router = identity_router();
    let (channel, proxy, server) = connected_worker(&router).await;
    let pool = router.worker_pool().expect("pool");
    let user = current_user();
    let worker = router.worker(None).await.expect("worker");
    {
        let mut evidence = router.ledger_evidence.lock().await;
        for index in 0..MAX_LEDGER_IDENTITIES {
            evidence.insert(
                GuestUserName::parse(format!("lost-{index}")).expect("identity"),
                LedgerEvidence::Lost,
            );
        }
    }
    let error = router
        .update_ledger(worker.clone(), true, async {
            Ok(Response::new(EnableFileChangesResponse::default()))
        })
        .await
        .expect_err("new identity rejected before dispatch");
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    {
        let mut evidence = router.ledger_evidence.lock().await;
        evidence.remove(&GuestUserName::parse("lost-0").expect("identity"));
        evidence.insert(user.clone(), LedgerEvidence::Lost);
    }
    router
        .update_ledger(worker.clone(), false, async {
            Ok(Response::new(DisableFileChangesResponse {}))
        })
        .await
        .expect("disable lost ledger");
    assert_eq!(
        router.ledger_evidence.lock().await.len(),
        MAX_LEDGER_IDENTITIES
    );
    let old_worker = worker;
    pool.insert_test_client(user.clone(), channel)
        .await
        .expect("replacement");
    let worker = router.worker(None).await.expect("replacement worker");
    router
        .update_ledger(worker.clone(), true, async {
            Ok(Response::new(EnableFileChangesResponse::default()))
        })
        .await
        .expect("reenable existing identity at capacity");
    router
        .update_ledger(worker, false, async {
            Ok(Response::new(DisableFileChangesResponse {}))
        })
        .await
        .expect("disable acknowledged replacement");
    router.record_worker_failure(&old_worker).await;
    router.finish_enable_file_changes(&old_worker, &user).await;
    assert_eq!(
        router.ledger_evidence.lock().await.len(),
        MAX_LEDGER_IDENTITIES - 1
    );
    stop_worker(proxy, server).await;
}
