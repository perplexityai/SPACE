//! The test executable is also the real identity-worker child executable.
//! This exercises the daemon-selected executable, credential setup, socket
//! inheritance, RPCs, cancellation and replacement rather than substituting an
//! in-process store.
#![cfg(test)]

#[cfg(target_os = "linux")]
fn main() {
    if pplx_space_spaced_guest_server::run_filesystem_worker_from_environment()
        .expect("worker entry")
    {
        return;
    }
    // Support libtest discovery so nextest executes the harness once, rather
    // than running the subprocess scenario while listing the test inventory.
    if std::env::args().any(|argument| argument == "--list") {
        if !std::env::args().any(|argument| argument == "--ignored") {
            println!("worker_subprocess: test");
        }
        return;
    }
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(60), run())
                .await
                .expect("worker lifecycle scenario deadline");
        });
}

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
async fn run() {
    use pplx_space_common_core::types::GuestUserName;
    use pplx_space_spaced_common::proto::bridge_guest::{
        StatRequest, WriteOpen, WriteRequest, filesystem_client::FilesystemClient,
        write_request::Message,
    };
    use pplx_space_spaced_guest_server::{
        FilesystemExecutionMode, HostConnectorType, SpacedGuestService, SpacedGuestServiceConfig,
    };
    use tokio_util::sync::CancellationToken;
    let directory = tempfile::tempdir().expect("directory");
    let socket = directory.path().join("guest.sock");
    let user = nix::unistd::User::from_uid(nix::unistd::Uid::current())
        .expect("lookup")
        .expect("account");
    let shutdown = CancellationToken::new();
    let service = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            default_operation_user: GuestUserName::parse(user.name).expect("user"),
            filesystem_execution: FilesystemExecutionMode::IdentityWorkers,
            host_connector_type: HostConnectorType::Uds(directory.path().join("unused-host.sock")),
            local_socket_path: None,
            bridge_socket_path: Some(socket.clone()),
            // The FIFO-blocked batch below only settles when the router gives
            // up waiting and reaps, so the deadline is a floor on this test's
            // runtime. One second is far above the microseconds every
            // cancellation that can finish actually takes here, so the
            // non-escalating cases still complete inside it.
            cancellation_grace: std::time::Duration::from_secs(1),
            ..Default::default()
        },
    )
    .expect("guest service");
    service.ready();
    let task = tokio::spawn(service.clone().run());
    assert!(service.wait_until_listening().await);
    let channel = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let socket = socket.clone();
            let result = tonic::transport::Endpoint::from_static("http://[::]:50051")
                .connect_with_connector(tower::service_fn(move |_| {
                    let socket = socket.clone();
                    async move {
                        tokio::net::UnixStream::connect(socket)
                            .await
                            .map(hyper_util::rt::TokioIo::new)
                    }
                }))
                .await;
            if let Ok(channel) = result {
                break channel;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connect");
    let mut client = FilesystemClient::new(channel);
    // Public messages at the wire limit must fit after the router adds its
    // private execution identity. Keep a live worker to detect collateral reset.
    client
        .stat_info(StatRequest {
            path: directory.path().display().to_string(),
            user: None,
        })
        .await
        .expect("start worker");
    let original_worker = children().await;
    {
        use pplx_space_spaced_common::proto::bridge_guest::{WriteBatchEntry, WriteBatchRequest};
        use prost::Message as _;
        let limit = pplx_space_spaced_common::filesystem_limits::MAX_MESSAGE_BYTES;
        for keyed in [false, true] {
            let path = directory.path().join(format!("maximum-batch-{keyed}"));
            let mut batch = WriteBatchRequest {
                entries: vec![WriteBatchEntry {
                    path: path.display().to_string(),
                    data: vec![42; limit],
                    append: keyed,
                }],
                idempotency_key: keyed.then(|| "k".to_owned()),
                user: None,
            };
            let overhead = batch.encoded_len() - limit;
            batch
                .entries
                .first_mut()
                .expect("batch entry")
                .data
                .truncate(limit - overhead);
            assert_eq!(batch.encoded_len(), limit);
            client
                .write_batch(batch.clone())
                .await
                .expect("maximum public batch fits private hop");
            if keyed {
                client
                    .write_batch(batch.clone())
                    .await
                    .expect("replay maximum batch");
            }
            assert_eq!(
                tokio::fs::read(&path).await.expect("batch effect"),
                batch.entries.first().expect("batch entry").data
            );
            batch
                .entries
                .first_mut()
                .expect("batch entry")
                .data
                .push(42);
            assert_eq!(
                client
                    .write_batch(batch)
                    .await
                    .expect_err("public cap remains enforced")
                    .code(),
                tonic::Code::OutOfRange,
            );
            assert_eq!(
                children().await,
                original_worker,
                "batch must preserve healthy worker"
            );
        }
    }
    // ENOSPC shares the admission status code, but can follow partial effects.
    // Its same-key retry must replay failure without appending a second time.
    {
        use pplx_space_spaced_common::proto::bridge_guest::{WriteBatchEntry, WriteBatchRequest};
        let path = directory.path().join("partial-enospc");
        let batch = WriteBatchRequest {
            entries: vec![
                WriteBatchEntry {
                    path: path.display().to_string(),
                    data: b"once".to_vec(),
                    append: true,
                },
                WriteBatchEntry {
                    path: "/dev/full".into(),
                    data: b"fails".to_vec(),
                    append: true,
                },
            ],
            idempotency_key: Some("partial-enospc".into()),
            user: None,
        };
        for _ in 0..2 {
            assert_eq!(
                client
                    .write_batch(batch.clone())
                    .await
                    .expect_err("disk full")
                    .code(),
                tonic::Code::ResourceExhausted
            );
            assert_eq!(
                tokio::fs::read(&path).await.expect("partial effect"),
                b"once"
            );
            assert_eq!(
                children().await,
                original_worker,
                "operation failure preserves worker"
            );
        }
    }
    for kill_worker in [false, true] {
        let path = directory.path().join(format!("append-{kill_worker}"));
        let open = WriteRequest {
            message: Some(Message::Open(WriteOpen {
                path: path.display().to_string(),
                append: true,
                idempotency_key: Some(format!("append-{kill_worker}")),
                user: None,
            })),
        };
        let data = WriteRequest {
            message: Some(Message::Data(b"once".to_vec())),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(open.clone()).await.expect("open");
        tx.send(data.clone()).await.expect("data");
        let mut uploading_client = client.clone();
        let upload = tokio::spawn(async move {
            uploading_client
                .write(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if tokio::fs::read(&path)
                    .await
                    .is_ok_and(|bytes| bytes == b"once")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("partial append visible");
        let before = children().await;
        assert_eq!(before.len(), 1, "exactly one owned identity worker");
        if kill_worker {
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(*before.first().expect("owned identity worker")),
                nix::sys::signal::Signal::SIGKILL,
            )
            .expect("kill owned worker");
            upload
                .await
                .expect("upload join")
                .expect_err("worker death");
        } else {
            upload.abort();
            assert!(upload.await.expect_err("cancelled upload").is_cancelled());
        }
        // A new request must use a replacement after worker death; replay must
        // still consult the same router-owned record.
        let stat = StatRequest {
            path: path.display().to_string(),
            user: None,
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if client.stat_info(stat.clone()).await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker available");
        let retry = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.write(tokio_stream::iter([open, data])),
        )
        .await
        .expect("retry settles")
        .expect_err("same key cannot execute again");
        drop(tx);
        assert_eq!(retry.code(), tonic::Code::FailedPrecondition);
        assert_eq!(tokio::fs::read(&path).await.expect("file"), b"once");
        let after = children().await;
        assert_eq!(after.len(), 1);
        assert_eq!(
            before == after,
            !kill_worker,
            "only actual worker death replaces the process"
        );
    }
    // A batch can have durable partial effects before blocking on its next
    // entry. Killing that worker must not permit replay of the first append.
    use pplx_space_spaced_common::proto::bridge_guest::{
        RenameRequest, WriteBatchEntry, WriteBatchRequest,
    };
    for kill_worker in [true, false] {
        let batch_path = directory.path().join(format!("batch-{kill_worker}"));
        let fifo = directory
            .path()
            .join(format!("blocked-entry-{kill_worker}"));
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .expect("mkfifo")
                .success()
        );
        let batch = WriteBatchRequest {
            entries: vec![
                WriteBatchEntry {
                    path: batch_path.display().to_string(),
                    data: b"once".to_vec(),
                    append: true,
                },
                WriteBatchEntry {
                    path: fifo.display().to_string(),
                    data: b"blocked".to_vec(),
                    append: true,
                },
            ],
            idempotency_key: Some(format!("partial-batch-{kill_worker}")),
            user: None,
        };
        let mut batch_client = client.clone();
        let dispatch = batch.clone();
        let batch_task = tokio::spawn(async move { batch_client.write_batch(dispatch).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if tokio::fs::read(&batch_path)
                    .await
                    .is_ok_and(|bytes| bytes == b"once")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first batch effect visible");
        let owned = children().await;
        assert_eq!(owned.len(), 1);
        if kill_worker {
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(*owned.first().expect("owned batch worker")),
                nix::sys::signal::Signal::SIGKILL,
            )
            .expect("kill batch worker");
            batch_task
                .await
                .expect("batch joined")
                .expect_err("interrupted batch");
        } else {
            // The second open is blocked on a FIFO. Cancelling the RPC cannot
            // cancel that kernel operation: the router must escalate and reap.
            batch_task.abort();
            assert!(
                batch_task
                    .await
                    .expect_err("batch cancelled")
                    .is_cancelled()
            );
        }
        let retry =
            tokio::time::timeout(std::time::Duration::from_secs(8), client.write_batch(batch))
                .await
                .expect("bounded cleanup")
                .expect_err("batch retry fenced");
        assert!(
            children().await.is_empty(),
            "unknown published after reaping"
        );
        assert_eq!(retry.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            tokio::fs::read(&batch_path).await.expect("batch file"),
            b"once"
        );
    }

    // Completed rename records survive another worker's death as well.
    let old = directory.path().join("rename-old");
    let new = directory.path().join("rename-new");
    tokio::fs::write(&old, b"original")
        .await
        .expect("rename source");
    let rename = RenameRequest {
        old_path: old.display().to_string(),
        new_path: new.display().to_string(),
        idempotency_key: Some("rename".into()),
        user: None,
    };
    client.rename(rename.clone()).await.expect("rename");
    let owned = children().await;
    assert_eq!(owned.len(), 1);
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(*owned.first().expect("owned completed worker")),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("kill completed worker");
    tokio::fs::write(&old, b"replacement source")
        .await
        .expect("new source");
    client
        .rename(rename)
        .await
        .expect("replay completed rename");
    assert_eq!(
        tokio::fs::read(&old).await.expect("new source remains"),
        b"replacement source"
    );
    assert_eq!(
        tokio::fs::read(&new).await.expect("original target"),
        b"original"
    );
    shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(12), task)
        .await
        .expect("shutdown deadline")
        .expect("service join")
        .expect("service result");
    assert!(children().await.is_empty(), "shutdown reaps owned workers");
}

#[cfg(target_os = "linux")]
async fn children() -> Vec<i32> {
    let mut threads = tokio::fs::read_dir("/proc/self/task")
        .await
        .expect("threads");
    let mut children = Vec::new();
    while let Some(thread) = threads.next_entry().await.expect("thread") {
        if let Ok(list) = tokio::fs::read_to_string(thread.path().join("children")).await {
            children.extend(
                list.split_whitespace()
                    .map(|pid| pid.parse::<i32>().expect("child pid")),
            );
        }
    }
    children.sort_unstable();
    children.dedup();
    children
}
