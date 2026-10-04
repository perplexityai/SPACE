use crate::fixture::HostFixture;
use crate::mock_bridge_guest::start_mock_bridge_guest;
use crate::time::timestamp_from_millis;
use std::time::Duration;

#[tokio::test]
async fn metadata_fallback_only_on_opening_unimplemented() {
    use crate::mock_bridge_guest::{MockBridgeGuest, start_mock_bridge_guest_with_service};
    use futures_util::StreamExt;
    use tonic::Code;

    for opening_error in [
        None,
        Some(Code::Unimplemented),
        Some(Code::PermissionDenied),
        Some(Code::Unavailable),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("guest.sock");
        let bridge = start_mock_bridge_guest_with_service(
            socket.clone(),
            MockBridgeGuest {
                metadata_open_error: opening_error,
                list_body_error: true,
                ..Default::default()
            },
        )
        .await;
        let fixture = HostFixture::start(Some(socket)).await;
        let client = fixture.client().await;
        let stat = client.stat("/tmp/test-file".into()).await;
        let list = client
            .list_stream_with_depth_as_user("/tmp/test-dir".into(), Some(1), None)
            .await;
        if opening_error == Some(Code::Unimplemented) {
            let info = stat.expect("legacy stat").info.expect("stat info");
            assert_eq!(info.path, "/tmp/test-file");
            assert_eq!(info.size_bytes, 123);
            assert_eq!(info.mode, 0o100644);
            assert_eq!(info.modified_at, Some(timestamp_from_millis(1_000)));
            let entries = list.expect("legacy list").collect::<Vec<_>>().await;
            let entries = entries
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("entries");
            assert_eq!(entries.len(), 2);
            for (entry, name, kind) in [
                (
                    &entries[0],
                    "a.txt",
                    pplx_space_spaced_host_client::proto::FileType::File,
                ),
                (
                    &entries[1],
                    "nested",
                    pplx_space_spaced_host_client::proto::FileType::Directory,
                ),
            ] {
                assert_eq!(entry.name.as_deref(), Some(name));
                assert_eq!(entry.path, format!("/tmp/test-dir/{name}"));
                assert_eq!(entry.file_type(), kind);
            }
        } else if let Some(code) = opening_error {
            assert_eq!(stat.unwrap_err().code(), code);
            assert_eq!(list.expect_err("no legacy fallback").code(), code);
        } else {
            assert_eq!(stat.unwrap().info.unwrap().size_bytes, 321);
            let mut list = list.unwrap();
            assert_eq!(
                list.next().await.unwrap().unwrap().name.as_deref(),
                Some("canonical.txt")
            );
            assert_eq!(
                list.next().await.unwrap().unwrap_err().code(),
                Code::Unimplemented
            );
            assert!(
                list.next().await.is_none(),
                "a started stream is never replayed"
            );
        }
        fixture.stop().await;
        bridge.abort();
    }
}

#[tokio::test]
async fn interrupted_upload_through_host_and_guest_is_not_successful_eof() {
    use pplx_space_common_core::types::GuestUserName;
    use pplx_space_spaced_guest_server::{
        FilesystemExecutionMode, HostConnectorType, SpacedGuestService, SpacedGuestServiceConfig,
    };
    use pplx_space_spaced_host_client::proto::{
        WriteOpen, WriteRequest, filesystem_client::FilesystemClient, write_request::Message,
    };
    use tokio_util::sync::CancellationToken;
    let directory = tempfile::tempdir().expect("directory");
    let socket = directory.path().join("guest.sock");
    let account = tokio::process::Command::new("id")
        .arg("-un")
        .output()
        .await
        .expect("current account");
    assert!(account.status.success());
    let user = GuestUserName::parse(
        String::from_utf8(account.stdout)
            .expect("account name")
            .trim(),
    )
    .expect("user");
    let shutdown = CancellationToken::new();
    let guest = SpacedGuestService::with_config(
        shutdown.clone(),
        SpacedGuestServiceConfig {
            default_operation_user: user,
            filesystem_execution: FilesystemExecutionMode::Direct,
            host_connector_type: HostConnectorType::Uds(directory.path().join("unused.sock")),
            local_socket_path: None,
            bridge_socket_path: Some(socket.clone()),
            ..Default::default()
        },
    );
    guest.ready();
    let guest_task = tokio::spawn(guest.run());
    let fixture = HostFixture::start(Some(socket.clone())).await;
    fixture.client().await;
    let channel = pplx_space_common_grpc::connect_uds_channel("test host", &fixture.socket_path)
        .await
        .expect("host channel");
    let mut client = FilesystemClient::new(channel);
    for invalid_input in [true, false] {
        let path = directory.path().join(format!("append-{invalid_input}"));
        let open = WriteRequest {
            message: Some(Message::Open(WriteOpen {
                path: path.display().to_string(),
                append: true,
                idempotency_key: Some(format!("host-upload-{invalid_input}")),
                user: None,
            })),
        };
        let data = WriteRequest {
            message: Some(Message::Data(b"once".to_vec())),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(open.clone()).await.expect("open");
        tx.send(data.clone()).await.expect("data");
        let mut uploading = client.clone();
        let task = tokio::spawn(async move {
            uploading
                .write(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
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
        .expect("partial effect through both services");
        if invalid_input {
            tx.send(WriteRequest { message: None })
                .await
                .expect("malformed input");
            task.await
                .expect("request joined")
                .expect_err("input error");
            // Remove the listener's original address temporarily: a healthy
            // cached connection still works, but an unnecessary reconnect cannot.
            let parked = socket.with_extension("parked");
            tokio::fs::rename(&socket, &parked)
                .await
                .expect("park listener");
            let probe = tokio::time::timeout(
                Duration::from_secs(2),
                client.stat(pplx_space_spaced_host_client::proto::StatRequest {
                    path: path.display().to_string(),
                    user: None,
                }),
            )
            .await;
            tokio::fs::rename(&parked, &socket)
                .await
                .expect("restore listener");
            probe
                .expect("reuse existing bridge connection")
                .expect("input failure preserves cached client");
        } else {
            task.abort();
            assert!(task.await.expect_err("caller cancelled").is_cancelled());
        }
        let retry = tokio::time::timeout(
            Duration::from_secs(5),
            client.write(tokio_stream::iter([open, data])),
        )
        .await
        .expect("retry deadline")
        .expect_err("unknown outcome must not become success");
        assert_eq!(retry.code(), tonic::Code::FailedPrecondition);
        assert_eq!(tokio::fs::read(path).await.expect("file"), b"once");
        drop(tx);
    }
    let completed = directory.path().join("completed");
    let response = client
        .write(tokio_stream::iter([
            WriteRequest {
                message: Some(Message::Open(WriteOpen {
                    path: completed.display().to_string(),
                    append: true,
                    idempotency_key: Some("normal-eof".into()),
                    user: None,
                })),
            },
            WriteRequest {
                message: Some(Message::Data(b"complete".to_vec())),
            },
        ]))
        .await
        .expect("normal EOF through both services");
    assert_eq!(response.into_inner().bytes_written, 8);
    assert_eq!(
        tokio::fs::read(completed).await.expect("completed file"),
        b"complete"
    );
    fixture.stop().await;
    shutdown.cancel();
    guest_task
        .await
        .expect("guest joined")
        .expect("guest stopped");
}

#[tokio::test]
async fn read_relays_to_guest_bridge() {
    use crate::mock_bridge_guest::{MockBridgeGuest, start_mock_bridge_guest_with_service};
    let read_continue = std::sync::Arc::new(tokio::sync::Notify::new());
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let bridge_task = start_mock_bridge_guest_with_service(
        bridge_guest_socket_path.clone(),
        MockBridgeGuest {
            read_continue: Some(read_continue.clone()),
            ..Default::default()
        },
    )
    .await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    let mut stream = tokio::time::timeout(
        Duration::from_secs(2),
        client.read_stream_as_user("/tmp/test-file".into(), 1, Some(3), None),
    )
    .await
    .expect("read timed out")
    .expect("read response");
    assert_eq!(&stream.message().await.unwrap().unwrap().data[..], b"bc");
    let mut stopping = tokio::spawn(fixture.stop_gracefully());
    // A live stream belongs to the drain, even after new admission closes.
    tokio::time::timeout(Duration::from_millis(20), &mut stopping)
        .await
        .expect_err("active stream holds drain");
    read_continue.notify_one();
    assert_eq!(&stream.message().await.unwrap().unwrap().data[..], b"d");
    assert!(stream.message().await.unwrap().is_none());
    stopping.await.unwrap();
    bridge_task.abort();
}

#[tokio::test]
async fn write_relays_to_guest_bridge() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let bridge_task = start_mock_bridge_guest(bridge_guest_socket_path.clone()).await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client.write("/tmp/test-file".into(), b"hello".to_vec(), false),
    )
    .await
    .expect("write timed out")
    .expect("write response");
    assert_eq!(response.bytes_written, 5);

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn mkdir_and_remove_relay_to_guest_bridge() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket_path = temp_dir.path().join("spaced-bridge-guest.sock");
    let bridge_task = start_mock_bridge_guest(bridge_guest_socket_path.clone()).await;
    let fixture = HostFixture::start(Some(bridge_guest_socket_path)).await;
    let client = fixture.client().await;

    client
        .mkdir("/tmp/test-dir".into(), true, true, None)
        .await
        .expect("mkdir response");
    client
        .remove("/tmp/test-dir".into(), true, true)
        .await
        .expect("remove response");

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn watch_relays_to_guest_bridge() {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let bridge_guest_socket = temp_dir.path().join("spaced-bridge-guest.sock");
    let bridge_task = start_mock_bridge_guest(bridge_guest_socket.clone()).await;
    let fixture = HostFixture::start(Some(bridge_guest_socket)).await;
    let client = fixture.client().await;

    let mut events = client
        .watch_stream("/tmp/test-dir".into(), false)
        .await
        .expect("watch response");
    assert_eq!(events.message().await.unwrap().unwrap().path, "created.txt");
    assert!(events.message().await.unwrap().is_none());

    fixture.stop().await;
    bridge_task.abort();
}

#[tokio::test]
async fn lost_mutation_replies_are_not_replayed_by_host_or_client() {
    use crate::mock_bridge_guest::{MockBridgeGuest, start_mock_bridge_guest_with_service};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let directory = tempfile::tempdir().expect("directory");
    let socket = directory.path().join("guest.sock");
    let calls = Arc::new(AtomicUsize::new(0));
    let bridge = start_mock_bridge_guest_with_service(
        socket.clone(),
        MockBridgeGuest {
            lost_reply_calls: Some(calls.clone()),
            ..Default::default()
        },
    )
    .await;
    let fixture = HostFixture::start(Some(socket)).await;
    let client = fixture.client().await;
    for operation in 0..3 {
        calls.store(0, Ordering::SeqCst);
        let result = match operation {
            0 => client
                .remove_as_user("/tmp/test-dir".into(), true, true, None)
                .await
                .map(|_| ()),
            1 => client
                .create_watcher_with_options("/tmp/test-dir".into(), Default::default())
                .await
                .map(|_| ()),
            _ => client
                .get_watcher_events("watcher-1".into())
                .await
                .map(|_| ()),
        };
        assert_eq!(
            result.expect_err("ambiguous outcome").code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "operation {operation} was replayed"
        );
    }
    fixture.stop().await;
    bridge.abort();
}
