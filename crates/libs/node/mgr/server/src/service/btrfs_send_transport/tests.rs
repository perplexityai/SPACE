use super::*;
use crate::btrfs;
use pplx_space_common_svc::PeerIdentity;
use tokio_util::sync::CancellationToken;

fn authorized<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(PeerIdentity::new(0, 0, None));
    request
}

#[tokio::test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
async fn durable_lease_survives_producer_removal_and_cancelled_grpc_reader() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: SPACE_NODEMGR_TEST_BTRFS_DIR is unset");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let temp = tempfile::tempdir_in(root).unwrap();
    let root = temp.path().canonicalize().unwrap();
    let directory = trusted_paths::open_directory(&root).unwrap();
    btrfs::create(&directory, "parent".as_ref()).unwrap();
    let parent = artifact_fs::directory(&directory, "parent".as_ref()).unwrap();
    std::fs::write(root.join("parent/rootfs.ext4"), b"template").unwrap();
    btrfs::stamp(&parent, Uuid::now_v7()).unwrap();
    btrfs::set_readonly(&parent, true).unwrap();
    btrfs::snapshot(&parent, &directory, "instance".as_ref(), false).unwrap();
    let expected = vec![42; 4 * 1024 * 1024];
    std::fs::write(root.join("instance/rootfs.ext4"), &expected).unwrap();
    let sandbox = Uuid::now_v7().to_string();
    let sandbox_path =
        Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR).join(&sandbox);
    std::fs::create_dir(&sandbox_path).unwrap();
    let artifact = sandbox_path.join("snapshot");
    std::fs::create_dir(&artifact).unwrap();
    let source =
        btrfs_send_source::capture(&artifact, &root.join("parent"), &root.join("instance"))
            .unwrap();
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let lease = BtrfsSendLease {
        lease_id: Uuid::now_v7().to_string(),
    };
    service
        .btrfs_read_send_source(Request::new(lease.clone()))
        .await
        .map(|_| ())
        .expect_err("missing identity must be denied");
    service
        .btrfs_stage_send_source(authorized(BtrfsStageSendSourceRequest {
            sandbox_id: sandbox.clone(),
            artifact_dir: artifact.to_string_lossy().into_owned(),
            source_id: source.to_string(),
            lease_id: lease.lease_id.clone(),
        }))
        .await
        .unwrap();
    // Destroy the original owner and parent. Only the acknowledged lease can
    // supply a retry now; a pointer to the producer's path would fail.
    artifact_fs::remove(&sandbox_path).unwrap();
    artifact_fs::remove_at(&directory, "instance".as_ref()).unwrap();
    artifact_fs::remove_at(&directory, "parent".as_ref()).unwrap();
    drop(parent);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let server = crate::proto::node_manager_server::NodeManagerServer::with_interceptor(
        service.clone(),
        |mut req: Request<()>| {
            req.extensions_mut().insert(PeerIdentity::new(0, 0, None));
            Ok(req)
        },
    );
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stop.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    let mut client =
        crate::proto::node_manager_client::NodeManagerClient::connect(format!("http://{address}"))
            .await
            .unwrap();
    let mut interrupted = client
        .btrfs_read_send_source(lease.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(
        !interrupted
            .message()
            .await
            .unwrap()
            .unwrap()
            .data
            .is_empty()
    );
    drop(interrupted);
    // A new reader waits for the old kernel writer to release its source lock.
    // Retry from persisted identity rather than replaying the old byte stream.
    for _ in 0..2 {
        let mut stream = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client.btrfs_read_send_source(lease.clone()),
        )
        .await
        .unwrap()
        .unwrap()
        .into_inner();
        let mut bytes = Vec::new();
        let mut completion = None;
        while let Some(frame) = stream.message().await.unwrap() {
            if let Some(total) = frame.completed_bytes {
                completion = Some(total);
            } else {
                assert!(completion.is_none());
                bytes.extend(frame.data);
            }
        }
        assert_eq!(completion, Some(bytes.len() as u64));
        let received = root.join("received");
        std::fs::create_dir(&received).unwrap();
        let wire = root.join("wire");
        std::fs::write(&wire, &bytes).unwrap();
        let output = std::process::Command::new("btrfs")
            .arg("receive")
            .arg(&received)
            .stdin(File::open(&wire).unwrap())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read(received.join("instance.ro/rootfs.ext4")).unwrap(),
            expected
        );
        artifact_fs::remove_at(&directory, "received".as_ref()).unwrap();
    }
    // Deletion also drains an abandoned stream and remains idempotent.
    let interrupted = client
        .btrfs_read_send_source(lease.clone())
        .await
        .unwrap()
        .into_inner();
    drop(interrupted);
    for _ in 0..2 {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client.btrfs_delete_send_source(lease.clone()),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert!(!Path::new(ROOT).join(&lease.lease_id).exists());
    client
        .btrfs_read_send_source(lease)
        .await
        .map(|_| ())
        .expect_err("released lease must not be readable");
    shutdown.cancel();
    task.await.unwrap();
}
