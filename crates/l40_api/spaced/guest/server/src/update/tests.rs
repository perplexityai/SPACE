use super::*;
use pplx_space_spaced_update::{
    ArtifactDigest, Binary, BinaryIdentity, Journal, Selection, Slot, TrialProgress,
};
use std::{fs, os::unix::fs::PermissionsExt};

fn fixture() -> (tempfile::TempDir, SpacedGuestService, Attempt) {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("slots")).unwrap();
    let mut selections = Vec::new();
    for (slot, bytes) in [
        (Slot::A, b"old".as_slice()),
        (Slot::B, b"candidate".as_slice()),
    ] {
        let directory = root.path().join("slots").join(slot.name());
        fs::create_dir(&directory).unwrap();
        for binary in Binary::ALL {
            fs::write(directory.join(binary.file_name()), bytes).unwrap();
            fs::set_permissions(
                directory.join(binary.file_name()),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        let identity = BinaryIdentity {
            length: bytes.len() as u64,
            digest: ArtifactDigest::of_bytes(bytes),
        };
        let descriptor = BundleDescriptor {
            schema: 1,
            source_revision: "a".repeat(40),
            built_at: "2026-01-01T00:00:00Z".into(),
            version: "0.8.1".parse().unwrap(),
            guest_update_required_since: "0.8.1".parse().unwrap(),
            compatibility_family: "0.8".into(),
            architecture: "x86_64".parse().unwrap(),
            seed_contracts: vec![pplx_space_spaced_update::SEED_CONTRACT.into()],
            service: identity.clone(),
            cli: identity,
        };
        fs::write(
            directory.join("artifact.json"),
            descriptor.canonical_bytes().unwrap(),
        )
        .unwrap();
        selections.push(Selection {
            slot,
            digest: descriptor.digest().unwrap(),
        });
    }
    pplx_space_spaced_guest_update::initialize_at(root.path()).unwrap();
    let mut service = SpacedGuestService::with_config(
        CancellationToken::new(),
        SpacedGuestServiceConfig {
            host_connector_type: HostConnectorType::Uds(root.path().join("host.sock")),
            local_socket_path: Some(root.path().join("local.sock")),
            bridge_socket_path: Some(root.path().join("bridge.sock")),
            filesystem_execution: FilesystemExecutionMode::Direct,
            ..Default::default()
        },
    )
    .unwrap();
    let inner = Arc::get_mut(&mut service.inner).unwrap();
    inner.update.root = root.path().into();
    inner.update.clock = || Ok((Uuid::from_u128(1), 10));
    inner.update.boot.set(Uuid::from_u128(1)).unwrap();
    inner.update.enabled.store(true, Ordering::Release);
    let attempt = Attempt {
        operation: Uuid::now_v7(),
        execution: Uuid::now_v7(),
        boot: Uuid::from_u128(1),
        incarnation: inner.service_generation.parse().unwrap(),
        configuration_revision: 1,
        target: selections[1].clone(),
        confirmation_deadline_ms: 100,
    };
    let journal = Journal {
        schema: 1,
        committed: selections[0].clone(),
        state: JournalState::Trial {
            attempt: attempt.clone(),
            progress: TrialProgress::AwaitingHost,
        },
    };
    fs::write(
        root.path().join("update-state"),
        serde_json::to_vec(&journal).unwrap(),
    )
    .unwrap();
    fs::remove_file(root.path().join("current")).unwrap();
    std::os::unix::fs::symlink(Slot::B.selector(), root.path().join("current")).unwrap();
    inner.update.gated.store(true, Ordering::Release);
    (root, service, attempt)
}

#[tokio::test]
async fn commit_retry_finishes_link_repair_before_releasing_readiness() {
    let (root, service, attempt) = fixture();
    *service.inner.update.configured.lock().await = Some(attempt.clone());
    fs::create_dir(root.path().join(".committed.next")).unwrap();
    let commit = || {
        Request::new(wire::CommitRequest {
            attempt: Some(attempt.clone().into()),
        })
    };
    GuestUpdate::commit(&service, commit()).await.unwrap_err();
    assert!(matches!(
        SlotStore::open(root.path())
            .unwrap()
            .journal()
            .unwrap()
            .state,
        JournalState::Committed { .. }
    ));
    assert!(!*service.inner.ready.borrow());
    fs::remove_dir(root.path().join(".committed.next")).unwrap();
    GuestUpdate::commit(&service, commit()).await.unwrap();
    assert!(
        *service.inner.ready.borrow(),
        "a durable commit retry must complete READY delivery"
    );
    assert!(!service.inner.update.trial_gated());
    assert_eq!(
        fs::read_link(root.path().join("committed")).unwrap(),
        std::path::Path::new("slots/b")
    );
    service.inner.shutdown.cancel();
    service.inner.update.drain().await;
}

#[tokio::test]
async fn replaying_an_armed_receipt_still_requests_systemd_restart() {
    let (root, service, attempt) = fixture();
    let store = SlotStore::open(root.path()).unwrap();
    let mut journal = store.journal().unwrap();
    journal.state = JournalState::Armed {
        attempt: attempt.clone(),
    };
    fs::write(
        root.path().join("update-state"),
        serde_json::to_vec(&journal).unwrap(),
    )
    .unwrap();
    drop(store);
    GuestUpdate::arm(
        &service,
        Request::new(wire::ArmRequest {
            attempt: Some(attempt.into()),
            force: true,
        }),
    )
    .await
    .unwrap();
    assert!(
        service.inner.shutdown.is_cancelled(),
        "the durable Arm cannot be stranded by a lost reply"
    );
    service.inner.shutdown.cancel();
    service.inner.update.drain().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn transfer_error_after_complete_payload_does_not_publish_staged_authority() {
    use pplx_space_spaced_common::proto::bridge_guest::{
        guest_update_client::GuestUpdateClient, guest_update_server::GuestUpdateServer,
    };
    if !pplx_space_test_env::root_or_skip("inspect slot references before staging") {
        return;
    }
    let (root, service, attempt) = fixture();
    let store = SlotStore::open(root.path()).unwrap();
    let descriptor = store.descriptor(Slot::B).unwrap();
    let journal = Journal::stable(store.journal().unwrap().committed);
    fs::write(
        root.path().join("update-state"),
        serde_json::to_vec(&journal).unwrap(),
    )
    .unwrap();
    fs::remove_file(root.path().join("current")).unwrap();
    std::os::unix::fs::symlink(Slot::A.selector(), root.path().join("current")).unwrap();
    drop(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let stopped = shutdown.clone();
    let serving = service.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(GuestUpdateServer::new(serving))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stopped.cancelled(),
            )
            .await
            .unwrap();
    });
    let mut client = GuestUpdateClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut messages = vec![wire::StageRequest {
        part: Some(wire::stage_request::Part::Header(Box::new(
            wire::StageHeader {
                descriptor: Some(descriptor.into()),
                attempt: Some(attempt.into()),
            },
        ))),
    }];
    for binary in [wire::Binary::Service, wire::Binary::Cli] {
        messages.push(wire::StageRequest {
            part: Some(wire::stage_request::Part::Chunk(wire::Chunk {
                binary: binary as i32,
                offset: 0,
                data: b"candidate".to_vec(),
            })),
        });
    }
    messages.push(wire::StageRequest { part: None });
    let result = client.stage(tokio_stream::iter(messages)).await;
    let persisted = SlotStore::open(root.path()).unwrap().journal().unwrap();
    shutdown.cancel();
    service.inner.shutdown.cancel();
    service.inner.update.drain().await;
    server.await.unwrap();
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    assert_eq!(persisted.state, JournalState::Stable);
}
