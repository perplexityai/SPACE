use pplx_space_common_core::types::SandboxId;

use super::*;

#[tokio::test]
async fn sandbox_publication_rejects_missing_or_empty_reconstructed_rootfs() {
    for disk in [None, Some(b"".as_slice()), Some(b"disk".as_slice())] {
        let temp = tempfile::tempdir().unwrap();
        let mut paths = SandboxImportPaths::new("unused");
        paths.instance_staging = temp.path().join("staging").to_str().unwrap().into();
        paths.instance_dir = temp.path().join("instance").to_str().unwrap().into();
        std::fs::create_dir(&paths.instance_staging).unwrap();
        if let Some(disk) = disk {
            std::fs::write(
                std::path::Path::new(&paths.instance_staging).join("rootfs.ext4"),
                disk,
            )
            .unwrap();
        }
        let service =
            crate::service::tests::service_tests::service_for_teardown_test(crate::db::test_conn());
        let result = publish_imported_instance(&service, &paths).await;
        let valid = disk.is_some_and(|bytes| !bytes.is_empty());
        assert_eq!(result.is_ok(), valid);
        assert_eq!(std::path::Path::new(&paths.instance_dir).exists(), valid);
        assert_eq!(
            std::path::Path::new(&paths.instance_staging).exists(),
            !valid
        );
        if let Err(error) = result {
            assert_eq!(error.code(), tonic::Code::DataLoss);
        }
    }
}

#[tokio::test]
async fn delta_import_rejects_a_streamed_full_rootfs_before_creating_it() {
    let temp = tempfile::tempdir().unwrap();
    let mut paths = SandboxImportPaths::new(&SandboxId::new().to_string());
    paths.instance_staging = temp.path().to_string_lossy().into_owned();
    let mut stream = tokio_stream::iter([Ok(ImportSandboxRequest {
        payload: Some(crate::proto::import_sandbox_request::Payload::Chunk(
            crate::proto::ImportSandboxFileChunk {
                file_name: "rootfs.ext4".into(),
                data: vec![1],
                ..Default::default()
            },
        )),
    })]);
    receive_sandbox_import_body(&mut stream, &paths, true, receive::ImportDeadline::new())
        .await
        .unwrap_err();
    assert!(!temp.path().join("rootfs.ext4").exists());
}

#[tokio::test]
async fn import_rejects_reserved_owner_before_reserving_identity_or_waiting_for_body() {
    use crate::service::activation_support::sandbox_peer_authorizer;
    use crate::service::tests::service_tests::service_for_teardown_test;

    let service = service_for_teardown_test(crate::db::test_conn());
    let peer = sandbox_peer_authorizer()
        .authorize_identity(
            PeerIdentity::new(0, 0, None),
            "ImportSandbox",
            ALLOW_SANDBOX_OPERATOR,
        )
        .unwrap();
    for owner in ["tmplmgr@local", "orphaned@local"] {
        let id = SandboxId::new().to_string();
        let stream = tokio_stream::iter([Ok(ImportSandboxRequest {
            payload: Some(crate::proto::import_sandbox_request::Payload::Header(
                crate::proto::ImportSandboxHeader {
                    sandbox_id: id.clone(),
                    template_id: "seed".to_owned(),
                    content_hash: "sha256:abc".to_owned(),
                    runtime: "fc".to_owned(),
                    owner: owner.to_owned(),
                    ..Default::default()
                },
            )),
        })]);
        let error = service
            .import_sandbox_owned(stream, peer.clone())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied, "{owner}");
        assert!(!crate::db::sandbox_exists(&service.db_conn().unwrap(), &id).unwrap());
    }
}

#[tokio::test]
async fn import_rejects_local_network_policy_from_forwarded_service_peer() {
    use crate::service::activation_support::sandbox_peer_authorizer;
    use crate::service::tests::service_tests::service_for_teardown_test;
    use pplx_space_common_core::host::linux::identities::users;
    use pplx_space_net_common::{NetworkPolicyV1, proto::PolicyAuthority};

    let service = service_for_teardown_test(crate::db::test_conn());
    let peer = sandbox_peer_authorizer()
        .authorize_identity(
            PeerIdentity::new(
                users::SPACE_NODEAPI.uid,
                users::SPACE_NODEAPI.primary_group.gid,
                None,
            ),
            "ImportSandbox",
            ALLOW_SANDBOX_OPERATOR,
        )
        .unwrap();
    let id = SandboxId::new().to_string();
    let stream = tokio_stream::iter([Ok(ImportSandboxRequest {
        payload: Some(crate::proto::import_sandbox_request::Payload::Header(
            crate::proto::ImportSandboxHeader {
                sandbox_id: id.clone(),
                template_id: "seed".to_owned(),
                content_hash: "sha256:abc".to_owned(),
                runtime: "fc".to_owned(),
                owner: "root@local".to_owned(),
                network_policy: Some((&NetworkPolicyV1::allow_all()).into()),
                network_policy_authority: PolicyAuthority::Local as i32,
                ..Default::default()
            },
        )),
    })]);

    let error = service
        .import_sandbox_owned(stream, peer)
        .await
        .expect_err("forwarded import must not persist a local network policy");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(!crate::db::sandbox_exists(&service.db_conn().unwrap(), &id).unwrap());
}

#[tokio::test]
async fn import_rejects_local_auto_lifecycle_from_forwarded_service_peer() {
    use crate::service::activation_support::sandbox_peer_authorizer;
    use crate::service::tests::service_tests::service_for_teardown_test;
    use pplx_space_common_core::host::linux::identities::users;
    use pplx_space_net_common::proto::PolicyAuthority;

    let service = service_for_teardown_test(crate::db::test_conn());
    let peer = sandbox_peer_authorizer()
        .authorize_identity(
            PeerIdentity::new(
                users::SPACE_NODEAPI.uid,
                users::SPACE_NODEAPI.primary_group.gid,
                None,
            ),
            "ImportSandbox",
            ALLOW_SANDBOX_OPERATOR,
        )
        .unwrap();
    let id = SandboxId::new().to_string();
    let stream = tokio_stream::iter([Ok(ImportSandboxRequest {
        payload: Some(crate::proto::import_sandbox_request::Payload::Header(
            crate::proto::ImportSandboxHeader {
                sandbox_id: id.clone(),
                template_id: "seed".to_owned(),
                content_hash: "sha256:abc".to_owned(),
                runtime: "fc".to_owned(),
                owner: "root@local".to_owned(),
                auto_lifecycle_authority: PolicyAuthority::Local as i32,
                ..Default::default()
            },
        )),
    })]);

    let error = service
        .import_sandbox_owned(stream, peer)
        .await
        .expect_err("forwarded import must not persist a local auto-lifecycle policy");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(!crate::db::sandbox_exists(&service.db_conn().unwrap(), &id).unwrap());
}

#[tokio::test]
async fn sparse_transfer_preserves_holes_and_delta_export_keeps_other_files() {
    let temp = tempfile::tempdir().unwrap();
    let instance = temp.path().join("instance");
    std::fs::create_dir(&instance).unwrap();
    std::fs::write(instance.join("rootfs.ext4"), vec![0; 1024 * 1024]).unwrap();
    std::fs::write(instance.join("runtime.json"), b"runtime identity").unwrap();
    let delta = temp.path().join("rootfs.diff");
    std::fs::write(&delta, b"delta bytes").unwrap();
    let (files, _) = receive::sandbox_export_files(&instance, None, Some(&delta)).unwrap();
    assert_eq!(
        files
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["rootfs.ext4.diff", "runtime.json"]
    );
    let mut chunks = Vec::new();
    receive::stream_sparse_file(
        &instance.join("rootfs.ext4"),
        true,
        || false,
        |offset, data, eof| {
            chunks.push((offset, data, eof));
            Ok(())
        },
    )
    .unwrap();
    let target = temp.path().join("received");
    let mut receiver = receive::SparseImport::default();
    for (offset, data, eof) in chunks {
        receiver
            .write("rootfs.ext4", &target, offset, &data, eof)
            .await
            .unwrap();
    }
    assert_eq!(
        receiver.finish().unwrap(),
        0,
        "an all-zero file need not send zero-filled chunks"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        std::fs::read(instance.join("rootfs.ext4")).unwrap()
    );
    let checks = std::cell::Cell::new(0);
    let cancelled = receive::stream_sparse_file(
        &instance.join("rootfs.ext4"),
        true,
        || {
            checks.set(checks.get() + 1);
            checks.get() == 2
        },
        |_, _, _| panic!("cancelled zero-only scan must not reach EOF"),
    );
    assert_eq!(cancelled.unwrap_err().code(), tonic::Code::Cancelled);
}

#[tokio::test(start_paused = true)]
async fn idle_import_stream_has_a_terminal_owner_deadline() {
    let mut stream = tokio_stream::pending::<Result<ImportSandboxRequest, Status>>();
    assert_eq!(
        receive::next_message(&mut stream, receive::ImportDeadline::new())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
}

#[tokio::test]
async fn import_reservation_preserves_existing_files_and_fences_completion() {
    use crate::service::tests::service_tests::service_for_teardown_test;
    for occupied in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let id = uuid::Uuid::now_v7().to_string();
        let mut paths = SandboxImportPaths::new(&id);
        paths.sandbox_base = temp.path().join(&id).to_string_lossy().into_owned();
        if occupied {
            std::fs::create_dir(&paths.sandbox_base).unwrap();
            std::fs::write(
                std::path::Path::new(&paths.sandbox_base).join("existing"),
                b"preserve",
            )
            .unwrap();
        }
        let service = service_for_teardown_test(crate::db::test_conn());
        let header = crate::proto::ImportSandboxHeader {
            sandbox_id: id.clone(),
            template_id: "seed".to_owned(),
            content_hash: "sha256:abc".to_owned(),
            runtime: "fc".to_owned(),
            owner: "test".to_owned(),
            ..Default::default()
        };
        let validated = validate_import_sandbox_header(&header).unwrap();
        let cleanup_id = SandboxCleanupEpisodeId::new();
        let result = service.reserve_imported_sandbox_row(
            &header,
            &SandboxOwner::parse(header.owner.clone()).unwrap(),
            &validated,
            cleanup_id,
            &paths,
        );
        let db = service.db_conn().unwrap();
        if occupied {
            assert_eq!(result.unwrap_err().code(), tonic::Code::AlreadyExists);
            assert!(!crate::db::sandbox_exists(&db, &id).unwrap());
            assert_eq!(
                std::fs::read(std::path::Path::new(&paths.sandbox_base).join("existing")).unwrap(),
                b"preserve"
            );
        } else {
            result.unwrap();
            assert_eq!(
                crate::db::get_sandbox(&db, &id).unwrap().unwrap().state,
                SandboxState::Creating
            );
            assert!(crate::db::has_cleanup_episode(&db, &id).unwrap());
            assert!(
                !crate::db::transfer::complete_import(
                    &db,
                    &id,
                    SandboxCleanupEpisodeId::new(),
                    false
                )
                .unwrap()
            );
            assert!(crate::db::transfer::complete_import(&db, &id, cleanup_id, false).unwrap());
            assert_eq!(
                crate::db::get_sandbox(&db, &id).unwrap().unwrap().state,
                SandboxState::Stopped
            );
            assert!(!crate::db::has_cleanup_episode(&db, &id).unwrap());
            assert!(!crate::db::transfer::complete_import(&db, &id, cleanup_id, true).unwrap());
        }
    }
}

#[tokio::test]
async fn sandbox_import_rejects_incomplete_or_rewritten_artifacts() {
    use crate::proto::{ImportSandboxFileChunk, import_sandbox_request::Payload};
    let chunk = |name: &str, offset, eof, data: &[u8]| ImportSandboxRequest {
        payload: Some(Payload::Chunk(ImportSandboxFileChunk {
            file_name: name.to_owned(),
            offset,
            eof,
            data: data.to_vec(),
        })),
    };
    for (case, chunks) in [
        (
            "partial snapshot",
            vec![
                chunk("rootfs.ext4", 4, true, &[]),
                chunk("snapshot/vmstate", 4, true, &[]),
            ],
        ),
        (
            "duplicate complete file",
            vec![
                chunk("rootfs.ext4", 4, true, &[]),
                chunk("rootfs.ext4", 4, true, &[]),
            ],
        ),
        (
            "overlapping writes",
            vec![
                chunk("rootfs.ext4", 0, false, b"abcd"),
                chunk("rootfs.ext4", 2, false, b"ef"),
                chunk("rootfs.ext4", 4, true, &[]),
            ],
        ),
        (
            "truncating eof",
            vec![
                chunk("rootfs.ext4", 0, false, b"abcd"),
                chunk("rootfs.ext4", 2, true, &[]),
            ],
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut paths = SandboxImportPaths::new("unused");
        paths.instance_staging = temp.path().join("instance").to_string_lossy().into_owned();
        paths.snapshot_staging = temp.path().join("snapshot").to_string_lossy().into_owned();
        tokio::fs::create_dir(&paths.instance_staging)
            .await
            .unwrap();
        let mut stream = tokio_stream::iter(chunks.into_iter().map(Ok));
        assert!(
            receive_sandbox_import_body(&mut stream, &paths, false, receive::ImportDeadline::new())
                .await
                .is_err(),
            "{case}"
        );
    }
}

#[test]
fn import_header_execution_metadata_backfills_legacy_firecracker_header() {
    let header = crate::proto::ImportSandboxHeader {
        runtime: "fc".to_string(),
        execution: None,
        ..Default::default()
    };

    let execution = import_header_execution_metadata(&header).unwrap();

    assert_eq!(
        execution.runtime().kind,
        pplx_space_common_core::types::RuntimeKind::Firecracker
    );
    assert_eq!(execution.runtime().version.as_str(), "1.15.1");
    assert_eq!(execution.state_format().unwrap().version.as_str(), "9.0.0");
}

#[test]
fn import_sandbox_file_name_validation_classifies_instance_and_snapshot_files() {
    assert_eq!(
        validate_import_sandbox_file_name("rootfs.ext4").unwrap(),
        ("rootfs.ext4", false)
    );
    assert_eq!(
        validate_import_sandbox_file_name("snapshot/vmstate").unwrap(),
        ("vmstate", true)
    );
}

#[test]
fn import_sandbox_file_name_validation_rejects_path_traversal() {
    for name in [
        "",
        ".",
        "snapshot/.",
        "../rootfs.ext4",
        "rootfs..ext4",
        "nested/rootfs.ext4",
        "snapshot/",
        "snapshot/../vmstate",
        "snapshot/nested/vmstate",
        "snapshot\\vmstate",
    ] {
        let error = validate_import_sandbox_file_name(name)
            .expect_err("malformed import file name should be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}

#[test]
fn import_sandbox_header_validation_rejects_unsupported_rootfs_sizes() {
    for size_mib in [0, 128 * 1024 + 1, u32::MAX] {
        let header = crate::proto::ImportSandboxHeader {
            sandbox_id: SandboxId::new().to_string(),
            template_id: "seed".to_string(),
            content_hash: "sha256:abc".to_string(),
            runtime: "fc".to_string(),
            rootfs_size_mb: Some(size_mib),
            ..Default::default()
        };

        let error = match validate_import_sandbox_header(&header) {
            Ok(_) => panic!("unsupported rootfs size should be rejected: {size_mib}"),
            Err(error) => error,
        };

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("rootfs_size_mb"));
    }
}

#[tokio::test]
async fn import_identity_normalization_and_existing_owner_detection_agree() {
    use crate::service::tests::service_tests::{insert_test_sandbox, service_for_teardown_test};
    let canonical = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    for spelling in [
        canonical.to_owned(),
        canonical.to_uppercase(),
        canonical.replace('-', ""),
        format!("urn:uuid:{canonical}"),
        format!("{{{canonical}}}"),
    ] {
        let header = crate::proto::ImportSandboxHeader {
            sandbox_id: spelling.clone(),
            template_id: "seed".to_owned(),
            content_hash: "sha256:abc".to_owned(),
            runtime: "fc".to_owned(),
            ..Default::default()
        };
        let validated = validate_import_sandbox_header(&header).unwrap();
        assert_eq!(validated.sandbox_id.to_string(), canonical);
        let conn = crate::db::test_conn();
        insert_test_sandbox(&conn, canonical, crate::state::SandboxState::Stopped);
        let service = service_for_teardown_test(conn);
        assert_eq!(
            service
                .reject_existing_sandbox_import(validated.sandbox_id)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::AlreadyExists
        );
    }
    for bad in ["", "../sandbox", "not-a-uuid"] {
        let header = crate::proto::ImportSandboxHeader {
            sandbox_id: bad.to_owned(),
            ..Default::default()
        };
        assert!(validate_import_sandbox_header(&header).is_err());
    }
}
