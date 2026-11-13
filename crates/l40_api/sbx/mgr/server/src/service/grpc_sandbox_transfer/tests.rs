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
    use pplx_space_host_layout::identities::users;
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
    use pplx_space_host_layout::identities::users;
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
                SandboxStatus::from(SandboxState::Creating)
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
                SandboxStatus::from(SandboxState::Stopped)
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
        pplx_space_execution_spec::RuntimeKind::Firecracker
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

/// A delta base is the rootfs of the build template-manager's catalog
/// answers for, never a build dir that merely exists: the lookup is
/// `cached_only`, carries no caller org, and its answer must carry its scope's
/// receipt or none, and say it made the `cached_only` checks, which a
/// template-manager that predates them doesn't. Here a rowless partial legacy
/// dir sits next to the verified twin the lookup returns. With no verified
/// build there's no base, and a delta
/// import is refused: only an absent build is told to pull the template, a
/// build that can't serve isn't, and a template-manager that couldn't say is
/// a retryable `UNAVAILABLE`.
#[tokio::test]
async fn a_delta_base_is_the_catalog_s_verified_build_or_none() {
    use crate::template_resolver::fake::FakeTemplateResolver;
    use pplx_space_template_mgr_client::lookup_scope::TemplateLookupScope;
    use pplx_space_template_mgr_client::proto::{GetTemplateResponse, TemplateRef};

    let hash = format!("sha256:{}", "a".repeat(64));
    let root = tempfile::tempdir().unwrap();
    let legacy_dir = root.path().join("seed").join(&hash);
    let verified_dir = root
        .path()
        .join("seed")
        .join(format!("{hash}@sha256:{}", "b".repeat(64)));
    for (dir, bytes) in [
        (&legacy_dir, &b"partial"[..]),
        (&verified_dir, &b"verified"[..]),
    ] {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("rootfs.ext4"), bytes).unwrap();
    }
    let answering_with =
        |dir: &std::path::Path, receipt: Option<TemplateLookupScope>, cached_only: bool| {
            let answer_dir = dir.to_string_lossy().into_owned();
            let answer_hash = hash.clone();
            FakeTemplateResolver::new(move || {
                let mut response = tonic::Response::new(GetTemplateResponse {
                    template: Some(TemplateRef {
                        name: "seed".into(),
                        content_hash: Some(answer_hash.clone()),
                        tag: None,
                        environment: None,
                    }),
                    template_dir: answer_dir.clone(),
                    cached_only,
                    ..Default::default()
                });
                if let Some(scope) = receipt {
                    scope.receipt(&mut response);
                }
                Ok(response)
            })
        };
    let answering = |dir: &std::path::Path| answering_with(dir, None, true);
    let verified = answering(&verified_dir);

    assert_eq!(
        verified_template_rootfs(&verified, "seed", &hash).await,
        Some(verified_dir.join("rootfs.ext4"))
    );
    assert_eq!(
        verified_template_rootfs(
            &answering_with(
                &verified_dir,
                Some(TemplateLookupScope::LocalExecution),
                true
            ),
            "seed",
            &hash
        )
        .await,
        Some(verified_dir.join("rootfs.ext4"))
    );
    let [request] = verified.requests().try_into().unwrap();
    assert!(request.cached_only);
    assert_eq!(request.caller_org_id, None);
    let template = request.template.unwrap();
    assert_eq!(template.name, "seed");
    assert_eq!(template.content_hash.as_deref(), Some(hash.as_str()));

    let header = crate::proto::ImportSandboxHeader {
        is_delta: true,
        template_id: "seed".into(),
        content_hash: hash.clone(),
        ..Default::default()
    };
    // Only an absent build is worth pulling; a build that can't serve, or a
    // refused lookup, needs a full transfer; an unanswered one, a retry.
    const PULL: &str = "Pull the template first";
    const UNUSABLE: &str = "can't serve as a delta base";
    const RETRY: &str = "Retry";
    for (unverified, refusal, says) in [
        (
            FakeTemplateResolver::refusing(tonic::Status::not_found("no ready build")),
            tonic::Code::FailedPrecondition,
            PULL,
        ),
        (
            FakeTemplateResolver::refusing(tonic::Status::failed_precondition("wrong size")),
            tonic::Code::FailedPrecondition,
            UNUSABLE,
        ),
        (
            answering_with(
                &verified_dir,
                Some(TemplateLookupScope::Node("127.0.0.2".parse().unwrap())),
                true,
            ),
            tonic::Code::FailedPrecondition,
            UNUSABLE,
        ),
        // An ordinary lookup's answer, as from a template-manager that
        // ignores `cached_only`.
        (
            answering_with(&verified_dir, None, false),
            tonic::Code::FailedPrecondition,
            UNUSABLE,
        ),
        (
            FakeTemplateResolver::refusing(tonic::Status::invalid_argument("bad hash")),
            tonic::Code::FailedPrecondition,
            UNUSABLE,
        ),
        (
            FakeTemplateResolver::refusing(tonic::Status::permission_denied("not a reader")),
            tonic::Code::FailedPrecondition,
            UNUSABLE,
        ),
        (
            FakeTemplateResolver::refusing(tonic::Status::unavailable("template-manager down")),
            tonic::Code::Unavailable,
            RETRY,
        ),
        (
            FakeTemplateResolver::refusing(tonic::Status::internal("db")),
            tonic::Code::Unavailable,
            RETRY,
        ),
    ] {
        assert_eq!(
            verified_template_rootfs(&unverified, "seed", &hash).await,
            None
        );
        let status = delta_import_base(&unverified, &header).await.unwrap_err();
        assert_eq!(status.code(), refusal, "{status}");
        assert!(status.message().contains(says), "{status}");
    }
    assert_eq!(
        delta_import_base(&verified, &crate::proto::ImportSandboxHeader::default())
            .await
            .unwrap(),
        None
    );
}

/// An export looks its delta base up before taking the sandbox's locks, so it
/// keeps the base only while the sandbox, read again under them, pins the same
/// build; it looks nothing up when it wants no delta or has no pinned build.
#[tokio::test]
async fn an_early_delta_base_is_kept_only_for_the_build_still_pinned() {
    use crate::template_resolver::fake::FakeTemplateResolver;
    use pplx_space_template_mgr_client::proto::{GetTemplateResponse, TemplateRef};

    let hash = format!("sha256:{}", "a".repeat(64));
    let other = format!("sha256:{}", "b".repeat(64));
    let template = tempfile::tempdir().unwrap();
    std::fs::write(template.path().join("rootfs.ext4"), b"verified").unwrap();
    let answer_dir = template.path().to_string_lossy().into_owned();
    let answer_hash = hash.clone();
    let resolver = FakeTemplateResolver::new(move || {
        Ok(tonic::Response::new(GetTemplateResponse {
            template: Some(TemplateRef {
                name: "seed".into(),
                content_hash: Some(answer_hash.clone()),
                tag: None,
                environment: None,
            }),
            template_dir: answer_dir.clone(),
            cached_only: true,
            ..Default::default()
        }))
    });
    let rootfs = template.path().join("rootfs.ext4");

    let early = || EarlyDeltaBase::look_up(&resolver, true, "seed", Some(&hash));
    assert_eq!(
        EarlyDeltaBase::for_pinned(early().await, "seed", &hash),
        Some(rootfs)
    );
    assert_eq!(
        EarlyDeltaBase::for_pinned(early().await, "seed", &other),
        None
    );
    assert_eq!(
        EarlyDeltaBase::for_pinned(early().await, "other", &hash),
        None
    );
    assert_eq!(resolver.requests().len(), 3);

    assert!(
        EarlyDeltaBase::look_up(&resolver, false, "seed", Some(&hash))
            .await
            .is_none()
    );
    assert!(
        EarlyDeltaBase::look_up(&resolver, true, "seed", None)
            .await
            .is_none()
    );
    assert_eq!(resolver.requests().len(), 3);
}

/// Sandbox and snapshot export take a delta base only from a `cached_only`
/// lookup of the build their sandbox pins, and look nothing up when they want
/// no delta. Each export then stops at its missing host directories.
#[tokio::test]
async fn exports_look_up_the_pinned_build_only_for_a_delta() {
    use crate::service::activation_support::internal_request;
    use crate::service::tests::service_tests::service_for_teardown_test;
    use crate::template_resolver::fake::FakeTemplateResolver;

    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let resolver = std::sync::Arc::new(FakeTemplateResolver::refusing(tonic::Status::not_found(
        "no ready build",
    )));
    let mut service = service_for_teardown_test(conn);
    service.template_resolver = resolver.clone();

    for use_delta in [false, true] {
        let _ = service
            .export_sandbox(internal_request(crate::proto::ExportSandboxRequest {
                sandbox_id: id.clone(),
                use_delta,
            }))
            .await;
    }
    for (include_rootfs, use_delta) in [(false, true), (true, false), (true, true)] {
        let _ = service
            .export_snapshot(internal_request(crate::proto::ExportSnapshotRequest {
                sandbox_id: id.clone(),
                snapshot_name: "d-1m-20260922T120000Z".into(),
                include_rootfs,
                use_delta,
            }))
            .await;
    }

    let requests = resolver.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    for request in requests {
        assert!(request.cached_only);
        assert_eq!(request.caller_org_id, None);
        let template = request.template.unwrap();
        assert_eq!(template.name, "seed");
        assert_eq!(template.content_hash.as_deref(), Some("hash"));
    }
}

/// `ImportSandbox` refuses a delta import whose base this node can't verify
/// before it reserves the sandbox or reads the body.
#[tokio::test]
async fn import_refuses_a_delta_without_a_verified_base_before_its_body() {
    use crate::service::activation_support::sandbox_peer_authorizer;
    use crate::service::tests::service_tests::service_for_teardown_test;
    use crate::template_resolver::fake::FakeTemplateResolver;

    let resolver = std::sync::Arc::new(FakeTemplateResolver::refusing(tonic::Status::not_found(
        "no ready build",
    )));
    let mut service = service_for_teardown_test(crate::db::test_conn());
    service.template_resolver = resolver.clone();
    let peer = sandbox_peer_authorizer()
        .authorize_identity(
            PeerIdentity::new(0, 0, None),
            "ImportSandbox",
            ALLOW_SANDBOX_OPERATOR,
        )
        .unwrap();
    let id = SandboxId::new().to_string();
    let hash = format!("sha256:{}", "a".repeat(64));
    let stream = tokio_stream::iter([Ok(ImportSandboxRequest {
        payload: Some(crate::proto::import_sandbox_request::Payload::Header(
            crate::proto::ImportSandboxHeader {
                sandbox_id: id.clone(),
                template_id: "seed".to_owned(),
                content_hash: hash.clone(),
                runtime: "fc".to_owned(),
                owner: "root@local".to_owned(),
                is_delta: true,
                ..Default::default()
            },
        )),
    })]);

    let error = service
        .import_sandbox_owned(stream, peer)
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(
        error
            .message()
            .starts_with("delta import requires template"),
        "{error}"
    );
    let [request] = resolver.requests().try_into().unwrap();
    assert!(request.cached_only);
    assert_eq!(
        request.template.unwrap().content_hash.as_deref(),
        Some(hash.as_str())
    );
    assert!(!crate::db::sandbox_exists(&service.db_conn().unwrap(), &id).unwrap());
}

/// A retried delta import still learns what it would without a delta: an
/// existing sandbox answers `ALREADY_EXISTS` and a cleanup in progress
/// `ABORTED`, before any delta-base lookup, so a retry of an import that
/// already landed isn't told to pull its template.
#[tokio::test]
async fn a_delta_import_answers_already_exists_and_aborted_before_its_base_lookup() {
    use crate::service::activation_support::sandbox_peer_authorizer;
    use crate::service::tests::service_tests::service_for_teardown_test;
    use crate::template_resolver::fake::FakeTemplateResolver;

    let resolver = std::sync::Arc::new(FakeTemplateResolver::refusing(tonic::Status::not_found(
        "no ready build",
    )));
    let conn = crate::db::test_conn();
    let existing = SandboxId::new().to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &existing, SandboxState::Stopped);
    let mut service = service_for_teardown_test(conn);
    service.template_resolver = resolver.clone();
    let peer = sandbox_peer_authorizer()
        .authorize_identity(
            PeerIdentity::new(0, 0, None),
            "ImportSandbox",
            ALLOW_SANDBOX_OPERATOR,
        )
        .unwrap();
    let delta_import = |sandbox_id: &str| {
        tokio_stream::iter([Ok(ImportSandboxRequest {
            payload: Some(crate::proto::import_sandbox_request::Payload::Header(
                crate::proto::ImportSandboxHeader {
                    sandbox_id: sandbox_id.to_owned(),
                    template_id: "seed".to_owned(),
                    content_hash: format!("sha256:{}", "a".repeat(64)),
                    runtime: "fc".to_owned(),
                    owner: "root@local".to_owned(),
                    is_delta: true,
                    ..Default::default()
                },
            )),
        })])
    };

    let status = service
        .import_sandbox_owned(delta_import(&existing), peer.clone())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status}");

    let cleaning = SandboxId::new().to_string();
    let fence = service.try_cleanup_fence(&cleaning);
    assert!(matches!(fence, CleanupLockTryResult::Acquired(_)));
    let status = service
        .import_sandbox_owned(delta_import(&cleaning), peer)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Aborted, "{status}");
    drop(fence);

    assert!(resolver.requests().is_empty(), "{:?}", resolver.requests());
}

/// A delta import's body can take until the import deadline, so the base
/// verified before the body arrived is looked up again before the diff is
/// applied. A build removed in between refuses the apply, and nothing is
/// written.
#[tokio::test]
async fn a_delta_import_verifies_its_base_again_before_applying() {
    use crate::service::tests::service_tests::service_for_teardown_test;
    use crate::template_resolver::fake::FakeTemplateResolver;
    use pplx_space_template_mgr_client::proto::{GetTemplateResponse, TemplateRef};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let hash = format!("sha256:{}", "a".repeat(64));
    let template = tempfile::tempdir().unwrap();
    std::fs::write(template.path().join("rootfs.ext4"), b"verified").unwrap();
    let staging = tempfile::tempdir().unwrap();
    std::fs::write(staging.path().join("rootfs.ext4.diff"), b"diff").unwrap();
    let mut paths = SandboxImportPaths::new(&SandboxId::new().to_string());
    paths.instance_staging = staging.path().to_string_lossy().into_owned();
    let header = crate::proto::ImportSandboxHeader {
        is_delta: true,
        template_id: "seed".into(),
        content_hash: hash.clone(),
        ..Default::default()
    };
    let answer_dir = template.path().to_string_lossy().into_owned();
    let lookups = AtomicUsize::new(0);
    let resolver = Arc::new(FakeTemplateResolver::new(move || {
        if lookups.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(tonic::Response::new(GetTemplateResponse {
                template: Some(TemplateRef {
                    name: "seed".into(),
                    content_hash: Some(hash.clone()),
                    tag: None,
                    environment: None,
                }),
                template_dir: answer_dir.clone(),
                cached_only: true,
                ..Default::default()
            }))
        } else {
            Err(crate::template_resolver::TemplateLookupError::Rpc(
                tonic::Status::not_found("build removed"),
            ))
        }
    }));
    let mut service = service_for_teardown_test(crate::db::test_conn());
    service.template_resolver = resolver.clone();

    delta_import_base(service.template_resolver.as_ref(), &header)
        .await
        .unwrap()
        .unwrap();
    let status = apply_sandbox_delta_import(&service, &header, &paths)
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
    assert!(
        status
            .message()
            .starts_with("delta import requires template"),
        "{status}"
    );
    assert_eq!(resolver.requests().len(), 2);
    assert!(!staging.path().join("rootfs.ext4").exists());
}

/// The apply reads its base by path, so a build evicted and pulled again
/// between the lookup and the read could hand it unverified bytes. It keeps
/// the result only if a lookup after the apply still answers with the same
/// path naming the inode it read: a base replaced in between is `ABORTED`,
/// even with the catalog answering, and an untouched one applies.
#[tokio::test]
async fn a_delta_apply_keeps_its_result_only_for_the_base_it_read() {
    use crate::service::tests::service_tests::service_for_teardown_test;
    use crate::template_resolver::fake::FakeTemplateResolver;
    use pplx_space_template_mgr_client::proto::{GetTemplateResponse, TemplateRef};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    for replaced in [false, true] {
        let hash = format!("sha256:{}", "a".repeat(64));
        let template = tempfile::tempdir().unwrap();
        let base = template.path().join("rootfs.ext4");
        std::fs::write(&base, vec![1u8; 16384]).unwrap();
        let staging = tempfile::tempdir().unwrap();
        let current = staging.path().join("current");
        let mut changed = vec![1u8; 16384];
        changed[5000] = 2;
        std::fs::write(&current, &changed).unwrap();
        let diff = staging.path().join("rootfs.ext4.diff");
        pplx_space_node_runtime_core::file_delta::compute_diff(&base, &current, &diff).unwrap();
        let mut paths = SandboxImportPaths::new(&SandboxId::new().to_string());
        paths.instance_staging = staging.path().to_string_lossy().into_owned();
        let header = crate::proto::ImportSandboxHeader {
            is_delta: true,
            template_id: "seed".into(),
            content_hash: hash.clone(),
            ..Default::default()
        };
        let answer_dir = template.path().to_string_lossy().into_owned();
        let lookups = AtomicUsize::new(0);
        let swap = base.clone();
        let resolver = Arc::new(FakeTemplateResolver::new(move || {
            // The second lookup runs after the apply read the base: replace
            // it with a new file, as an eviction and a fresh pull would.
            if replaced && lookups.fetch_add(1, Ordering::SeqCst) == 1 {
                let fresh = swap.with_extension("fresh");
                std::fs::write(&fresh, vec![1u8; 16384]).unwrap();
                std::fs::rename(&fresh, &swap).unwrap();
            }
            Ok(tonic::Response::new(GetTemplateResponse {
                template: Some(TemplateRef {
                    name: "seed".into(),
                    content_hash: Some(hash.clone()),
                    tag: None,
                    environment: None,
                }),
                template_dir: answer_dir.clone(),
                cached_only: true,
                ..Default::default()
            }))
        }));
        let mut service = service_for_teardown_test(crate::db::test_conn());
        service.template_resolver = resolver.clone();

        let result = apply_sandbox_delta_import(&service, &header, &paths).await;

        assert_eq!(resolver.requests().len(), 2);
        if replaced {
            let status = result.unwrap_err();
            assert_eq!(status.code(), tonic::Code::Aborted, "{status}");
        } else {
            result.unwrap();
            assert_eq!(
                std::fs::read(staging.path().join("rootfs.ext4")).unwrap(),
                changed
            );
        }
    }
}

/// A delta export computes its diff from the base by path, so it keeps the
/// diff only if a lookup after the computation still answers with the same
/// path naming the file it read. A base replaced in between, deleted and
/// written again (which can reuse its inode number, unless the base is held
/// open), or a build gone by then, sends the full rootfs, and the diff is
/// removed.
#[tokio::test(flavor = "multi_thread")]
async fn a_delta_export_keeps_its_diff_only_for_the_base_it_read() {
    use crate::template_resolver::fake::FakeTemplateResolver;
    use pplx_space_template_mgr_client::proto::{GetTemplateResponse, TemplateRef};

    #[derive(Clone, Copy, Debug)]
    enum After {
        Untouched,
        Replaced,
        Recreated,
        Gone,
    }
    for after in [
        After::Untouched,
        After::Replaced,
        After::Recreated,
        After::Gone,
    ] {
        let hash = format!("sha256:{}", "a".repeat(64));
        let template = tempfile::tempdir().unwrap();
        let base = template.path().join("rootfs.ext4");
        std::fs::write(&base, vec![1u8; 16384]).unwrap();
        let work = tempfile::tempdir().unwrap();
        let current = work.path().join("rootfs.ext4");
        let mut changed = vec![1u8; 16384];
        changed[5000] = 2;
        std::fs::write(&current, &changed).unwrap();
        let diff = work.path().join("rootfs.diff");
        let answer_dir = template.path().to_string_lossy().into_owned();
        let swap = base.clone();
        let resolver = FakeTemplateResolver::new(move || {
            match after {
                After::Untouched => {}
                After::Replaced => {
                    let fresh = swap.with_extension("fresh");
                    std::fs::write(&fresh, vec![1u8; 16384]).unwrap();
                    std::fs::rename(&fresh, &swap).unwrap();
                }
                After::Recreated => {
                    std::fs::remove_file(&swap).unwrap();
                    std::fs::write(&swap, vec![1u8; 16384]).unwrap();
                }
                After::Gone => {
                    return Err(crate::template_resolver::TemplateLookupError::Rpc(
                        tonic::Status::not_found("build removed"),
                    ));
                }
            }
            Ok(tonic::Response::new(GetTemplateResponse {
                template: Some(TemplateRef {
                    name: "seed".into(),
                    content_hash: Some(hash.clone()),
                    tag: None,
                    environment: None,
                }),
                template_dir: answer_dir.clone(),
                cached_only: true,
                ..Default::default()
            }))
        });
        let runtime = tokio::runtime::Handle::current();
        let resolver = std::sync::Arc::new(resolver);
        let lookups = std::sync::Arc::clone(&resolver);
        let (base_path, current_path, diff_path) = (base.clone(), current.clone(), diff.clone());
        let hash_for_diff = format!("sha256:{}", "a".repeat(64));
        let kept = tokio::task::spawn_blocking(move || {
            diff_against_verified_base(
                &runtime,
                resolver.as_ref(),
                ("seed", &hash_for_diff),
                base_path,
                &current_path,
                &diff_path,
            )
        })
        .await
        .unwrap();

        assert_eq!(lookups.requests().len(), 1, "{after:?}");
        assert_eq!(kept, matches!(after, After::Untouched), "{after:?}");
        assert_eq!(diff.exists(), kept, "{after:?}");
    }
}

/// A template-manager that accepts the lookup but never answers can't hold a
/// transfer, and the sandbox locks it holds, past the lookup's bound: it gives
/// no verified base, and a delta import is refused as retryable.
#[tokio::test(start_paused = true)]
async fn a_delta_base_lookup_that_never_answers_gives_no_base() {
    struct Silent;
    #[async_trait::async_trait]
    impl crate::template_resolver::TemplateResolver for Silent {
        async fn get_template(
            &self,
            _: tonic::Request<pplx_space_template_mgr_client::proto::GetTemplateRequest>,
        ) -> Result<
            tonic::Response<pplx_space_template_mgr_client::proto::GetTemplateResponse>,
            crate::template_resolver::TemplateLookupError,
        > {
            std::future::pending().await
        }

        async fn get_template_on_shared_channel(
            &self,
            _: tonic::Request<pplx_space_template_mgr_client::proto::GetTemplateRequest>,
        ) -> Result<
            tonic::Response<pplx_space_template_mgr_client::proto::GetTemplateResponse>,
            tonic::Status,
        > {
            std::future::pending().await
        }
    }
    let hash = format!("sha256:{}", "a".repeat(64));
    let started = tokio::time::Instant::now();

    // The outer bound turns a lookup that waits forever into a failure.
    let base = tokio::time::timeout(
        DELTA_BASE_LOOKUP_TIMEOUT * 2,
        verified_template_rootfs(&Silent, "seed", &hash),
    )
    .await
    .expect("the delta-base lookup outlived its bound");
    assert_eq!(base, None);
    assert_eq!(started.elapsed(), DELTA_BASE_LOOKUP_TIMEOUT);

    // A delta import it can't confirm is refused as retryable.
    let status = required_delta_base(&Silent, "seed", &hash)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");
}
