use super::*;
use crate::db::test_fixtures::state_write_fixtures::{admit_stop, complete_stop};

#[test]
fn snapshot_inventory_does_not_return_partial_success_for_oversized_sidecars() {
    use pplx_space_common_core::inventory::MAX_ENCODED_BYTES;
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("f-large");
    std::fs::create_dir(&directory).unwrap();
    for name in [".transfer.json", "snapshot.json"] {
        let path = directory.join(name);
        std::fs::File::create(&path)
            .unwrap()
            .set_len((MAX_ENCODED_BYTES + 1) as u64)
            .unwrap();
        let error = project_snapshot_inventory(
            "sandbox",
            "seed",
            None,
            root.path(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted, "{name}");
        std::fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn rollback_checkout_excludes_gc_and_rejects_existing_delete_owner() {
    use crate::service::tests::service_tests::service_for_teardown_test;
    use pplx_space_common_core::types::{ArtifactKind, LocalArtifactId};
    let service = service_for_teardown_test(crate::db::test_conn());
    let sandbox = uuid::Uuid::now_v7().to_string();
    let name = "f-checkout";
    assert!(
        service
            .checkout_rollback_snapshot(&sandbox, name)
            .unwrap()
            .is_none()
    );
    let artifact = LocalArtifactId::new();
    service
        .local_artifacts
        .publish_named_snapshot(
            &sandbox,
            artifact,
            ArtifactKind::Full,
            &name.parse().unwrap(),
            &pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(&sandbox, name),
        )
        .unwrap();
    let reader = service
        .checkout_rollback_snapshot(&sandbox, name)
        .unwrap()
        .unwrap();
    assert!(matches!(
        service.local_artifacts.check_out_for_delete(artifact),
        Err(crate::local_artifacts::CheckoutError::NotReady { .. })
    ));
    drop(reader);
    let deletion = service
        .local_artifacts
        .check_out_for_delete(artifact)
        .unwrap();
    let error = service
        .checkout_rollback_snapshot(&sandbox, name)
        .err()
        .unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    drop(deletion);
    assert!(
        service
            .checkout_rollback_snapshot(&sandbox, name)
            .unwrap()
            .is_some()
    );
}

#[test]
fn transferred_disk_snapshot_inventory_requires_publication_and_valid_binding() {
    use crate::local_artifacts::{ArtifactState, NamedArtifactPublication};
    use pplx_space_common_core::types::{ArtifactKind, SnapshotName};
    let root = tempfile::tempdir().unwrap();
    let mut btrfs = Vec::new();
    let mut publications = Vec::new();
    for (name, published, binding) in [
        ("d-transfer-unpublished", false, "valid"),
        ("d-transfer-published", true, "valid"),
        ("d-transfer-corrupt", true, "corrupt"),
        ("d-transfer-wrong-owner", true, "wrong"),
        ("d-transfer-wrong-template", true, "template"),
        ("d-transfer-wrong-hash", true, "hash"),
        ("d-transfer-no-rootfs", true, "valid"),
        ("d-legacy", false, "none"),
    ] {
        let path = root.path().join(name);
        std::fs::create_dir(&path).unwrap();
        if name != "d-transfer-no-rootfs" {
            std::fs::write(path.join("rootfs.ext4"), b"rootfs").unwrap();
        }
        if binding != "none" {
            let bytes = if binding == "corrupt" {
                b"invalid".to_vec()
            } else {
                serde_json::to_vec(&crate::db::transfer::TransferBinding {
                    sandbox_id: if binding == "wrong" { "other" } else { "sbx" }.into(),
                    template: if binding == "template" {
                        "other"
                    } else {
                        "template"
                    }
                    .into(),
                    content_hash: if binding == "hash" { "other" } else { "hash" }.into(),
                    stream_fingerprint: "f".repeat(64),
                })
                .unwrap()
            };
            std::fs::write(path.join(".transfer.json"), bytes).unwrap();
        }
        if published {
            publications.push(NamedArtifactPublication {
                snapshot_name: SnapshotName::parse(name).unwrap(),
                local_path: path.to_str().unwrap().into(),
                artifact_kind: ArtifactKind::Disk,
                state: ArtifactState::Ready,
            });
        }
        btrfs.push(pplx_space_node_mgr_client::proto::BtrfsSnapshotInfo {
            name: name.into(),
            ..Default::default()
        });
    }
    let snapshots = project_snapshot_inventory(
        "sbx",
        "template",
        Some("hash"),
        root.path(),
        btrfs,
        publications,
    )
    .unwrap();
    for (name, state) in [
        ("d-transfer-unpublished", "unknown"),
        ("d-transfer-published", "ready"),
        ("d-transfer-corrupt", "incomplete"),
        ("d-transfer-wrong-owner", "incomplete"),
        ("d-transfer-wrong-template", "incomplete"),
        ("d-transfer-wrong-hash", "incomplete"),
        ("d-transfer-no-rootfs", "incomplete"),
        ("d-legacy", "ready"),
    ] {
        assert_eq!(
            snapshots
                .iter()
                .find(|snapshot| snapshot.id == name)
                .unwrap()
                .state,
            state,
            "{name}"
        );
    }
}

#[test]
fn snapshot_inventory_requires_publication_and_capture_files_and_never_invents_provenance() {
    use crate::local_artifacts::{ArtifactState, NamedArtifactPublication};
    use pplx_space_common_core::types::{ArtifactKind, SnapshotName};
    let root = tempfile::tempdir().unwrap();
    let cases = [
        ("f-ready", Some(ArtifactState::Ready), true, true, "ready"),
        (
            "f-incomplete",
            Some(ArtifactState::Ready),
            false,
            true,
            "incomplete",
        ),
        ("f-legacy", None, true, true, "unknown"),
        ("f-interrupted", None, false, true, "incomplete"),
        (
            "f-restoring",
            Some(ArtifactState::Restoring),
            true,
            true,
            "restoring",
        ),
        (
            "f-deleting",
            Some(ArtifactState::Deleting),
            false,
            true,
            "deleting",
        ),
        (
            "f-other-path",
            Some(ArtifactState::Ready),
            true,
            false,
            "unknown",
        ),
    ];
    let mut publications = Vec::new();
    let mut btrfs = Vec::new();
    for (name, state, complete, matching_path, _) in cases {
        let path = root.path().join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("vmstate"), b"state").unwrap();
        if complete {
            std::fs::write(path.join("memory"), b"memory").unwrap();
        }
        if let Some(state) = state {
            publications.push(NamedArtifactPublication {
                snapshot_name: SnapshotName::parse(name).unwrap(),
                local_path: if matching_path {
                    path.to_str().unwrap().into()
                } else {
                    "/different/path".into()
                },
                artifact_kind: ArtifactKind::Full,
                state,
            });
        }
        // A btrfs observation is not evidence that VM capture completed.
        btrfs.push(pplx_space_node_mgr_client::proto::BtrfsSnapshotInfo {
            name: name.into(),
            exclusive_bytes: Some(17),
            ..Default::default()
        });
    }
    std::fs::write(root.path().join("f-not-directory"), b"not a snapshot").unwrap();
    std::fs::write(root.path().join("f-ready/snapshot.json"), br#"{"sandbox_id":"sbx","name":"f-ready","label":"captured","timestamp":"capture-time","source":{"template_id":"captured-template","content_hash":"captured-hash"}}"#).unwrap();
    btrfs.push(pplx_space_node_mgr_client::proto::BtrfsSnapshotInfo {
        name: "d-manual".into(),
        timestamp: "disk-time".into(),
        ..Default::default()
    });
    btrfs.push(pplx_space_node_mgr_client::proto::BtrfsSnapshotInfo {
        name: "d-stop-internal".into(),
        ..Default::default()
    });
    let snapshots = project_snapshot_inventory(
        "sbx",
        "template",
        Some("hash"),
        root.path(),
        btrfs,
        publications,
    )
    .unwrap();
    assert_eq!(snapshots.len(), cases.len() + 1);
    for (name, _, _, _, expected_state) in cases {
        let snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.id == name)
            .unwrap();
        assert_eq!(snapshot.state, expected_state, "{name}");
        assert_eq!(snapshot.size_bytes, Some(17));
        assert_eq!(snapshot.vcpu, 0);
        assert_eq!(snapshot.memory_mib, 0);
        assert!(snapshot.locations.is_empty());
        if name == "f-ready" {
            assert_eq!(snapshot.template_id.as_deref(), Some("captured-template"));
            assert_eq!(snapshot.content_hash.as_deref(), Some("captured-hash"));
            assert_eq!(snapshot.label.as_deref(), Some("captured"));
            assert_eq!(snapshot.timestamp.as_deref(), Some("capture-time"));
        } else {
            assert_eq!(snapshot.template_id, None);
            assert_eq!(snapshot.content_hash, None);
        }
    }
    let disk = snapshots
        .iter()
        .find(|snapshot| snapshot.id == "d-manual")
        .unwrap();
    assert_eq!(disk.state, "ready");
    assert_eq!(disk.template_id, None);
    assert_eq!(disk.timestamp.as_deref(), Some("disk-time"));
}

#[test]
fn snapshot_inventory_ignores_mismatched_or_malformed_capture_metadata() {
    for metadata in [
        br#"{"sandbox_id":"other","source":{"template_id":"wrong"}}"#.as_slice(),
        br#"{"sandbox_id":"sbx","name":"other","source":{"template_id":"wrong"}}"#.as_slice(),
        b"not json".as_slice(),
    ] {
        let root = tempfile::tempdir().unwrap();
        let snapshot = root.path().join("f-legacy");
        std::fs::create_dir(&snapshot).unwrap();
        std::fs::write(snapshot.join("snapshot.json"), metadata).unwrap();
        let snapshots = project_snapshot_inventory(
            "sbx",
            "template",
            Some("hash"),
            root.path(),
            vec![],
            vec![],
        )
        .unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].state, "incomplete");
        assert_eq!(snapshots[0].template_id, None);
        assert_eq!(snapshots[0].content_hash, None);
    }
}

#[test]
fn rollback_replaces_optional_sidecars_or_reports_unremovable_stale_evidence() {
    for (source_present, target_present, target_is_directory) in [
        (true, true, false),
        (false, true, false),
        (false, false, false),
        (false, true, true),
    ] {
        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let filename = "runtime.json";
        let destination = target.path().join(filename);
        if source_present {
            std::fs::write(source.path().join(filename), b"replacement").unwrap();
        }
        if target_is_directory {
            std::fs::create_dir(&destination).unwrap();
        } else if target_present {
            std::fs::write(&destination, b"stale").unwrap();
        }
        let result = replace_optional_snapshot_sidecar(
            source.path().to_str().unwrap(),
            target.path().to_str().unwrap(),
            filename,
        );
        if target_is_directory {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Internal);
            assert!(destination.is_dir());
        } else {
            result.unwrap();
            if source_present {
                assert_eq!(std::fs::read(destination).unwrap(), b"replacement");
            } else {
                assert!(!destination.exists());
            }
        }
    }
}

fn sandbox_state(conn: &crate::db::Connection, id: &str) -> SandboxState {
    crate::db::get_sandbox(conn, id).unwrap().unwrap().state
}

/// The full rollback holds the artifact lock, which Stop does not take, while
/// it rolls the disk back and copies the snapshot. A Stop admitted in that
/// window must not be overwritten by the restage to `suspended`: it keeps its
/// `stopping` row and its completion still settles it.
#[test]
fn full_rollback_restage_leaves_a_stop_admitted_after_its_own_stop() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox_with_runtime(
        &conn,
        &id,
        SandboxState::Started,
    );
    // The rollback's own stop.
    admit_stop(&conn, &id);
    complete_stop(&conn, &id);
    assert_eq!(sandbox_state(&conn, &id), SandboxState::Stopped);
    admit_stop(&conn, &id);
    let before = crate::db::test_fixtures::database_rows(&conn);

    let error = restage_rolled_back_full_snapshot(&conn, &id)
        .expect_err("the restage must not overwrite a concurrent Stop");

    assert_eq!(error.code(), tonic::Code::Aborted);
    assert_eq!(
        error.message(),
        "complete process snapshot suspension: sandbox state write was superseded: sandbox is stopping"
    );
    assert_eq!(crate::db::test_fixtures::database_rows(&conn), before);
    complete_stop(&conn, &id);
    let row = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(row.cleanup_completion, None);
}

/// The restage writes only over a `stopped` row and accepts one that is
/// already `suspended` without a write. Every other row keeps every table
/// unchanged and the rollback reports why. Rows carry a failure reason, a
/// receipt for another target and backdated timestamps, which an in-place
/// rewrite of a `suspended` row would clear or restamp.
#[test]
fn full_rollback_restage_writes_only_over_a_stopped_row() {
    enum Expected {
        Restaged,
        AlreadySuspended,
        Refused(String),
    }
    for state in SandboxState::all() {
        let expected = match state {
            SandboxState::Stopped => Expected::Restaged,
            SandboxState::Suspended => Expected::AlreadySuspended,
            // The generic guard: a deleting row owns its `delete_row` cleanup.
            SandboxState::Deleting => Expected::Refused(
                "complete process snapshot suspension: sandbox state write was not applied: \
                 superseded_by_cleanup_episode"
                    .to_string(),
            ),
            SandboxState::Quarantined => Expected::Refused(
                "complete process snapshot suspension: sandbox state write was not applied: \
                 blocked_by_quarantine"
                    .to_string(),
            ),
            SandboxState::Creating
            | SandboxState::Created
            | SandboxState::Starting
            | SandboxState::Started
            | SandboxState::Stopping
            | SandboxState::Pausing
            | SandboxState::Paused
            | SandboxState::Resuming
            | SandboxState::Checkpointing
            | SandboxState::Suspending
            | SandboxState::Restoring
            | SandboxState::CleanupRequired
            | SandboxState::CleanupFailed
            | SandboxState::Failed => Expected::Refused(format!(
                "complete process snapshot suspension: sandbox state write was superseded: \
                 sandbox is {state}"
            )),
        };
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox_with_runtime(&conn, &id, state);
        crate::db::test_fixtures::state_write_fixtures::decorate(&conn, &id, SandboxState::Started);
        let before = crate::db::test_fixtures::database_rows(&conn);

        let result = restage_rolled_back_full_snapshot(&conn, &id);

        match expected {
            Expected::Restaged => {
                result.unwrap();
                assert_eq!(sandbox_state(&conn, &id), SandboxState::Suspended);
            }
            Expected::AlreadySuspended => {
                result.unwrap();
                assert_eq!(
                    crate::db::test_fixtures::database_rows(&conn),
                    before,
                    "{state}"
                );
            }
            Expected::Refused(message) => {
                let error = result.expect_err("a refused restage must fail");
                assert_eq!(error.code(), tonic::Code::Aborted, "{state}");
                assert_eq!(error.message(), message, "{state}");
                assert_eq!(
                    crate::db::test_fixtures::database_rows(&conn),
                    before,
                    "{state}"
                );
            }
        }
    }
}

/// DeleteSnapshots maps each `delete_named_snapshot` outcome to its answer,
/// by name and for `all=true`; that helper's tests own the row and byte
/// ordering. An in-use row, including one a failed row commit left `deleting`,
/// refuses the name. An unreadable row answers `Internal`, and a row naming
/// another path `FailedPrecondition`. Delete-all omits each of them from
/// `deleted` and goes on. None reaches node-manager. A catalogued snapshot is
/// renamed to GC staging, and node-manager deletes that path once the
/// artifact lock is released; once the rename and the row delete are done the
/// snapshot is deleted, even if node-manager then fails. A snapshot without a
/// row is deleted in place under the lock, as before, and a failed
/// byte delete answers `Internal` by name and passes node-manager's status
/// through for all.
#[tokio::test]
async fn delete_snapshots_answers_each_named_delete_outcome() {
    use pplx_space_common_core::types::{ArtifactKind, LocalArtifactId, SnapshotName};
    use pplx_space_node_mgr_client::proto::{
        BtrfsSnapshotInfo, DeleteCacheArtifactRequest, DeleteCacheArtifactResponse,
        ListBtrfsSnapshotsRequest, ListBtrfsSnapshotsResponse,
    };
    use std::path::Path;
    use tonic::Code;

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Setup {
        Deletable,
        StagingDeleteFails,
        Pinned,
        AlreadyDeleting,
        UnreadableRow,
        PathDiffers,
        UncataloguedDeleteFails,
    }
    /// The path node-manager is asked to delete.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Deletes {
        Nothing,
        LocalPath,
        Staging,
    }
    const NAME: &str = "d-user-keep";
    // (setup, byte delete requested, single-name answer, delete-all answer:
    // whether the snapshot is listed as deleted; an error is its code and the
    // start of its message)
    type Answer<T> = Result<T, (Code, &'static str)>;
    let cases: [(Setup, Deletes, Answer<()>, Answer<bool>); 7] = [
        (Setup::Deletable, Deletes::Staging, Ok(()), Ok(true)),
        (
            Setup::StagingDeleteFails,
            Deletes::Staging,
            Ok(()),
            Ok(true),
        ),
        (
            Setup::Pinned,
            Deletes::Nothing,
            Err((Code::FailedPrecondition, "snapshot is in use")),
            Ok(false),
        ),
        (
            Setup::AlreadyDeleting,
            Deletes::Nothing,
            Err((Code::FailedPrecondition, "snapshot is in use")),
            Ok(false),
        ),
        (
            Setup::UnreadableRow,
            Deletes::Nothing,
            Err((Code::Internal, "inspect snapshot publication")),
            Ok(false),
        ),
        (
            Setup::PathDiffers,
            Deletes::Nothing,
            Err((
                Code::FailedPrecondition,
                "delete snapshot publication path differs",
            )),
            Ok(false),
        ),
        (
            Setup::UncataloguedDeleteFails,
            Deletes::LocalPath,
            Err((Code::Internal, "delete snapshot failed")),
            Err((Code::Unavailable, "injected byte delete failure")),
        ),
    ];
    for all in [false, true] {
        for (setup, byte_delete, single, delete_all) in cases {
            let case = format!("{setup:?}, all={all}");
            let sandbox = SandboxId::new().to_string();
            // Each requested path, with whether the sandbox's artifact lock
            // was held while node-manager deleted it.
            let deletions = Arc::new(std::sync::Mutex::new(Vec::new()));
            let coordinator = Arc::new(std::sync::OnceLock::<
                crate::auto_lifecycle::AutoLifecycleCoordinator,
            >::new());
            let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default()
                .unary("ListBtrfsSnapshots", |_: ListBtrfsSnapshotsRequest| {
                    Ok(ListBtrfsSnapshotsResponse {
                        snapshots: vec![BtrfsSnapshotInfo {
                            name: NAME.into(),
                            ..Default::default()
                        }],
                        next_page_token: String::new(),
                    })
                })
                .unary("DeleteCacheArtifact", {
                    let (deletions, coordinator, sandbox) = (
                        Arc::clone(&deletions),
                        Arc::clone(&coordinator),
                        sandbox.clone(),
                    );
                    move |request: DeleteCacheArtifactRequest| {
                        let lock_held = coordinator
                            .get()
                            .expect("the service is built before any delete")
                            .artifact_lock(&sandbox)
                            .try_lock_owned()
                            .is_err();
                        deletions
                            .lock()
                            .unwrap()
                            .push((request.local_path, lock_held));
                        if matches!(
                            setup,
                            Setup::StagingDeleteFails | Setup::UncataloguedDeleteFails
                        ) {
                            return Err(Status::unavailable("injected byte delete failure"));
                        }
                        Ok(DeleteCacheArtifactResponse { was_present: true })
                    }
                })
                .serve();
            let scratch = tempfile::tempdir().unwrap();
            let conn = crate::db::test_conn();
            crate::db::fixture::insert_test_sandbox(&conn, &sandbox, SandboxState::Stopped);
            let service = SandboxManagerService::new_with_options(
                conn,
                SandboxManagerServiceOptions {
                    node_manager: node_manager.connector(),
                    ..SandboxManagerServiceOptions::for_test_in(scratch.path())
                },
            );
            coordinator
                .set(service.auto_lifecycle.clone())
                .unwrap_or_else(|_| unreachable!());
            // `for_test_in` roots the sandbox cache here.
            let local_path = format!(
                "{}/cache/{sandbox}/snapshots/{NAME}",
                scratch.path().display()
            );
            std::fs::create_dir_all(&local_path).unwrap();
            let artifact = LocalArtifactId::new();
            let staging = format!(
                "{}/cache/{sandbox}/snapshots/.gc-pending-{artifact}",
                scratch.path().display()
            );
            if matches!(
                setup,
                Setup::Deletable
                    | Setup::StagingDeleteFails
                    | Setup::Pinned
                    | Setup::AlreadyDeleting
                    | Setup::PathDiffers
            ) {
                let published = if setup == Setup::PathDiffers {
                    format!("{local_path}-elsewhere")
                } else {
                    local_path.clone()
                };
                service
                    .local_artifacts
                    .publish_named_snapshot(
                        &sandbox,
                        artifact,
                        ArtifactKind::Disk,
                        &SnapshotName::parse(NAME).unwrap(),
                        &published,
                    )
                    .unwrap();
            }
            let db = service.db.lock().unwrap();
            match setup {
                Setup::Pinned => {
                    db.execute(
                        "UPDATE sandboxes SET suspend_predecessor_artifact_id = ?1",
                        [artifact.to_string()],
                    )
                    .unwrap();
                }
                // As a failed row commit leaves it.
                Setup::AlreadyDeleting => {
                    db.execute("UPDATE local_artifacts SET state = 'deleting'", [])
                        .unwrap();
                }
                // An id that is not a UUID does not decode.
                Setup::UnreadableRow => {
                    db.execute(
                        "INSERT INTO local_artifacts (local_artifact_id, sandbox_id,
                             artifact_kind, artifact_origin, snapshot_name, local_path,
                             created_at)
                         VALUES ('not-a-uuid', ?1, 'disk', 'snapshot', ?2, ?3, '0')",
                        [sandbox.clone(), NAME.to_string(), local_path.clone()],
                    )
                    .unwrap();
                }
                Setup::Deletable
                | Setup::StagingDeleteFails
                | Setup::PathDiffers
                | Setup::UncataloguedDeleteFails => {}
            }
            drop(db);

            let result = service
                .delete_snapshots(crate::service::activation_support::internal_request(
                    DeleteSnapshotsRequest {
                        sandbox_id: sandbox.clone(),
                        all,
                        snapshot_name: (!all).then(|| NAME.to_string()),
                    },
                ))
                .await
                .map(|response| response.into_inner().deleted == [NAME]);
            let expected = if all {
                delete_all
            } else {
                single.map(|()| true)
            };
            match (result, expected) {
                (Ok(listed), Ok(expected)) => assert_eq!(listed, expected, "{case}"),
                (Err(status), Err((code, message))) => {
                    assert_eq!(status.code(), code, "{case}");
                    assert!(
                        status.message().starts_with(message),
                        "{case}: {}",
                        status.message()
                    );
                }
                (result, expected) => panic!("{case}: {result:?}, expected {expected:?}"),
            }
            // A snapshot without a row is deleted in place under the lock, as
            // before; staging is deleted once the lock is released.
            let requested = match byte_delete {
                Deletes::Nothing => Vec::new(),
                Deletes::LocalPath => vec![(local_path.clone(), true)],
                Deletes::Staging => vec![(staging.clone(), false)],
            };
            assert_eq!(*deletions.lock().unwrap(), requested, "{case}");
            // The fake deletes nothing, so the bytes are wherever sbx-mgr left them.
            let staged = byte_delete == Deletes::Staging;
            assert_eq!(Path::new(&local_path).exists(), !staged, "{case}");
            assert_eq!(Path::new(&staging).exists(), staged, "{case}");
        }
    }
}

/// GC staging, in the fixed-length `.gc-pending-<id>` shape and in the earlier
/// `<name>.gc-pending-<id>` one that keeps the snapshot's own name.
fn gc_staging_names() -> [String; 2] {
    let id = pplx_space_common_core::types::LocalArtifactId::new();
    [
        format!(".gc-pending-{id}"),
        format!("d-user-keep.gc-pending-{id}"),
    ]
}

/// Node-manager lists GC staging among a sandbox's snapshots, and ListSnapshots
/// leaves it out, from that listing and from its own scan for full snapshots.
#[test]
fn snapshot_inventory_omits_gc_staging() {
    let root = tempfile::tempdir().unwrap();
    let id = pplx_space_common_core::types::LocalArtifactId::new();
    // Only on disk: the scan for full snapshots finds it.
    std::fs::create_dir(
        root.path()
            .join(format!("f-manual-20260101T000000Z.gc-pending-{id}")),
    )
    .unwrap();
    let btrfs = std::iter::once("d-user-keep".to_string())
        .chain(gc_staging_names())
        .map(|name| {
            std::fs::create_dir(root.path().join(&name)).unwrap();
            pplx_space_node_mgr_client::proto::BtrfsSnapshotInfo {
                name,
                ..Default::default()
            }
        })
        .collect();
    let snapshots =
        project_snapshot_inventory("sbx", "template", None, root.path(), btrfs, Vec::new())
            .unwrap();
    let listed: Vec<_> = snapshots
        .iter()
        .map(|snapshot| snapshot.id.as_str())
        .collect();
    assert_eq!(listed, ["d-user-keep"]);
}

/// GC staging is logically deleted bytes that only the orphan sweep removes, so
/// no snapshot RPC treats it as a snapshot. Deleting it by name, rolling back to
/// it and exporting it are refused, and delete-all passes over it. None reaches
/// node-manager's delete, and the staging stays for the sweep.
#[tokio::test]
async fn snapshot_rpcs_refuse_gc_staging() {
    use pplx_space_node_mgr_client::proto::{
        BtrfsSnapshotInfo, DeleteCacheArtifactRequest, DeleteCacheArtifactResponse,
        ListBtrfsSnapshotsRequest, ListBtrfsSnapshotsResponse,
    };

    let staging = gc_staging_names();
    let deletions = Arc::new(std::sync::Mutex::new(Vec::new()));
    let listed = staging.clone();
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary("ListBtrfsSnapshots", move |_: ListBtrfsSnapshotsRequest| {
            Ok(ListBtrfsSnapshotsResponse {
                snapshots: listed
                    .iter()
                    .map(|name| BtrfsSnapshotInfo {
                        name: name.clone(),
                        ..Default::default()
                    })
                    .collect(),
                next_page_token: String::new(),
            })
        })
        .unary("DeleteCacheArtifact", {
            let deletions = Arc::clone(&deletions);
            move |request: DeleteCacheArtifactRequest| {
                deletions.lock().unwrap().push(request.local_path);
                Ok(DeleteCacheArtifactResponse { was_present: true })
            }
        })
        .serve();
    let scratch = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let sandbox = SandboxId::new().to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &sandbox, SandboxState::Stopped);
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node_manager.connector(),
            ..SandboxManagerServiceOptions::for_test_in(scratch.path())
        },
    );
    let refused = |result: Result<(), Status>, rpc: &str, name: &str, message: &str| {
        let status = result.expect_err(rpc);
        assert_eq!(
            status.code(),
            tonic::Code::FailedPrecondition,
            "{rpc} {name}: {status}"
        );
        assert_eq!(status.message(), message, "{rpc} {name}");
    };
    for name in &staging {
        let deleted = service
            .delete_snapshots(crate::service::activation_support::internal_request(
                DeleteSnapshotsRequest {
                    sandbox_id: sandbox.clone(),
                    all: false,
                    snapshot_name: Some(name.clone()),
                },
            ))
            .await
            .map(drop);
        refused(
            deleted,
            "DeleteSnapshots",
            name,
            "cannot delete an internal snapshot artifact",
        );
        let rolled_back = service
            .rollback(crate::service::activation_support::internal_request(
                RollbackRequest {
                    sandbox_id: sandbox.clone(),
                    snapshot_name: name.clone(),
                    ..Default::default()
                },
            ))
            .await
            .map(drop);
        refused(
            rolled_back,
            "Rollback",
            name,
            "cannot rollback to an internal snapshot artifact",
        );
        let exported = service
            .export_snapshot(crate::service::activation_support::internal_request(
                crate::proto::ExportSnapshotRequest {
                    sandbox_id: sandbox.clone(),
                    snapshot_name: name.clone(),
                    ..Default::default()
                },
            ))
            .await
            .map(drop);
        refused(
            exported,
            "ExportSnapshot",
            name,
            "cannot export an internal snapshot artifact",
        );
    }
    let all = service
        .delete_snapshots(crate::service::activation_support::internal_request(
            DeleteSnapshotsRequest {
                sandbox_id: sandbox.clone(),
                all: true,
                snapshot_name: None,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(all.deleted.is_empty(), "{:?}", all.deleted);
    assert!(deletions.lock().unwrap().is_empty());
}
