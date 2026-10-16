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
