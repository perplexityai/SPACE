use super::*;

#[test]
fn checkpoint_execution_metadata_backfills_legacy_scheduled_snapshot_sidecar() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("snapshot.json"),
        r#"{
            "sandbox_id": "sandbox-1",
            "name": "f-10m-example",
            "type": "full",
            "retention_tier": "10m",
            "timestamp": "2026-06-13T00:00:00Z"
        }"#,
    )
    .unwrap();

    let execution = checkpoint_execution_metadata(directory.path().to_str().unwrap())
        .unwrap()
        .into_execution();

    assert_eq!(
        execution.runtime().kind,
        pplx_space_common_core::types::RuntimeKind::Firecracker
    );
    assert_eq!(execution.runtime().version.as_str(), "1.15.1");
    assert_eq!(execution.state_format().unwrap().version.as_str(), "9.0.0");
}

#[test]
fn checkpoint_budget_survives_runtime_and_suspend_metadata_roundtrips() {
    use pplx_space_common_core::types::{SnapshotExecutionMetadata, VcpuCount};
    let directory = tempfile::tempdir().unwrap();
    let budget = Some(VcpuCount::try_from_u32(8).unwrap());
    let runtime = SnapshotExecutionMetadata::new(initial_firecracker_execution_metadata().unwrap())
        .unwrap()
        .with_cpu_budget(budget);
    for (name, contents) in [
        ("runtime.json", serde_json::to_value(&runtime).unwrap()),
        (
            "snapshot.json",
            serde_json::json!({
                "version": 2,
                "execution": runtime,
                "sandbox_id": "sandbox-example",
                "size_bytes": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "source": {"runtime": "fc", "template_id": "seed"}
            }),
        ),
    ] {
        let path = directory.path().join(name);
        std::fs::write(&path, serde_json::to_vec(&contents).unwrap()).unwrap();
        assert_eq!(
            checkpoint_execution_metadata(directory.path().to_str().unwrap())
                .unwrap()
                .cpu_budget(),
            budget
        );
        let mut malformed = contents;
        if name == "runtime.json" {
            malformed["cpu_budget"] = serde_json::json!(0);
        } else {
            malformed["execution"]["cpu_budget"] = serde_json::json!(0);
        }
        std::fs::write(&path, serde_json::to_vec(&malformed).unwrap()).unwrap();
        checkpoint_execution_metadata(directory.path().to_str().unwrap()).unwrap_err();
        std::fs::remove_file(path).unwrap();
    }
    assert_eq!(
        checkpoint_execution_metadata(directory.path().to_str().unwrap())
            .unwrap()
            .cpu_budget(),
        None
    );
}

#[test]
fn restore_uses_explicit_or_captured_allocation_without_inventing_legacy_evidence() {
    use pplx_space_common_core::types::VcpuCount;
    for (requested, captured, expected) in [
        (Some(8), Some(2), Some(8)),
        (Some(4), None, Some(4)),
        (None, Some(8), Some(8)),
        (None, None, None),
    ] {
        assert_eq!(
            super::super::create::restore_cpu_budget(
                requested,
                captured.map(|cpu| VcpuCount::try_from_u32(cpu).unwrap())
            )
            .unwrap()
            .map(|cpu| cpu.get().get()),
            expected
        );
    }
    assert_eq!(
        super::super::create::restore_cpu_budget(Some(0), None)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn snapshot_identity_uses_durable_binding_when_artifact_metadata_is_missing_or_stale() {
    use pplx_space_common_core::types::{SandboxArtifactMetadata, VcpuCount};
    use pplx_space_cpu_arch::Arch;
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Paused);
    let mut row = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
    let catalog =
        pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let execution = catalog
        .iter()
        .find(|entry| entry.name() == "ch-54.0-windows-uefi")
        .unwrap()
        .definition()
        .execution_requirements()
        .unwrap();
    row.runtime = "ch".into();
    row.execution_metadata = Some(execution.clone());
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join(&id);
    std::fs::create_dir(&dir).unwrap();
    assert_eq!(
        sandbox_snapshot_execution_metadata_in(root.path(), &row)
            .unwrap()
            .execution(),
        &execution
    );
    let budget = VcpuCount::try_from_u32(4).unwrap();
    let mut stale = SandboxArtifactMetadata::new(
        id.parse().unwrap(),
        initial_firecracker_execution_metadata().unwrap(),
        "seed".parse().unwrap(),
        None,
    );
    stale.cpu_budget = Some(budget);
    std::fs::write(
        dir.join("metadata.json"),
        serde_json::to_vec(&stale).unwrap(),
    )
    .unwrap();
    let snapshot = sandbox_snapshot_execution_metadata_in(root.path(), &row).unwrap();
    assert_eq!(snapshot.execution(), &execution);
    assert_eq!(snapshot.cpu_budget(), Some(budget));
    row.execution_metadata = None;
    assert!(matches!(
        sandbox_snapshot_execution_metadata_in(root.path(), &row),
        Err(RuntimeArtifactReadError::MissingBinding)
    ));
    row.runtime = "fc".into();
    std::fs::remove_file(dir.join("metadata.json")).unwrap();
    assert_eq!(
        sandbox_snapshot_execution_metadata_in(root.path(), &row)
            .unwrap()
            .execution()
            .runtime()
            .kind,
        RuntimeKind::Firecracker
    );
}

/// A btrfs-send stream's parent build follows the sidecar's binding when it
/// reads and the row's durable binding when it doesn't: an unreadable sidecar
/// on a sandbox bound to a non-initial environment still names that
/// environment's build, not the legacy one.
#[test]
fn codec_environment_falls_back_to_the_row_binding_when_the_sidecar_is_unreadable() {
    use pplx_space_common_core::types::SnapshotExecutionMetadata;
    use pplx_space_cpu_arch::Arch;
    let catalog =
        pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let execution = |name: &str| {
        catalog
            .iter()
            .find(|entry| entry.name() == name)
            .unwrap()
            .definition()
            .execution_requirements()
            .unwrap()
    };
    let e1 = execution("fc-1.17.0-linux-6.1.155");
    let other = execution("fc-1.17.0-linux-6.18.44");
    assert!(e1.environment().is_some());
    assert_ne!(e1.environment(), other.environment());

    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Paused);
    let mut row = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
    row.execution_metadata = Some(e1.clone());
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join(&id)).unwrap();
    std::fs::write(root.path().join(&id).join("metadata.json"), b"{not json").unwrap();
    let unreadable = sandbox_snapshot_execution_metadata_in(root.path(), &row);
    assert!(matches!(
        unreadable,
        Err(RuntimeArtifactReadError::Parse { .. })
    ));

    assert_eq!(
        codec_environment(&id, unreadable, row.execution_metadata.as_ref()),
        e1.environment()
    );
    assert_eq!(
        codec_environment(
            &id,
            Ok(SnapshotExecutionMetadata::new(other.clone()).unwrap()),
            Some(&e1),
        ),
        other.environment(),
        "a readable sidecar wins"
    );
    assert_eq!(
        codec_environment(&id, Err(RuntimeArtifactReadError::MissingBinding), None),
        None
    );
}
