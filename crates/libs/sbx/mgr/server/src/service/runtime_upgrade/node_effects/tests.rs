//! The production effects against a test-served node-manager and a temporary
//! cache root.

use super::*;
use std::path::PathBuf;

use crate::node_manager_transport::fake::{FakeNodeManager, ServedNodeManager};
use crate::service::SandboxManagerServiceOptions;
use pplx_space_common_core::types::{LocalArtifactId, SandboxActivationAttemptId};
use pplx_space_common_core::types::{
    SandboxArtifactMetadata, SandboxId, SnapshotExecutionMetadata,
};
use pplx_space_node_mgr_client::proto::{
    CreateBtrfsSnapshotResponse, DeleteBtrfsSnapshotResponse, RestoreInstanceSnapshotResponse,
};
use std::sync::{Arc, Mutex};

struct Fixture {
    _directory: tempfile::TempDir,
    cache: PathBuf,
    _node: ServedNodeManager,
    service: SandboxManagerService,
    sandbox_id: String,
    source: ExecutionMetadata,
    target: ExecutionMetadata,
}

fn environments() -> (ExecutionMetadata, ExecutionMetadata) {
    let compiled = pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
        pplx_space_common_core::types::Arch::X86_64,
    )
    .unwrap();
    (
        compiled[0].definition().execution_requirements().unwrap(),
        compiled[1].definition().execution_requirements().unwrap(),
    )
}

fn fixture(state: SandboxState, encrypted: bool, node: FakeNodeManager) -> Fixture {
    let (source, target) = environments();
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().join("cache");
    let conn = crate::db::test_conn();
    let id = SandboxId::new();
    crate::db::fixture::insert_test_sandbox(&conn, &id.to_string(), state);
    if encrypted {
        conn.execute(
            "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
            crate::db::fixture::params![id.to_string()],
        )
        .unwrap();
    }
    crate::db::upsert_sandbox_execution_metadata(&conn, &id.to_string(), &source).unwrap();
    let sandbox_dir = cache.join(id.to_string());
    std::fs::create_dir_all(sandbox_dir.join("instance")).unwrap();
    std::fs::write(sandbox_dir.join("instance/rootfs.ext4"), b"disk").unwrap();
    if encrypted {
        std::fs::write(sandbox_dir.join("instance/cow.luks"), b"cow").unwrap();
    }
    std::fs::write(
        sandbox_dir.join("metadata.json"),
        serde_json::to_vec(&SandboxArtifactMetadata::new(
            id,
            source.clone(),
            "seed".parse().unwrap(),
            None,
        ))
        .unwrap(),
    )
    .unwrap();
    if state == SandboxState::Suspended {
        let checkpoint = sandbox_dir.join("snapshot");
        std::fs::create_dir_all(&checkpoint).unwrap();
        std::fs::write(
            checkpoint.join("runtime.json"),
            serde_json::to_vec(&SnapshotExecutionMetadata::new(source.clone()).unwrap()).unwrap(),
        )
        .unwrap();
        std::fs::write(checkpoint.join("vmstate"), b"vmstate").unwrap();
    }
    let node = node.serve();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            create_cache_dir: Some(cache.clone()),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    Fixture {
        _directory: directory,
        cache,
        _node: node,
        service,
        sandbox_id: id.to_string(),
        source,
        target,
    }
}

impl Fixture {
    fn effects(&self) -> NodeRuntimeUpgradeEffects {
        NodeRuntimeUpgradeEffects::for_recovery(self.service.clone())
    }

    fn row(&self) -> SandboxRow {
        self.service.lookup_sandbox(&self.sandbox_id).unwrap()
    }

    fn checkpoint(&self) -> PathBuf {
        self.cache.join(&self.sandbox_id).join("snapshot")
    }

    /// Fence an upgrade of the sandbox and return its record, as recorded
    /// with `identity` for the checkpoint directory.
    fn record(&self, identity: Option<&str>) -> RuntimeUpgradeRecord {
        crate::db::test_fixtures::fence_runtime_upgrade(
            &self.service.db.lock().unwrap(),
            &self.sandbox_id,
            self.target.environment().unwrap(),
            identity,
        )
    }

    /// The verified boot cleared the pin, as `mark_start_succeeded` does.
    fn clear_pin(&self) {
        self.service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes
                    SET current_local_artifact_id = NULL,
                        current_local_artifact_kind = NULL,
                        current_local_artifact_origin = NULL
                  WHERE id = ?1",
                crate::db::fixture::params![self.sandbox_id],
            )
            .unwrap();
    }

    fn adopt_checkpoint(&self) -> LocalArtifactId {
        let artifact = LocalArtifactId::new();
        assert!(
            crate::db::adopt_suspended_local_artifact(
                &self.service.db.lock().unwrap(),
                &self.sandbox_id,
                artifact,
                &self.checkpoint().to_string_lossy(),
            )
            .unwrap()
        );
        artifact
    }

    fn staged(&self, id: impl std::fmt::Display) -> PathBuf {
        PathBuf::from(crate::local_artifacts::gc_pending_staging_path(
            &self.checkpoint().to_string_lossy(),
            id,
        ))
    }

    /// Wait for the immediate reclaim to remove what retirement handed it.
    async fn reclaimed(&self, path: &Path) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while path.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{} was never reclaimed", path.display()));
    }

    /// Entries in the suspend-garbage lane.
    fn lane_garbage(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.cache.join(&self.sandbox_id).join("snapshots"))
            .map(|entries| {
                entries
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| {
                        path.file_name()
                            .unwrap()
                            .to_string_lossy()
                            .starts_with(".suspend-gc-pending-")
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn recording(calls: &Arc<Mutex<Vec<String>>>, entry: String) {
    calls.lock().unwrap().push(entry);
}

#[tokio::test]
async fn inspection_reads_the_disk_and_the_checkpoint() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let disk = f.effects().inspect_local_disk(&f.row()).await.unwrap();
    assert_eq!(disk.execution, f.source);
    assert_eq!(disk.checkpoint_execution, Some(f.source.clone()));
    let identity = disk.checkpoint_dir_identity.unwrap();
    let metadata = std::fs::metadata(f.checkpoint()).unwrap();
    assert!(
        identity.starts_with(&format!("{}:{}:", metadata.dev(), metadata.ino())),
        "{identity}"
    );

    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let disk = f.effects().inspect_local_disk(&f.row()).await.unwrap();
    assert_eq!(disk.checkpoint_execution, None);
    assert_eq!(disk.checkpoint_dir_identity, None);
}

#[tokio::test]
async fn inspection_refuses_a_sandbox_without_its_local_disk() {
    // A cold start without the rootfs would boot a fresh template clone.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    std::fs::remove_file(f.cache.join(&f.sandbox_id).join("instance/rootfs.ext4")).unwrap();
    let error = f.effects().inspect_local_disk(&f.row()).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("rootfs"), "{error}");

    // An encrypted sandbox needs its COW too.
    let f = fixture(SandboxState::Suspended, true, FakeNodeManager::default());
    f.effects().inspect_local_disk(&f.row()).await.unwrap();
    std::fs::remove_file(f.cache.join(&f.sandbox_id).join("instance/cow.luks")).unwrap();
    let error = f.effects().inspect_local_disk(&f.row()).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("COW"), "{error}");
}

#[tokio::test]
async fn the_preserved_copy_is_read_only_and_replaces_a_stale_one() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (deleted, created, restored) = (calls.clone(), calls.clone(), calls.clone());
    let node = FakeNodeManager::default()
        .unary(
            "DeleteBtrfsSnapshot",
            move |request: DeleteBtrfsSnapshotRequest| {
                recording(&deleted, format!("delete {}", request.snapshot_name));
                Err::<DeleteBtrfsSnapshotResponse, _>(Status::not_found("absent"))
            },
        )
        .unary(
            "CreateBtrfsSnapshot",
            move |request: CreateBtrfsSnapshotRequest| {
                assert!(!request.writable);
                recording(&created, format!("create {}", request.snapshot_name));
                Ok(CreateBtrfsSnapshotResponse {})
            },
        )
        .unary(
            "RestoreInstanceSnapshot",
            move |request: RestoreInstanceSnapshotRequest| {
                recording(&restored, format!("restore {}", request.snapshot_name));
                Ok(RestoreInstanceSnapshotResponse {})
            },
        );
    let f = fixture(SandboxState::Stopped, false, node);
    let effects = f.effects();
    let metadata = f.cache.join(&f.sandbox_id).join("metadata.json");
    let original = std::fs::read(&metadata).unwrap();

    effects
        .preserve_disk(&f.sandbox_id, "d-operation-runtime-upgrade-x")
        .await
        .unwrap();
    // The boot rewrites the disk's record of its execution, outside the
    // instance the copy holds.
    std::fs::write(&metadata, b"{\"written\": \"by the new boot\"}").unwrap();
    effects
        .restore_disk(&f.sandbox_id, "d-operation-runtime-upgrade-x")
        .await
        .unwrap();
    assert_eq!(std::fs::read(&metadata).unwrap(), original);
    // An absent copy is already discarded.
    effects
        .discard_disk(&f.sandbox_id, "d-operation-runtime-upgrade-x")
        .await
        .unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        [
            "delete d-operation-runtime-upgrade-x",
            "create d-operation-runtime-upgrade-x",
            "restore d-operation-runtime-upgrade-x",
            "delete d-operation-runtime-upgrade-x",
        ]
    );
    // Nothing of the preserved copy is left beside the sandbox.
    let left: Vec<_> = std::fs::read_dir(f.cache.join(&f.sandbox_id))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().contains("runtime-upgrade"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

#[tokio::test]
async fn a_failed_discard_is_reported() {
    let node =
        FakeNodeManager::default().unary("DeleteBtrfsSnapshot", |_: DeleteBtrfsSnapshotRequest| {
            Err::<DeleteBtrfsSnapshotResponse, _>(Status::internal("btrfs is unhappy"))
        });
    let f = fixture(SandboxState::Stopped, false, node);
    let error = f
        .effects()
        .discard_disk(&f.sandbox_id, "d-operation-runtime-upgrade-x")
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
}

#[tokio::test]
async fn recovery_never_starts_or_stops_a_sandbox() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let effects = f.effects();
    let operation_id = RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::now_v7());
    assert_eq!(
        effects
            .cold_boot(operation_id, StartRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        effects
            .stop(&f.sandbox_id, SandboxActivationAttemptId::new())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(f.row().state, SandboxState::Stopped);
}

/// Suspend, then stop: the stop drops the pin but leaves the checkpoint, which
/// a warm start would load over the disk the upgrade's boot writes.
fn stop_after_suspend(f: &Fixture) {
    f.clear_pin();
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();
}

#[tokio::test]
async fn a_stopped_sandbox_that_kept_its_checkpoint_has_it_retired() {
    // Catalogued, as suspend publishes it, and a legacy uncatalogued one.
    for catalogued in [true, false] {
        let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
        let artifact = catalogued.then(|| f.adopt_checkpoint());
        stop_after_suspend(&f);
        let disk = f.effects().inspect_local_disk(&f.row()).await.unwrap();
        assert!(
            disk.checkpoint_dir_identity.is_some(),
            "catalogued {catalogued}"
        );
        assert_eq!(disk.checkpoint_execution, None);
        let record = f.record(disk.checkpoint_dir_identity.as_deref());
        assert_eq!(record.source_state, SandboxState::Stopped);

        f.effects().retire_checkpoint(&record).await.unwrap();

        assert!(!f.checkpoint().exists(), "catalogued {catalogued}");
        match artifact {
            Some(artifact) => f.reclaimed(&f.staged(artifact)).await,
            None => assert_eq!(f.lane_garbage().len(), 1),
        }
    }
}

#[tokio::test]
async fn retirement_moves_the_pinned_checkpoint_to_gc_staging_and_drops_its_row() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let artifact = f.adopt_checkpoint();
    let record = f.record(None);
    assert_eq!(record.checkpoint_artifact_id, Some(artifact));

    // While the sandbox still pins it, the checkpoint stays.
    let error = f.effects().retire_checkpoint(&record).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert!(f.checkpoint().join("vmstate").exists());

    f.clear_pin();
    f.effects().retire_checkpoint(&record).await.unwrap();

    assert!(!f.checkpoint().exists());
    assert!(
        crate::db::read::local_artifact(
            &f.service.db.lock().unwrap(),
            crate::db::read::LocalArtifactKey::Id(artifact)
        )
        .unwrap()
        .is_none()
    );
    f.reclaimed(&f.staged(artifact)).await;
    // Retiring again finds nothing left to retire.
    f.effects().retire_checkpoint(&record).await.unwrap();
}

#[tokio::test]
async fn retirement_never_takes_a_newer_checkpoint() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let old = f.adopt_checkpoint();
    let record = f.record(None);
    // The upgraded sandbox suspended again before the retirement ran: its
    // publication replaces the old row with a new artifact at the same path.
    let newer = LocalArtifactId::new();
    {
        let db = f.service.db.lock().unwrap();
        db.execute(
            "UPDATE sandboxes SET state = 'suspending' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();
        crate::db::pin_suspended_artifact_for_cleanup(
            &db,
            &f.sandbox_id,
            newer,
            &f.checkpoint().to_string_lossy(),
        )
        .unwrap();
    }
    assert_ne!(newer, old);

    f.effects().retire_checkpoint(&record).await.unwrap();

    assert!(f.checkpoint().join("vmstate").exists());
    assert!(!f.staged(old).exists());
    assert!(
        crate::db::read::local_artifact(
            &f.service.db.lock().unwrap(),
            crate::db::read::LocalArtifactKey::Id(newer)
        )
        .unwrap()
        .is_some()
    );
}

#[tokio::test]
async fn retirement_refuses_bytes_another_row_also_claims() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    f.adopt_checkpoint();
    let record = f.record(None);
    f.clear_pin();
    // Adoption records a second row at the same path without removing the
    // first: the bytes are no longer identifiably the old checkpoint's.
    f.adopt_checkpoint();
    f.clear_pin();

    let error = f.effects().retire_checkpoint(&record).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(f.checkpoint().join("vmstate").exists());
}

#[tokio::test]
async fn retirement_of_an_uncatalogued_checkpoint_matches_its_directory() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let identity = f
        .effects()
        .inspect_local_disk(&f.row())
        .await
        .unwrap()
        .checkpoint_dir_identity;
    let record = f.record(identity.as_deref());
    assert_eq!(record.checkpoint_artifact_id, None);

    // A different directory now holds the path: leave it.
    let replaced = f.cache.join(&f.sandbox_id).join("snapshot.replaced");
    std::fs::rename(f.checkpoint(), &replaced).unwrap();
    std::fs::create_dir(f.checkpoint()).unwrap();
    f.effects().retire_checkpoint(&record).await.unwrap();
    assert!(f.checkpoint().exists());
    assert!(f.lane_garbage().is_empty());

    // The directory the upgrade saw is retired through the suspend-garbage
    // lane, whose sweep owns whatever the immediate reclaim leaves.
    std::fs::remove_dir(f.checkpoint()).unwrap();
    std::fs::rename(&replaced, f.checkpoint()).unwrap();
    f.effects().retire_checkpoint(&record).await.unwrap();
    assert!(!f.checkpoint().exists());
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !f.lane_garbage().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_checkpoint_catalogued_after_the_fence_is_retired_through_its_row() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let identity = f
        .effects()
        .inspect_local_disk(&f.row())
        .await
        .unwrap()
        .checkpoint_dir_identity;
    let record = f.record(identity.as_deref());
    assert_eq!(record.checkpoint_artifact_id, None);
    // The directory the upgrade saw was catalogued after the fence.
    let artifact = f.adopt_checkpoint();
    f.clear_pin();

    f.effects().retire_checkpoint(&record).await.unwrap();

    assert!(!f.checkpoint().exists());
    assert!(f.lane_garbage().is_empty());
    assert!(
        crate::db::read::local_artifact(
            &f.service.db.lock().unwrap(),
            crate::db::read::LocalArtifactKey::Id(artifact)
        )
        .unwrap()
        .is_none()
    );
    f.reclaimed(&f.staged(artifact)).await;
}

#[tokio::test]
async fn an_uncatalogued_checkpoint_two_rows_now_claim_is_refused() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let identity = f
        .effects()
        .inspect_local_disk(&f.row())
        .await
        .unwrap()
        .checkpoint_dir_identity;
    let record = f.record(identity.as_deref());
    f.adopt_checkpoint();
    f.clear_pin();
    f.adopt_checkpoint();
    f.clear_pin();

    let error = f.effects().retire_checkpoint(&record).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(f.checkpoint().join("vmstate").exists());
}

#[tokio::test(start_paused = true)]
async fn retirement_gives_up_on_busy_artifacts_rather_than_waiting() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    f.adopt_checkpoint();
    let record = f.record(None);
    f.clear_pin();
    let _busy = f
        .service
        .auto_lifecycle
        .artifact_lock(&f.sandbox_id)
        .lock_owned()
        .await;

    let error = f.effects().retire_checkpoint(&record).await.unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert!(f.checkpoint().join("vmstate").exists());
}

#[tokio::test]
async fn a_stopped_sandbox_has_no_checkpoint_to_retire() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let record = f.record(None);
    f.effects().retire_checkpoint(&record).await.unwrap();
    assert!(f.lane_garbage().is_empty());
}

fn set_execution(f: &Fixture, execution: SandboxActivationAttemptId) {
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET node_execution_id = ?2 WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id, execution.to_string()],
        )
        .unwrap();
}

/// The race the stop guard closes: the upgrade sees its boot's execution
/// running and decides to stop it, then the user stops the sandbox and starts
/// it again before the stop is admitted. The stop must leave the user's new
/// execution alone.
#[tokio::test]
async fn the_upgrade_never_stops_an_execution_started_after_its_check() {
    let f = fixture(SandboxState::Started, false, FakeNodeManager::default());
    let boot = SandboxActivationAttemptId::new();
    set_execution(&f, boot);
    let effects = NodeRuntimeUpgradeEffects::for_caller(
        f.service.clone(),
        tonic::metadata::MetadataMap::new(),
        tonic::Extensions::new(),
    );
    // The orchestrator's check passed against the boot's execution; then the
    // user's stop and start replace it.
    let replacement = SandboxActivationAttemptId::new();
    set_execution(&f, replacement);

    effects.stop(&f.sandbox_id, boot).await.unwrap();

    let row = f.row();
    assert_eq!(row.state, SandboxState::Started);
    assert_eq!(row.node_execution_id, Some(replacement));
    assert!(
        !crate::db::read::sandbox_view(&f.service.db.lock().unwrap(), &f.sandbox_id)
            .cleanup_episode()
            .present()
            .unwrap()
    );
}

/// The guard admits the stop of the boot's own execution, running or paused,
/// and nothing else.
#[tokio::test]
async fn the_stop_is_admitted_only_for_the_boots_execution() {
    for state in [SandboxState::Started, SandboxState::Paused] {
        let f = fixture(state, false, FakeNodeManager::default());
        let boot = SandboxActivationAttemptId::new();
        set_execution(&f, boot);
        let sandbox_id: pplx_space_common_core::types::SandboxId = f.sandbox_id.parse().unwrap();

        let refused = f
            .service
            .admit_stop_for_activation(sandbox_id, state, SandboxActivationAttemptId::new())
            .await
            .err()
            .expect("another execution's stop must be refused")
            .into_status();
        assert_eq!(refused.code(), tonic::Code::Aborted, "{state}: {refused}");
        assert_eq!(f.row().state, state);

        let admitted = f
            .service
            .admit_stop_for_activation(sandbox_id, state, boot)
            .await;
        assert!(admitted.is_ok(), "{state}");
        assert_eq!(f.row().state, SandboxState::Stopping, "{state}");
    }
}

/// The user suspended the boot's execution between the upgrade's check and
/// its stop: the execution is the same, but it is at rest, so the stop leaves
/// it alone.
#[tokio::test]
async fn the_stop_leaves_the_boots_execution_alone_once_it_is_at_rest() {
    for state in [SandboxState::Suspended, SandboxState::Stopped] {
        let f = fixture(state, false, FakeNodeManager::default());
        let boot = SandboxActivationAttemptId::new();
        set_execution(&f, boot);
        let sandbox_id: pplx_space_common_core::types::SandboxId = f.sandbox_id.parse().unwrap();

        let refused = f
            .service
            .admit_stop_for_activation(sandbox_id, state, boot)
            .await
            .err()
            .expect("an execution at rest must not be stopped")
            .into_status();
        assert_eq!(refused.code(), tonic::Code::Aborted, "{state}: {refused}");

        assert!(
            !f.service
                .stop_activation(&f.sandbox_id, boot)
                .await
                .unwrap(),
            "{state}"
        );
        assert_eq!(f.row().state, state);
    }
}

/// The execution check runs only in a persisted cleanup claim, so a stop
/// whose claim can't open its transaction refuses rather than fall back to
/// unpersisted cleanup.
#[tokio::test]
async fn a_stop_that_cant_open_its_claim_is_refused() {
    let f = fixture(SandboxState::Started, false, FakeNodeManager::default());
    let boot = SandboxActivationAttemptId::new();
    set_execution(&f, boot);
    let sandbox_id: pplx_space_common_core::types::SandboxId = f.sandbox_id.parse().unwrap();
    // A transaction left open makes the claim's own BEGIN fail.
    f.service.db.lock().unwrap().execute_batch("BEGIN").unwrap();

    let refused = f
        .service
        .admit_stop_for_activation(sandbox_id, SandboxState::Started, boot)
        .await
        .err()
        .expect("a stop that can't check its execution must be refused")
        .into_status();

    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused}");
    f.service
        .db
        .lock()
        .unwrap()
        .execute_batch("ROLLBACK")
        .unwrap();
    assert_eq!(f.row().state, SandboxState::Started);
}

/// A Wake that carries `upgrade`, shaped as the control plane sends one.
fn upgrade_wake(
    sandbox_id: &str,
    upgrade: Option<crate::proto::WakeRuntimeUpgrade>,
) -> crate::proto::WakeRequest {
    crate::proto::WakeRequest {
        sandbox_id: sandbox_id.to_string(),
        blocking: true,
        start_context: Some(crate::proto::WakeStartContext {
            route_identity: Some(crate::proto::SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id.to_string()),
                route_generation: Some(1),
                current_node_id: Some(uuid::Uuid::now_v7().to_string()),
                substrate_kind: crate::proto::SandboxSubstrateKind::Direct as i32,
                ..Default::default()
            }),
            ..Default::default()
        }),
        runtime_upgrade: upgrade,
        ..Default::default()
    }
}

/// The environment the compiled catalog certifies an upgrade edge to from
/// `source`.
fn certified_target(source: &ExecutionMetadata) -> ExecutionMetadata {
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let compiled =
        CompiledEnvironment::for_architecture(pplx_space_common_core::types::Arch::X86_64).unwrap();
    let target = CompiledEnvironment::of_execution(&compiled, source)
        .unwrap()
        .upgrade_targets()[0];
    compiled
        .iter()
        .find(|environment| environment.definition().reference() == target)
        .unwrap()
        .definition()
        .execution_requirements()
        .unwrap()
}

fn wake_upgrade_to(
    target: &ExecutionMetadata,
    operation_id: &str,
) -> crate::proto::WakeRuntimeUpgrade {
    crate::proto::WakeRuntimeUpgrade {
        operation_id: operation_id.to_string(),
        target_environment: target.environment().unwrap().to_string(),
        authorization: Some(crate::proto::RuntimeUpgradeAuthorization {
            authorization_id: "environment-block".into(),
            reason: "the source environment is blocked".into(),
            memory_loss: true,
        }),
        checkpoint_local_artifact_id: None,
    }
}

#[tokio::test]
async fn a_wake_carrying_an_upgrade_runs_the_upgrade_instead_of_a_start() {
    // The fake answers the upgrade's disk copy but not its target
    // resolution, so the upgrade fences, preserves the disk under the Wake's
    // operation id and rolls back before booting. That shows the Wake ran
    // the upgrade rather than a plain start, and left the old pair.
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (deleted, created) = (calls.clone(), calls.clone());
    let node = FakeNodeManager::default()
        .unary(
            "DeleteBtrfsSnapshot",
            move |request: DeleteBtrfsSnapshotRequest| {
                recording(&deleted, format!("delete {}", request.snapshot_name));
                Err::<DeleteBtrfsSnapshotResponse, _>(Status::not_found("absent"))
            },
        )
        .unary(
            "CreateBtrfsSnapshot",
            move |request: CreateBtrfsSnapshotRequest| {
                recording(&created, format!("create {}", request.snapshot_name));
                Ok(CreateBtrfsSnapshotResponse {})
            },
        );
    let f = fixture(SandboxState::Stopped, false, node);
    let target = certified_target(&f.source);
    let operation_id = uuid::Uuid::now_v7().to_string();
    let preserved = format!("d-operation-runtime-upgrade-{operation_id}");

    let error = f
        .service
        .wake(crate::service::activation_support::internal_request(
            upgrade_wake(&f.sandbox_id, Some(wake_upgrade_to(&target, &operation_id))),
        ))
        .await
        .expect_err("the harness can't boot a sandbox");

    assert!(error.message().contains("rolled back"), "{error}");
    assert_eq!(
        *calls.lock().unwrap(),
        [
            format!("delete {preserved}"),
            format!("create {preserved}"),
            format!("delete {preserved}"),
        ],
        "{error}"
    );
    // The failed boot left the old pair: stopped, unfenced and bound to the
    // environment it ran under.
    let row = f.row();
    assert_eq!(row.state, SandboxState::Stopped, "{error}");
    assert_eq!(
        row.execution_metadata
            .as_ref()
            .and_then(ExecutionMetadata::environment),
        f.source.environment()
    );
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn a_wake_upgrade_of_a_sandbox_already_on_its_target_wakes_it_as_usual() {
    // Nothing answers the upgrade's node work, so reaching any of it fails
    // the test; a sandbox already on the target goes straight to its start.
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    crate::db::upsert_sandbox_execution_metadata(
        &f.service.db.lock().unwrap(),
        &f.sandbox_id,
        &f.target,
    )
    .unwrap();
    let plain = f
        .service
        .wake(crate::service::activation_support::internal_request(
            upgrade_wake(&f.sandbox_id, None),
        ))
        .await
        .expect_err("the harness can't boot a sandbox");
    let upgraded = f
        .service
        .wake(crate::service::activation_support::internal_request(
            upgrade_wake(
                &f.sandbox_id,
                Some(wake_upgrade_to(
                    &f.target,
                    &uuid::Uuid::now_v7().to_string(),
                )),
            ),
        ))
        .await
        .expect_err("the harness can't boot a sandbox");
    assert_eq!(upgraded.code(), plain.code(), "{upgraded} / {plain}");
    assert_eq!(upgraded.message(), plain.message());
    assert!(
        !f._node
            .calls()
            .iter()
            .any(|call| call.contains("BtrfsSnapshot") || call.contains("InstanceSnapshot")),
        "{:?}",
        f._node.calls()
    );
}

#[tokio::test]
async fn a_wake_upgrade_must_be_an_internal_blocking_started_wake() {
    let f = fixture(SandboxState::Stopped, false, FakeNodeManager::default());
    let upgrade = || {
        Some(wake_upgrade_to(
            &f.target,
            &uuid::Uuid::now_v7().to_string(),
        ))
    };
    let mut without_context = upgrade_wake(&f.sandbox_id, upgrade());
    without_context.start_context = None;
    let mut nonblocking = upgrade_wake(&f.sandbox_id, upgrade());
    nonblocking.blocking = false;
    let mut paused = upgrade_wake(&f.sandbox_id, upgrade());
    paused.target_state = crate::proto::WakeTargetState::Paused as i32;
    let mut stopped = upgrade_wake(&f.sandbox_id, upgrade());
    stopped.target_state = crate::proto::WakeTargetState::Stopped as i32;
    let mut unparsable = upgrade_wake(&f.sandbox_id, upgrade());
    unparsable
        .runtime_upgrade
        .as_mut()
        .unwrap()
        .target_environment = "not-a-reference".into();
    let mut unauthorized = upgrade_wake(&f.sandbox_id, upgrade());
    unauthorized.runtime_upgrade.as_mut().unwrap().authorization = None;
    for (case, request) in [
        ("without start_context", without_context),
        ("nonblocking", nonblocking),
        ("paused", paused),
        ("stopped", stopped),
        ("unparsable target", unparsable),
        ("no authorization", unauthorized),
    ] {
        let error = f
            .service
            .wake(crate::service::activation_support::internal_request(
                request,
            ))
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{case}: {error}"
        );
    }
    assert_eq!(f.row().state, SandboxState::Stopped);
    assert!(f._node.calls().is_empty(), "{:?}", f._node.calls());
}

#[tokio::test]
async fn staging_for_an_upgrade_leaves_a_present_sandbox_alone() {
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    f.service
        .stage_missing_checkpoint(crate::proto::StartRequest {
            sandbox_id: f.sandbox_id.clone(),
            checkpoint: Some("suspend".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(f.row().state, SandboxState::Suspended);
    assert!(f._node.calls().is_empty(), "{:?}", f._node.calls());
}

#[test]
fn every_caller_allowed_a_wake_start_context_may_also_upgrade() {
    // A Wake's upgrade runs as its caller, so the Wake checks the upgrade's
    // authority too; today it never refuses a caller the start context
    // admitted, and this pins that the two lists stay in step.
    for principal in crate::service::ALLOW_SANDBOX_START_CONTEXT {
        assert!(
            crate::service::ALLOW_SANDBOX_RUNTIME_UPGRADE
                .iter()
                .any(|allowed| principal.satisfies(*allowed)),
            "{principal:?}"
        );
    }
}

/// A suspended Wake upgrade naming another node as the checkpoint's source
/// and a backup to stage, as the control plane sends for a pod placed away
/// from the checkpoint.
fn remote_upgrade_wake(
    f: &Fixture,
    checkpoint: Option<LocalArtifactId>,
) -> crate::proto::WakeRequest {
    let target = certified_target(&f.source);
    let mut upgrade = wake_upgrade_to(&target, &uuid::Uuid::now_v7().to_string());
    upgrade.checkpoint_local_artifact_id = checkpoint.map(|id| id.to_string());
    let mut request = upgrade_wake(&f.sandbox_id, Some(upgrade));
    let context = request.start_context.as_mut().unwrap();
    context.suspend_restore_source_node = Some("another-node".into());
    context.suspend_restore_storage_uri = Some("s3://bucket/newer-checkpoint".into());
    request
}

#[tokio::test]
async fn a_wake_upgrade_never_boots_a_checkpoint_other_than_the_one_decided_on() {
    // A stale copy: the row pins another artifact than the checkpoint the
    // upgrade was decided from. An upgrade naming no checkpoint has nothing
    // to check the copy against, whatever placement says.
    // (named checkpoint, whether the row pins a checkpoint of its own)
    for (decided, pinned) in [
        (Some(LocalArtifactId::new()), true),
        (None, true),
        (None, false),
    ] {
        let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
        if pinned {
            f.adopt_checkpoint();
        }
        let error = f
            .service
            .wake(crate::service::activation_support::internal_request(
                remote_upgrade_wake(&f, decided),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("decided from"), "{error}");
        assert_eq!(f.row().state, SandboxState::Suspended);
        assert!(f._node.calls().is_empty(), "{:?}", f._node.calls());
        assert_eq!(
            crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
                .unwrap(),
            None
        );
    }
}

#[tokio::test]
async fn a_wake_upgrade_boots_the_checkpoint_it_was_decided_from_wherever_it_was_staged() {
    // The row pins the decided checkpoint, as staging it here leaves it, so
    // the upgrade proceeds although the restore names another source node.
    let f = fixture(SandboxState::Suspended, false, FakeNodeManager::default());
    let checkpoint = f.adopt_checkpoint();
    let error = f
        .service
        .wake(crate::service::activation_support::internal_request(
            remote_upgrade_wake(&f, Some(checkpoint)),
        ))
        .await
        .unwrap_err();
    // It reached the upgrade's node work, which this fake doesn't answer.
    assert!(
        f._node
            .calls()
            .iter()
            .any(|call| call.contains("BtrfsSnapshot")),
        "{error}: {:?}",
        f._node.calls()
    );
}
