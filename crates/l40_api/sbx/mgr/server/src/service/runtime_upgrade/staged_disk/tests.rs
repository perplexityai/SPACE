//! Finishing a staged checkpoint's disk, against a test-served node-manager
//! and a temporary cache root. Each case seeds what staging leaves: a
//! suspended row pinning its checkpoint, the checkpoint under `snapshot/`, no
//! `metadata.json`, and for an encrypted sandbox no `instance/`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, SandboxActivationAttemptId,
    SandboxArtifactMetadata,
};
use pplx_space_node_mgr_client::proto::{
    BtrfsCreateSandboxInstanceRequest, BtrfsCreateSandboxInstanceResponse,
    CreateBtrfsSnapshotRequest, CreateBtrfsSnapshotResponse, DeleteBtrfsSnapshotRequest,
    DeleteBtrfsSnapshotResponse, PrepareSandboxArtifactDirRequest,
    PrepareSandboxArtifactDirResponse,
};
use tonic::Status;

use super::super::node_effects::tests::{Fixture, fixture, remote_upgrade_wake, set_execution};
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::state::SandboxState;

/// The template digest the staged row and its checkpoint record.
const DIGEST: &str = "sha256:5e0f1d2c3b4a59687766554433221100ffeeddccbbaa99887766554433221100";

/// The guest's writes, as the checkpoint's sealed COW holds them.
const GUEST_WRITES: &[u8] = b"sealed guest writes";

/// Where the fixture's sandbox lives, for a node-manager handler that makes
/// what node-manager would.
type SandboxDir = Arc<Mutex<Option<PathBuf>>>;

/// A node-manager that creates `instance/` as a subvolume (or answers that it
/// found a plain directory, when `subvolume` is false), repairs a retained
/// instance's traversal, keeps the preserved copy, and answers nothing else.
fn node(sandbox_dir: &SandboxDir, subvolume: bool) -> FakeNodeManager {
    let created = Arc::clone(sandbox_dir);
    let repaired = Arc::clone(sandbox_dir);
    FakeNodeManager::default()
        .unary(
            "PrepareSandboxArtifactDir",
            move |_: PrepareSandboxArtifactDirRequest| {
                let dir = repaired.lock().unwrap().clone().unwrap();
                set_mode(&dir.join("instance"), 0o755);
                Ok(PrepareSandboxArtifactDirResponse::default())
            },
        )
        .unary(
            "BtrfsCreateSandboxInstance",
            move |request: BtrfsCreateSandboxInstanceRequest| {
                assert!(request.target.is_some(), "the target must be sent");
                let dir = created.lock().unwrap().clone().unwrap();
                std::fs::create_dir_all(dir.join("instance")).unwrap();
                Ok(BtrfsCreateSandboxInstanceResponse { subvolume })
            },
        )
        .unary("DeleteBtrfsSnapshot", |_: DeleteBtrfsSnapshotRequest| {
            Err::<DeleteBtrfsSnapshotResponse, _>(Status::not_found("absent"))
        })
        .unary(
            "CreateBtrfsSnapshot",
            |request: CreateBtrfsSnapshotRequest| {
                assert!(!request.writable);
                Ok(CreateBtrfsSnapshotResponse {})
            },
        )
}

fn set_mode(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// The sidecar of a suspend checkpoint of `f`'s sandbox, naming `artifact`.
fn checkpoint_sidecar(
    f: &Fixture,
    encrypted: bool,
    artifact: Option<LocalArtifactId>,
) -> crate::snapshot_upload::SnapshotMeta {
    crate::snapshot_upload::SnapshotMeta {
        version: 1,
        execution: None,
        sandbox_id: f.sandbox_id.clone(),
        size_bytes: 900,
        rootfs_size_bytes: None,
        created_at: "2026-11-09T00:00:00Z".to_string(),
        source: crate::snapshot_upload::SnapshotSource {
            runtime: "fc".to_string(),
            runtime_version: None,
            state_format_version: None,
            template_id: "seed".to_string(),
            content_hash: Some(DIGEST.to_string()),
            rootfs_size_mib: None,
            rootfs_version: None,
            kernel_version: None,
            kernel_digest: None,
            disk_io_max_operations_per_sec: None,
            disk_io_max_bandwidth_mib_per_sec: None,
        },
        sandbox: None,
        rootfs_diff_format: None,
        rootfs_send_source_id: None,
        encrypted,
        rootfs_encryption_source: None,
        local_artifact_id: artifact,
        artifact_kind: Some(ArtifactKind::Full),
        artifact_origin: Some(ArtifactOrigin::Suspend),
        artifact_snapshot_name: None,
    }
}

/// A suspended sandbox whose checkpoint staging just laid down here, pinned
/// and on a node execution, as staging leaves it.
fn staged(encrypted: bool, subvolume: bool) -> (Fixture, LocalArtifactId) {
    let sandbox_dir = SandboxDir::default();
    let f = fixture(
        SandboxState::Suspended,
        encrypted,
        node(&sandbox_dir, subvolume),
    );
    *sandbox_dir.lock().unwrap() = Some(f.dir());
    set_execution(&f, SandboxActivationAttemptId::new());
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET template_hash = ?2 WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id, DIGEST],
        )
        .unwrap();
    let checkpoint = f.adopt_checkpoint();
    std::fs::remove_file(f.dir().join("metadata.json")).unwrap();
    f.write_sidecar(checkpoint_sidecar(&f, encrypted, Some(checkpoint)));
    if encrypted {
        std::fs::remove_dir_all(f.dir().join("instance")).unwrap();
        std::fs::write(f.checkpoint().join("cow.luks"), GUEST_WRITES).unwrap();
    }
    (f, checkpoint)
}

impl Fixture {
    fn laid_cow(&self) -> PathBuf {
        self.dir().join("instance/cow.luks")
    }

    fn write_sidecar(&self, meta: crate::snapshot_upload::SnapshotMeta) {
        std::fs::write(
            self.checkpoint().join("snapshot.json"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
    }

    async fn complete(&self) -> Result<(), Status> {
        self.service.complete_staged_upgrade_disk(&self.row()).await
    }

    fn recorded_execution(&self) -> Option<pplx_space_execution_spec::ExecutionMetadata> {
        let bytes = std::fs::read(self.dir().join("metadata.json")).ok()?;
        Some(
            serde_json::from_slice::<SandboxArtifactMetadata>(&bytes)
                .unwrap()
                .execution,
        )
    }
}

/// An encrypted checkpoint staged here has its sealed COW laid into a fresh
/// instance subvolume, byte for byte, and the disk's execution recorded as
/// the checkpoint's producer. The rootfs is left to the cold start's
/// template clone.
#[tokio::test]
async fn an_encrypted_checkpoint_staged_here_gets_its_cow_laid_into_its_instance() {
    let (f, _) = staged(true, true);

    f.complete().await.unwrap();

    assert_eq!(std::fs::read(f.laid_cow()).unwrap(), GUEST_WRITES);
    assert_eq!(
        std::fs::read(f.checkpoint().join("cow.luks")).unwrap(),
        GUEST_WRITES,
        "the checkpoint keeps its own COW"
    );
    assert!(!f.dir().join("instance/rootfs.ext4").exists());
    assert_eq!(f.recorded_execution(), Some(f.source.clone()));
    assert_eq!(f._node.calls(), ["BtrfsCreateSandboxInstance"]);

    // A retry finds the disk finished and asks node-manager nothing.
    f.complete().await.unwrap();
    assert_eq!(f._node.calls(), ["BtrfsCreateSandboxInstance"]);
}

/// An unencrypted checkpoint staged here already has its rootfs in the
/// instance; only the disk's record of its execution is written.
#[tokio::test]
async fn an_unencrypted_checkpoint_staged_here_gets_its_disk_execution_recorded() {
    let (f, _) = staged(false, true);
    f.complete().await.unwrap();
    assert_eq!(f.recorded_execution(), Some(f.source.clone()));
    assert!(f._node.calls().is_empty(), "{:?}", f._node.calls());

    // Its rootfs is its whole disk, so no template digest is needed, unlike
    // an encrypted COW's origin.
    let (f, checkpoint) = staged(false, true);
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET template_hash = NULL WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();
    let mut meta = checkpoint_sidecar(&f, false, Some(checkpoint));
    meta.source.content_hash = None;
    f.write_sidecar(meta);
    f.complete().await.unwrap();
    assert_eq!(f.recorded_execution(), Some(f.source.clone()));
}

/// A copy suspended on this node keeps its own instance COW and record, even
/// beside its checkpoint's sealed copy.
#[tokio::test]
async fn a_copy_suspended_here_is_left_alone() {
    let sandbox_dir = SandboxDir::default();
    let f = fixture(SandboxState::Suspended, true, node(&sandbox_dir, true));
    set_execution(&f, SandboxActivationAttemptId::new());
    f.write_sidecar(checkpoint_sidecar(&f, true, None));
    std::fs::write(f.checkpoint().join("cow.luks"), GUEST_WRITES).unwrap();
    let (cow, metadata) = (
        std::fs::read(f.laid_cow()).unwrap(),
        std::fs::read(f.dir().join("metadata.json")).unwrap(),
    );

    f.complete().await.unwrap();

    assert_eq!(std::fs::read(f.laid_cow()).unwrap(), cow);
    assert_eq!(
        std::fs::read(f.dir().join("metadata.json")).unwrap(),
        metadata
    );
    assert!(f._node.calls().is_empty(), "{:?}", f._node.calls());
}

/// A disk is finished only from the encrypted suspend checkpoint the row
/// pins. A disk artifact forked from another sandbox, a sidecar that says the
/// checkpoint isn't encrypted, one naming another sandbox, one naming
/// another artifact than the pin, and one written over another template than
/// the row's, whose origin the cold start would clone, are refused, with
/// nothing laid, no record written and node-manager unasked.
#[tokio::test]
async fn only_the_encrypted_suspend_checkpoint_the_row_pins_is_laid() {
    let changes: [fn(&mut crate::snapshot_upload::SnapshotMeta); 5] = [
        |meta| meta.artifact_kind = Some(ArtifactKind::Disk),
        |meta| meta.encrypted = false,
        |meta| meta.sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string(),
        |meta| meta.local_artifact_id = Some(LocalArtifactId::new()),
        |meta| {
            meta.source.content_hash = Some(
                "sha256:0000000000000000000000000000000000000000000000000000000000000001".into(),
            )
        },
    ];
    for change in changes {
        let (f, checkpoint) = staged(true, true);
        let mut meta = checkpoint_sidecar(&f, true, Some(checkpoint));
        change(&mut meta);
        f.write_sidecar(meta);

        let error = f.complete().await.unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("checkpoint it pins"), "{error}");
        assert!(!f.laid_cow().exists());
        assert_eq!(
            f.recorded_execution(),
            None,
            "the disk's record came from it"
        );
        assert!(f._node.calls().is_empty(), "{:?}", f._node.calls());
    }
}

fn names(dir: &std::path::Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}

fn publish(source: &std::path::Path, directory: &std::path::Path) -> Option<bool> {
    super::publish_new_copy(
        &std::fs::File::open(source).unwrap(),
        &std::fs::File::open(directory).unwrap(),
        "cow.luks",
    )
    .unwrap()
}

/// A copy that outlived its request publishes nothing over a COW another
/// attempt laid first, which a running guest may already have open, and
/// leaves no copy of its own behind. A temporary an interrupted process left
/// goes with the next publish.
#[test]
fn a_late_copy_never_replaces_a_laid_cow() {
    let source = tempfile::tempdir().unwrap();
    let staged = source.path().join("cow.luks");
    std::fs::write(&staged, b"late").unwrap();
    let instance = tempfile::tempdir().unwrap();
    let laid = instance.path().join("cow.luks");
    std::fs::write(&laid, GUEST_WRITES).unwrap();

    assert_eq!(publish(&staged, instance.path()), None);
    assert_eq!(std::fs::read(&laid).unwrap(), GUEST_WRITES);
    assert_eq!(names(instance.path()), ["cow.luks"]);

    std::fs::remove_file(&laid).unwrap();
    std::fs::write(instance.path().join(".cow.luks.interrupted"), b"partial").unwrap();
    assert!(publish(&staged, instance.path()).is_some());
    assert_eq!(std::fs::read(&laid).unwrap(), b"late");
    assert_eq!(names(instance.path()), ["cow.luks"]);
}

/// A copy pinned to one instance publishes into it even after another
/// instance takes its path, as an eviction and a new stage would leave it:
/// the new instance never gets the old checkpoint's COW.
#[test]
fn a_copy_publishes_only_into_the_instance_it_pinned() {
    let source = tempfile::tempdir().unwrap();
    let staged = source.path().join("cow.luks");
    std::fs::write(&staged, b"old checkpoint").unwrap();
    let sandbox = tempfile::tempdir().unwrap();
    let instance = sandbox.path().join("instance");
    std::fs::create_dir(&instance).unwrap();
    let pinned = std::fs::File::open(&instance).unwrap();
    std::fs::rename(&instance, sandbox.path().join("evicted")).unwrap();
    std::fs::create_dir(&instance).unwrap();

    let published =
        super::publish_new_copy(&std::fs::File::open(&staged).unwrap(), &pinned, "cow.luks")
            .unwrap();

    assert!(published.is_some());
    assert!(names(&instance).is_empty());
    assert_eq!(
        std::fs::read(sandbox.path().join("evicted/cow.luks")).unwrap(),
        b"old checkpoint"
    );
}

/// A retained instance an older node-manager left private to the VM owner is
/// repaired before its COW is looked for, instead of refusing every retry.
#[tokio::test]
async fn a_private_retained_instance_is_repaired_before_its_cow_is_looked_for() {
    let sandbox_dir = SandboxDir::default();
    let f = fixture(SandboxState::Suspended, true, node(&sandbox_dir, true));
    *sandbox_dir.lock().unwrap() = Some(f.dir());
    f.write_sidecar(checkpoint_sidecar(&f, true, None));
    std::fs::write(f.checkpoint().join("cow.luks"), GUEST_WRITES).unwrap();
    let instance = f.dir().join("instance");
    set_mode(&instance, 0o000);
    if std::fs::read_dir(&instance).is_ok() {
        // Root reads through any mode, so nothing here is private to it.
        set_mode(&instance, 0o755);
        return;
    }

    let result = f.complete().await;

    set_mode(&instance, 0o755);
    result.unwrap();
    assert_eq!(f._node.calls(), ["PrepareSandboxArtifactDir"]);
}

/// An instance node-manager can't make a subvolume of couldn't be preserved
/// for rollback, so nothing is laid into it.
#[tokio::test]
async fn a_cow_is_laid_only_into_a_subvolume() {
    let (f, _) = staged(true, false);
    let error = f.complete().await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("subvolume"), "{error}");
    assert!(!f.laid_cow().exists());
}

/// A sandbox an upgrade fences, or one whose upgrade still owes its
/// checkpoint's retirement, is the upgrade's to settle: nothing is written.
#[tokio::test]
async fn a_sandbox_an_upgrade_owns_is_left_to_it() {
    let (f, _) = staged(true, true);
    crate::db::test_fixtures::fence_runtime_upgrade(
        &f.service.db.lock().unwrap(),
        &f.sandbox_id,
        f.target.environment().unwrap(),
        crate::db::RuntimeUpgradeTargetState::Started,
        None,
    );
    f.complete().await.unwrap();
    assert!(!f.laid_cow().exists());
    assert_eq!(f.recorded_execution(), None);
    assert!(f._node.calls().is_empty(), "{:?}", f._node.calls());
}

/// A sandbox that isn't suspended has no staged checkpoint to finish.
#[tokio::test]
async fn only_a_suspended_sandbox_is_finished() {
    let (f, _) = staged(true, true);
    f.service
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
            crate::db::fixture::params![f.sandbox_id],
        )
        .unwrap();
    f.complete().await.unwrap();
    assert!(!f.laid_cow().exists());
    assert_eq!(f.recorded_execution(), None);
}

/// A staged encrypted checkpoint, retried after staging adopted its row, is
/// upgraded from its own COW: the Wake's staging finishes the disk, and the
/// upgrade inspects it and keeps its copy instead of refusing for the
/// missing COW. The harness stops at the cold boot.
#[tokio::test]
async fn an_encrypted_checkpoint_staged_here_is_upgraded_from_its_cow() {
    let (f, checkpoint) = staged(true, true);

    let error = f
        .service
        .wake(crate::service::activation_support::internal_request(
            remote_upgrade_wake(&f, Some(checkpoint)),
        ))
        .await
        .unwrap_err();

    assert!(!error.message().contains("COW"), "{error}");
    let calls = f._node.calls();
    assert_eq!(
        calls.get(..3),
        Some(
            &[
                "BtrfsCreateSandboxInstance".to_owned(),
                "DeleteBtrfsSnapshot".to_owned(),
                "CreateBtrfsSnapshot".to_owned(),
            ][..]
        ),
        "{error}: {calls:?}"
    );
    assert_eq!(std::fs::read(f.laid_cow()).unwrap(), GUEST_WRITES);
}

/// A zero-byte COW, staged or already laid, is refused before the upgrade
/// fences the sandbox: the rollback of its preserved copy would refuse it
/// after. Nothing is laid or preserved, and the sandbox stays suspended and
/// unfenced.
#[tokio::test]
async fn an_empty_cow_is_refused_before_the_upgrade_fences() {
    for laid in [false, true] {
        let (f, checkpoint) = staged(true, true);
        if laid {
            std::fs::create_dir_all(f.dir().join("instance")).unwrap();
            std::fs::write(f.laid_cow(), b"").unwrap();
        } else {
            std::fs::write(f.checkpoint().join("cow.luks"), b"").unwrap();
        }

        let error = f
            .service
            .wake(crate::service::activation_support::internal_request(
                remote_upgrade_wake(&f, Some(checkpoint)),
            ))
            .await
            .unwrap_err();

        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{laid}: {error}"
        );
        assert!(error.message().contains("empty"), "{laid}: {error}");
        assert_eq!(f.row().state.unowned(), Some(SandboxState::Suspended));
        assert_eq!(
            crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
                .unwrap(),
            None,
            "{laid}"
        );
        let calls = f._node.calls();
        assert!(
            !calls
                .iter()
                .any(|call| call == "CreateBtrfsSnapshot" || call == "BtrfsCreateSandboxInstance"),
            "{laid}: {calls:?}"
        );
        assert_eq!(
            std::fs::read(f.laid_cow()).ok(),
            laid.then(Vec::new),
            "{laid}"
        );
    }
}
