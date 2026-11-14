//! Finishing a staged checkpoint's disk, against a test-served node-manager
//! of each release a mixed-version rollout can pair with this one and a
//! temporary cache root. Each case seeds what staging leaves: a suspended
//! row pinning its checkpoint, the checkpoint under `snapshot/`, no
//! `metadata.json`, and for an encrypted sandbox no `instance/`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use pplx_space_artifact_model::SandboxArtifactMetadata;
use pplx_space_node_mgr_client::proto::{
    CloneCapabilitiesRequest, CloneCapabilitiesResponse, CreateBtrfsSnapshotRequest,
    CreateBtrfsSnapshotResponse, DeleteBtrfsSnapshotRequest, DeleteBtrfsSnapshotResponse,
    MaterializeStagedCheckpointDiskRequest, MaterializeStagedCheckpointDiskResponse,
};
use pplx_space_sandbox_model::SandboxActivationAttemptId;
use pplx_space_snapshot_model::{ArtifactKind, ArtifactOrigin, LocalArtifactId};
use tonic::Status;

use super::super::node_effects::tests::{Fixture, fixture, remote_upgrade_wake, set_execution};
use super::StagedDiskPart;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::state::SandboxState;

/// The template digest the staged row and its checkpoint record.
const DIGEST: &str = "sha256:5e0f1d2c3b4a59687766554433221100ffeeddccbbaa99887766554433221100";

/// The guest's writes, as the checkpoint's sealed COW holds them.
const GUEST_WRITES: &[u8] = b"sealed guest writes";
/// What an instance holds after a boot that wrote to it and failed, leaving
/// the row suspended on the checkpoint it pins.
const EARLIER_BOOT: &[u8] = b"an earlier boot's writes";

/// Where the fixture's sandbox lives, for a node-manager handler that makes
/// what node-manager would.
type SandboxDir = Arc<Mutex<Option<PathBuf>>>;

/// The node-manager a sandbox-manager finds on the node.
#[derive(Clone, Copy, Debug, PartialEq)]
enum NodeRelease {
    /// Advertises and serves `MaterializeStagedCheckpointDisk` over a btrfs
    /// cache.
    Current,
    /// The same, over a cache that isn't btrfs.
    NoSubvolumes,
    /// Answers `GetCloneCapabilities` without the bit.
    WithoutTheBit,
    /// Predates `GetCloneCapabilities` itself.
    Older,
    /// Serves the RPC but answers that it kept the instance's COW, which
    /// isn't known to be the pinned checkpoint's.
    KeepsTheCow,
    /// Serves the RPC but answers that it prepared another checkpoint.
    PreparesAnother,
    /// Serves the RPC but answers that the checkpoint is over another
    /// template.
    PreparesAnotherTemplate,
}

/// What node-manager did with each `MaterializeStagedCheckpointDisk`.
type Requests = Arc<Mutex<Vec<MaterializeStagedCheckpointDiskRequest>>>;

/// A node-manager that finishes the disk as `staged_disk.rs` in node-manager
/// does, keeps the preserved copy, and answers nothing else.
fn node(sandbox_dir: &SandboxDir, release: NodeRelease, requests: &Requests) -> FakeNodeManager {
    let dir = Arc::clone(sandbox_dir);
    let requests = Arc::clone(requests);
    let mut node = FakeNodeManager::default()
        .unary("DeleteBtrfsSnapshot", |_: DeleteBtrfsSnapshotRequest| {
            Err::<DeleteBtrfsSnapshotResponse, _>(Status::not_found("absent"))
        })
        .unary(
            "CreateBtrfsSnapshot",
            |request: CreateBtrfsSnapshotRequest| {
                assert!(!request.writable);
                Ok(CreateBtrfsSnapshotResponse {})
            },
        );
    if release == NodeRelease::Older {
        return node;
    }
    node = node.unary(
        "GetCloneCapabilities",
        move |_: CloneCapabilitiesRequest| {
            Ok(CloneCapabilitiesResponse {
                prepared_clone_disposal: true,
                staged_checkpoint_disk: release != NodeRelease::WithoutTheBit,
            })
        },
    );
    if release == NodeRelease::WithoutTheBit {
        return node;
    }
    node.unary(
        "MaterializeStagedCheckpointDisk",
        move |request: MaterializeStagedCheckpointDiskRequest| {
            assert!(request.target.is_some(), "the target must be sent");
            let dir = dir.lock().unwrap().clone().unwrap();
            requests.lock().unwrap().push(request.clone());
            let non_empty = |path: PathBuf| std::fs::metadata(path).map(|m| m.len() > 0);
            let mut response = MaterializeStagedCheckpointDiskResponse {
                metadata: StagedDiskPart::NotRequested.into(),
                cow: StagedDiskPart::NotRequested.into(),
                // As node-manager reads them from the sidecar it checked.
                local_artifact_id: if release == NodeRelease::PreparesAnother {
                    LocalArtifactId::new().to_string()
                } else {
                    request.local_artifact_id.clone()
                },
                template_digest: if release == NodeRelease::PreparesAnotherTemplate {
                    "sha256:0000000000000000000000000000000000000000000000000000000000000001".into()
                } else {
                    request.template_digest.clone()
                },
            };
            if request.encrypted {
                if release == NodeRelease::NoSubvolumes {
                    return Err(Status::failed_precondition(
                        "instance isn't a btrfs subvolume",
                    ));
                }
                std::fs::create_dir_all(dir.join("instance")).unwrap();
                if !non_empty(dir.join("snapshot/cow.luks")).unwrap_or(false) {
                    return Err(Status::failed_precondition(
                        "the staged checkpoint's COW is empty or absent",
                    ));
                }
                // As node-manager does: laid over whatever the instance holds.
                std::fs::copy(dir.join("snapshot/cow.luks"), dir.join("instance/cow.luks"))
                    .unwrap();
                response.cow = if release == NodeRelease::KeepsTheCow {
                    StagedDiskPart::Kept
                } else {
                    StagedDiskPart::Written
                }
                .into();
            }
            if !request.metadata_json.is_empty() {
                let path = dir.join("metadata.json");
                response.metadata = if path.exists() {
                    StagedDiskPart::Kept
                } else {
                    std::fs::write(path, &request.metadata_json).unwrap();
                    StagedDiskPart::Written
                }
                .into();
            }
            Ok(response)
        },
    )
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
/// and on a node execution, as staging leaves it, with the switch on.
fn staged(encrypted: bool, release: NodeRelease) -> (Fixture, LocalArtifactId, Requests) {
    let sandbox_dir = SandboxDir::default();
    let requests = Requests::default();
    let mut f = fixture(
        SandboxState::Suspended,
        encrypted,
        node(&sandbox_dir, release, &requests),
    );
    f.service.staged_checkpoint_disk = true;
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
    (f, checkpoint, requests)
}

/// A copy suspended on this node: its instance COW and `metadata.json` are
/// there beside its pinned checkpoint's sealed copy.
fn suspended_here(release: NodeRelease) -> (Fixture, LocalArtifactId, Requests) {
    let (f, checkpoint, requests) = staged(true, release);
    std::fs::create_dir_all(f.dir().join("instance")).unwrap();
    std::fs::write(f.laid_cow(), EARLIER_BOOT).unwrap();
    std::fs::write(f.dir().join("metadata.json"), b"its own record").unwrap();
    (f, checkpoint, requests)
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

fn calls(f: &Fixture) -> Vec<String> {
    f._node.calls()
}

const FINISHED: [&str; 2] = ["GetCloneCapabilities", "MaterializeStagedCheckpointDisk"];

/// An encrypted checkpoint staged here has node-manager lay its sealed COW
/// into its instance, byte for byte, and record the disk's execution as the
/// checkpoint's producer, asked once with the checkpoint's id. The rootfs is
/// left to the cold start's template clone. A retry asks again, writes
/// nothing, and sends no record, since one is there.
#[tokio::test]
async fn an_encrypted_checkpoint_staged_here_gets_its_disk_finished() {
    let (f, checkpoint, requests) = staged(true, NodeRelease::Current);

    f.complete().await.unwrap();

    assert_eq!(std::fs::read(f.laid_cow()).unwrap(), GUEST_WRITES);
    assert_eq!(
        std::fs::read(f.checkpoint().join("cow.luks")).unwrap(),
        GUEST_WRITES,
        "the checkpoint keeps its own COW"
    );
    assert!(!f.dir().join("instance/rootfs.ext4").exists());
    assert_eq!(f.recorded_execution(), Some(f.source.clone()));
    assert_eq!(calls(&f), FINISHED);
    {
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0].local_artifact_id, checkpoint.to_string());
        assert!(requests[0].encrypted);
        assert!(!requests[0].metadata_json.is_empty());
    }

    f.complete().await.unwrap();
    assert_eq!(calls(&f), [FINISHED, FINISHED].concat());
    assert!(requests.lock().unwrap()[1].metadata_json.is_empty());
    assert_eq!(std::fs::read(f.laid_cow()).unwrap(), GUEST_WRITES);
}

/// An unencrypted checkpoint staged here already has its rootfs in the
/// instance; only the disk's record of its execution is written, and once
/// it's there node-manager isn't asked again.
#[tokio::test]
async fn an_unencrypted_checkpoint_staged_here_gets_its_disk_execution_recorded() {
    let (f, _, requests) = staged(false, NodeRelease::Current);
    f.complete().await.unwrap();
    assert_eq!(f.recorded_execution(), Some(f.source.clone()));
    assert_eq!(calls(&f), FINISHED);
    assert!(!requests.lock().unwrap()[0].encrypted);
    f.complete().await.unwrap();
    assert_eq!(calls(&f), FINISHED);

    // Its rootfs is its whole disk, so no template digest is needed, unlike
    // an encrypted COW's origin.
    let (f, checkpoint, _) = staged(false, NodeRelease::Current);
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

/// A copy suspended on this node keeps its own record, but its instance COW
/// isn't known to be the checkpoint's, so the pinned checkpoint's is laid
/// over it, as a restore would, and the copy the upgrade preserves is that
/// disk.
#[tokio::test]
async fn a_copy_suspended_here_gets_its_checkpoints_cow() {
    let (f, _, _) = suspended_here(NodeRelease::Current);

    f.complete().await.unwrap();

    assert_eq!(std::fs::read(f.laid_cow()).unwrap(), GUEST_WRITES);
    assert_eq!(
        std::fs::read(f.dir().join("metadata.json")).unwrap(),
        b"its own record"
    );
}

/// A disk is finished only from the encrypted suspend checkpoint the row
/// pins, even when nothing is left to finish. A disk artifact forked from
/// another sandbox, a sidecar that says the checkpoint isn't encrypted, one
/// naming another sandbox, one naming another artifact than the pin, one
/// naming none, and one written over another template than the row's, whose
/// origin the cold start would clone, are refused, with nothing laid, no
/// record written and node-manager unasked.
#[tokio::test]
async fn only_the_encrypted_suspend_checkpoint_the_row_pins_is_finished() {
    let changes: [fn(&mut crate::snapshot_upload::SnapshotMeta); 6] = [
        |meta| meta.artifact_kind = Some(ArtifactKind::Disk),
        |meta| meta.encrypted = false,
        |meta| meta.sandbox_id = pplx_space_sandbox_model::SandboxId::new().to_string(),
        |meta| meta.local_artifact_id = Some(LocalArtifactId::new()),
        |meta| meta.local_artifact_id = None,
        |meta| {
            meta.source.content_hash = Some(
                "sha256:0000000000000000000000000000000000000000000000000000000000000001".into(),
            )
        },
    ];
    for finished in [false, true] {
        for change in changes {
            let (f, checkpoint, _) = if finished {
                suspended_here(NodeRelease::Current)
            } else {
                staged(true, NodeRelease::Current)
            };
            let mut meta = checkpoint_sidecar(&f, true, Some(checkpoint));
            change(&mut meta);
            f.write_sidecar(meta);

            let error = f.complete().await.unwrap_err();

            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(error.message().contains("checkpoint it pins"), "{error}");
            if finished {
                assert_eq!(std::fs::read(f.laid_cow()).unwrap(), EARLIER_BOOT);
            } else {
                assert!(!f.laid_cow().exists());
                assert_eq!(
                    f.recorded_execution(),
                    None,
                    "the disk's record came from it"
                );
            }
            assert!(calls(&f).is_empty(), "{:?}", calls(&f));
        }
    }
}

/// With the switch off, the upgrade sees what staging left, as before this
/// change: nothing is validated, written or asked.
#[tokio::test]
async fn the_switch_off_finishes_nothing() {
    let (mut f, _, _) = staged(true, NodeRelease::Current);
    f.service.staged_checkpoint_disk = false;
    let mut meta = checkpoint_sidecar(&f, true, Some(LocalArtifactId::new()));
    meta.local_artifact_id = None;
    f.write_sidecar(meta);

    f.complete().await.unwrap();

    assert!(!f.laid_cow().exists());
    assert_eq!(f.recorded_execution(), None);
    assert!(calls(&f).is_empty(), "{:?}", calls(&f));
}

/// A node-manager's refusal keeps its code, so whether a retry can help
/// isn't decided here: an instance that isn't a subvolume couldn't be
/// preserved for rollback, and nothing is laid into it.
#[tokio::test]
async fn a_node_managers_refusal_keeps_its_code() {
    let (f, _, _) = staged(true, NodeRelease::NoSubvolumes);
    let error = f.complete().await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("subvolume"), "{error}");
    assert!(!f.laid_cow().exists());
    assert_eq!(f.recorded_execution(), None);
}

/// An answer that the instance's COW was kept, not laid from the pinned
/// checkpoint, isn't a finished disk.
#[tokio::test]
async fn a_kept_cow_isnt_a_finished_disk() {
    let (f, _, _) = staged(true, NodeRelease::KeepsTheCow);
    let error = f.complete().await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal, "{error}");
    assert!(error.message().contains("Kept"), "{error}");
}

/// The node's evidence must name the pinned checkpoint and its template:
/// an answer naming another of either isn't a finished disk.
#[tokio::test]
async fn the_node_must_name_the_pinned_checkpoint_it_prepared() {
    for release in [
        NodeRelease::PreparesAnother,
        NodeRelease::PreparesAnotherTemplate,
    ] {
        let (f, checkpoint, requests) = staged(true, release);
        let error = f.complete().await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal, "{release:?}: {error}");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "{release:?}");
        assert_eq!(
            requests[0].local_artifact_id,
            checkpoint.to_string(),
            "{release:?}"
        );
        assert_eq!(requests[0].template_digest, DIGEST, "{release:?}");
    }
}

/// A sandbox an upgrade fences, or one whose upgrade still owes its
/// checkpoint's retirement, is the upgrade's to settle: nothing is written.
#[tokio::test]
async fn a_sandbox_an_upgrade_owns_is_left_to_it() {
    let (f, _, _) = staged(true, NodeRelease::Current);
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
    assert!(calls(&f).is_empty(), "{:?}", calls(&f));
}

/// A sandbox that isn't suspended has no staged checkpoint to finish.
#[tokio::test]
async fn only_a_suspended_sandbox_is_finished() {
    let (f, _, _) = staged(true, NodeRelease::Current);
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
    assert!(calls(&f).is_empty(), "{:?}", calls(&f));
}

/// A staged encrypted checkpoint, retried after staging adopted its row, is
/// upgraded from its own COW: the upgrade finishes the disk, then inspects
/// it and keeps its copy instead of refusing for the missing COW. The
/// harness stops at the cold boot.
#[tokio::test]
async fn an_encrypted_checkpoint_staged_here_is_upgraded_from_its_cow() {
    let (f, checkpoint, _) = staged(true, NodeRelease::Current);

    let error = f
        .service
        .wake(crate::service::activation_support::internal_request(
            remote_upgrade_wake(&f, Some(checkpoint)),
        ))
        .await
        .unwrap_err();

    assert!(!error.message().contains("COW"), "{error}");
    let calls = calls(&f);
    assert_eq!(
        calls.get(..5),
        Some(
            &[
                "GetCloneCapabilities".to_owned(),
                "MaterializeStagedCheckpointDisk".to_owned(),
                // The inspection asks again: the copy has no rootfs.
                "GetCloneCapabilities".to_owned(),
                "DeleteBtrfsSnapshot".to_owned(),
                "CreateBtrfsSnapshot".to_owned(),
            ][..]
        ),
        "{error}: {calls:?}"
    );
    assert_eq!(std::fs::read(f.laid_cow()).unwrap(), GUEST_WRITES);
}

/// A refusal to finish the disk comes before the upgrade fences the sandbox,
/// so it stays suspended and unfenced and nothing is preserved: a zero-byte
/// staged COW, which the rollback of its preserved copy would refuse after;
/// and, in a mixed-version rollout, a node-manager that
/// doesn't advertise the RPC, which is never sent it.
#[tokio::test]
async fn a_refusal_comes_before_the_upgrade_fences() {
    #[derive(Debug, Clone, Copy)]
    enum Case {
        EmptyStagedCow,
        NodeWithoutTheBit,
        OlderNode,
    }
    for case in [
        Case::EmptyStagedCow,
        Case::NodeWithoutTheBit,
        Case::OlderNode,
    ] {
        let release = match case {
            Case::NodeWithoutTheBit => NodeRelease::WithoutTheBit,
            Case::OlderNode => NodeRelease::Older,
            Case::EmptyStagedCow => NodeRelease::Current,
        };
        let (f, checkpoint, requests) = staged(true, release);
        match case {
            Case::EmptyStagedCow => {
                std::fs::write(f.checkpoint().join("cow.luks"), b"").unwrap();
            }
            Case::NodeWithoutTheBit | Case::OlderNode => {}
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
            "{case:?}: {error}"
        );
        let expected = match case {
            Case::EmptyStagedCow => "empty",
            Case::NodeWithoutTheBit | Case::OlderNode => "roll node-manager out first",
        };
        assert!(error.message().contains(expected), "{case:?}: {error}");
        assert_eq!(f.row().state.unowned(), Some(SandboxState::Suspended));
        assert_eq!(
            crate::db::read::runtime_upgrade_fence(&f.service.db.lock().unwrap(), &f.sandbox_id)
                .unwrap(),
            None,
            "{case:?}"
        );
        let calls = calls(&f);
        assert!(
            !calls.iter().any(|call| call == "CreateBtrfsSnapshot"),
            "{case:?}: {calls:?}"
        );
        if matches!(case, Case::NodeWithoutTheBit | Case::OlderNode) {
            assert!(requests.lock().unwrap().is_empty(), "{case:?}");
            assert!(!f.laid_cow().exists(), "{case:?}");
            assert_eq!(f.recorded_execution(), None, "{case:?}");
        }
        assert!(!f.laid_cow().exists(), "{case:?}");
    }
}
