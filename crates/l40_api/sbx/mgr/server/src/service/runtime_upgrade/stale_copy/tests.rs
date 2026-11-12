//! A runtime upgrade meeting a stale suspended copy, through `UpgradeRuntime`
//! (and the Wake, which leaves the copy alone), against a cluster-api stub that
//! answers the backup proof and
//! a test-served node-manager. Each case seeds the copy the node holds: a
//! backed-up one, an unproven one, an unpinned one and an encrypted one.
//!
//! The cluster-api stub serves the parent process and the case runs in a
//! child (`with_cluster_api`), so artifact ids the stub knows are fixed.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::{ExecutionMetadata, LocalArtifactId};
use pplx_space_node_api_client::proto::{
    GetArtifactBackupLocationRequest, GetArtifactBackupLocationResponse,
    LocalArtifactBackupIdentity, backup_readiness_query::Artifact,
};
use tonic::Status;

use super::super::node_effects::tests::{Fixture, certified_target, fixture, remote_upgrade_wake};
use super::super::rpc::tests::{authorization, operation, pod_upgrade, upgrade};
use crate::db::RuntimeUpgradeOperationId;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::proto::{RuntimeUpgradeStageFrom, UpgradeRuntimeRequest, WakeTargetState};
use crate::service::activation_support::internal_request;
use crate::service::grpc_lifecycle::{answer, with_cluster_api};
use crate::state::{CleanupState, SandboxState, SandboxStatus};

/// The checkpoint the node's stale copy pins.
const STALE: &str = "0192f0c0-0000-7000-8000-00000000a001";
/// The checkpoint the upgrade was decided from, whose id sorts after
/// [`STALE`]'s.
const DECIDED: &str = "0192f0c0-0000-7000-8000-00000000a002";
/// A checkpoint whose id sorts after [`DECIDED`]'s.
const NEWER: &str = "0192f0c0-0000-7000-8000-00000000a003";
/// A named snapshot the sandbox holds beside its copy.
const NAMED: &str = "0192f0c0-0000-7000-8000-00000000a004";

fn artifact(id: &str) -> LocalArtifactId {
    id.parse().unwrap()
}

/// A cluster-api stub answering the backup readiness checks.
#[derive(Clone, Copy)]
enum BackupReadiness {
    /// A ready backup of exactly `backed`, and of no other; and, when
    /// `captured_after` is `(later, earlier)`, `later`'s ready backup captured
    /// by a later execution than `earlier`'s, and no other order.
    Backs {
        backed: &'static str,
        captured_after: Option<(&'static str, &'static str)>,
    },
    /// Every check answers `UNAVAILABLE`.
    Unavailable,
    /// As [`Self::backs`] of [`STALE`], and a ready backup of the named
    /// snapshot [`NAMED`] when `named_backed`.
    AndNamed { named_backed: bool },
}

impl BackupReadiness {
    /// Backs `backed`, and shows the decided checkpoint captured after it.
    const fn backs(backed: &'static str) -> Self {
        Self::Backs {
            backed,
            captured_after: Some((DECIDED, backed)),
        }
    }

    fn answer(
        self,
        request: GetArtifactBackupLocationRequest,
    ) -> Result<GetArtifactBackupLocationResponse, Status> {
        if let Self::AndNamed { named_backed } = self {
            let asks_named = matches!(
                request.readiness.as_ref().and_then(|query| query.artifact.as_ref()),
                Some(Artifact::LocalArtifact(identity)) if identity.local_artifact_id == NAMED
            );
            if !asks_named {
                return Self::backs(STALE).answer(request);
            }
            return Ok(GetArtifactBackupLocationResponse {
                ready: Some(named_backed),
                ..Default::default()
            });
        }
        let Self::Backs {
            backed,
            captured_after,
        } = self
        else {
            return Err(Status::unavailable("cluster-api is restarting"));
        };
        // A location lookup, as a backup's admission makes: the cluster
        // knows a location only for an artifact it holds a backup of.
        if request.readiness.is_none() && request.local_artifact_id != backed {
            return Err(Status::not_found("no backup of that artifact"));
        }
        let ready = match request.readiness.and_then(|query| query.artifact) {
            Some(Artifact::LocalArtifact(identity)) => identity.local_artifact_id == backed,
            Some(Artifact::CapturedAfter(order)) => {
                let id = |identity: Option<LocalArtifactBackupIdentity>| {
                    identity.map(|identity| identity.local_artifact_id)
                };
                captured_after.is_some_and(|(later, earlier)| {
                    id(order.later).as_deref() == Some(later)
                        && id(order.earlier).as_deref() == Some(earlier)
                })
            }
            _ => false,
        };
        Ok(GetArtifactBackupLocationResponse {
            ready: Some(ready),
            ..Default::default()
        })
    }
}

/// Backs the decided checkpoint and not the copy, so a proof of the wrong
/// artifact passes where it must not, and shows the decided checkpoint
/// captured after the copy.
const BACKS_ONLY_DECIDED: BackupReadiness = BackupReadiness::Backs {
    backed: DECIDED,
    captured_after: Some((DECIDED, STALE)),
};

impl tonic::server::NamedService for BackupReadiness {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl<B> tower::Service<http::Request<B>> for BackupReadiness
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let stub = *self;
        Box::pin(async move {
            Ok(
                if request.uri().path().ends_with("/GetArtifactBackupLocation") {
                    answer(request, move |request| stub.answer(request)).await
                } else if request
                    .uri()
                    .path()
                    .ends_with("/GetSandboxSnapshotStorageUri")
                {
                    // No org override: a backup goes to the node's default.
                    answer(
                        request,
                        |_: pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriRequest| {
                            Ok(
                                pplx_space_node_api_client::proto::GetSandboxSnapshotStorageUriResponse {
                                    storage_uri: None,
                                },
                            )
                        },
                    )
                    .await
                } else {
                    Status::unimplemented(request.uri().path().to_owned()).into_http()
                },
            )
        })
    }
}

/// The `UpgradeRuntime` a pod's shim sends for a restore pod placed away from
/// the decided checkpoint: staged from its storage URI.
fn staged_pod_upgrade(
    f: &Fixture,
    target: &ExecutionMetadata,
    operation_id: RuntimeUpgradeOperationId,
) -> UpgradeRuntimeRequest {
    let mut request = pod_upgrade(
        &f.sandbox_id,
        target,
        operation_id,
        WakeTargetState::Started,
    );
    request.stage_from = Some(RuntimeUpgradeStageFrom {
        suspend_restore_storage_uri: staging_request(f).suspend_restore_storage_uri.unwrap(),
        suspend_restore_source_node: Some("worker-source".into()),
    });
    request.checkpoint_local_artifact_id = Some(DECIDED.to_string());
    request
}

/// The staging `Start` either caller builds for the decided checkpoint, with
/// the pod's route identity.
fn staging_request(f: &Fixture) -> crate::proto::StartRequest {
    let start = pod_upgrade(
        &f.sandbox_id,
        &f.target_environment(),
        operation(),
        WakeTargetState::Started,
    )
    .start
    .unwrap();
    crate::proto::StartRequest {
        checkpoint: Some(crate::service::create_support::SUSPEND_CHECKPOINT.to_string()),
        suspend_restore_storage_uri: Some(format!(
            "s3://space-test/suspend/{}/{DECIDED}?region=us-east-1",
            f.sandbox_id
        )),
        ..start
    }
}

/// The Wake the control plane sends a Direct sandbox's restore with a
/// resume-time upgrade decided from the checkpoint.
fn decided_wake(f: &Fixture) -> crate::proto::WakeRequest {
    remote_upgrade_wake(f, Some(artifact(DECIDED)))
}

/// A suspended sandbox whose copy on the node pins `pin`, with the cleanups
/// its eviction hands off recorded and answered as applied.
fn stale_copy(encrypted: bool, pin: Option<&str>) -> (Fixture, Arc<Mutex<Vec<String>>>) {
    let f = fixture(
        SandboxState::Suspended,
        encrypted,
        FakeNodeManager::default(),
    );
    if let Some(pin) = pin {
        f.adopt_checkpoint_as(artifact(pin));
    }
    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let recorded = cleanups.clone();
    *f.service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |sandbox_id, _| {
        recorded.lock().unwrap().push(sandbox_id);
        Box::pin(async { Ok(()) })
    }));
    (f, cleanups)
}

impl Fixture {
    fn target_environment(&self) -> ExecutionMetadata {
        certified_target(&self.source)
    }

    async fn pod_upgrade_error(&self, operation_id: RuntimeUpgradeOperationId) -> Status {
        upgrade(
            &self.service,
            staged_pod_upgrade(self, &self.target_environment(), operation_id),
        )
        .await
        .expect_err("this harness has no object store to stage from")
    }

    async fn wake_error(&self, request: crate::proto::WakeRequest) -> Status {
        self.service
            .wake(internal_request(request))
            .await
            .expect_err("this harness has no object store to stage from")
    }

    fn local_artifact_exists(&self, id: &str) -> bool {
        self.service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM local_artifacts WHERE local_artifact_id = ?1",
                crate::db::fixture::params![id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    }

    /// The copy is exactly as seeded: suspended, pinning `pin`, and nothing
    /// was asked of node-manager or the cleanup executor.
    fn assert_copy_untouched(&self, pin: Option<&str>, cleanups: &Mutex<Vec<String>>) {
        let row = self.row();
        assert_eq!(row.state, SandboxStatus::Unowned(SandboxState::Suspended));
        assert_eq!(row.current_local_artifact_id, pin.map(artifact));
        assert!(cleanups.lock().unwrap().is_empty());
        assert!(self._node.calls().is_empty(), "{:?}", self._node.calls());
    }

    /// The copy was evicted once: the cleanup ran, and the sandbox is no
    /// longer this node's suspended copy of the stale checkpoint.
    fn assert_copy_evicted(&self, cleanups: &Mutex<Vec<String>>) {
        assert_eq!(*cleanups.lock().unwrap(), vec![self.sandbox_id.clone()]);
        assert!(!self.local_artifact_exists(STALE));
        if let Ok(row) = self.service.lookup_sandbox(&self.sandbox_id) {
            assert_ne!(row.current_local_artifact_id, Some(artifact(STALE)));
        }
    }
}

/// A pod's upgrade meeting a backed-up copy of an older checkpoint evicts it
/// and stages the decided checkpoint in its place,
/// where it would otherwise be refused. The harness has no object store, so
/// staging is what fails. A retry finds the sandbox absent and leaves it to
/// staging, evicting nothing more; staging's own retry of an interrupted
/// stage is covered with staging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backed_up_stale_copy_makes_way_for_the_decided_checkpoint() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let operation_id = operation();

        let error = f.pod_upgrade_error(operation_id).await;

        assert!(!error.message().contains("decided from"), "{error}");
        assert!(error.message().contains("stage"), "{error}");
        f.assert_copy_evicted(&cleanups);

        f.service
            .evict_stale_checkpoint_copy(
                &f.sandbox_id,
                operation_id,
                Some(artifact(DECIDED)),
                true,
                &staging_request(&f),
            )
            .await
            .unwrap();
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

/// The Wake carries only a Direct sandbox's upgrade, and nothing orders a
/// Direct sandbox's captures, so the Wake never replaces a copy: its stale
/// copy, backed up or not, is refused as it was before replacement existed,
/// and the cluster isn't asked. The stub answers every question
/// `UNAVAILABLE`, so a Wake that asked would be refused as retryable instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_wake_upgrade_keeps_refusing_a_stale_copy() {
    with_cluster_api(BackupReadiness::Unavailable, async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));

        let error = f.wake_error(decided_wake(&f)).await;

        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("decided from"), "{error}");
        f.assert_copy_untouched(Some(STALE), &cleanups);

        // Nor does the Wake take the staging route, whose checks a storage
        // URI it then never reads would fail.
        let mut malformed = decided_wake(&f);
        malformed
            .start_context
            .as_mut()
            .unwrap()
            .suspend_restore_storage_uri = Some("not a storage uri".into());
        let error = f.wake_error(malformed).await;
        assert!(error.message().contains("decided from"), "{error}");

        // The replacement itself leaves a Direct request's copy alone
        // without asking the cluster, whichever caller hands it one.
        let mut direct = staging_request(&f);
        let identity = direct.route_identity.as_mut().unwrap();
        identity.substrate_kind = crate::proto::SandboxSubstrateKind::Direct as i32;
        identity.pod_uid = None;
        f.service
            .evict_stale_checkpoint_copy(
                &f.sandbox_id,
                operation(),
                Some(artifact(DECIDED)),
                true,
                &direct,
            )
            .await
            .unwrap();
        f.assert_copy_untouched(Some(STALE), &cleanups);
    })
    .await;
}

/// A copy nothing proves backed up may be the only copy of something: it is
/// refused before any node work. The stub backs the
/// decided checkpoint, so only a proof of the copy's own pin counts. An
/// unpinned copy has no artifact to prove.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unproven_stale_copy_is_refused_before_any_node_work() {
    with_cluster_api(BACKS_ONLY_DECIDED, async || {
        for pin in [Some(STALE), None] {
            let (f, cleanups) = stale_copy(false, pin);
            let refused = f.pod_upgrade_error(operation()).await;
            assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
            assert!(refused.message().contains("backed up"), "{refused}");
            f.assert_copy_untouched(pin, &cleanups);
        }
    })
    .await;
}

/// A proof the cluster couldn't give is retryable, and leaves the copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unanswered_backup_proof_leaves_the_copy_for_a_retry() {
    with_cluster_api(BackupReadiness::Unavailable, async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));

        let refused = f.pod_upgrade_error(operation()).await;

        assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused}");
        f.assert_copy_untouched(Some(STALE), &cleanups);
    })
    .await;
}

/// The copy's own backup marker, stamped once its upload was confirmed, is
/// proof enough without asking the cluster, as it is for eviction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stamped_backup_marker_proves_the_copy() {
    with_cluster_api(BACKS_ONLY_DECIDED, async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        crate::db::mark_local_artifact_backup_completed(&f.service.db.lock().unwrap(), STALE)
            .unwrap();

        let error = f.pod_upgrade_error(operation()).await;

        assert!(error.message().contains("stage"), "{error}");
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

/// A copy another upgrade still owns is left to it, retryably: one that
/// fences it holds that upgrade's preserved disk, which its recovery
/// finishes, and one that verified its boot still owes the old checkpoint's
/// retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_another_upgrade_owns_is_left_to_it() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        // (the other upgrade's phase, what the refusal says)
        for (phase, says) in [
            (None, "is moving this sandbox"),
            (
                Some("verified"),
                "is retiring this sandbox's old checkpoint",
            ),
        ] {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            let record = crate::db::test_fixtures::fence_runtime_upgrade(
                &f.service.db.lock().unwrap(),
                &f.sandbox_id,
                f.target.environment().unwrap(),
                crate::db::RuntimeUpgradeTargetState::Started,
                None,
            );
            if let Some(phase) = phase {
                f.service
                    .db
                    .lock()
                    .unwrap()
                    .execute(
                        "UPDATE sandbox_runtime_upgrades SET phase = ?1 WHERE operation_id = ?2",
                        crate::db::fixture::params![phase, record.operation_id.to_string()],
                    )
                    .unwrap();
            }

            let error = f
                .service
                .evict_stale_checkpoint_copy(
                    &f.sandbox_id,
                    operation(),
                    Some(artifact(DECIDED)),
                    true,
                    &staging_request(&f),
                )
                .await
                .unwrap_err();

            assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
            assert!(error.message().contains(says), "{error}");
            f.assert_copy_untouched(Some(STALE), &cleanups);
        }
    })
    .await;
}

/// A sandbox this operation already has an outcome for is left to that
/// record: the copy it holds now came after the operation, and the upgrade's
/// own check refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_after_this_operations_outcome_is_left_to_the_upgrade() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let operation_id = operation();
        assert!(
            crate::db::record_runtime_upgrade_already_on_target(
                &f.service.db.lock().unwrap(),
                &crate::db::RuntimeUpgradeRequestIdentity {
                    operation_id,
                    sandbox_id: f.sandbox_id.clone(),
                    target_environment: f.target_environment().environment().unwrap(),
                    target_state: crate::db::RuntimeUpgradeTargetState::Started,
                    authorization_id: authorization().authorization_id,
                },
                &crate::db::new_runtime_upgrade_attempt_id(),
            )
            .unwrap()
        );

        let refused = f.pod_upgrade_error(operation_id).await;

        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(refused.message().contains("decided from"), "{refused}");
        f.assert_copy_untouched(Some(STALE), &cleanups);
    })
    .await;
}

/// A request whose stage would be refused stages nothing in the copy's
/// place, so even a backed-up copy stays: a malformed storage URI, or a route
/// identity the stage couldn't admit once the row is gone (bound to another
/// sandbox, naming another node, or missing), is refused before the copy is
/// read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_that_cant_stage_leaves_the_copy() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        const MALFORMED: &str = "not a storage uri";
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let mut pod = staged_pod_upgrade(&f, &f.target_environment(), operation());
        pod.stage_from.as_mut().unwrap().suspend_restore_storage_uri = MALFORMED.into();

        let with_identity = |change: &dyn Fn(&mut crate::proto::SpaceApiRouteIdentity)| {
            let mut request = staged_pod_upgrade(&f, &f.target_environment(), operation());
            change(
                request
                    .start
                    .as_mut()
                    .unwrap()
                    .route_identity
                    .as_mut()
                    .unwrap(),
            );
            request
        };
        let misbound = with_identity(&|identity| {
            identity.sandbox_id = Some(pplx_space_common_core::types::SandboxId::new().to_string());
        });
        let elsewhere = with_identity(&|identity| {
            identity.current_node_id = Some(uuid::Uuid::now_v7().to_string());
        });
        let mut identity_free = staged_pod_upgrade(&f, &f.target_environment(), operation());
        identity_free.start.as_mut().unwrap().route_identity = None;

        for (refused, code) in [
            (
                upgrade(&f.service, pod).await.unwrap_err(),
                tonic::Code::InvalidArgument,
            ),
            (
                upgrade(&f.service, misbound).await.unwrap_err(),
                tonic::Code::InvalidArgument,
            ),
            (
                upgrade(&f.service, elsewhere).await.unwrap_err(),
                tonic::Code::FailedPrecondition,
            ),
            (
                upgrade(&f.service, identity_free).await.unwrap_err(),
                tonic::Code::FailedPrecondition,
            ),
        ] {
            assert_eq!(refused.code(), code, "{refused}");
            f.assert_copy_untouched(Some(STALE), &cleanups);
        }
    })
    .await;
}

/// The eviction deletes every local artifact of the sandbox, so a copy with
/// an upload still pending is left for a retry once it finishes, even when
/// its pin is backed up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_with_a_pending_upload_is_left_for_a_retry() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        f.service
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO jobs (job_id, kind, status, payload, subject_id, created_at)
                 VALUES (?1, 'snapshot_upload', 'pending', '{}', ?2, '1')",
                crate::db::fixture::params![uuid::Uuid::now_v7().to_string(), f.sandbox_id],
            )
            .unwrap();

        let refused = f.pod_upgrade_error(operation()).await;

        assert_eq!(refused.code(), tonic::Code::Aborted, "{refused}");
        assert!(refused.message().contains("upload"), "{refused}");
        f.assert_copy_untouched(Some(STALE), &cleanups);
    })
    .await;
}

/// The upgrade refuses to replace the copy pinning `pin`, backed up as it
/// is, saying `says`, before any node work.
async fn assert_left_in_place(encrypted: bool, pin: &'static str, says: &str) {
    let (f, cleanups) = stale_copy(encrypted, Some(pin));
    let refused = f.pod_upgrade_error(operation()).await;
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
    assert!(refused.message().contains(says), "{refused}");
    f.assert_copy_untouched(Some(pin), &cleanups);
}

/// An encrypted copy stays, even backed up: an encrypted checkpoint staged
/// in its place can't be cold-booted, so the upgrade would only fail later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_stale_copy_is_left_in_place() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        assert_left_in_place(true, STALE, "encrypted checkpoint").await;
    })
    .await;
}

/// A newer local copy is never replaced by an older decided checkpoint,
/// whatever the minting nodes' clocks say or which backup registered last.
/// The copy's id sorts before the decided checkpoint's, as a slow clock on
/// the node that suspended it would mint it, but the cluster shows the copy
/// captured by the later execution, so it stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newer_local_copy_is_never_replaced_whatever_the_clocks_say() {
    let copy_captured_later = BackupReadiness::Backs {
        backed: STALE,
        captured_after: Some((STALE, DECIDED)),
    };
    with_cluster_api(copy_captured_later, async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        assert!(artifact(STALE).as_uuid() < artifact(DECIDED).as_uuid());

        let refused = f.pod_upgrade_error(operation()).await;
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(refused.message().contains("captured after"), "{refused}");
        f.assert_copy_untouched(Some(STALE), &cleanups);
    })
    .await;
}

/// The other order: a copy whose id sorts after the decided checkpoint's, as
/// a fast clock would mint it, captured by an earlier execution, is replaced
/// like any stale copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_copy_is_replaced_whatever_the_clocks_say() {
    with_cluster_api(BackupReadiness::backs(NEWER), async || {
        let (f, cleanups) = stale_copy(false, Some(NEWER));
        assert!(artifact(NEWER).as_uuid() > artifact(DECIDED).as_uuid());

        let error = f.pod_upgrade_error(operation()).await;

        assert!(error.message().contains("stage"), "{error}");
        assert_eq!(*cleanups.lock().unwrap(), vec![f.sandbox_id.clone()]);
    })
    .await;
}

/// Without a capture order nothing shows which checkpoint is newer, so the
/// copy is refused before any node work, as every copy pinning another
/// checkpoint was before replacement existed: backups that record no capture
/// generation. So is an upgrade whose authorization doesn't accept losing the
/// sandbox's memory, which the upgrade refuses anyway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unordered_or_unauthorized_upgrade_leaves_the_copy() {
    let unordered = BackupReadiness::Backs {
        backed: STALE,
        captured_after: None,
    };
    with_cluster_api(unordered, async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let refused = f.pod_upgrade_error(operation()).await;
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(refused.message().contains("captured after"), "{refused}");
        f.assert_copy_untouched(Some(STALE), &cleanups);

        let (f, cleanups) = stale_copy(false, Some(STALE));
        let mut request = staged_pod_upgrade(&f, &f.target_environment(), operation());
        request.authorization.as_mut().unwrap().memory_loss = false;
        let refused = upgrade(&f.service, request).await.unwrap_err();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        f.assert_copy_untouched(Some(STALE), &cleanups);
    })
    .await;
}

/// An activation the row would refuse must not get past it by removing the
/// row: a request at a generation the copy's row already retained is refused
/// as stale, and the copy stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_the_row_would_fence_leaves_the_copy() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        // The pod's identity is at generation 4.
        f.service
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sandboxes
                    SET last_observed_route_generation = 9,
                        last_observed_execution_generation = 9
                  WHERE id = ?1",
                crate::db::fixture::params![f.sandbox_id],
            )
            .unwrap();

        let refused = f.pod_upgrade_error(operation()).await;

        assert_eq!(refused.code(), tonic::Code::Aborted, "{refused}");
        assert!(
            refused.message().contains("retained generation"),
            "{refused}"
        );
        f.assert_copy_untouched(Some(STALE), &cleanups);
    })
    .await;
}

/// A cluster-api stub answering the backup checks as `backups` does and
/// `GetExecutionCurrency` with `currency` for every execution.
#[derive(Clone, Copy)]
struct BackupsAndCurrency {
    backups: BackupReadiness,
    currency: pplx_space_node_api_client::proto::ExecutionCurrency,
}

impl tonic::server::NamedService for BackupsAndCurrency {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl<B> tower::Service<http::Request<B>> for BackupsAndCurrency
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = futures_util::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let stub = *self;
        Box::pin(async move {
            let path = request.uri().path().to_owned();
            Ok(if path.ends_with("/GetExecutionCurrency") {
                answer(
                    request,
                    move |_: pplx_space_node_api_client::proto::GetExecutionCurrencyRequest| {
                        Ok(
                            pplx_space_node_api_client::proto::GetExecutionCurrencyResponse {
                                currency: stub.currency.into(),
                            },
                        )
                    },
                )
                .await
            } else if path.ends_with("/GetArtifactBackupLocation") {
                answer(request, move |request| stub.backups.answer(request)).await
            } else {
                Status::unimplemented(path).into_http()
            })
        })
    }
}

impl Fixture {
    /// Leave the copy as a restore of it whose prepare failed leaves it: the
    /// cleanup rolled the row back to `suspended` with its pin, recorded the
    /// rollback receipt, and kept the activation's claim open, so kubelet's
    /// retry of the same pod matches it. Returns that pod's route identity.
    fn roll_back_a_restore(&self) -> pplx_space_common_core::types::SpaceApiRouteIdentity {
        use pplx_space_common_core::types::{
            ExecutionGeneration, NodeManagedBy, RouteGeneration, SandboxActivationAttemptId,
        };
        let generation = ExecutionGeneration::new(4).unwrap();
        let attempt = SandboxActivationAttemptId::new();
        let route = pplx_space_common_core::types::SpaceApiRouteIdentity {
            sandbox_id: Some(self.sandbox_id.parse().unwrap()),
            route_generation: Some(RouteGeneration::new(4).unwrap()),
            execution_generation: Some(generation),
            current_node_id: Some(crate::service::test_options::test_node_id()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            activation_attempt_id: Some(attempt),
            pod_uid: Some("retried-pod".parse().unwrap()),
        };
        let provenance =
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced;
        let claim =
            crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(
                &route, provenance,
            )
            .unwrap();
        let db = self.service.db.lock().unwrap();
        let episode = crate::db::begin_activation_with_route_evidence(
            &db,
            &self.sandbox_id,
            SandboxState::Suspended,
            crate::db::ActivationKind::Restore,
            Some(generation),
            Some(attempt),
            crate::db::ActivationRouteAdmission {
                evidence: crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(
                    &route, provenance,
                ),
                open_claim: Some(&claim),
                ..Default::default()
            },
        )
        .unwrap()
        .expect("the restore claims the suspended copy");
        crate::db::request_cleanup_episode(
            &db,
            &self.sandbox_id,
            episode.id,
            pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed,
            "restore prepare failed",
        )
        .unwrap();
        crate::db::test_fixtures::complete_cleanup_episode(&db, &self.sandbox_id, episode.id)
            .unwrap()
            .expect("the rollback completes");
        let view = crate::db::read::sandbox_view(&db, &self.sandbox_id);
        assert!(view.route_claim().matches(&claim).unwrap());
        assert!(view.route_claim().rolled_back(&claim).unwrap());
        route
    }

    /// The pod's upgrade, retried under `route`.
    async fn retried_pod_upgrade(
        &self,
        route: &pplx_space_common_core::types::SpaceApiRouteIdentity,
    ) -> Status {
        let mut request = staged_pod_upgrade(self, &self.target_environment(), operation());
        request.start.as_mut().unwrap().route_identity =
            crate::service::activation_support::space_api_route_identity_to_wire(route);
        upgrade(&self.service, request)
            .await
            .expect_err("this harness has no object store to stage from")
    }
}

/// Kubelet's retry of an activation the copy's row rolled back matches the
/// claim the row kept open for it, which is how each later attempt of that
/// activation is asked whether its execution was retired. Replacing the copy
/// would close the claim and leave a retry after it unasked, so a retry never
/// replaces the copy, even when the control plane would answer that its
/// execution is current: the copy, its pin and its claim stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_of_a_rolled_back_activation_never_replaces_the_copy() {
    with_cluster_api(
        BackupsAndCurrency {
            backups: BackupReadiness::backs(STALE),
            currency: pplx_space_node_api_client::proto::ExecutionCurrency::Current,
        },
        async || {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            let route = f.roll_back_a_restore();

            let error = f.retried_pod_upgrade(&route).await;

            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(error.message().contains("retries"), "{error}");
            f.assert_copy_untouched(Some(STALE), &cleanups);
            let db = f.service.db.lock().unwrap();
            assert!(
                crate::db::route_activation_claims::claim_exists(&db, &f.sandbox_id).unwrap(),
                "the claim a later retry is asked about stays"
            );
        },
    )
    .await;
}

/// The control for [`a_retry_of_a_rolled_back_activation_never_replaces_the_copy`]:
/// a new activation of the same row, with an attempt and a generation of its
/// own, matches no claim and replaces the copy, so the refusal there isn't
/// vacuous.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_activation_after_a_rollback_replaces_the_copy() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        use pplx_space_common_core::types::{
            ExecutionGeneration, RouteGeneration, SandboxActivationAttemptId,
        };
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let rolled_back = f.roll_back_a_restore();
        let route = pplx_space_common_core::types::SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(5).unwrap()),
            execution_generation: Some(ExecutionGeneration::new(5).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            pod_uid: Some("replacement-pod".parse().unwrap()),
            ..rolled_back
        };

        let error = f.retried_pod_upgrade(&route).await;

        assert!(error.message().contains("stage"), "{error}");
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

/// A cleanup that fails after the eviction's claim leaves the row's cleanup
/// failed, which is how the replacement tells it from a claim another
/// eviction won; the upgrade is answered as retryable either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cleanup_that_fails_after_the_claim_leaves_the_row_cleanup_failed() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, _) = stale_copy(false, Some(STALE));
        *f.service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(|_, _| {
            Box::pin(async { Err(Status::failed_precondition("node-manager refused")) })
        }));

        let error = f.pod_upgrade_error(operation()).await;

        assert_eq!(error.code(), tonic::Code::Aborted, "{error}");
        assert_eq!(f.row().state.cleanup(), Some(CleanupState::Failed));
    })
    .await;
}

/// The report names a cleanup failure as one, and leaves the claim's own
/// refusals, including a claim another eviction won, unreported.
#[test]
fn only_a_failed_cleanup_is_reported_as_a_failure() {
    let refused = || Err(Status::failed_precondition("refused"));
    for (row_state, reported) in [
        (Some(SandboxStatus::CLEANUP_FAILED), tonic::Code::Internal),
        (
            Some(SandboxStatus::Unowned(SandboxState::Deleting)),
            tonic::Code::FailedPrecondition,
        ),
        (
            Some(SandboxStatus::Unowned(SandboxState::Suspended)),
            tonic::Code::FailedPrecondition,
        ),
        (None, tonic::Code::FailedPrecondition),
    ] {
        assert_eq!(
            super::reported_eviction_result(&refused(), row_state)
                .unwrap_err()
                .code(),
            reported,
            "{row_state:?}"
        );
    }
    super::reported_eviction_result(&Ok(()), Some(SandboxStatus::CLEANUP_FAILED)).unwrap();
    assert_eq!(
        super::reported_eviction_result(
            &Err(Status::unavailable("later")),
            Some(SandboxStatus::CLEANUP_FAILED)
        )
        .unwrap_err()
        .code(),
        tonic::Code::Unavailable
    );
}

/// A backup's admission (`enqueue_artifact_backup`) holds the sandbox's
/// artifact lock from reading the row until its upload job is persisted, and
/// the eviction's claim sees only a persisted job. Paused just before it
/// persists the job, the admission still holds the lock, and the eviction is
/// refused with the copy untouched; the job is persisted only after that.
/// The stub backs only the decided checkpoint, so the admission finds no
/// backup of the copy ready and goes on to persist one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backup_being_admitted_holds_off_the_eviction() {
    with_cluster_api(BACKS_ONLY_DECIDED, async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        // The copy's bytes, which the admission checks before it persists.
        let local_path: String = f
            .service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT local_path FROM local_artifacts WHERE local_artifact_id = ?1",
                crate::db::fixture::params![STALE],
                |row| row.get(0),
            )
            .unwrap();
        crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
            std::path::Path::new(&local_path),
            &f.sandbox_id,
            artifact(STALE),
            b"stale-copy",
        );
        let mut service = f.service.clone();
        service.snapshot_upload_config =
            Some(Arc::new(crate::snapshot_upload::SnapshotUploadConfig {
                bucket: "space-test".to_string(),
                region: "us-east-1".to_string(),
                key_prefix: String::new(),
                endpoint: None,
                path_style: false,
            }));
        let service = Arc::new(service);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        *service
            .auto_lifecycle
            .backup_admission_barrier
            .lock()
            .unwrap() = Some(Arc::clone(&barrier));
        let admission = tokio::spawn(admit_backup(Arc::clone(&service), f.sandbox_id.clone()));
        let upload = || {
            service
                .snapshot_upload_worker
                .find_job_by_dedup_key(crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND, STALE)
                .unwrap()
        };

        // The admission is about to persist its upload job.
        if tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
            .await
            .is_err()
        {
            panic!(
                "the admission never reached its upload job: {:?}",
                admission.await.unwrap()
            );
        }
        let refused = service
            .evict_local_sandbox(
                &f.sandbox_id,
                crate::service::EvictionClaim::SuspendedPinned(Some(artifact(STALE))),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(
            refused.message().contains("another operation holds"),
            "{refused}"
        );
        assert!(upload().is_none(), "the job isn't persisted yet");
        let row = f.row();
        assert_eq!(row.state, SandboxStatus::Unowned(SandboxState::Suspended));
        assert_eq!(row.current_local_artifact_id, Some(artifact(STALE)));
        assert!(cleanups.lock().unwrap().is_empty());

        barrier.wait().await;
        // The admission may fail after persisting, registering against a stub
        // that serves no registration; the job is what it leaves either way.
        let _ = admission.await.unwrap();
        assert!(upload().is_some(), "the admission persisted its upload job");
    })
    .await;
}

/// Admit a backup of [`STALE`], the copy's pinned artifact, as
/// `EnqueueArtifactBackup` does.
async fn admit_backup(
    service: Arc<crate::service::SandboxManagerService>,
    sandbox_id: String,
) -> Result<tonic::Response<crate::proto::EnqueueArtifactBackupResponse>, Status> {
    service
        .enqueue_artifact_backup(internal_request(
            crate::proto::EnqueueArtifactBackupRequest {
                sandbox_id,
                expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                source: Some(
                    crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                        STALE.to_string(),
                    ),
                ),
            },
        ))
        .await
}

/// A backup admitted while an eviction claims the row waits for the artifact
/// lock the claim holds, then finds the row `deleting`: it refuses, and
/// persists no upload whose source the eviction's cleanup deletes. The
/// eviction is paused holding the lock before its claim, the admission is
/// seen to reach the lock and stay blocked on it, and the eviction's cleanup
/// is held open, so the row is still `deleting` when the admission gets it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backup_admitted_mid_eviction_backs_up_nothing() {
    with_cluster_api(BACKS_ONLY_DECIDED, async || {
        let (f, _) = stale_copy(false, Some(STALE));
        let service = Arc::new(f.service.clone());
        let cleanup_released = Arc::new(tokio::sync::Notify::new());
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new({
            let cleanup_released = Arc::clone(&cleanup_released);
            move |_, _| {
                let cleanup_released = Arc::clone(&cleanup_released);
                Box::pin(async move {
                    cleanup_released.notified().await;
                    Ok(())
                })
            }
        }));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        *service
            .auto_lifecycle
            .eviction_claim_barrier
            .lock()
            .unwrap() = Some(Arc::clone(&barrier));
        let eviction = tokio::spawn({
            let service = Arc::clone(&service);
            let sandbox_id = f.sandbox_id.clone();
            async move {
                service
                    .evict_local_sandbox(
                        &sandbox_id,
                        crate::service::EvictionClaim::SuspendedPinned(Some(artifact(STALE))),
                        None,
                    )
                    .await
            }
        });
        // The eviction holds the artifact lock and hasn't claimed the row.
        tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
            .await
            .expect("the eviction reaches its claim holding the lock");
        let at_lock = Arc::new(tokio::sync::Notify::new());
        *service
            .auto_lifecycle
            .artifact_lock_wait_hook
            .lock()
            .unwrap() = Some(Arc::clone(&at_lock));
        let mut admission = tokio::spawn(admit_backup(Arc::clone(&service), f.sandbox_id.clone()));
        tokio::time::timeout(std::time::Duration::from_secs(10), at_lock.notified())
            .await
            .expect("the admission reaches the artifact lock");
        // The admission waits for the lock the eviction holds: it can't
        // answer, however long it's given, until the eviction lets the lock go.
        // Without the eviction's lock it would answer at once.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(500), &mut admission)
                .await
                .is_err(),
            "the admission must wait for the eviction's artifact lock"
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
            .await
            .expect("the eviction resumes");

        let refused = tokio::time::timeout(std::time::Duration::from_secs(10), admission)
            .await
            .expect("the admission answers")
            .unwrap()
            .unwrap_err();

        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(refused.message().contains("being deleted"), "{refused}");
        assert!(
            service
                .snapshot_upload_worker
                .find_job_by_dedup_key(crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND, STALE)
                .unwrap()
                .is_none(),
            "no upload of the evicted copy is persisted"
        );
        cleanup_released.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), eviction)
            .await
            .expect("the eviction finishes")
            .unwrap()
            .unwrap();

        // Nor once the eviction's cleanup owns the row, which then reads as
        // the cleanup it's under rather than `deleting`.
        let (g, _) = stale_copy(false, Some(STALE));
        {
            let db = g.service.db.lock().unwrap();
            assert!(
                crate::db::claim_suspended_sandbox_for_eviction(
                    &db,
                    &g.sandbox_id,
                    Some(STALE),
                    None
                )
                .unwrap()
            );
            db.execute(
                "UPDATE sandboxes
                    SET cleanup_state = 'required', cleanup_completion = 'delete_row',
                        cleanup_keep_filesystem = 0
                  WHERE id = ?1",
                crate::db::fixture::params![g.sandbox_id],
            )
            .unwrap();
        }
        assert_eq!(g.row().state.cleanup(), Some(CleanupState::Required));
        let refused = g
            .service
            .enqueue_artifact_backup(internal_request(
                crate::proto::EnqueueArtifactBackupRequest {
                    sandbox_id: g.sandbox_id.clone(),
                    expected_artifact_kind: crate::proto::ArtifactKind::Full as i32,
                    source: Some(
                        crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                            STALE.to_string(),
                        ),
                    ),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(refused.message().contains("being deleted"), "{refused}");
    })
    .await;
}

impl Fixture {
    /// Give the sandbox a named snapshot beside its copy, with no backup
    /// stamp: the eviction would delete it with the copy.
    fn add_unstamped_named_snapshot(&self) {
        self.service
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO local_artifacts (
                     local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                     snapshot_name, local_path, created_at
                 ) VALUES (?1, ?2, 'disk', 'snapshot', 'd-named', '/space/cache/named', '0')",
                crate::db::fixture::params![NAMED, self.sandbox_id],
            )
            .unwrap();
    }

    fn named_snapshot_stamped(&self) -> Option<bool> {
        self.service
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT backup_completed_at IS NOT NULL FROM local_artifacts
                  WHERE local_artifact_id = ?1",
                crate::db::fixture::params![NAMED],
                |row| row.get(0),
            )
            .ok()
    }
}

/// Replacing the copy deletes every local artifact of the sandbox, so a named
/// snapshot beside it that nothing backed up holds the replacement off: it's
/// refused before any node work, and the copy and the snapshot stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unproven_named_snapshot_holds_off_the_replacement() {
    with_cluster_api(
        BackupReadiness::AndNamed {
            named_backed: false,
        },
        async || {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            f.add_unstamped_named_snapshot();

            let refused = f.pod_upgrade_error(operation()).await;

            assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
            assert!(
                refused.message().contains("another local artifact"),
                "{refused}"
            );
            f.assert_copy_untouched(Some(STALE), &cleanups);
            assert_eq!(f.named_snapshot_stamped(), Some(false));
        },
    )
    .await;
}

/// The control: once the cluster shows the named snapshot backed, it's
/// stamped and the copy is replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proven_named_snapshot_lets_the_replacement_go() {
    with_cluster_api(
        BackupReadiness::AndNamed { named_backed: true },
        async || {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            f.add_unstamped_named_snapshot();

            let error = f.pod_upgrade_error(operation()).await;

            assert!(error.message().contains("stage"), "{error}");
            f.assert_copy_evicted(&cleanups);
        },
    )
    .await;
}

/// The disk-pressure eviction deletes every local artifact of the sandbox
/// too: with a named snapshot nothing backed up beside a proven copy, the
/// tick skips the sandbox and leaves both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unproven_named_snapshot_holds_off_the_disk_pressure_eviction() {
    with_cluster_api(
        BackupReadiness::AndNamed {
            named_backed: false,
        },
        async || {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            f.add_unstamped_named_snapshot();

            let (evicted, skipped) = crate::sandbox_eviction::evict_tick_for_test(&f.service).await;

            assert_eq!((evicted, skipped), (0, 1));
            f.assert_copy_untouched(Some(STALE), &cleanups);
            assert_eq!(f.named_snapshot_stamped(), Some(false));
        },
    )
    .await;
}

/// The control: a named snapshot the cluster shows backed is stamped, and the
/// tick evicts the sandbox.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proven_named_snapshot_lets_the_disk_pressure_eviction_go() {
    with_cluster_api(
        BackupReadiness::AndNamed { named_backed: true },
        async || {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            f.add_unstamped_named_snapshot();

            let (evicted, skipped) = crate::sandbox_eviction::evict_tick_for_test(&f.service).await;

            assert_eq!((evicted, skipped), (1, 0));
            f.assert_copy_evicted(&cleanups);
        },
    )
    .await;
}

mod unrecorded;
