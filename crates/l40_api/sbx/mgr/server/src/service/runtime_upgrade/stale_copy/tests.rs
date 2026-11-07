//! A runtime upgrade meeting a stale suspended copy, through `UpgradeRuntime`
//! and the Wake, against a cluster-api stub that answers the backup proof and
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
use crate::state::SandboxState;

/// The checkpoint the node's stale copy pins.
const STALE: &str = "0192f0c0-0000-7000-8000-00000000a001";
/// The checkpoint the upgrade was decided from, whose id sorts after
/// [`STALE`]'s.
const DECIDED: &str = "0192f0c0-0000-7000-8000-00000000a002";
/// A checkpoint whose id sorts after [`DECIDED`]'s.
const NEWER: &str = "0192f0c0-0000-7000-8000-00000000a003";

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
        let Self::Backs {
            backed,
            captured_after,
        } = self
        else {
            return Err(Status::unavailable("cluster-api is restarting"));
        };
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
        assert_eq!(row.state, SandboxState::Suspended);
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

/// The Wake replaces the copy the same way, before it stages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wake_upgrade_evicts_a_backed_up_stale_copy_before_staging() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));

        let error = f.wake_error(decided_wake(&f)).await;

        assert!(!error.message().contains("decided from"), "{error}");
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

/// A copy nothing proves backed up may be the only copy of something: it is
/// refused before any node work, through either caller. The stub backs the
/// decided checkpoint, so only a proof of the copy's own pin counts. An
/// unpinned copy has no artifact to prove.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unproven_stale_copy_is_refused_before_any_node_work() {
    with_cluster_api(BACKS_ONLY_DECIDED, async || {
        for pin in [Some(STALE), None] {
            let (f, cleanups) = stale_copy(false, pin);
            for refused in [
                f.pod_upgrade_error(operation()).await,
                f.wake_error(decided_wake(&f)).await,
            ] {
                assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
                assert!(refused.message().contains("backed up"), "{refused}");
                f.assert_copy_untouched(pin, &cleanups);
            }
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

/// The Wake replaces a copy only when it can stage the decided checkpoint in
/// its place, and never for an implicit wake, whose policy isn't known
/// before the Wake lock: both leave even a backed-up copy where it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wake_that_cant_stage_or_is_implicit_leaves_the_copy() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let unstageable = |request: &mut crate::proto::WakeRequest| {
            let context = request.start_context.as_mut().unwrap();
            context.suspend_restore_storage_uri = None;
            context.suspend_restore_source_node = None;
        };
        let implicit = |request: &mut crate::proto::WakeRequest| {
            request.implicit_wake_source = crate::proto::AutoLifecycleWakeSource::Access as i32;
        };
        for shape in [
            &unstageable as &dyn Fn(&mut crate::proto::WakeRequest),
            &implicit,
        ] {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            let mut request = decided_wake(&f);
            shape(&mut request);

            let refused = f.wake_error(request).await;

            assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
            f.assert_copy_untouched(Some(STALE), &cleanups);
        }
    })
    .await;
}

/// A request whose stage would be refused stages nothing in the copy's
/// place, so even a backed-up copy stays: a malformed storage URI, through
/// either caller, or a route identity the stage couldn't admit once the row
/// is gone (bound to another sandbox, naming another node, or missing), is
/// refused before the copy is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_that_cant_stage_leaves_the_copy() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        const MALFORMED: &str = "not a storage uri";
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let mut pod = staged_pod_upgrade(&f, &f.target_environment(), operation());
        pod.stage_from.as_mut().unwrap().suspend_restore_storage_uri = MALFORMED.into();
        let mut wake = decided_wake(&f);
        wake.start_context
            .as_mut()
            .unwrap()
            .suspend_restore_storage_uri = Some(MALFORMED.into());

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
            (f.wake_error(wake).await, tonic::Code::InvalidArgument),
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

/// Both callers refuse to replace the copy pinning `pin`, backed up as it
/// is, saying `says`, before any node work.
async fn assert_left_in_place(encrypted: bool, pin: &'static str, says: &str) {
    let (f, cleanups) = stale_copy(encrypted, Some(pin));
    for refused in [
        f.pod_upgrade_error(operation()).await,
        f.wake_error(decided_wake(&f)).await,
    ] {
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(refused.message().contains(says), "{refused}");
        f.assert_copy_untouched(Some(pin), &cleanups);
    }
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
/// captured by the later execution, so it stays, through either caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newer_local_copy_is_never_replaced_whatever_the_clocks_say() {
    let copy_captured_later = BackupReadiness::Backs {
        backed: STALE,
        captured_after: Some((STALE, DECIDED)),
    };
    with_cluster_api(copy_captured_later, async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        assert!(artifact(STALE).as_uuid() < artifact(DECIDED).as_uuid());

        for refused in [
            f.pod_upgrade_error(operation()).await,
            f.wake_error(decided_wake(&f)).await,
        ] {
            assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
            assert!(refused.message().contains("captured after"), "{refused}");
            f.assert_copy_untouched(Some(STALE), &cleanups);
        }
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
