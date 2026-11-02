//! A retried activation asks the control plane whether its execution is still
//! current before it starts the runtime: kubelet retries a pod whose first
//! launch failed with the identity the failure kept open, after the control
//! plane may have retired that execution.

use super::*;

use pplx_space_common_core::types::{RouteGeneration, SandboxActivationAttemptId};
use pplx_space_node_api_client::proto::{
    ExecutionCurrency, GetExecutionCurrencyRequest, GetExecutionCurrencyResponse,
};

/// A cluster-api stub whose `GetExecutionCurrency` answers `currency` for
/// every execution.
#[derive(Clone, Copy)]
struct CurrencyClusterApi {
    currency: ExecutionCurrency,
}

impl tonic::server::NamedService for CurrencyClusterApi {
    const NAME: &'static str = "space.node.api.v1.ClusterApi";
}

impl tonic::server::UnaryService<GetExecutionCurrencyRequest> for CurrencyClusterApi {
    type Response = GetExecutionCurrencyResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;

    fn call(&mut self, _: tonic::Request<GetExecutionCurrencyRequest>) -> Self::Future {
        std::future::ready(Ok(tonic::Response::new(GetExecutionCurrencyResponse {
            currency: self.currency.into(),
        })))
    }
}

impl<B> tower::Service<http::Request<B>> for CurrencyClusterApi
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
        let path = request.uri().path().to_owned();
        let stub = *self;
        Box::pin(async move {
            if path.ends_with("/GetExecutionCurrency") {
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                Ok(grpc.unary(stub, request).await)
            } else {
                Ok(tonic::Status::unimplemented(path).into_http())
            }
        })
    }
}

/// A suspended sandbox whose restore of `GENERATION` began under a cluster-
/// fenced route identity, leaving the open claim a retry of that pod matches.
fn begun_restore() -> (
    SandboxManagerService,
    String,
    crate::db::route_activation_claims::OpenRouteActivationClaim,
    pplx_space_common_core::types::SpaceApiRouteIdentity,
) {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Suspended);
    // The suspend's capture, which a failed restore's cleanup keeps.
    crate::service::tests::service_tests::register_full_suspend_artifact(&conn, &id);
    let (claim, _, route) = begin_restore(&conn, &id, None);
    (SandboxManagerService::for_test(conn), id, claim, route)
}

/// Begin a restore of the suspended row `id` at `GENERATION` under a fresh
/// cluster-fenced route identity, and the operation attempt `operation` when
/// an API-driven wake names one apart from the route's, returning its open
/// claim, episode and route identity.
fn begin_restore(
    conn: &crate::db::Connection,
    id: &str,
    operation: Option<SandboxActivationAttemptId>,
) -> (
    crate::db::route_activation_claims::OpenRouteActivationClaim,
    crate::db::CleanupEpisode,
    pplx_space_common_core::types::SpaceApiRouteIdentity,
) {
    let attempt = SandboxActivationAttemptId::new();
    let route = pplx_space_common_core::types::SpaceApiRouteIdentity {
        sandbox_id: Some(id.parse().unwrap()),
        route_generation: Some(RouteGeneration::new(GENERATION as u64).unwrap()),
        execution_generation: Some(generation(GENERATION)),
        current_node_id: Some(crate::service::test_options::test_node_id()),
        substrate_kind: Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes),
        activation_attempt_id: Some(attempt),
        pod_uid: Some("retried-pod".parse().unwrap()),
    };
    let provenance = crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced;
    let claim =
        crate::db::route_activation_claims::OpenRouteActivationClaim::for_cluster_fenced_identity(
            &route, provenance,
        )
        .unwrap();
    let episode = crate::db::begin_activation_with_route_evidence(
        conn,
        id,
        SandboxState::Suspended,
        crate::db::ActivationKind::Restore,
        Some(generation(GENERATION)),
        Some(attempt),
        crate::db::ActivationRouteAdmission {
            evidence: crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(
                &route, provenance,
            ),
            open_claim: Some(&claim),
            operation_activation_attempt_id: operation,
            ..Default::default()
        },
    )
    .unwrap()
    .expect("the restore claims the suspended row");
    (claim, episode, route)
}

/// A restore of `GENERATION` whose prepare failed: the node's cleanup rolled
/// the row back to `suspended`, recorded the rollback receipt the control
/// plane applies to retire the execution, and kept the claim open, so
/// kubelet's retry of the same pod matches it.
fn rolled_back_restore(
    operation: Option<SandboxActivationAttemptId>,
) -> (
    SandboxManagerService,
    String,
    crate::db::route_activation_claims::OpenRouteActivationClaim,
) {
    let conn = crate::db::test_conn();
    let (id, _) =
        crate::db::test_fixtures::state_write_fixtures::insert_suspended_with_artifact(&conn);
    let (claim, episode, _) = begin_restore(&conn, &id, operation);
    crate::db::request_cleanup_episode(
        &conn,
        &id,
        episode.id,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed,
        "restore prepare failed",
    )
    .unwrap();
    crate::db::complete_cleanup_episode(&conn, &id, episode.id)
        .unwrap()
        .expect("the rollback completes");
    assert_eq!(
        crate::db::get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxState::Suspended
    );
    let view = crate::db::read::sandbox_view(&conn, &id);
    assert!(
        view.route_claim().matches(&claim).unwrap(),
        "the rollback keeps the claim open"
    );
    assert!(view.route_claim().rolled_back(&claim).unwrap());
    (SandboxManagerService::for_test(conn), id, claim)
}

/// Asks whether kubelet's retry of [`rolled_back_restore`]'s pod may start.
async fn retry_of_rolled_back_restore() -> Result<(), tonic::Status> {
    let (service, id, claim) = rolled_back_restore(None);
    service
        .refuse_retired_activation_retry(&id, Some(&claim))
        .await
}

/// Asks whether a retry of [`begun_restore`] may start.
async fn retry_of_begun_restore() -> Result<(), tonic::Status> {
    let (service, id, claim, _) = begun_restore();
    service
        .refuse_retired_activation_retry(&id, Some(&claim))
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_of_a_retired_execution_does_not_start() {
    let retired = CurrencyClusterApi {
        currency: ExecutionCurrency::Retired,
    };
    with_cluster_api(retired, async || {
        let error = retry_of_begun_restore()
            .await
            .expect_err("the retired execution's runtime must not start");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    })
    .await;
}

/// With no rollback behind it, only an answer that the execution was retired
/// holds a retry back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_of_a_current_execution_starts() {
    let current = CurrencyClusterApi {
        currency: ExecutionCurrency::Current,
    };
    with_cluster_api(current, async || {
        retry_of_begun_restore()
            .await
            .expect("a current execution's retry starts");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_the_cluster_cannot_place_starts() {
    let unknown = CurrencyClusterApi {
        currency: ExecutionCurrency::Unspecified,
    };
    with_cluster_api(unknown, async || {
        retry_of_begun_restore()
            .await
            .expect("no answer starts the retry as before");
    })
    .await;
}

/// A control plane that predates the question answers `Unimplemented`, and
/// the retry starts as it did before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_against_a_control_plane_without_the_question_starts() {
    with_cluster_api(RETIRING, async || {
        retry_of_begun_restore()
            .await
            .expect("an unanswered question starts the retry as before");
    })
    .await;
}

/// An activation with no open claim of its own is a first one, which the
/// control plane isn't asked about.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_activation_is_not_asked() {
    let retired = CurrencyClusterApi {
        currency: ExecutionCurrency::Retired,
    };
    with_cluster_api(retired, async || {
        let (service, _, claim, _) = begun_restore();
        let other = SandboxId::new().to_string();
        crate::db::fixture::insert_test_sandbox(
            &service.db.lock().unwrap(),
            &other,
            SandboxState::Suspended,
        );
        service
            .refuse_retired_activation_retry(&other, Some(&claim))
            .await
            .expect("a first activation starts unasked");
    })
    .await;
}

/// A retried in-place restore of a retired execution is refused before it
/// stages its snapshot, so the retries kubelet repeats until the pod is
/// deleted never begin a restore.
///
/// The test holds the sandbox's staging lock, so a request that reached
/// staging would wait out the admission cap and fail `Unavailable` there,
/// before any snapshot lookup or volume-manager call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_checkpoint_restore_of_a_retired_execution_is_refused_before_staging() {
    let retired = CurrencyClusterApi {
        currency: ExecutionCurrency::Retired,
    };
    with_cluster_api(retired, async || {
        // The restore's first attempt failed, and its runtime cleanup rolled
        // the row back to `suspended` with the claim kept open for the retry.
        let (service, id, _, route) = begun_restore();
        {
            let db = service.db.lock().unwrap();
            let episode = crate::db::cleanup_episode(&db, &id).unwrap().unwrap();
            crate::db::request_cleanup_episode(
                &db,
                &id,
                episode.id,
                pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed,
                "restore prepare failed",
            )
            .unwrap();
            assert!(
                crate::db::complete_cleanup_episode(&db, &id, episode.id)
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Suspended
        );
        let _staging = Arc::clone(&service.auto_lifecycle.staging_lock(&id))
            .lock_owned()
            .await;
        let error =
            service
                .start_sandbox(
                    pplx_space_sbx_mgr_client::proto::StartRequest {
                        sandbox_id: id,
                        checkpoint: Some("suspend".to_string()),
                        route_identity:
                            crate::service::activation_support::space_api_route_identity_to_wire(
                                &route,
                            ),
                        ..Default::default()
                    },
                    false,
                )
                .await
                .expect_err("the retired execution's runtime must not start");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("was retired"), "{error}");
    })
    .await;
}

/// The relaunch a rollback must not allow: the node rolled the restore back,
/// and kubelet's retry of the same pod reaches a control plane that can't say
/// the execution is current. Started, its runtime would run in a pod nothing
/// routes to, for an execution the control plane retires when it applies the
/// receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_this_node_rolled_back_needs_the_control_plane_to_say_it_is_current() {
    let unknown = CurrencyClusterApi {
        currency: ExecutionCurrency::Unspecified,
    };
    with_cluster_api(unknown, async || {
        let error = retry_of_rolled_back_restore()
            .await
            .expect_err("a rolled-back activation's runtime must not start unconfirmed");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    })
    .await;
}

/// A control plane that predates the question can't confirm it either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_this_node_rolled_back_does_not_start_unanswered() {
    with_cluster_api(RETIRING, async || {
        let error = retry_of_rolled_back_restore()
            .await
            .expect_err("an unanswered question must not start a rolled-back activation");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_this_node_rolled_back_does_not_start_once_retired() {
    let retired = CurrencyClusterApi {
        currency: ExecutionCurrency::Retired,
    };
    with_cluster_api(retired, async || {
        let error = retry_of_rolled_back_restore()
            .await
            .expect_err("the retired execution's runtime must not start");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    })
    .await;
}

/// Before the control plane applies the rollback receipt, the execution is
/// still current, and the retry is the continuation the open claim keeps
/// possible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_this_node_rolled_back_starts_while_still_current() {
    let current = CurrencyClusterApi {
        currency: ExecutionCurrency::Current,
    };
    with_cluster_api(current, async || {
        retry_of_rolled_back_restore()
            .await
            .expect("a current execution's retry starts");
    })
    .await;
}

/// An API-driven wake names its operation's attempt apart from the route
/// attempt the claim holds, and the rollback receipt records the operation's.
/// The retry of that claim still follows the rollback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_activation_rolled_back_under_an_operation_attempt_does_not_start_unconfirmed() {
    let unknown = CurrencyClusterApi {
        currency: ExecutionCurrency::Unspecified,
    };
    with_cluster_api(unknown, async || {
        let operation = SandboxActivationAttemptId::new();
        let (service, id, claim) = rolled_back_restore(Some(operation));
        {
            // The receipt records the operation's attempt, not the route
            // attempt the claim holds.
            let metadata = crate::db::sandbox_state_publication_metadata(
                &service.db.lock().unwrap(),
                std::slice::from_ref(&id),
            )
            .unwrap()
            .remove(&id)
            .unwrap()
            .unwrap()
            .decode(SandboxState::Suspended)
            .unwrap();
            assert!(
                matches!(&metadata, crate::db::SandboxStatePublicationMetadata::TransitionReceipt(receipt)
                    if receipt.activation_attempt_id() == Some(operation)),
                "{metadata:?}"
            );
        }
        let error = service
            .refuse_retired_activation_retry(&id, Some(&claim))
            .await
            .expect_err("a rolled-back activation's runtime must not start unconfirmed");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    })
    .await;
}

/// A rollback receipt for another generation is no record that this claim's
/// activation was rolled back, so an unanswered question starts the retry as
/// it would with no receipt at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rollback_receipt_for_another_generation_does_not_hold_a_retry_back() {
    let unknown = CurrencyClusterApi {
        currency: ExecutionCurrency::Unspecified,
    };
    with_cluster_api(unknown, async || {
        let (service, id, claim) = rolled_back_restore(None);
        {
            let db = service.db.lock().unwrap();
            let rolled_back = |db: &crate::db::Connection| {
                crate::db::read::sandbox_view(db, &id)
                    .route_claim()
                    .rolled_back(&claim)
                    .unwrap()
            };
            assert!(rolled_back(&db));
            db.execute_batch(&format!(
                "UPDATE sandboxes SET transition_receipt_execution_generation = {}
                  WHERE id = '{id}'",
                GENERATION - 1
            ))
            .unwrap();
            assert!(!rolled_back(&db), "the receipt names another generation");
            assert!(
                crate::db::read::sandbox_view(&db, &id)
                    .route_claim()
                    .matches(&claim)
                    .unwrap(),
                "the claim still matches, so the retry is asked about"
            );
        }
        service
            .refuse_retired_activation_retry(&id, Some(&claim))
            .await
            .expect("another generation's rollback doesn't hold this retry back");
    })
    .await;
}

/// A retried restore whose first attempt's rollback cleanup is still running
/// when the retry arrives: the cleanup holds the activation lock, has not yet
/// written its receipt, and finishes only once the retry waits on that lock.
/// The retry asks under the lock, so it sees the receipt the cleanup just
/// wrote and is refused before it claims the row. Asked before the lock, it
/// would see no receipt, start on the unanswered question, and claim the row
/// the rollback had just returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_waiting_on_a_rollback_cleanup_sees_its_receipt() {
    use crate::node_manager_transport::fake::FakeNodeManager;
    use pplx_space_common_core::types::{
        Arch, AutoLifecyclePolicy, ContentHash, RuntimeKind, SandboxArtifactMetadata, SandboxOwner,
        SandboxRuntimeLabel, SnapshotExecutionMetadata,
    };
    use pplx_space_node_mgr_client::proto::{
        AllocateRequest, AllocateResponse, ExecutionEnvironmentSelection, NetworkPoolStatusRequest,
        NetworkPoolStatusResponse, ResolveExecutionEnvironmentRequest,
        ResolveExecutionEnvironmentResponse,
    };
    let unknown = CurrencyClusterApi {
        currency: ExecutionCurrency::Unspecified,
    };
    with_cluster_api(unknown, async || {
        let compiled =
            pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
                Arch::X86_64,
            )
            .unwrap();
        let definition = compiled[0].definition().clone();
        let producer = definition.execution_requirements().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let cache = directory.path().join("cache");
        let db_path = directory.path().join("sandbox.sqlite");
        crate::db::create_test_schema_on_disk(&db_path);
        let conn = crate::db::Connection::open_at(&db_path).unwrap();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        let template_hash: ContentHash = format!("sha256:{}", "ab".repeat(32)).parse().unwrap();
        crate::db::insert_sandbox(
            &conn,
            crate::db::InsertSandboxParams {
                id: &id,
                uuid8: &SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
                state: SandboxState::Suspended,
                template: "active-template",
                template_hash: Some(&template_hash.to_string()),
                user: &SandboxOwner::parse("user").unwrap(),
                runtime: RuntimeKind::Firecracker.alias(),
                guest_image: Default::default(),
                snapshots_enabled: false,
                rootfs_size_mb: Some(2048),
                disk_io_limits: Default::default(),
                // A pool member skips the effective network policy lookup.
                pool_template: Some("pool-template"),
                volume_encryption: false,
                process_env: &Default::default(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: None,
                network_policy_generation: 0,
                network_policy_authority: crate::db::PolicyAuthority::Cluster,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
            },
        )
        .unwrap();
        crate::db::upsert_sandbox_execution_metadata(&conn, &id, &producer).unwrap();
        crate::service::tests::service_tests::register_full_suspend_artifact(&conn, &id);
        let instance = cache.join(&id).join("instance");
        let snapshot = cache.join(&id).join("snapshot");
        std::fs::create_dir_all(&instance).unwrap();
        std::fs::create_dir_all(&snapshot).unwrap();
        std::fs::write(instance.join("rootfs.ext4"), []).unwrap();
        std::fs::write(
            instance.parent().unwrap().join("metadata.json"),
            serde_json::to_vec(&SandboxArtifactMetadata::new(
                sandbox_id,
                producer.clone(),
                "active-template".parse().unwrap(),
                Some(template_hash),
            ))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            snapshot.join("runtime.json"),
            serde_json::to_vec(&SnapshotExecutionMetadata::new(producer.clone()).unwrap()).unwrap(),
        )
        .unwrap();

        // The suspended row the retry starts from, as kubelet's retry sees it.
        let row = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
        // The first attempt claimed the row, failed, and requested its
        // rollback cleanup, which hasn't completed.
        let (claim, episode, route) = begin_restore(&conn, &id, None);
        crate::db::request_cleanup_episode(
            &conn,
            &id,
            episode.id,
            pplx_space_common_core::types::SandboxQuarantineReasonCode::RestorePrepareFailed,
            "restore prepare failed",
        )
        .unwrap();

        let allocations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_allocations = allocations.clone();
        let selected = producer.clone();
        let node = FakeNodeManager::default()
            .unary("GetNetworkPoolStatus", |_: NetworkPoolStatusRequest| {
                Ok(NetworkPoolStatusResponse {
                    sandbox_cgroup_memory_mib: 4096,
                    sandbox_guest_memory_max_mib: 3968,
                    sandbox_memory_headroom_mib: 128,
                    ..Default::default()
                })
            })
            .unary(
                "ResolveExecutionEnvironment",
                move |_: ResolveExecutionEnvironmentRequest| {
                    Ok(ResolveExecutionEnvironmentResponse {
                        accepted_selection: ExecutionEnvironmentSelection::Restore as i32,
                        definition: Some((&definition).into()),
                        execution: Some((&selected).into()),
                    })
                },
            )
            .unary("Allocate", move |_: AllocateRequest| {
                observed_allocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(AllocateResponse::default())
            })
            .serve();
        let service = SandboxManagerService::new_with_options(
            conn,
            crate::service::SandboxManagerServiceOptions {
                node_manager: node.connector(),
                create_cache_dir: Some(cache),
                ..crate::service::SandboxManagerServiceOptions::for_test()
            },
        );

        // The rollback cleanup holds the activation lock while it runs.
        let lock = service.cleanup_lock(&id);
        let cleanup = Arc::clone(&lock).lock_owned().await;
        let holders = Arc::strong_count(&lock);
        let retry =
            service.start_from_checkpoint(crate::service::create::StartFromCheckpointLaunch {
                vcpu: None,
                activation_auto_lifecycle: None,
                operation_activation_attempt_id: None,
                sandbox: &row,
                blocking: false,
                netns_path: None,
                target_state: SandboxState::Started,
                execution_generation: route.execution_generation,
                space_api_route_identity: route.clone(),
                route_identity_provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
                open_activation_claim_eligible: true,
                restored_snapshot: Default::default(),
            });
        let finish_cleanup = async {
            // The retry takes its own handle on the lock just before it waits
            // for it, and keeps it while it waits; only then does the cleanup
            // finish and let it in. Requiring the extra holder over several
            // polls ignores a passing clone, such as a try-lock elsewhere.
            tokio::time::timeout(Duration::from_millis(4500), async {
                let mut held = 0;
                while held < 5 {
                    held = if Arc::strong_count(&lock) > holders {
                        held + 1
                    } else {
                        0
                    };
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the retry waits on the activation lock");
            {
                let db = service.db.lock().unwrap();
                assert!(
                    !crate::db::read::sandbox_view(&db, &id)
                        .route_claim()
                        .rolled_back(&claim)
                        .unwrap(),
                    "no receipt before the cleanup completes"
                );
                crate::db::complete_cleanup_episode(&db, &id, episode.id)
                    .unwrap()
                    .expect("the rollback completes");
            }
            drop(cleanup);
        };
        let (outcome, ()) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(retry, finish_cleanup)
        })
        .await
        .unwrap();

        let error = outcome.expect_err("the rolled-back activation's runtime must not start");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("rolled back"), "{error}");
        assert_eq!(
            allocations.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the retry must not claim the row"
        );
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            SandboxState::Suspended
        );
        service.shutdown_token.cancel();
        service.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(20), service.task_tracker.wait())
            .await
            .unwrap();
    })
    .await;
}
