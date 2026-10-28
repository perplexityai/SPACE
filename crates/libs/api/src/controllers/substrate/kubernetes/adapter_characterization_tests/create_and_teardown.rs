//! Refusal and failure arms of `create_execution` and `teardown_execution`.

use axum::http::{Method, StatusCode};

use super::super::scripted_test_doubles::{
    KubeLog, ScriptedReply, ScriptedWorkerConfig, kube_status, pod_path, pod_reply,
    requests_matching, scripted_kube_client, start_scripted_worker,
};
use super::super::tests::{
    insert_test_sandbox_row, node_with_network_slots, sandbox_pod_on_node,
    substrate_with_inventory_caches_and_pool, substrate_with_store_and_client,
    substrate_with_store_client_proxy_port_and_pod,
};
use super::*;
use crate::controllers::substrate::{PodIdentityFence, RecordedExecution};
use pplx_space_common_core::types::{PodName, PodResourceVersion, PodUid};

// ── create_execution ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum CreateBackpressure {
    CancelStorm,
    NetworkSlotsFull,
}

/// Admission backpressure refuses a create as retryable before it claims an
/// execution row or touches the apiserver.
#[rstest::rstest]
#[case::cancel_storm(CreateBackpressure::CancelStorm)]
#[case::network_slots_full(CreateBackpressure::NetworkSlotsFull)]
#[tokio::test(flavor = "current_thread")]
async fn create_execution_backpressure_refuses_before_claiming_an_execution(
    #[case] backpressure: CreateBackpressure,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let mut substrate = match backpressure {
        CreateBackpressure::CancelStorm => {
            substrate_with_inventory_caches_and_pool(Vec::new(), Vec::new(), db.pool.clone())
        }
        CreateBackpressure::NetworkSlotsFull => {
            let labels = SandboxPodConfigValues::default()
                .base_node_selector()
                .into_labels();
            substrate_with_inventory_caches_and_pool(
                vec![
                    (*sandbox_pod_on_node("space-sbx-neighbor-g1", WORKER_NODE, 1, "Running"))
                        .clone(),
                ],
                vec![
                    (*node_with_network_slots(WORKER_NODE, labels, Some(1), Some(1), true, false))
                        .clone(),
                ],
                db.pool.clone(),
            )
        }
    };
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    substrate.k8s.client = client;
    if let CreateBackpressure::CancelStorm = backpressure {
        // The tracker counts cancellations over a trailing one-second window
        // of its own clock, so the one recorded here is still counted when
        // the create checks it a few milliseconds later; only a runner stalled
        // for a full second between these lines could miss it.
        let tracker = CancelRateTracker::with_budget(0);
        tracker.record_cancel();
        substrate.cancel_tracker = Arc::new(tracker);
    }
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();

    let outcome = substrate
        .create_execution(inputs.create_request(sandbox_id, &template_ref))
        .await;

    assert!(
        matches!(outcome, Err(SubstrateError::Unavailable { .. })),
        "{backpressure:?}: expected a retryable refusal, got {:?}",
        outcome.map(|outcome| outcome.sandbox_id)
    );
    let executions: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        executions, 0,
        "{backpressure:?}: no execution may be claimed"
    );
    assert!(
        log.lock().unwrap().is_empty(),
        "{backpressure:?}: the apiserver must not be contacted"
    );
    Ok(())
}

/// A pod create the apiserver rejects fails the create and releases the
/// execution it had claimed, so the sandbox is not left bound to a pod that
/// never existed.
#[tokio::test(flavor = "current_thread")]
async fn create_execution_releases_tracking_when_the_pod_create_is_rejected() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let (client, log) = scripted_kube_client(|request| match request.method {
        Method::POST => kube_status(StatusCode::FORBIDDEN, "Forbidden"),
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();

    let outcome = substrate
        .create_execution(inputs.create_request(sandbox_id, &template_ref))
        .await;

    assert!(
        matches!(outcome, Err(SubstrateError::Failed { .. })),
        "expected a failed create, got {:?}",
        outcome.map(|outcome| outcome.sandbox_id)
    );
    assert!(
        !requests_matching(&log, &Method::POST).is_empty(),
        "the pod create must have been attempted"
    );
    assert_eq!(
        active_execution_count(&db.pool, sandbox_id).await?,
        0,
        "the claimed execution must be released"
    );
    Ok(())
}

/// A cold create seeds the pod's network policy on the generation lineage
/// node-ctl reconciles the sandbox with: an inherited policy at 0, below the
/// sandbox's generation, and an override at the sandbox's own generation,
/// never at the template's counter.
#[rstest::rstest]
#[case::inherited(false, "0")]
#[case::override_policy(true, "2")]
#[tokio::test(flavor = "current_thread")]
async fn cold_create_seeds_the_network_policy_generation_on_the_sandbox_lineage(
    #[case] has_override: bool,
    #[case] seeded_generation: &str,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    // The pod is recorded, then refused, so the create stops at the POST.
    let (client, log) = scripted_kube_client(|request| match request.method {
        Method::POST => kube_status(StatusCode::FORBIDDEN, "Forbidden"),
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let override_policy = NetworkPolicyV1::allow_all();
    let request = CreateExecutionRequest {
        network_policy_override: has_override.then_some(&override_policy),
        network_policy_generation: 2,
        template_network_policy_generation: 3,
        ..inputs.create_request(sandbox_id, &template_ref)
    };

    let _ = substrate.create_execution(request).await;

    let posts = requests_matching(&log, &Method::POST);
    let [pod_create] = posts.as_slice() else {
        panic!("expected one pod create, got {posts:?}");
    };
    let annotation = pod_create
        .body
        .as_ref()
        .and_then(|pod| pod.pointer("/metadata/annotations"))
        .and_then(|annotations| {
            annotations.get(
                pplx_space_common_core::kubernetes::SANDBOX_NETWORK_POLICY_GENERATION_ANNOTATION,
            )
        })
        .and_then(serde_json::Value::as_str);
    assert_eq!(annotation, Some(seeded_generation));
    Ok(())
}

// ── teardown_execution ──────────────────────────────────────────────────────

/// Apiserver for teardown: the pod is live in the reflector, DELETEs succeed.
fn teardown_kube_client() -> (Client, KubeLog) {
    scripted_kube_client(|request| match request.method {
        Method::DELETE => kube_status(StatusCode::OK, "Success"),
        _ => kube_status(StatusCode::NOT_FOUND, "NotFound"),
    })
}

async fn retire(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
) -> Result<RetireExecutionOutcome, SubstrateError> {
    substrate
        .retire_execution(RetireExecutionRequest {
            sandbox_id,
            fence: RetireFence::Unobserved,
        })
        .await
}

#[derive(Debug, Clone, Copy)]
enum TeardownCase {
    /// The pod worker cannot be dialed, so the in-VM stop cannot be sent.
    RetireWorkerUnreachable,
    RetireStopFails,
    RetireBackupFails,
    DeleteDefersToSuspendPublication,
    DeleteRefusesRetainedOnlyCopy,
}

/// A teardown whose node-side preservation step fails or is refused keeps the
/// execution tracked and the pod in place, so the retry owner can find both.
#[rstest::rstest]
#[case::retire_worker_unreachable(TeardownCase::RetireWorkerUnreachable)]
#[case::retire_stop_fails(TeardownCase::RetireStopFails)]
#[case::retire_backup_fails(TeardownCase::RetireBackupFails)]
#[case::delete_defers_to_suspend_publication(TeardownCase::DeleteDefersToSuspendPublication)]
#[case::delete_refuses_retained_only_copy(TeardownCase::DeleteRefusesRetainedOnlyCopy)]
#[tokio::test(flavor = "current_thread")]
async fn teardown_keeps_execution_tracked_when_the_node_cannot_finish(
    #[case] case: TeardownCase,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-teardown-g1";
    // The unreachable case's worker sits at an address nothing answers on;
    // every other case dials the scripted worker.
    let pod_ip = match case {
        TeardownCase::RetireWorkerUnreachable => UNDIALED_POD_IP,
        _ => "127.0.0.1",
    };
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            pod_ip: Some(pod_ip),
            ..ExecutionSeed::running(pod_name, "teardown-uid")
        },
    )
    .await?;
    let reply = match case {
        TeardownCase::RetireWorkerUnreachable => None,
        TeardownCase::RetireStopFails => Some((
            "WorkerApi/StopSandbox",
            ScriptedReply::Status(tonic::Status::internal("scripted stop failure")),
        )),
        TeardownCase::RetireBackupFails => Some((
            "WorkerApi/EnqueueSandboxArtifactBackup",
            ScriptedReply::Status(tonic::Status::internal("scripted backup failure")),
        )),
        TeardownCase::DeleteDefersToSuspendPublication => Some((
            "WorkerApi/DeleteSandbox",
            ScriptedReply::Status(
                pplx_space_sbx_mgr_client::suspend_publication_pending_status(
                    "scripted suspend publication pending",
                ),
            ),
        )),
        TeardownCase::DeleteRefusesRetainedOnlyCopy => Some((
            "WorkerApi/DeleteSandbox",
            ScriptedReply::Status(
                pplx_space_sbx_mgr_client::retained_filesystem_only_copy_status(
                    "scripted retained filesystem",
                ),
            ),
        )),
    };
    let worker = start_scripted_worker(ScriptedWorkerConfig::new(reply)).await?;
    worker
        .sim
        .ensure_default_running(&sandbox_id.to_string())
        .await?;
    let (client, log) = teardown_kube_client();
    let mut substrate = substrate_with_store_client_proxy_port_and_pod(
        db.pool.clone(),
        client,
        worker.port,
        Some(sandbox_pod(
            pod_name,
            "teardown-uid",
            sandbox_id,
            1,
            "Running",
            Some(pod_ip),
        )),
    );
    if let TeardownCase::RetireWorkerUnreachable = case {
        // The TEST-NET dial never connects. Where the host silently drops the
        // SYN, this budget ends it, and the bound below proves the substrate's
        // budget rather than the production constant is the one teardown
        // uses. Where the host answers `ENETUNREACH`, the dial fails at once
        // and the bound holds trivially.
        substrate.budgets.teardown_worker_connect = Duration::from_millis(300);
    }

    let started = Instant::now();
    let outcome = match case {
        TeardownCase::RetireWorkerUnreachable
        | TeardownCase::RetireStopFails
        | TeardownCase::RetireBackupFails => retire(&substrate, sandbox_id).await.map(|_| ()),
        TeardownCase::DeleteDefersToSuspendPublication
        | TeardownCase::DeleteRefusesRetainedOnlyCopy => {
            substrate.delete_execution(sandbox_id).await
        }
    };

    match (case, &outcome) {
        (
            TeardownCase::RetireWorkerUnreachable
            | TeardownCase::RetireStopFails
            | TeardownCase::RetireBackupFails,
            Err(SubstrateError::Failed { .. }),
        )
        | (
            TeardownCase::DeleteDefersToSuspendPublication,
            Err(SubstrateError::SuspendInFlight { .. }),
        )
        | (
            TeardownCase::DeleteRefusesRetainedOnlyCopy,
            Err(SubstrateError::RetainedFilesystemOnlyCopy { .. }),
        ) => {}
        (case, outcome) => panic!("{case:?}: unexpected teardown outcome {outcome:?}"),
    }
    if let TeardownCase::RetireWorkerUnreachable = case {
        assert!(
            started.elapsed() < POD_WORKER_CONNECT_TIMEOUT,
            "the substrate's teardown connect budget, not the production one, must end the dial"
        );
    }
    assert!(
        requests_matching(&log, &Method::DELETE).is_empty(),
        "{case:?}: the pod must not be deleted"
    );
    assert_eq!(
        active_execution_count(&db.pool, sandbox_id).await?,
        1,
        "{case:?}: the execution must stay tracked for retry"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum BestEffortNodeFailure {
    /// Delete's node-local delete fails for an ordinary reason.
    DeleteFails,
    /// Retire's stopped-disk backup is moot: the node has no sandbox row.
    RetireBackupMoot,
}

/// A node-side step that is best-effort for the teardown mode does not block
/// it: the pod is deleted and tracking released.
#[rstest::rstest]
#[case::delete_fails(BestEffortNodeFailure::DeleteFails)]
#[case::retire_backup_moot(BestEffortNodeFailure::RetireBackupMoot)]
#[tokio::test(flavor = "current_thread")]
async fn teardown_continues_past_a_best_effort_node_failure(
    #[case] failure: BestEffortNodeFailure,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-teardown-continue-g1";
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            pod_ip: Some("127.0.0.1"),
            ..ExecutionSeed::running(pod_name, "teardown-continue-uid")
        },
    )
    .await?;
    let (failed_method, status) = match failure {
        BestEffortNodeFailure::DeleteFails => (
            "WorkerApi/DeleteSandbox",
            tonic::Status::internal("scripted delete failure"),
        ),
        BestEffortNodeFailure::RetireBackupMoot => (
            "WorkerApi/EnqueueSandboxArtifactBackup",
            tonic::Status::not_found("scripted no sandbox"),
        ),
    };
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([(
        failed_method,
        ScriptedReply::Status(status),
    )]))
    .await?;
    worker
        .sim
        .ensure_default_running(&sandbox_id.to_string())
        .await?;
    let (client, log) = teardown_kube_client();
    let substrate = substrate_with_store_client_proxy_port_and_pod(
        db.pool.clone(),
        client,
        worker.port,
        Some(sandbox_pod(
            pod_name,
            "teardown-continue-uid",
            sandbox_id,
            1,
            "Running",
            Some("127.0.0.1"),
        )),
    );

    match failure {
        BestEffortNodeFailure::DeleteFails => substrate.delete_execution(sandbox_id).await?,
        BestEffortNodeFailure::RetireBackupMoot => assert_eq!(
            retire(&substrate, sandbox_id).await?,
            RetireExecutionOutcome::Retired
        ),
    }

    assert!(
        !worker.received(failed_method).is_empty(),
        "{failure:?}: the failing step must have run"
    );
    let deletes = requests_matching(&log, &Method::DELETE);
    assert!(
        deletes
            .iter()
            .any(|request| request.path == pod_path(pod_name)),
        "{failure:?}: the tracked pod must be deleted: {deletes:?}"
    );
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 0);
    Ok(())
}

/// An apiserver fault while resolving the runtime is not "no runtime": the
/// teardown fails before deleting anything.
#[tokio::test(flavor = "current_thread")]
async fn teardown_propagates_an_apiserver_fault_while_resolving_the_runtime() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "deleting").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running("space-sbx-teardown-fault-g1", "teardown-fault-uid"),
    )
    .await?;
    // Not in the reflector, and the authoritative read is refused.
    let (client, log) = scripted_kube_client(|_| kube_status(StatusCode::FORBIDDEN, "Forbidden"));
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);

    let outcome = substrate.delete_execution(sandbox_id).await;

    assert!(
        matches!(outcome, Err(SubstrateError::Failed { .. })),
        "{outcome:?}"
    );
    assert!(requests_matching(&log, &Method::DELETE).is_empty());
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
    Ok(())
}

/// The shape the sweeper's lifecycle-only revert leaves with abandoned-restore
/// retirement disabled, the default until every routing replica refuses to
/// route to a `restoring` sandbox: the restore's execution still active. A
/// restore pod that came up after the sweeper's live-pod check may then have
/// served a user request through a replica without that guard. The cleanup
/// that follows the revert retires under the candidate's fence, which sees the
/// pod at a newer resourceVersion and refuses: the pod and its execution are
/// kept, so the sweeper puts the row back to `restoring` rather than losing
/// what the pod served. (That the revert leaves this shape with retirement
/// disabled is pinned in the sweeper's own store tests.)
#[tokio::test(flavor = "current_thread")]
async fn without_retirement_the_sweeper_cleanup_keeps_a_restore_pod_that_came_up_after_its_check()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-late-serve-g1";
    insert_test_sandbox_row(&db.pool, sandbox_id, "suspended").await?;
    // What the lifecycle-only revert left: the restore's execution still
    // active and the row still placed.
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running(pod_name, "late-serve-uid"),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement = 'on_node' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    // The pod bound and came up after the check: a newer resourceVersion.
    let (client, log) = scripted_kube_client(move |request| match request.method {
        Method::DELETE => kube_status(StatusCode::OK, "Success"),
        _ => {
            let mut pod = (*sandbox_pod_on_node(pod_name, WORKER_NODE, 1, "Running")).clone();
            pod.metadata.uid = Some("late-serve-uid".to_string());
            pod.metadata.resource_version = Some("2".to_string());
            pod_reply(&pod)
        }
    });
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let observed = PodIdentityFence::new(PodName::parse(pod_name)?)
        .with_recorded(RecordedExecution::Read {
            pod_uid: Some(PodUid::parse("late-serve-uid")?),
            route_activation_id: None,
        })
        .with_generation(1)
        .with_pod_uid(PodUid::parse("late-serve-uid")?)
        .with_resource_version(PodResourceVersion::parse("1")?);

    let outcome = substrate
        .retire_execution(RetireExecutionRequest::fenced(sandbox_id, observed))
        .await?;

    assert!(
        matches!(outcome, RetireExecutionOutcome::PodIdentityChanged { .. }),
        "the cleanup must refuse a pod that came up after the check: {outcome:?}"
    );
    assert!(
        requests_matching(&log, &Method::DELETE).is_empty(),
        "a pod that may have served a request must not be deleted"
    );
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
    Ok(())
}

/// The transient-lifecycle sweeper reverts an aged `restoring` row by
/// retiring its execution as `replaced` in the revert's own transaction, and
/// only after checking the candidate's live pod. A restore pod that binds and
/// comes up after that check is the abandoned restore's leftover: the cleanup
/// that follows deletes it rather than letting it recover, the row stays
/// `suspended`, and restore still selects the durable pre-restore snapshot
/// the revert proved.
#[tokio::test(flavor = "current_thread")]
async fn sweeper_revert_cleanup_deletes_a_restore_pod_that_bound_after_its_check() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-late-bind-g1";
    let source_uri = "s3://space-test/pre-restore?region=us-east-1";
    insert_test_sandbox_row(&db.pool, sandbox_id, "suspended").await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id)
         VALUES ($1, $2, 'full', 'ready', NOW() - INTERVAL '10 minutes',
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         SELECT id, $2, org_id FROM snapshots WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(source_uri)
    .execute(&db.pool)
    .await?;
    // What the sweeper's revert left: its execution retired as an abandoned
    // restore, the row still reading as placed from when the pod bound, and
    // the placement clock the bind moved past the snapshot cleared.
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            state: "replaced",
            ..ExecutionSeed::running(pod_name, "late-bind-uid")
        },
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'on_node',
                placement_changed_at = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    // The live pod has moved on since the candidate observed it: it bound
    // and is Running at a newer resourceVersion.
    let (client, log) = scripted_kube_client(move |request| match request.method {
        Method::DELETE => kube_status(StatusCode::OK, "Success"),
        _ => {
            let mut pod = (*sandbox_pod_on_node(pod_name, WORKER_NODE, 1, "Running")).clone();
            pod.metadata.uid = Some("late-bind-uid".to_string());
            pod.metadata.resource_version = Some("2".to_string());
            pod_reply(&pod)
        }
    });
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let observed = PodIdentityFence::new(PodName::parse(pod_name)?)
        .with_recorded(RecordedExecution::Read {
            pod_uid: Some(PodUid::parse("late-bind-uid")?),
            route_activation_id: None,
        })
        .with_generation(1)
        .with_pod_uid(PodUid::parse("late-bind-uid")?)
        .with_resource_version(PodResourceVersion::parse("1")?);

    let outcome = substrate
        .retire_execution(RetireExecutionRequest::fenced(sandbox_id, observed))
        .await?;

    assert_eq!(outcome, RetireExecutionOutcome::Retired);
    let deletes = requests_matching(&log, &Method::DELETE);
    assert!(
        deletes
            .iter()
            .any(|request| request.path == pod_path(pod_name)),
        "the late-bound restore pod must be deleted: {deletes:?}"
    );
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 0);
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");
    assert_eq!(sandbox.placement, "unplaced");
    assert_eq!(
        substrate
            .store
            .selected_suspend_restore_storage_uri(&sandbox, false)
            .await?
            .as_deref(),
        Some(source_uri),
        "the next wake must restore from the durable pre-restore snapshot"
    );
    Ok(())
}
