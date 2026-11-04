//! No path serving a user request dials a sandbox's pod while the sandbox is
//! `restoring`: `access_runtime_client` and `runtime_client` join the
//! in-flight restore instead, and fail retryably when it doesn't reach
//! `started` in their budget; the non-recovering `reachable_runtime_client`
//! reports the runtime unreachable.

use axum::http::{Method, StatusCode};

use super::super::scripted_test_doubles::{
    KubeLog, ScriptedWorker, ScriptedWorkerConfig, kube_status, scripted_kube_client,
    start_scripted_worker,
};
use super::super::tests::{
    insert_ready_full_snapshot_relative_to_lifecycle, insert_test_sandbox_row,
    substrate_with_store_client_proxy_port_and_pod,
};
use super::*;
use crate::controllers::ControllerRetryAfterCause;

const POD_UID: &str = "restoring-pod-uid";
/// The restore's pod runs here, behind a live worker, so a client that
/// dialed it would connect.
const POD_IP: &str = "127.0.0.1";

struct Restoring {
    substrate: KubernetesSubstrate,
    pod_name: String,
    sandbox: Sandbox,
    lease_token: Uuid,
    kube_log: KubeLog,
    worker: ScriptedWorker,
}

/// A `restoring` sandbox whose restore pod is up, routable and dialable: its
/// execution is `running` with the pod's IP. `lease` is how long the restore
/// lease another request holds has left; a lapsed one leaves no owner.
async fn restoring_with_live_pod(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lease: &str,
) -> Result<Restoring, BoxError> {
    insert_test_sandbox_row(pool, sandbox_id, "restoring").await?;
    let lease_token = Uuid::now_v7();
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + $2::interval,
                restore_lease_token = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(lease)
    .bind(lease_token)
    .execute(pool)
    .await?;
    insert_execution(
        pool,
        sandbox_id,
        ExecutionSeed {
            pod_ip: Some(POD_IP),
            ..ExecutionSeed::running(&pod_name, POD_UID)
        },
    )
    .await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let (client, kube_log) = scripted_kube_client(|request| match request.method {
        Method::GET => kube_status(StatusCode::NOT_FOUND, "NotFound"),
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let substrate = substrate_with_store_client_proxy_port_and_pod(
        pool.clone(),
        client,
        worker.port,
        Some(sandbox_pod(
            &pod_name,
            POD_UID,
            sandbox_id,
            1,
            "Running",
            Some(POD_IP),
        )),
    );
    let sandbox = crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
        .await?
        .expect("sandbox row exists");
    Ok(Restoring {
        pod_name,
        substrate,
        sandbox,
        lease_token,
        kube_log,
        worker,
    })
}

/// The restore's owner finishes it after `delay`.
fn complete_restore_after(
    pool: &PgPool,
    sandbox_id: SandboxId,
    delay: Duration,
) -> tokio::task::JoinHandle<Result<(), BoxError>> {
    let pool = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&pool)
        .await?;
        Ok(())
    })
}

fn access_identity() -> SandboxApiMetricIdentity {
    SandboxApiMetricIdentity::native("restoring_access")
}

fn assert_retryable_restore_in_flight(error: &ControllerError) {
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{error:?}");
    assert_eq!(
        error.retry_after_cause(),
        Some(ControllerRetryAfterCause::RestoreInFlight),
        "{error:?}"
    );
}

/// An access request on a restoring sandbox waits for the restore and gets a
/// client only once the sandbox is `started`, although its pod was dialable
/// the whole time.
#[tokio::test(flavor = "current_thread")]
async fn access_on_a_restoring_sandbox_joins_the_restore_before_dialing() -> TestResult {
    // The joined client connects to the worker over internal RPC.
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
    let owner = complete_restore_after(&db.pool, sandbox_id, Duration::from_millis(300));

    let client = restoring
        .substrate
        .access_runtime_client(
            access_identity(),
            &restoring.sandbox,
            ExecutionActivityKind::Access,
            Duration::from_secs(10),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await;
    let lifecycle_at_return = sandbox_lifecycle(&db.pool, sandbox_id).await?;
    owner.await??;

    assert!(client.is_ok(), "{:?}", client.err());
    assert_eq!(
        lifecycle_at_return, "started",
        "no client may be handed out while the sandbox is restoring"
    );
    Ok(())
}

/// A request that wakes a `suspended` sandbox and joins an in-process
/// restore run gets the run's activation as soon as the restore's activation
/// completes, which can be before the row is `started` (the run's owner
/// commit may not have landed). The request still gets its client only once
/// the sandbox is `started`, and it keeps waiting while the row reads as
/// unowned.
///
/// The row has no live owner, so a request that didn't join the run would
/// claim the restore itself and fail at pod creation.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_run_that_completes_before_started_is_dialed_only_once_started() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "-1 hour").await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET updated_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let binding = ExecutionBinding {
        execution_object_id: restoring.pod_name.clone(),
        runtime_endpoint: KubernetesSubstrate::endpoint_for_pod_ip(
            PodIp::parse(POD_IP)?,
            restoring.worker.port,
        )?,
        execution_generation: 1,
    };
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let run_release = release.clone();
    let run_substrate = restoring.substrate.clone();
    let run = tokio::spawn(async move {
        run_substrate
            .wake_coalescer
            .join_or_start(
                sandbox_id,
                tokio::time::Instant::now() + Duration::from_secs(10),
                move || async move {
                    run_release.notified().await;
                    Ok(crate::controllers::substrate::ExecutionActivation::joined(
                        binding,
                    ))
                },
            )
            .await
            .is_ok()
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(restoring.substrate.wake_coalescer.live_runs(), 1);

    let request_substrate = restoring.substrate.clone();
    let request_sandbox = restoring.sandbox.clone();
    let request_pool = db.pool.clone();
    let request = tokio::spawn(async move {
        // The caller read the row before the restore claimed it.
        let mut suspended_view = request_sandbox;
        suspended_view.lifecycle = "suspended".to_string();
        let client = request_substrate
            .wake_and_dial_once_started(
                &suspended_view,
                Duration::from_secs(10),
                access_identity(),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await;
        let lifecycle_at_return = sandbox_lifecycle(&request_pool, sandbox_id).await;
        (client.map(|_| ()), lifecycle_at_return)
    });
    // Let the request reach the run and join it, then finish the run.
    tokio::time::sleep(Duration::from_millis(300)).await;
    release.notify_one();
    let owner = complete_restore_after(&db.pool, sandbox_id, Duration::from_millis(300));

    let (client, lifecycle_at_return) = request.await?;
    owner.await??;

    assert!(run.await?, "the seeded restore run completes");
    assert!(client.is_ok(), "{:?}", client.err());
    assert_eq!(
        lifecycle_at_return?, "started",
        "no client may be handed out while the sandbox is restoring"
    );
    Ok(())
}

/// A request that saw the sandbox `restoring` and finds it `started` when
/// it joins dials it, rather than failing a restore that already succeeded.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_that_finished_before_the_join_is_dialed() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
    complete_restore_after(&db.pool, sandbox_id, Duration::ZERO).await??;

    let client = restoring
        .substrate
        .join_restore_before_dial(
            sandbox_id,
            access_identity(),
            Duration::from_secs(10),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await;

    assert!(client.is_ok(), "{:?}", client.err());
    Ok(())
}

/// An access request whose restore doesn't finish in its budget gets a
/// retryable `RestoreInFlight`, never a client for the restoring pod.
#[tokio::test(flavor = "current_thread")]
async fn access_on_a_restoring_sandbox_is_refused_retryably_when_the_restore_lingers() -> TestResult
{
    // Credentials make the pod connectable, so only the gate can refuse it.
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;

    let error = restoring
        .substrate
        .access_runtime_client(
            access_identity(),
            &restoring.sandbox,
            ExecutionActivityKind::Access,
            Duration::from_secs(2),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await
        .expect_err("a restoring pod must not be dialed");

    assert_retryable_restore_in_flight(&error);
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "restoring");
    Ok(())
}

/// `runtime_client`'s user paths join the restore the same way.
#[tokio::test(flavor = "current_thread")]
async fn runtime_client_on_a_restoring_sandbox_joins_the_restore_before_dialing() -> TestResult {
    // The joined client connects to the worker over internal RPC.
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
    let owner = complete_restore_after(&db.pool, sandbox_id, Duration::from_millis(300));

    let client = restoring
        .substrate
        .runtime_client(
            sandbox_id,
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await;
    let lifecycle_at_return = sandbox_lifecycle(&db.pool, sandbox_id).await?;
    owner.await??;

    assert!(client.is_ok(), "{:?}", client.err());
    assert_eq!(lifecycle_at_return, "started");
    Ok(())
}

/// The non-recovering `reachable_runtime_client` neither dials a restoring
/// pod nor waits for its restore: its callers are best-effort (the ECW
/// keepalive refresh) or turn an unreachable runtime into a retryable 503,
/// so it reports the runtime unreachable at once, whether or not an owner is
/// still restoring the sandbox.
#[tokio::test(flavor = "current_thread")]
async fn reachable_runtime_client_finds_no_client_for_a_restoring_sandbox() -> TestResult {
    // Credentials make the pod connectable, so only the gate can refuse it.
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    for lease in ["120 seconds", "-1 hour"] {
        let sandbox_id = SandboxId::new();
        let restoring = restoring_with_live_pod(&db.pool, sandbox_id, lease).await?;
        let started = std::time::Instant::now();
        let client = restoring
            .substrate
            .reachable_runtime_client(sandbox_id)
            .await?;
        assert!(
            client.is_none(),
            "{lease}: a restoring pod must not be dialed"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{lease}: the probe must not wait out the restore: {:?}",
            started.elapsed()
        );
        assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "restoring");
    }
    Ok(())
}

/// A drain's evacuation restore owns its `restoring` row through the
/// evacuation marker rather than a lease, so the row reads as unowned. A user
/// request on it waits for the drain to move it to `started` and drives no
/// wake of its own (the scripted worker answers no wake).
#[tokio::test(flavor = "current_thread")]
async fn access_on_an_evacuating_sandbox_waits_for_the_drain() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "-1 hour").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let drain = complete_restore_after(&db.pool, sandbox_id, Duration::from_millis(300));

    let client = restoring
        .substrate
        .access_runtime_client(
            access_identity(),
            &restoring.sandbox,
            ExecutionActivityKind::Access,
            Duration::from_secs(10),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await;
    let lifecycle_at_return = sandbox_lifecycle(&db.pool, sandbox_id).await?;
    drain.await??;

    assert!(client.is_ok(), "{:?}", client.err());
    assert_eq!(lifecycle_at_return, "started");
    Ok(())
}

/// A request that joins a restore whose owner then gives up starts no
/// restore of its own: one the owner rolled back to `suspended` is refused
/// retryably, and one that went to a state that can't be woken gets that
/// state's error. Either way no pod is created; a fresh restore is for a
/// caller whose access-wake policy and admission allow it.
#[tokio::test(flavor = "current_thread")]
async fn a_joined_restore_that_its_owner_abandons_starts_no_restore() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    for abandoned_to in ["suspended", "failed"] {
        let sandbox_id = SandboxId::new();
        let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
        // A durable source, so a fresh restore of the reverted row could
        // start.
        insert_ready_full_snapshot_relative_to_lifecycle(
            &db.pool,
            sandbox_id,
            "s3://bucket/pre-restore?region=us-east-1",
            -60,
            Uuid::now_v7(),
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET created_at = NOW() WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
        let owner_substrate = restoring.substrate.clone();
        let owner_pool = db.pool.clone();
        let lease_token = restoring.lease_token;
        let owner = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            if abandoned_to == "suspended" {
                owner_substrate
                    .store
                    .revert_restore_claim_to_suspended(
                        sandbox_id,
                        Some(lease_token),
                        None,
                        crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
                    )
                    .await?;
            } else {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET lifecycle = 'failed' WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&owner_pool)
                .await?;
            }
            Ok::<_, BoxError>(())
        });

        let refused = restoring
            .substrate
            .access_runtime_client(
                access_identity(),
                &restoring.sandbox,
                ExecutionActivityKind::Access,
                Duration::from_secs(10),
                crate::controllers::substrate::RecoveryCharge::Exempt,
            )
            .await
            .expect_err("an abandoned restore serves no client");
        owner.await??;

        if abandoned_to == "suspended" {
            assert_retryable_restore_in_flight(&refused);
        } else {
            assert_eq!(refused.status(), StatusCode::CONFLICT, "{refused:?}");
        }
        assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, abandoned_to);
        let pod_writes = restoring
            .kube_log
            .lock()
            .expect("kube log")
            .iter()
            .filter(|request| request.method != Method::GET)
            .count();
        assert_eq!(pod_writes, 0, "{abandoned_to}: no restore may be started");
    }
    Ok(())
}

/// A request that arrives during a restore that then fails is refused
/// without a client, so when the API rollback retires the restore's execution
/// and deletes its pod, nothing that pod served is lost: it served nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_rolled_back_restore_discards_no_request_it_served() -> TestResult {
    // Credentials make the pod connectable, so only the gate can refuse it.
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &db.pool,
        sandbox_id,
        "s3://bucket/pre-restore?region=us-east-1",
        -60,
        Uuid::now_v7(),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET created_at = NOW() WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let refused = restoring
        .substrate
        .access_runtime_client(
            access_identity(),
            &restoring.sandbox,
            ExecutionActivityKind::Access,
            Duration::from_secs(2),
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await
        .expect_err("a restoring pod must not be dialed");
    assert_retryable_restore_in_flight(&refused);

    let revert = restoring
        .substrate
        .store
        .revert_restore_claim_to_suspended(
            sandbox_id,
            Some(restoring.lease_token),
            None,
            crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
        )
        .await?;

    assert_eq!(
        revert,
        RestoreClaimRevert::ExecutionRetired {
            pod_name: restoring.pod_name.clone()
        }
    );
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
    Ok(())
}

/// The substrate's rollback of a failed fresh restore claim retires the
/// restore's execution and deletes its pod only with abandoned-restore
/// retirement enabled. Disabled, the default until every routing replica
/// refuses to route to a `restoring` sandbox, it reverts only the lifecycle
/// and deletes nothing, so a pod an older replica may have routed a request
/// to survives the rollback.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_restore_rollback_deletes_its_pod_only_with_retirement_enabled() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    for retirement in [
        crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
        crate::controllers::substrate::AbandonedRestoreRetirement::Disabled,
    ] {
        let sandbox_id = SandboxId::new();
        let mut restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
        restoring.substrate.k8s.abandoned_restore_retirement = retirement;
        insert_ready_full_snapshot_relative_to_lifecycle(
            &db.pool,
            sandbox_id,
            "s3://bucket/pre-restore?region=us-east-1",
            -60,
            Uuid::now_v7(),
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET created_at = NOW() WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;

        restoring
            .substrate
            .revert_failed_restore_claim(
                sandbox_id,
                super::super::store::RestoreClaim::FreshFromSuspended {
                    restore_lease_token: Some(restoring.lease_token),
                },
                None,
            )
            .await;

        let deleted = restoring
            .kube_log
            .lock()
            .expect("kube log")
            .iter()
            .any(|request| request.method == Method::DELETE);
        let active: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
        assert_eq!(
            (deleted, active),
            if retirement.is_enabled() {
                (true, 0)
            } else {
                (false, 1)
            },
            "{retirement:?}"
        );
    }
    Ok(())
}

/// When the restoring sandbox's pod isn't resolvable yet (its execution is
/// still `creating`), the fallback joins the restore too, rather than
/// materializing a pod of its own: both `access_runtime_client` and
/// `runtime_client` return only once the owner has brought the sandbox to
/// `started`.
#[tokio::test(flavor = "current_thread")]
async fn a_restoring_sandbox_without_a_resolved_pod_joins_the_restore() -> TestResult {
    // The joined client connects to the worker over internal RPC.
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    for via_access in [true, false] {
        let sandbox_id = SandboxId::new();
        let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET state = 'creating', pod_uid = NULL, pod_ip = NULL
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
        let pool = db.pool.clone();
        let owner = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                    SET state = 'running', pod_uid = $2, pod_ip = $3::INET
                  WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .bind(POD_UID)
            .bind(POD_IP)
            .execute(&pool)
            .await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(&pool)
            .await?;
            Ok::<_, BoxError>(())
        });

        let client = if via_access {
            restoring
                .substrate
                .access_runtime_client(
                    access_identity(),
                    &restoring.sandbox,
                    ExecutionActivityKind::Access,
                    Duration::from_secs(10),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        } else {
            restoring
                .substrate
                .runtime_client(
                    sandbox_id,
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
        };
        let lifecycle_at_return = sandbox_lifecycle(&db.pool, sandbox_id).await?;
        owner.await??;

        assert!(
            client.is_ok(),
            "via_access={via_access}: {:?}",
            client.err()
        );
        assert_eq!(lifecycle_at_return, "started", "via_access={via_access}");
    }
    Ok(())
}

/// The reachability probe that replays a runtime RPC on a fresh client finds
/// no client for a restoring pod.
#[tokio::test(flavor = "current_thread")]
async fn probe_runtime_client_finds_no_client_for_a_restoring_pod() -> TestResult {
    // Credentials make the pod connectable, so only the gate can refuse it.
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let restoring = restoring_with_live_pod(&db.pool, sandbox_id, "120 seconds").await?;
    assert!(
        restoring
            .substrate
            .probe_runtime_client(sandbox_id)
            .await?
            .is_none()
    );
    Ok(())
}
