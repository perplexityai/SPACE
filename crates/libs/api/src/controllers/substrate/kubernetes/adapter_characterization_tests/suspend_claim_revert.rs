//! A suspend claim that fails is undone only when the node cannot have begun:
//! the claim's revert, what it restores, what it leaves to other writers, and
//! the failures that keep the claim.

use std::sync::Arc;

use axum::http::{Method, StatusCode};
use pplx_space_common_core::types::{ExecutionGeneration, LocalArtifactId, PodUid};
use pplx_space_sbx_mgr_client::proto as sbx;

use super::super::scripted_test_doubles::{
    ScriptedReply, ScriptedWorkerConfig, kube_status, pod_path, requests_matching,
    scripted_kube_client, start_scripted_worker,
};
use super::super::store::SuspendExecutionClaim;
use super::super::tests::{insert_test_sandbox_row, substrate_with_store_and_client};
use super::hydration_and_suspend::{BLOCKING_DELETE_TIMEOUT, substrate_with_node_api_on};
use super::*;

/// The sandbox's `command_ready_pending` flag.
async fn command_ready_pending(pool: &PgPool, sandbox_id: SandboxId) -> Result<bool, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT command_ready_pending FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The node-drain marker on the sandbox, if a drain claim set one.
async fn evacuation_marker(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<String>, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

fn pending_suspend_reply() -> ScriptedReply {
    ScriptedReply::message(&sbx::PrepareTeardownSuspendResponse {
        outcome: sbx::TeardownSuspendOutcome::Pending as i32,
        reason: None,
        published_artifact: None,
    })
}

#[derive(Debug, Clone, Copy)]
enum RefusedBeforeNode {
    /// The active execution has no `k8s_node_name`, so no node is asked.
    NoNodeName,
    /// The node's first answer is a peer-authorization refusal, which its
    /// handler returns before it records any suspend.
    NodeDeniesTheFirstCall,
}

/// A suspend that fails before the node can have begun puts the sandbox back
/// as the claim found it, so a retry claims afresh instead of joining a
/// refusal: the same lifecycle, and the `command_ready_pending` flag the
/// claim's lifecycle trigger cleared.
#[rstest::rstest]
#[case::no_node_name_from_started(RefusedBeforeNode::NoNodeName, "started", false)]
#[case::no_node_name_from_paused(RefusedBeforeNode::NoNodeName, "paused", false)]
#[case::no_node_name_restores_a_pending_readiness_check(
    RefusedBeforeNode::NoNodeName,
    "started",
    true
)]
#[case::node_denies_the_first_call(RefusedBeforeNode::NodeDeniesTheFirstCall, "started", false)]
#[tokio::test(flavor = "current_thread")]
async fn suspend_execution_reverts_a_claim_refused_before_the_node_begins(
    #[case] refusal: RefusedBeforeNode,
    #[case] previous: &str,
    #[case] pending: bool,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, previous).await?;
    if pending {
        sqlx::query(sql!(
            "UPDATE sandboxes SET command_ready_pending = TRUE WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    }
    let seed = ExecutionSeed::running("space-sbx-reverted-claim-g1", "reverted-claim-uid");
    let seed = match refusal {
        RefusedBeforeNode::NoNodeName => ExecutionSeed {
            k8s_node_name: None,
            ..seed
        },
        RefusedBeforeNode::NodeDeniesTheFirstCall => seed,
    };
    insert_execution(&db.pool, sandbox_id, seed).await?;
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let worker = match refusal {
        RefusedBeforeNode::NoNodeName => None,
        RefusedBeforeNode::NodeDeniesTheFirstCall => Some(
            start_scripted_worker(ScriptedWorkerConfig::new([(
                "WorkerApi/PrepareTeardownSuspend",
                ScriptedReply::Status(tonic::Status::permission_denied("peer not allowed")),
            )]))
            .await?,
        ),
    };
    let substrate = match &worker {
        Some(worker) => substrate_with_node_api_on(&db.pool, client, worker.port),
        None => substrate_with_store_and_client(db.pool.clone(), client),
    };

    let outcome = substrate.suspend_execution_tracked(sandbox_id).await;

    assert!(
        matches!(outcome, Err(SubstrateError::Unavailable { .. })),
        "{refusal:?}: expected a retryable refusal, got {outcome:?}"
    );
    if let Some(worker) = &worker {
        assert!(
            !worker
                .received("WorkerApi/PrepareTeardownSuspend")
                .is_empty(),
            "{refusal:?}: the refusal must come from the node"
        );
    }
    assert_eq!(
        sandbox_lifecycle(&db.pool, sandbox_id).await?,
        previous,
        "{refusal:?}: the claim must be reverted"
    );
    assert_eq!(
        command_ready_pending(&db.pool, sandbox_id).await?,
        pending,
        "{refusal:?}: the readiness flag must come back as the claim found it"
    );
    assert_eq!(evacuation_marker(&db.pool, sandbox_id).await?, None);
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
    assert!(log.lock().unwrap().is_empty(), "no pod may be retired");
    Ok(())
}

/// After a refused claim is reverted, a retry claims the sandbox afresh rather
/// than joining the refusal, and completes once the node accepts.
#[tokio::test(flavor = "current_thread")]
async fn a_suspend_retried_after_a_reverted_claim_completes() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-retried-claim-g1";
    let node_artifact = LocalArtifactId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running(pod_name, "retried-claim-uid"),
    )
    .await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([(
        "WorkerApi/PrepareTeardownSuspend",
        ScriptedReply::Sequence(Arc::from([
            ScriptedReply::Status(tonic::Status::permission_denied("peer not allowed")),
            ScriptedReply::message(&sbx::PrepareTeardownSuspendResponse {
                outcome: sbx::TeardownSuspendOutcome::Durable as i32,
                reason: None,
                published_artifact: Some(sbx::PublishedArtifact {
                    local_artifact_id: node_artifact.to_string(),
                    artifact_kind: sbx::ArtifactKind::Full as i32,
                    origin: sbx::ArtifactOrigin::Suspend as i32,
                    snapshot_name: None,
                }),
            }),
        ])),
    )]))
    .await?;
    let (client, log) = scripted_kube_client(move |request| {
        if request.path != pod_path(pod_name) {
            return kube_status(StatusCode::BAD_REQUEST, "BadRequest");
        }
        if request.method == Method::DELETE {
            kube_status(StatusCode::OK, "Success")
        } else {
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        }
    });
    let mut substrate = substrate_with_node_api_on(&db.pool, client, worker.port);
    substrate.k8s.k8s_blocking_delete_timeout = BLOCKING_DELETE_TIMEOUT;

    let refused = substrate.suspend_execution_tracked(sandbox_id).await;
    assert!(
        matches!(refused, Err(SubstrateError::Unavailable { .. })),
        "expected a retryable refusal, got {refused:?}"
    );
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "started");

    let returned = substrate
        .suspend_execution_tracked(sandbox_id)
        .await?
        .expect("Kubernetes suspend always returns its artifact");

    assert_eq!(returned.id, node_artifact);
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspended");
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 0);
    assert!(
        !requests_matching(&log, &Method::DELETE).is_empty(),
        "the retry must retire the pod"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum NodeMayHaveBegun {
    /// A transport failure the poll retried, which may have reached the node,
    /// then a refusal that alone would be safe to revert.
    RetriedFailureThenDenied,
    /// The node answered Pending, so it recorded the suspend, then refused.
    PendingThenDenied,
}

/// A refusal is reverted only when it is the node's first reply. After an
/// earlier call that may have reached the node, the claim stays for a retry
/// to join.
#[rstest::rstest]
#[case::retried_failure_then_denied(NodeMayHaveBegun::RetriedFailureThenDenied)]
#[case::pending_then_denied(NodeMayHaveBegun::PendingThenDenied)]
#[tokio::test(flavor = "current_thread")]
async fn suspend_execution_keeps_a_claim_the_node_may_have_begun(
    #[case] earlier: NodeMayHaveBegun,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running("space-sbx-begun-claim-g1", "begun-claim-uid"),
    )
    .await?;
    let first = match earlier {
        NodeMayHaveBegun::RetriedFailureThenDenied => {
            ScriptedReply::Status(tonic::Status::unavailable("node restarting"))
        }
        NodeMayHaveBegun::PendingThenDenied => pending_suspend_reply(),
    };
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([(
        "WorkerApi/PrepareTeardownSuspend",
        ScriptedReply::Sequence(Arc::from([
            first,
            ScriptedReply::Status(tonic::Status::permission_denied("peer not allowed")),
        ])),
    )]))
    .await?;
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let substrate = substrate_with_node_api_on(&db.pool, client, worker.port);

    let outcome = substrate.suspend_execution_tracked(sandbox_id).await;

    assert!(
        matches!(outcome, Err(SubstrateError::Unavailable { .. })),
        "{earlier:?}: expected a retryable refusal, got {outcome:?}"
    );
    assert_eq!(
        worker.received("WorkerApi/PrepareTeardownSuspend").len(),
        2,
        "{earlier:?}: the refusal must be the second call"
    );
    assert_eq!(
        sandbox_lifecycle(&db.pool, sandbox_id).await?,
        "suspending",
        "{earlier:?}: the claim must be kept"
    );
    assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
    assert!(log.lock().unwrap().is_empty(), "no pod may be retired");
    Ok(())
}

/// The call node-drain makes into `suspend_execution` joins its own marked
/// claim instead of making a fresh one, and a refusal never reverts it: the
/// drain reverts its claim itself.
#[tokio::test(flavor = "current_thread")]
async fn a_node_drain_claim_is_joined_and_never_reverted_by_a_refused_suspend() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed {
            k8s_node_name: None,
            ..ExecutionSeed::running("space-sbx-drain-claim-g1", "drain-claim-uid")
        },
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'suspending', evacuation_restore_target = 'started'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let claim = substrate
        .store
        .claim_suspend_execution(
            sandbox_id,
            ExecutionGeneration::new(1)?,
            &PodUid::parse("drain-claim-uid")?,
        )
        .await?;
    assert_eq!(claim, SuspendExecutionClaim::Existing);

    let outcome =
        <KubernetesSubstrate as SubstrateAdapter>::suspend_execution(&substrate, sandbox_id).await;

    assert!(
        matches!(outcome, Err(SubstrateError::Unavailable { .. })),
        "expected a retryable refusal, got {outcome:?}"
    );
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "suspending");
    assert_eq!(
        evacuation_marker(&db.pool, sandbox_id).await?.as_deref(),
        Some("started")
    );
    assert!(log.lock().unwrap().is_empty(), "no pod may be retired");
    Ok(())
}

/// The claim locks the sandbox row before it decides, so a writer that holds
/// the row and moves the lifecycle makes the claim lose instead of being
/// overwritten with `suspending`.
#[tokio::test(flavor = "current_thread")]
async fn a_suspend_claim_waits_for_a_concurrent_lifecycle_change_and_loses() -> TestResult {
    // One connection for the holder, one for the claim and one to watch it.
    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running("space-sbx-claim-lock-g1", "claim-lock-uid"),
    )
    .await?;
    let (client, _log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let mut holder = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'stopping' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *holder)
    .await?;

    let pod_uid = PodUid::parse("claim-lock-uid")?;
    let claim =
        substrate
            .store
            .claim_suspend_execution(sandbox_id, ExecutionGeneration::new(1)?, &pod_uid);
    let release = async {
        // Commit only once the claim is queued behind the holder's row lock.
        loop {
            let waiting: i64 = sqlx::query_scalar(sql!(
                "SELECT COUNT(*) FROM pg_stat_activity
                  WHERE datname = current_database() AND wait_event_type = 'Lock'"
            ))
            .fetch_one(&db.pool)
            .await?;
            if waiting > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        holder.commit().await?;
        Ok::<(), BoxError>(())
    };
    let (claimed, released) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(claim, release)
    })
    .await
    .map_err(|_| "the claim never queued behind the lifecycle change")?;
    released?;

    assert!(
        matches!(claimed, Err(SubstrateError::Unavailable { .. })),
        "the claim must lose to the concurrent change, got {claimed:?}"
    );
    assert_eq!(sandbox_lifecycle(&db.pool, sandbox_id).await?, "stopping");
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ClaimChangedUnderRevert {
    /// Nothing changed: the revert applies.
    Nothing,
    /// The sweeper or another writer moved the row on.
    SandboxFailed,
    /// A node-drain claim marked the row.
    DrainMarker,
    /// The execution committed its suspend.
    ExecutionSuspended,
    /// The execution is no longer the active one.
    ExecutionRetired,
}

/// The revert is a compare-and-swap on the claim it undoes: when another
/// writer changed the row or the execution since the claim, it changes
/// nothing.
#[rstest::rstest]
#[case::nothing_changed(ClaimChangedUnderRevert::Nothing, true, "started")]
#[case::sandbox_failed(ClaimChangedUnderRevert::SandboxFailed, false, "failed")]
#[case::drain_marker(ClaimChangedUnderRevert::DrainMarker, false, "suspending")]
#[case::execution_suspended(ClaimChangedUnderRevert::ExecutionSuspended, false, "suspending")]
#[case::execution_retired(ClaimChangedUnderRevert::ExecutionRetired, false, "suspending")]
#[tokio::test(flavor = "current_thread")]
async fn a_lost_claim_race_leaves_the_other_writers_state_alone(
    #[case] changed: ClaimChangedUnderRevert,
    #[case] reverted: bool,
    #[case] lifecycle_after: &str,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
    insert_execution(
        &db.pool,
        sandbox_id,
        ExecutionSeed::running("space-sbx-lost-race-g1", "lost-race-uid"),
    )
    .await?;
    let (client, _log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let substrate = substrate_with_store_and_client(db.pool.clone(), client);
    let generation = ExecutionGeneration::new(1)?;
    let pod_uid = PodUid::parse("lost-race-uid")?;
    let SuspendExecutionClaim::Claimed(restore) = substrate
        .store
        .claim_suspend_execution(sandbox_id, generation, &pod_uid)
        .await?
    else {
        return Err("the first claim must be fresh".into());
    };
    match changed {
        ClaimChangedUnderRevert::Nothing => {}
        ClaimChangedUnderRevert::SandboxFailed => {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'failed' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
        }
        ClaimChangedUnderRevert::DrainMarker => {
            sqlx::query(sql!(
                "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
        }
        ClaimChangedUnderRevert::ExecutionSuspended => {
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions SET state = 'suspended' WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
        }
        ClaimChangedUnderRevert::ExecutionRetired => {
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
        }
    }

    let outcome = substrate
        .store
        .revert_suspend_claim(sandbox_id, generation, &pod_uid, restore)
        .await?;

    assert_eq!(outcome, reverted, "{changed:?}");
    assert_eq!(
        sandbox_lifecycle(&db.pool, sandbox_id).await?,
        lifecycle_after,
        "{changed:?}"
    );
    Ok(())
}
