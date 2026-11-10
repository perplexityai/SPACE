//! The drain restore operation's restore through the shared executor
//! ([`KubernetesSubstrate::restore_drained_execution`]), and what a user's
//! call meets while its episode is open.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_sandbox_store::drain_restore::DrainRestoreRefusal;
use pplx_space_sandbox_store::sweeper::EvacuationRestoreTarget;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::{ActivationOptions, RecoveryCharge};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn options() -> ActivationOptions {
    ActivationOptions {
        blocking: true,
        ready_timeout: Duration::from_secs(2),
    }
}

/// A sandbox a forced drain suspended: no marker, its old execution quiet,
/// and its suspend snapshot, S, the current artifact. Returns the substrate,
/// the Wakes its worker counts, the worker, and S.
async fn drained_subject(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<
    (
        KubernetesSubstrate,
        Arc<AtomicUsize>,
        kubernetes_tests::SequencedWakeWorkerHandle,
        Uuid,
    ),
    BoxError,
> {
    drained_subject_waking_after(pool, sandbox_id, Some(1)).await
}

/// [`drained_subject`] whose worker refuses every Wake before attempt
/// `succeed_after`, or every one when `None`, as a node that can't bring the
/// VM up does.
async fn drained_subject_waking_after(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    succeed_after: Option<usize>,
) -> Result<
    (
        KubernetesSubstrate,
        Arc<AtomicUsize>,
        kubernetes_tests::SequencedWakeWorkerHandle,
        Uuid,
    ),
    BoxError,
> {
    let (substrate, wake_attempts, _pod_gets, worker) =
        paused_restore_test_subject(pool, sandbox_id, succeed_after, true).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET updated_at = NOW() - INTERVAL '1 hour',
                k8s_node_name = 'node-a'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let s: Uuid = sqlx::query_scalar(sql!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = sn.local_artifact_id,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
           FROM snapshots sn
          WHERE s.id = $1 AND sn.sandbox_id = s.id
          RETURNING sn.id"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    Ok((substrate, wake_attempts, worker, s))
}

fn drain_restore(target: EvacuationRestoreTarget, s: Uuid) -> DrainRestore {
    DrainRestore {
        target,
        snapshot_id: s,
        restore_lease_token: Uuid::now_v7(),
    }
}

/// Put `sandbox_id`'s VM on the scripted node in `status`, as the restore
/// pod's sandbox creation leaves it: running, whatever the target.
async fn node_vm(
    worker: &kubernetes_tests::SequencedWakeWorkerHandle,
    sandbox_id: SandboxId,
    status: &str,
) -> Result<(), BoxError> {
    let mut record = pplx_space_node_api_sim::state::SandboxRecord::new_creating(
        sandbox_id.to_string(),
        "base".to_owned(),
        "fc".to_owned(),
        "root".to_owned(),
        None,
        false,
    );
    record.status = status.to_owned();
    worker.sim_state.insert_new(record).await?;
    Ok(())
}

/// The node's view of `sandbox_id`'s VM, `None` when it has none.
async fn node_vm_status(
    worker: &kubernetes_tests::SequencedWakeWorkerHandle,
    sandbox_id: SandboxId,
) -> Option<String> {
    worker
        .sim_state
        .get(&sandbox_id.to_string())
        .await
        .ok()
        .map(|record| record.status)
}

/// The activation attempt the active execution still holds.
async fn held_attempt(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<Uuid>, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The row's lifecycle, placement and lease, and its active executions.
async fn landing(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<(String, String, Option<Uuid>, i64), BoxError> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, restore_lease_token,
                (SELECT COUNT(*) FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = s.id AND e.active)
           FROM sandboxes s
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Each target lands in its own lifecycle, on its node, its lease cleared:
/// one Wake, toward `started` whatever the target, one active execution,
/// the old pod kept, and the attempt the restore ran under released. The
/// node restored the VM running; a `paused` target pauses it, a `started`
/// one leaves it running.
#[rstest::rstest]
#[case(EvacuationRestoreTarget::Started)]
#[case(EvacuationRestoreTarget::Paused)]
#[tokio::test(flavor = "current_thread")]
async fn each_target_lands_with_one_execution(
    #[case] target: EvacuationRestoreTarget,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, worker, s) = drained_subject(&db.pool, sandbox_id).await?;
    node_vm(&worker, sandbox_id, "running").await?;

    let landed = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            options(),
            None,
            drain_restore(target, s),
        )
        .await?;

    assert_eq!(landed, DrainRestoreAttempt::Landed, "{target}");
    let expected_vm = match target {
        EvacuationRestoreTarget::Paused => "paused",
        _ => "running",
    };
    assert_eq!(
        node_vm_status(&worker, sandbox_id).await.as_deref(),
        Some(expected_vm),
        "{target}"
    );
    assert_eq!(held_attempt(&db.pool, sandbox_id).await?, None, "{target}");
    assert_eq!(
        landing(&db.pool, sandbox_id).await?,
        (target.as_str().to_owned(), "on_node".to_owned(), None, 1),
        "{target}"
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 1, "{target}");
    let received = worker.received.lock().unwrap().clone();
    assert!(
        received.iter().all(|wake| wake.runtime_upgrade.is_none()),
        "a drain restore keeps the sandbox's environment: {received:?}"
    );
    assert!(
        received.iter().all(|wake| wake.target_state
            == pplx_space_sbx_mgr_client::proto::WakeTargetState::Started as i32),
        "{target}: a drain restore wakes the VM started: {received:?}"
    );
    Ok(())
}

/// A `paused` target whose VM an earlier attempt already paused lands
/// without pausing it again, which the node would refuse.
#[tokio::test(flavor = "current_thread")]
async fn a_vm_already_paused_is_not_paused_again() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, _wake_attempts, worker, s) = drained_subject(&db.pool, sandbox_id).await?;
    node_vm(&worker, sandbox_id, "paused").await?;

    let landed = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            options(),
            None,
            drain_restore(EvacuationRestoreTarget::Paused, s),
        )
        .await?;

    assert_eq!(landed, DrainRestoreAttempt::Landed);
    assert_eq!(landing(&db.pool, sandbox_id).await?.0, "paused");
    assert_eq!(
        node_vm_status(&worker, sandbox_id).await.as_deref(),
        Some("paused")
    );
    Ok(())
}

/// A `paused` target whose VM can't be paused (here the node's is stopped) fails
/// the attempt: the claim is reverted to `suspended` rather than committed
/// over a running VM, and the attempt the restore ran under is released.
#[tokio::test(flavor = "current_thread")]
async fn a_pause_that_fails_reverts_the_claim_and_releases_the_attempt() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, _wake_attempts, worker, s) = drained_subject(&db.pool, sandbox_id).await?;
    node_vm(&worker, sandbox_id, "stopped").await?;

    let error = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            options(),
            None,
            drain_restore(EvacuationRestoreTarget::Paused, s),
        )
        .await
        .expect_err("no running VM to pause");

    assert!(matches!(error, SubstrateError::Failed { .. }), "{error:?}");
    let (lifecycle, _, lease, _) = landing(&db.pool, sandbox_id).await?;
    assert_eq!((lifecycle.as_str(), lease), ("suspended", None));
    let held: Vec<Option<Uuid>> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_all(&db.pool)
    .await?;
    assert!(held.iter().all(Option::is_none), "{held:?}");
    Ok(())
}

/// An attempt whose VM never comes up times out, and is reverted as a failed
/// attempt is: the row is `suspended` again with its lease cleared, so the
/// operation's retry claims it at once rather than waiting out the lease and
/// the sweeper. The attempt the restore ran under is released.
#[tokio::test(flavor = "current_thread")]
async fn an_attempt_that_times_out_reverts_the_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, _wake_attempts, _worker, s) =
        drained_subject_waking_after(&db.pool, sandbox_id, None).await?;

    let error = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            ActivationOptions {
                ready_timeout: Duration::from_millis(500),
                ..options()
            },
            None,
            drain_restore(EvacuationRestoreTarget::Started, s),
        )
        .await
        .expect_err("the VM never comes up");

    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    let (lifecycle, _, lease, _) = landing(&db.pool, sandbox_id).await?;
    assert_eq!((lifecycle.as_str(), lease), ("suspended", None));
    let held: Vec<Option<Uuid>> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_all(&db.pool)
    .await?;
    assert!(held.iter().all(Option::is_none), "{held:?}");
    Ok(())
}

/// Put the row `restoring` under a live lease `token`, as a drain restore's
/// claim leaves it.
async fn claim_held(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    token: Uuid,
) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring',
                restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '1 minute'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(token)
    .execute(pool)
    .await?;
    Ok(())
}

/// A claim the row refuses answers why before any node work, and leaves the
/// row as it was.
#[tokio::test(flavor = "current_thread")]
async fn a_refused_claim_sends_no_wake() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, _worker, s) = drained_subject(&db.pool, sandbox_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let refused = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            options(),
            None,
            drain_restore(EvacuationRestoreTarget::Started, s),
        )
        .await?;

    assert_eq!(
        refused,
        DrainRestoreAttempt::Refused(DrainRestoreRefusal::Superseded)
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(landing(&db.pool, sandbox_id).await?.0, "started");
    Ok(())
}

/// A user's wake while a drain restore holds the row waits for that restore
/// as it waits for any held restore, inside its readiness budget, and takes
/// nothing: no claim, no execution of its own, the restore's row untouched.
#[tokio::test(flavor = "current_thread")]
async fn a_wake_waits_for_a_held_drain_restore_and_takes_nothing() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, _wake_attempts, _worker, _s) = drained_subject(&db.pool, sandbox_id).await?;
    claim_held(&db.pool, sandbox_id, Uuid::now_v7()).await?;
    // The restore's attempt on the execution it materialized.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '1 minute',
                activation_attempt_target_state = 'started'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    let sandbox = substrate.store.require_sandbox(sandbox_id).await?;

    let before = landing(&db.pool, sandbox_id).await?;

    let error = substrate
        .wake_execution_with_metric_identity(
            &sandbox,
            Duration::from_millis(500),
            SandboxApiMetricIdentity::ecw("connect"),
            RecoveryCharge::Exempt,
        )
        .await
        .expect_err("nothing lands the held restore here");
    assert!(
        matches!(error, SubstrateError::RestoreInFlight { .. }),
        "{error:?}"
    );
    assert_eq!(
        landing(&db.pool, sandbox_id).await?,
        before,
        "the restore keeps the row and its one execution"
    );
    let generations: Vec<i64> = sqlx::query_scalar(sql!(
        "SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(generations, [1], "no second execution");
    Ok(())
}
