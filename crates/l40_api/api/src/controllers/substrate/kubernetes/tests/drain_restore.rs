//! The drain restore operation's restore through the shared executor
//! ([`KubernetesSubstrate::restore_drained_execution`]), and what a user's
//! call meets while its episode is open.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_sandbox_store::drain_restore::DrainRestoreRefusal;
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

fn drain_restore(s: Uuid) -> DrainRestore {
    DrainRestore {
        snapshot_id: s,
        restore_lease_token: Uuid::now_v7(),
    }
}

/// Put `sandbox_id`'s VM on the scripted node in `status`, as the restore
/// pod's sandbox creation leaves it: running.
pub(crate) async fn node_vm(
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
pub(crate) async fn node_vm_status(
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

/// The restore lands `started`, on its node, its lease cleared: one Wake,
/// toward `started`, one active execution, the old pod kept, the VM left
/// running, and the attempt the restore ran under released.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_lands_started_with_one_execution() -> Result<(), BoxError> {
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
            drain_restore(s),
        )
        .await?;

    assert!(matches!(landed, DrainRestoreAttempt::Landed), "{landed:?}");
    assert_eq!(
        node_vm_status(&worker, sandbox_id).await.as_deref(),
        Some("running")
    );
    assert_eq!(held_attempt(&db.pool, sandbox_id).await?, None);
    assert_eq!(
        landing(&db.pool, sandbox_id).await?,
        ("started".to_owned(), "on_node".to_owned(), None, 1)
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 1);
    let received = worker.received.lock().unwrap().clone();
    assert!(
        received.iter().all(|wake| wake.runtime_upgrade.is_none()),
        "a drain restore keeps the sandbox's environment: {received:?}"
    );
    assert!(
        received.iter().all(|wake| wake.target_state
            == pplx_space_sbx_mgr_client::proto::WakeTargetState::Started as i32),
        "a drain restore wakes the VM started: {received:?}"
    );
    Ok(())
}

/// [`drained_subject`] with abandoned-restore retirement off, as production
/// runs by default: a reverted claim leaves the restore's execution active.
pub(crate) async fn drained_subject_without_retirement(
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
    let (mut substrate, wake_attempts, worker, s) = drained_subject(pool, sandbox_id).await?;
    substrate.k8s.abandoned_restore_retirement =
        pplx_space_common_core::types::AbandonedRestoreRetirement::Disabled;
    Ok((substrate, wake_attempts, worker, s))
}

/// The node's activation-completed receipt for `attempt` on `generation`,
/// toward `started`, through the ingest path that projects it. Returns
/// whether it landed the row.
async fn started_receipt(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    attempt: Uuid,
) -> Result<bool, BoxError> {
    let node: pplx_space_accounts::NodeId =
        sqlx::query_scalar(sql!("SELECT active_node_id FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
    let rows = sqlx::types::Json(serde_json::json!([{
        "sandbox_id": sandbox_id,
        "published_lifecycle": "started",
        "transition_receipt_kind":
            pplx_space_common_core::types::SandboxTransitionReceiptKind::ActivationCompleted.as_str(),
        "transition_receipt_target_state": "started",
        "transition_receipt_execution_generation": generation,
        "transition_receipt_activation_attempt_id": attempt,
    }]));
    let landed =
        pplx_space_sandbox_store::ingest::release_completed_activations(pool, node, rows).await?;
    Ok(landed == [sandbox_id])
}

/// An attempt whose Wake was sent but whose VM came up only after the
/// attempt gave up keeps its claim: the row stays `restoring` under the
/// operation's lease, on its one execution, with the attempt held for the
/// node's receipt, instead of `suspended` over a live VM. The next attempt
/// finds the row held and sends no Wake; once the receipt lands the row
/// `started`, the next one finds nothing to claim. One Wake in all.
#[tokio::test(flavor = "current_thread")]
async fn a_timeout_after_the_wake_was_sent_keeps_the_claim_for_the_receipt() -> Result<(), BoxError>
{
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, worker, s) =
        drained_subject_without_retirement(&db.pool, sandbox_id).await?;
    // The node answers the Wake only after the attempt's budget.
    worker.delay_wakes(Duration::from_millis(1500));
    let restore = drain_restore(s);
    let attempt = || {
        substrate.restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            ActivationOptions {
                ready_timeout: Duration::from_millis(500),
                ..options()
            },
            None,
            restore,
        )
    };

    let error = attempt().await.expect_err("the attempt times out");
    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    let (lifecycle, _, lease, executions) = landing(&db.pool, sandbox_id).await?;
    assert_eq!(
        (lifecycle.as_str(), lease, executions),
        ("restoring", Some(restore.restore_lease_token), 1),
        "the claim stays the operation's"
    );
    let held = held_attempt(&db.pool, sandbox_id)
        .await?
        .expect("the attempt stays held for the node's receipt");

    // The VM comes up after the attempt gave up.
    node_vm(&worker, sandbox_id, "running").await?;
    let second = attempt().await?;
    assert!(
        matches!(
            second,
            DrainRestoreAttempt::Refused(DrainRestoreRefusal::InFlight)
        ),
        "{second:?}"
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 1, "no second Wake");

    let generation: i64 = sqlx::query_scalar(sql!(
        "SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(started_receipt(&db.pool, sandbox_id, generation, held).await?);
    let third = attempt().await?;
    assert!(
        matches!(
            third,
            DrainRestoreAttempt::Refused(DrainRestoreRefusal::Superseded)
        ),
        "{third:?}"
    );
    assert_eq!(
        landing(&db.pool, sandbox_id).await?,
        ("started".to_owned(), "on_node".to_owned(), None, 1)
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 1, "one Wake in all");
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
            drain_restore(s),
        )
        .await?;

    assert!(
        matches!(
            refused,
            DrainRestoreAttempt::Refused(DrainRestoreRefusal::Superseded)
        ),
        "{refused:?}"
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
            RecoveryCharge::exempt_for_tests(),
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

/// Only the drain restore's claim hands back the attempt its wake ran
/// under, for its own commit to release. A user's start of a suspended
/// sandbox runs the same wake and gets none, so the start's caller has no
/// attempt of its own to release.
#[tokio::test(flavor = "current_thread")]
async fn a_users_start_of_a_suspended_sandbox_hands_back_no_attempt() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, worker, _s) = drained_subject(&db.pool, sandbox_id).await?;
    node_vm(&worker, sandbox_id, "running").await?;

    let activation = crate::controllers::substrate::SubstrateAdapter::start_execution(
        &substrate,
        SandboxApiMetricIdentity::native("start"),
        sandbox_id,
        options(),
        RecoveryCharge::exempt_for_tests(),
    )
    .await?;

    assert_eq!(wake_attempts.load(Ordering::SeqCst), 1);
    assert!(
        activation.lease_completion.is_none(),
        "{:?}",
        activation.lease_completion
    );
    Ok(())
}
