//! The drain restore operation's restore through the shared executor
//! ([`KubernetesSubstrate::restore_drained_execution`]), and what a user's
//! call meets while its episode is open.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_sandbox_store::drain_restore::DrainRestoreTarget;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::{ActivationOptions, RecoveryCharge, SubstrateAdapter};

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
    let (substrate, wake_attempts, _pod_gets, worker) =
        paused_restore_test_subject(pool, sandbox_id, Some(1), true).await?;
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

fn drain_restore(target: DrainRestoreTarget, s: Uuid) -> DrainRestore {
    DrainRestore {
        target,
        snapshot_id: s,
        restore_lease_token: Uuid::now_v7(),
    }
}

/// The row's episode columns and its active executions.
async fn landing(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<(String, String, Option<String>, Option<Uuid>, i64), BoxError> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, restore_episode_target, restore_lease_token,
                (SELECT COUNT(*) FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = s.id AND e.active)
           FROM sandboxes s
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Each target lands in its own lifecycle, on its node, with the episode
/// over: one Wake, one active execution, and the old pod kept.
#[rstest::rstest]
#[case(DrainRestoreTarget::Started)]
#[case(DrainRestoreTarget::Paused)]
#[tokio::test(flavor = "current_thread")]
async fn each_target_lands_with_one_execution(
    #[case] target: DrainRestoreTarget,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, worker, s) = drained_subject(&db.pool, sandbox_id).await?;

    let landed = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            options(),
            None,
            drain_restore(target, s),
        )
        .await?;

    assert!(landed, "{target}: the commit found nothing to land");
    assert_eq!(
        landing(&db.pool, sandbox_id).await?,
        (
            target.as_str().to_owned(),
            "on_node".to_owned(),
            None,
            None,
            1
        ),
        "{target}"
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 1, "{target}");
    let received = worker.received.lock().unwrap().clone();
    assert!(
        received.iter().all(|wake| wake.runtime_upgrade.is_none()),
        "a drain restore keeps the sandbox's environment: {received:?}"
    );
    Ok(())
}

/// An episode an earlier attempt left open, its lease lapsed, is taken back
/// under the new attempt's lease and landed.
#[tokio::test(flavor = "current_thread")]
async fn a_redrive_lands_an_episode_an_earlier_attempt_left_open() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, _wake_attempts, _worker, s) = drained_subject(&db.pool, sandbox_id).await?;
    leave_episode_open(&db.pool, sandbox_id, "started", Uuid::now_v7(), false).await?;

    let restore = drain_restore(DrainRestoreTarget::Started, s);
    let landed = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            options(),
            None,
            restore,
        )
        .await?;

    assert!(landed);
    assert_eq!(landing(&db.pool, sandbox_id).await?.0, "started");
    Ok(())
}

/// Put the row in a drain restore's episode toward `target` under `token`,
/// as an attempt that reached the node and released its lease, or one still
/// holding it, leaves it.
async fn leave_episode_open(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    target: &str,
    token: Uuid,
    lease_live: bool,
) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring',
                restore_episode_target = $2,
                restore_lease_token = $3,
                restore_lease_expires_at = NOW() + CASE WHEN $4 THEN INTERVAL '1 minute'
                                                        ELSE INTERVAL '-1 minute' END
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(target)
    .bind(token)
    .bind(lease_live)
    .execute(pool)
    .await?;
    Ok(())
}

/// A claim the row refuses fails as `EvacuationClaimLost` before any node
/// work, and leaves the row as it was.
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

    let error = substrate
        .restore_drained_execution(
            SandboxApiMetricIdentity::native("drain_restore"),
            sandbox_id,
            options(),
            None,
            drain_restore(DrainRestoreTarget::Started, s),
        )
        .await
        .expect_err("a started row refuses the claim");

    assert!(
        matches!(error, SubstrateError::EvacuationClaimLost { .. }),
        "{error:?}"
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(landing(&db.pool, sandbox_id).await?.0, "started");
    Ok(())
}

/// While a drain restore's episode is open, an ECW `resume` (a user
/// restore) is refused as a conflict and an ECW `pause` (a suspend) as
/// unavailable, and neither touches the episode.
#[tokio::test(flavor = "current_thread")]
async fn ecw_resume_and_pause_are_refused_during_the_episode() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, _worker, _s) = drained_subject(&db.pool, sandbox_id).await?;
    let token = Uuid::now_v7();
    leave_episode_open(&db.pool, sandbox_id, "started", token, true).await?;
    let open = landing(&db.pool, sandbox_id).await?;

    let resume = substrate
        .restore_execution(
            SandboxApiMetricIdentity::ecw("resume"),
            sandbox_id,
            options(),
            RecoveryCharge::Exempt,
        )
        .await
        .expect_err("a user restore of a drain restore's episode");
    assert!(
        matches!(resume, SubstrateError::Conflict { .. }),
        "ECW resume: {resume:?}"
    );

    let pause = substrate
        .suspend_execution(sandbox_id)
        .await
        .expect_err("a suspend of a drain restore's episode");
    assert!(
        matches!(pause, SubstrateError::Unavailable { .. }),
        "ECW pause: {pause:?}"
    );

    assert_eq!(landing(&db.pool, sandbox_id).await?, open);
    assert_eq!(wake_attempts.load(Ordering::SeqCst), 0);
    Ok(())
}

/// A user's wake of a drain restore's episode joins the restore's live
/// activation attempt when it heads for the same lifecycle, materializing
/// nothing of its own and leaving the commit to the restore, and fails as
/// `ActivationInFlight` when the restore heads for another.
#[rstest::rstest]
#[case("started", true)]
#[case("paused", false)]
#[tokio::test(flavor = "current_thread")]
async fn a_wake_joins_a_same_target_episode_and_waits_out_another(
    #[case] target: &str,
    #[case] joins: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, _wake_attempts, _worker, _s) = drained_subject(&db.pool, sandbox_id).await?;
    leave_episode_open(&db.pool, sandbox_id, target, Uuid::now_v7(), true).await?;
    // The restore's attempt on the execution it materialized.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '1 minute',
                activation_attempt_target_state = $3
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .bind(target)
    .execute(&db.pool)
    .await?;
    let sandbox = substrate.store.require_sandbox(sandbox_id).await?;

    let woke = substrate
        .wake_execution_with_metric_identity(
            &sandbox,
            Duration::from_secs(2),
            SandboxApiMetricIdentity::ecw("connect"),
            RecoveryCharge::Exempt,
        )
        .await;

    if joins {
        let woke = woke.map_err(|error| format!("the same-target wake must join: {error:?}"))?;
        assert_eq!(woke.binding.execution_generation, 1, "{woke:?}");
    } else {
        let error = woke.expect_err("a wake toward another lifecycle");
        assert!(
            matches!(error, SubstrateError::ActivationInFlight { .. }),
            "{error:?}"
        );
    }
    let (lifecycle, _, episode_target, _, active) = landing(&db.pool, sandbox_id).await?;
    assert_eq!(
        (lifecycle.as_str(), episode_target.as_deref(), active),
        ("restoring", Some(target), 1),
        "the restore keeps its episode and its one execution"
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
