//! A start whose Wake sandbox-manager refuses for an unordered restore source
//! gives its activation attempt back only when no earlier send of that Wake
//! can still be landing, and a restore reverts its claim on the same terms.

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sql_locator::sql;

use super::*;
use crate::controllers::substrate::{ActivationOptions, RecoveryCharge, SubstrateAdapter};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The refusal comes before sandbox-manager stages or boots anything, so the
/// refused send leaves no node-side work. A send answered "not ready" before
/// it may still be landing, and keeps the attempt for that Wake to converge.
#[rstest::rstest]
#[case::the_first_send(1, false)]
#[case::a_send_after_a_not_ready_answer(2, true)]
#[tokio::test(flavor = "current_thread")]
async fn a_start_refused_for_an_unordered_restore_source_keeps_its_attempt_only_after_an_earlier_send(
    #[case] refused_send: usize,
    #[case] attempt_kept: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, _pod_gets, worker) =
        paused_restore_test_subject(&db.pool, sandbox_id, Some(refused_send), true).await?;
    // A resident paused sandbox, whose start wakes the pod it already has.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'paused', evacuation_restore_target = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    *worker.rejection.lock().unwrap() =
        Some(pplx_space_sbx_mgr_client::restore_source_unordered_status(
            "sandbox captures can't be ordered",
        ));

    let error = substrate
        .start_execution(
            SandboxApiMetricIdentity::native("start"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(2),
            },
            RecoveryCharge::exempt_for_tests(),
        )
        .await
        .expect_err("a refused Wake starts nothing");

    assert!(
        matches!(error, SubstrateError::RestoreContextUnavailable { .. }),
        "{error:?}"
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), refused_send);
    let attempt_held: bool = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id IS NOT NULL
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(attempt_held, attempt_kept);
    Ok(())
}

/// A restore's claim returns to `suspended` on the same evidence: a refusal
/// of its only send left nothing on the node, while one that follows a "not
/// ready" answer leaves the row `restoring` for the earlier send to converge.
#[rstest::rstest]
#[case::the_first_send(1, "suspended")]
#[case::a_send_after_a_not_ready_answer(2, "restoring")]
#[tokio::test(flavor = "current_thread")]
async fn a_restore_refused_for_an_unordered_restore_source_reverts_its_claim_only_on_the_first_send(
    #[case] refused_send: usize,
    #[case] lifecycle_after: &str,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let (substrate, wake_attempts, _pod_gets, worker) =
        paused_restore_test_subject(&db.pool, sandbox_id, Some(refused_send), true).await?;
    *worker.rejection.lock().unwrap() =
        Some(pplx_space_sbx_mgr_client::restore_source_unordered_status(
            "sandbox captures can't be ordered",
        ));

    let error = substrate
        .restore_evacuated_execution(
            SandboxApiMetricIdentity::ecw("evacuation"),
            sandbox_id,
            ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_secs(2),
            },
            None,
            SandboxLifecycle::Paused,
        )
        .await
        .expect_err("a refused Wake restores nothing");

    assert!(
        matches!(error, SubstrateError::RestoreContextUnavailable { .. }),
        "{error:?}"
    );
    assert_eq!(wake_attempts.load(Ordering::SeqCst), refused_send);
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(lifecycle, lifecycle_after);
    Ok(())
}
