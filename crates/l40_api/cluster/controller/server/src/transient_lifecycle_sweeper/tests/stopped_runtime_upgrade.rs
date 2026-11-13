//! A stopped sandbox's runtime upgrade boots its disk on an execution whose
//! activation attempt targets `stopped`, while the row stays `stopped`. No
//! pass of the sweep lists that row or moves it: not while the upgrade's
//! lease runs, and not once a failed attempt's lease expired, which the
//! upgrade's retry takes over, nor the row an unsettled upgrade leaves without a
//! pod.

use std::sync::Arc;

use pplx_space_api::controllers::substrate::Substrate;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::super::lifecycle_writer_tests::age;
use super::super::{
    Reconciler, ReconcilerConfig, SWEEPABLE_LIFECYCLES, SubstrateSweepSink, SweepSink, TickReport,
};
use super::activation_owner::lifecycle_of;
use super::episode_deadline::execution_active;
use super::sweep_config;
use crate::lifecycle_writer_test_support::{
    ExecutionSeed, SandboxSeed, UnsettledUpgradePod, assert_left_for_retry, insert_execution,
    insert_node, insert_sandbox, insert_unsettled_stopped_runtime_upgrade, kubernetes_substrate,
    unsettled_upgrade_row,
};

/// A `stopped` Kubernetes row an hour past every age threshold, with a
/// running execution the upgrade claimed under a lease ending `lease_secs`
/// from now.
async fn upgrading_row(pool: &PgPool, lease_secs: i64) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let node_id = insert_node(pool, None, None).await?;
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new("stopped", "k8s").on_node(node_id),
    )
    .await?;
    insert_execution(pool, sandbox_id, ExecutionSeed::RUNNING).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = attempt.id,
                route_activation_id = attempt.id,
                activation_attempt_expires_at = NOW() + make_interval(secs => $2::BIGINT),
                activation_attempt_target_state = 'stopped',
                updated_at = NOW() - INTERVAL '1 hour'
           FROM (SELECT space_uuid_v7() AS id) attempt
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(lease_secs)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - INTERVAL '1 hour',
                command_ready_pending = TRUE
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

#[tokio::test(flavor = "current_thread")]
async fn no_sweep_pass_takes_a_stopped_runtime_upgrade_execution() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = SubstrateSweepSink::new(
        pool.clone(),
        Arc::new(Substrate::from_kubernetes(kubernetes_substrate(pool)?)),
    );

    for lease_secs in [300, -3600] {
        let id = upgrading_row(pool, lease_secs).await?;
        let mut taken = Vec::new();
        for &lifecycle in SWEEPABLE_LIFECYCLES {
            if sink
                .list_aged_transient(lifecycle, age())
                .await?
                .iter()
                .any(|candidate| candidate.sandbox_id == id)
            {
                taken.push(format!("aged {lifecycle}"));
            }
        }
        if sink
            .list_unrestorable_restoring(age())
            .await?
            .iter()
            .any(|(candidate, _)| *candidate == id)
        {
            taken.push("unrestorable".to_string());
        }
        if sink
            .list_failed_active_executions(1000)
            .await?
            .iter()
            .any(|candidate| candidate.sandbox_id == id)
        {
            taken.push("failed cleanup".to_string());
        }
        if sink
            .list_command_ready_pending(age())
            .await?
            .iter()
            .any(|candidate| candidate.sandbox_id == id)
        {
            taken.push("command-ready pending".to_string());
        }

        assert!(taken.is_empty(), "lease {lease_secs}s: {taken:?}");
        assert_eq!(
            lifecycle_of(pool, id).await?,
            SandboxLifecycle::Stopped.as_str()
        );
        assert!(execution_active(pool, id).await?, "lease {lease_secs}s");
    }
    Ok(())
}

/// An unsettled upgrade leaves an active execution, with its pod or with
/// none, an expired claim and the recorded upgrade, for its retry to reuse. A
/// tick with every pass enabled lists nothing and leaves the row alone.
#[rstest::rstest]
#[case::with_its_pod(UnsettledUpgradePod::Running)]
#[case::before_its_pod(UnsettledUpgradePod::NeverCreated)]
#[tokio::test(flavor = "current_thread")]
async fn an_unsettled_stopped_runtime_upgrade_row_is_left_for_its_retry(
    #[case] pod: UnsettledUpgradePod,
) -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = insert_unsettled_stopped_runtime_upgrade(pool, pod).await?;
    let before = unsettled_upgrade_row(pool, sandbox_id)
        .await?
        .ok_or("seeded row missing")?;
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            command_ready_pending_enabled: true,
            ..sweep_config()
        },
        SubstrateSweepSink::new(
            pool.clone(),
            Arc::new(Substrate::from_kubernetes(kubernetes_substrate(pool)?)),
        ),
    );

    let report = Box::pin(reconciler.tick()).await;

    assert_eq!(report, TickReport::default());
    assert_left_for_retry(
        &before,
        unsettled_upgrade_row(pool, sandbox_id).await?.as_ref(),
        "transient-lifecycle sweep tick",
    );
    Ok(())
}
