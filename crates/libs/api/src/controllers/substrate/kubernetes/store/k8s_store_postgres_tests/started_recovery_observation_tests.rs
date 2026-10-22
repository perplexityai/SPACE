//! Postgres behaviors for a started-recovery follower's observation
//! (`K8sStore::observe_started_recovery`).
//!
//! A sibling module rather than more lines in the parent, which is already far
//! past the repository's file-size guidance. Driven from
//! `k8s_store_postgres_behaviors` against the parent's fixture.

use pplx_space_common_core::types::SandboxId;

use super::*;

/// A follower of started recovery at generation 1 reports success only for a
/// running execution on a `started` sandbox. If a suspend and a restore landed
/// in between, the generation it finds running belongs to that restore, and
/// while the sandbox is still `restoring` the follower keeps waiting: handing
/// it out would route a user request to a restore that can still be rolled
/// back.
pub(super) async fn started_recovery_followers_succeed_only_on_a_started_sandbox(
    fixture: &PostgresStore,
) -> TestResult {
    let mut mismatches = Vec::new();
    for (lifecycle, expected_success, expected_pending) in [
        ("started", true, false),
        ("restoring", false, true),
        ("suspended", false, false),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(&fixture.pool, sandbox_id, lifecycle).await?;
        // The generation a later restore created, already running.
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active, pod_ip)
             VALUES ($1, 2, $2, 'space', 'uid-observed-g2', 'running', TRUE, '10.80.0.9')"
        ))
        .bind(sandbox_id)
        .bind(format!("space-sbx-{}-g2", sandbox_id.as_uuid().simple()))
        .execute(&fixture.pool)
        .await?;

        let observation = fixture
            .store
            .observe_started_recovery(sandbox_id, 1)
            .await?;

        let succeeded = matches!(observation, StartedRecoveryObservation::Succeeded { .. });
        let pending = matches!(observation, StartedRecoveryObservation::Pending);
        if (succeeded, pending) != (expected_success, expected_pending) {
            mismatches.push(format!("{lifecycle}: observed {observation:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    Ok(())
}
