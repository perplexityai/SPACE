//! Postgres behaviors for the restore owner's own `restoring -> started`
//! commit (`K8sStore::complete_owned_user_restore`).
//!
//! A sibling module rather than more lines in the parent, which is already far
//! past the repository's file-size guidance. Driven from
//! `k8s_store_postgres_behaviors` against the parent's fixture.

use pplx_space_common_core::types::SandboxId;

use super::*;

/// What stands between the owner's wake and its commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// The owner's restore is live behind a running execution with a pod IP.
    Routable,
    /// A newer claim holds the row under another lease token.
    AnotherOwner,
    /// A rollback moved the row back to `suspended`.
    RolledBack,
    /// The execution has no pod IP yet.
    Unroutable,
}

/// The owner of a user restore commits `started` only for its own lease, on
/// a row still `restoring` with a routable execution. Once it has, the API
/// rollback that would abandon the restore finds nothing to revert.
pub(super) async fn owned_restore_completion_requires_its_lease_and_a_routable_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let mut mismatches = Vec::new();
    for shape in [
        Shape::Routable,
        Shape::AnotherOwner,
        Shape::RolledBack,
        Shape::Unroutable,
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
        let token = user_fresh_restore_token(
            fixture
                .store
                .begin_restore_lifecycle(sandbox_id, false)
                .await?,
        );
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
                  pod_ip)
             VALUES ($1, 1, $2, 'space', 'uid-owned-g1', 'running', TRUE,
                     CASE WHEN $3 THEN NULL ELSE '10.80.0.7'::inet END)"
        ))
        .bind(sandbox_id)
        .bind(format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple()))
        .bind(shape == Shape::Unroutable)
        .execute(&fixture.pool)
        .await?;
        match shape {
            Shape::AnotherOwner => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET restore_lease_token = $2 WHERE id = $1"
                ))
                .bind(sandbox_id)
                .bind(Uuid::now_v7())
                .execute(&fixture.pool)
                .await?;
            }
            Shape::RolledBack => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&fixture.pool)
                .await?;
            }
            Shape::Routable | Shape::Unroutable => {}
        }
        let lifecycle_before: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(&fixture.pool)
                .await?;

        let completed = fixture
            .store
            .complete_owned_user_restore(sandbox_id, token)
            .await?;
        let reverted = fixture
            .store
            .revert_restore_claim_to_suspended(
                sandbox_id,
                Some(token),
                None,
                crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
            )
            .await?;
        let (lifecycle, placement): (String, String) = sqlx::query_as(sql!(
            "SELECT lifecycle, placement FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;

        let expected_completed = shape == Shape::Routable;
        if completed != expected_completed {
            mismatches.push(format!(
                "{shape:?}: completed {completed}, expected {expected_completed}"
            ));
        }
        if expected_completed {
            if (lifecycle.as_str(), placement.as_str()) != ("started", "on_node") {
                mismatches.push(format!(
                    "{shape:?}: row is {lifecycle}/{placement} after the commit"
                ));
            }
            if reverted != RestoreClaimRevert::NotReverted {
                mismatches.push(format!(
                    "{shape:?}: a rollback after the commit reported {reverted:?}"
                ));
            }
        } else if lifecycle == "started" {
            mismatches.push(format!(
                "{shape:?}: the row moved to started from {lifecycle_before}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    Ok(())
}
