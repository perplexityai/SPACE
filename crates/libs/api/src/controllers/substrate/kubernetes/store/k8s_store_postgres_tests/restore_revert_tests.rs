//! Postgres behaviors for rolling a failed fresh restore claim back to
//! `suspended` (`K8sStore::revert_restore_claim_to_suspended`).
//!
//! A sibling module rather than more lines in the parent, which is already far
//! past the repository's file-size guidance. Driven from
//! `k8s_store_postgres_behaviors` against the parent's fixture.

use pplx_space_common_core::types::{
    LocalArtifactId, NodeId, SandboxActivationAttemptId, SandboxId,
};

use super::*;

/// Which execution row the sandbox carries when the revert runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Execution {
    /// The restore claim created it and its pod materialized (UID recorded,
    /// placed on a node), then the restore failed before dispatch.
    CreatedByClaim,
    /// It was already active when the claim reused it, so this restore didn't
    /// create it.
    PredatesClaim,
}

/// The activation attempt the rollback names as its claim's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimAttempt {
    /// The claim failed before it was assigned one.
    Unassigned,
    /// The attempt the execution carries.
    Assigned,
    /// Another attempt: a redrive took the execution over since.
    Superseded,
}

struct Case {
    name: &'static str,
    marker: bool,
    /// A ready, stored pre-restore snapshot (of the marker's artifact on a
    /// marker row), which is what proves restore selection stays durable.
    durable_source: bool,
    execution: Execution,
    /// A node converged the row to `started` before the rollback ran.
    converged_to_started: bool,
    /// Revert with the claim's lease token, or tokenless as an evacuation
    /// claim does.
    with_token: bool,
    attempt: ClaimAttempt,
    expected: Expected,
}

/// What the revert should report; the retired pod's name is per sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expected {
    ExecutionRetired,
    LifecycleOnly,
    ExecutionLeftActive,
    NotReverted,
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct RevertedRow {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    current_local_artifact_id: Option<LocalArtifactId>,
    execution_active: bool,
    execution_state: String,
}

async fn reverted_row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<RevertedRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, s.target_node_id,
                s.current_local_artifact_id,
                e.active AS execution_active, e.state AS execution_state
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

fn execution_pod_name(sandbox_id: SandboxId) -> String {
    format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple())
}

/// `created` offsets the row's `created_at` and `updated_at` from now. An
/// older row needs both: a recently updated active execution would block the
/// restore claim itself.
async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    created: &str,
    activation_attempt_id: SandboxActivationAttemptId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
              created_at, updated_at, activation_attempt_id, activation_attempt_expires_at,
              activation_attempt_target_state)
         VALUES ($1, 1, $3, 'space', 'uid-revert-g1', 'created', TRUE,
                 NOW() + $2::interval, NOW() + $2::interval, $4,
                 NOW() + INTERVAL '5 minutes', 'started')"
    ))
    .bind(sandbox_id)
    .bind(created)
    .bind(execution_pod_name(sandbox_id))
    .bind(activation_attempt_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A failed fresh restore claim's rollback records the abandonment on the
/// execution the claim created, retiring it as `replaced` with the row's
/// placement, while keeping the wake source (`target_node_id`, the artifact
/// marker). It retires nothing it can't prove disposable: an execution the
/// claim didn't create, or one whose sandbox has no proven durable restore
/// source, stays active; and a row that left `restoring` isn't touched.
pub(super) async fn failed_restore_revert_retires_only_its_own_proven_execution(
    fixture: &PostgresStore,
) -> TestResult {
    let cases = [
        Case {
            name: "claimed execution, durable markerless source",
            marker: false,
            durable_source: true,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::ExecutionRetired,
        },
        Case {
            name: "claimed execution, durable marker source",
            marker: true,
            durable_source: true,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Assigned,
            expected: Expected::ExecutionRetired,
        },
        Case {
            name: "claimed execution, tokenless claim with its attempt",
            marker: false,
            durable_source: true,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: false,
            attempt: ClaimAttempt::Assigned,
            expected: Expected::ExecutionRetired,
        },
        Case {
            name: "claimed execution, tokenless claim with no attempt",
            marker: false,
            durable_source: true,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: false,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::LifecycleOnly,
        },
        Case {
            name: "execution taken over by a tokenless redrive",
            marker: false,
            durable_source: true,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: false,
            attempt: ClaimAttempt::Superseded,
            expected: Expected::LifecycleOnly,
        },
        Case {
            name: "claimed execution, no durable source",
            marker: false,
            durable_source: false,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::ExecutionLeftActive,
        },
        Case {
            name: "execution predates the claim",
            marker: false,
            durable_source: true,
            execution: Execution::PredatesClaim,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::LifecycleOnly,
        },
        Case {
            name: "row converged to started first",
            marker: false,
            durable_source: true,
            execution: Execution::CreatedByClaim,
            converged_to_started: true,
            with_token: false,
            attempt: ClaimAttempt::Assigned,
            expected: Expected::NotReverted,
        },
    ];

    let mut mismatches = Vec::new();
    for case in cases {
        let sandbox_id = SandboxId::new();
        let node_id = NodeId::new();
        insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.11', 'worker', 'k8s', 'ready')"
        ))
        .bind(node_id)
        .execute(&fixture.pool)
        .await?;
        let execution_attempt = SandboxActivationAttemptId::new();
        let claim_attempt = match case.attempt {
            ClaimAttempt::Unassigned => None,
            ClaimAttempt::Assigned => Some((execution_attempt, 1)),
            ClaimAttempt::Superseded => Some((SandboxActivationAttemptId::new(), 1)),
        };
        let marker = case.marker.then(LocalArtifactId::new);
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET target_node_id = $2,
                    current_local_artifact_id = $3,
                    current_local_artifact_kind = CASE WHEN $3::uuid IS NULL THEN NULL
                                                       ELSE 'full' END,
                    current_local_artifact_origin = CASE WHEN $3::uuid IS NULL THEN NULL
                                                         ELSE 'suspend' END
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        .bind(marker)
        .execute(&fixture.pool)
        .await?;
        if case.durable_source {
            insert_ready_full_snapshot_relative_to_lifecycle(
                &fixture.pool,
                sandbox_id,
                "s3://bucket/pre-restore",
                -1,
                marker,
            )
            .await?;
        }
        if case.execution == Execution::PredatesClaim {
            insert_execution(&fixture.pool, sandbox_id, "-1 hour", execution_attempt).await?;
        }

        let token = user_fresh_restore_token(
            fixture
                .store
                .begin_restore_lifecycle(sandbox_id, false)
                .await?,
        );

        if case.execution == Execution::CreatedByClaim {
            insert_execution(&fixture.pool, sandbox_id, "0 seconds", execution_attempt).await?;
        }
        // The pod bound, so the row reads as placed on that node.
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET placement = 'on_node',
                    active_node_id = $2
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        .execute(&fixture.pool)
        .await?;
        if case.converged_to_started {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(&fixture.pool)
            .await?;
        }
        let before = reverted_row(&fixture.pool, sandbox_id).await?;

        let outcome = fixture
            .store
            .revert_restore_claim_to_suspended(
                sandbox_id,
                case.with_token.then_some(token),
                claim_attempt,
            )
            .await?;

        let (expected_outcome, expected_row) = match case.expected {
            Expected::ExecutionRetired => (
                RestoreClaimRevert::ExecutionRetired {
                    pod_name: execution_pod_name(sandbox_id),
                },
                RevertedRow {
                    lifecycle: "suspended".to_string(),
                    placement: "unplaced".to_string(),
                    active_node_id: None,
                    execution_active: false,
                    execution_state: "replaced".to_string(),
                    ..before
                },
            ),
            Expected::LifecycleOnly => (
                RestoreClaimRevert::LifecycleOnly,
                RevertedRow {
                    lifecycle: "suspended".to_string(),
                    ..before
                },
            ),
            Expected::ExecutionLeftActive => (
                RestoreClaimRevert::ExecutionLeftActive,
                RevertedRow {
                    lifecycle: "suspended".to_string(),
                    ..before
                },
            ),
            Expected::NotReverted => (RestoreClaimRevert::NotReverted, before),
        };
        // The expected rows keep `before`'s wake source: `target_node_id`
        // and the artifact marker.
        let actual = reverted_row(&fixture.pool, sandbox_id).await?;
        if outcome != expected_outcome || actual != expected_row {
            mismatches.push(format!("{}: {outcome:?}, {actual:?}", case.name));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
