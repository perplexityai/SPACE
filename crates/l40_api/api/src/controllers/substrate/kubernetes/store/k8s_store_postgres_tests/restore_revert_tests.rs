//! Postgres behaviors for rolling a failed fresh restore claim back to
//! `suspended` (`K8sStore::revert_restore_claim_to_suspended`).
//!
//! A sibling module rather than more lines in the parent, which is already very
//! large. Driven from `k8s_store_postgres_behaviors` against the parent's
//! fixture.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_snapshot_model::LocalArtifactId;

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
    /// The claim failed before any execution existed.
    Absent,
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
    /// A ready full snapshot registered after the restore claim, which would
    /// outrank the pre-claim source if reverting reset `lifecycle_changed_at`.
    post_claim_snapshot: bool,
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
    execution_active: Option<bool>,
    execution_state: Option<String>,
}

async fn reverted_row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<RevertedRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, s.target_node_id,
                s.current_local_artifact_id,
                e.active AS execution_active, e.state AS execution_state
           FROM sandboxes s
           LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

const PRE_RESTORE_URI: &str = "s3://bucket/pre-restore?region=us-east-1";
const POST_RESTORE_URI: &str = "s3://bucket/post-restore?region=us-east-1";

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
            post_claim_snapshot: false,
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
            post_claim_snapshot: false,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Assigned,
            expected: Expected::ExecutionRetired,
        },
        Case {
            name: "claimed execution, post-claim snapshot keeps restoring window",
            marker: false,
            durable_source: true,
            post_claim_snapshot: true,
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
            post_claim_snapshot: false,
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
            post_claim_snapshot: false,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: false,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::ExecutionLeftActive,
        },
        Case {
            name: "execution taken over by a tokenless redrive",
            marker: false,
            durable_source: true,
            post_claim_snapshot: false,
            execution: Execution::CreatedByClaim,
            converged_to_started: false,
            with_token: false,
            attempt: ClaimAttempt::Superseded,
            expected: Expected::ExecutionLeftActive,
        },
        Case {
            name: "claimed execution, no durable source",
            marker: false,
            durable_source: false,
            post_claim_snapshot: false,
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
            post_claim_snapshot: false,
            execution: Execution::PredatesClaim,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::ExecutionLeftActive,
        },
        Case {
            name: "no execution, durable source",
            marker: false,
            durable_source: true,
            post_claim_snapshot: false,
            execution: Execution::Absent,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::LifecycleOnly,
        },
        Case {
            name: "no execution, post-claim snapshot keeps restoring window",
            marker: false,
            durable_source: true,
            post_claim_snapshot: true,
            execution: Execution::Absent,
            converged_to_started: false,
            with_token: true,
            attempt: ClaimAttempt::Unassigned,
            expected: Expected::LifecycleOnly,
        },
        Case {
            name: "row converged to started first",
            marker: false,
            durable_source: true,
            post_claim_snapshot: false,
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
                PRE_RESTORE_URI,
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
        let claim_boundary: Option<OffsetDateTime> = sqlx::query_scalar(sql!(
            "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await?;
        if case.post_claim_snapshot {
            insert_ready_full_snapshot_relative_to_lifecycle(
                &fixture.pool,
                sandbox_id,
                POST_RESTORE_URI,
                1,
                Some(LocalArtifactId::new()),
            )
            .await?;
        }

        if case.execution == Execution::CreatedByClaim {
            insert_execution(&fixture.pool, sandbox_id, "0 seconds", execution_attempt).await?;
        }
        // The pod bound, so the row reads as placed on that node, as of now:
        // after the pre-restore snapshot was taken.
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET placement = 'on_node',
                    active_node_id = $2,
                    placement_changed_at = NOW()
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
                crate::controllers::substrate::AbandonedRestoreRetirement::Enabled,
            )
            .await?;

        let reverted_lifecycle = if case.post_claim_snapshot {
            "restoring"
        } else {
            "suspended"
        };
        let (expected_outcome, expected_row) = match case.expected {
            Expected::ExecutionRetired => (
                RestoreClaimRevert::ExecutionRetired {
                    pod_name: execution_pod_name(sandbox_id),
                },
                RevertedRow {
                    lifecycle: reverted_lifecycle.to_string(),
                    placement: "unplaced".to_string(),
                    active_node_id: None,
                    execution_active: Some(false),
                    execution_state: Some("replaced".to_string()),
                    ..before
                },
            ),
            Expected::LifecycleOnly => (
                RestoreClaimRevert::LifecycleOnly,
                RevertedRow {
                    lifecycle: reverted_lifecycle.to_string(),
                    ..before
                },
            ),
            Expected::ExecutionLeftActive => (
                RestoreClaimRevert::ExecutionLeftActive,
                RevertedRow {
                    lifecycle: reverted_lifecycle.to_string(),
                    ..before
                },
            ),
            Expected::NotReverted => (RestoreClaimRevert::NotReverted, before),
        };
        // The expected rows keep `before`'s wake source: `target_node_id`
        // and the artifact marker.
        let actual = reverted_row(&fixture.pool, sandbox_id).await?;
        // A reverted row with a durable source still restores from it,
        // although the bind moved its placement past that snapshot, and a row
        // kept in `restoring` to preserve its pre-claim source window can be
        // immediately re-claimed by the next wake without losing `claim_boundary`.
        let selected = if case.expected == Expected::NotReverted {
            None
        } else {
            let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
                .await?
                .expect("sandbox row exists");
            let selected_sandbox = if case.post_claim_snapshot {
                let prepared = fixture
                    .store
                    .prepare_wake_sandbox(&sandbox, sandbox_id, None)
                    .await?;
                let retry_boundary: Option<OffsetDateTime> = sqlx::query_scalar(sql!(
                    "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
                ))
                .bind(sandbox_id)
                .fetch_one(&fixture.pool)
                .await?;
                assert_eq!(retry_boundary, claim_boundary, "{}", case.name);
                assert!(prepared.restore_claim.is_some(), "{}", case.name);
                prepared.sandbox
            } else {
                sandbox
            };
            fixture
                .store
                .selected_suspend_restore_storage_uri(
                    &selected_sandbox,
                    RestoreSnapshotChoice::SourceNodeOrFresh,
                )
                .await?
        };
        // A reverted, proven row whose active execution is gone has its
        // placement clock cleared, so it selects its durable source. One that
        // keeps an execution the claim didn't create keeps the clock: nothing
        // proves that execution's pod holds no state of its own.
        let clock_cleared = case.durable_source
            && (case.expected == Expected::ExecutionRetired
                || (case.expected == Expected::LifecycleOnly
                    && case.execution == Execution::Absent));
        let expected_selected = clock_cleared.then_some(PRE_RESTORE_URI);
        // ...and is durably restorable at once, which is what lets a fenced
        // release of the abandoned pod's runtime go ahead.
        let durable =
            pplx_space_sandbox_store::durably_restorable(&fixture.pool, sandbox_id).await?;
        if outcome != expected_outcome
            || actual != expected_row
            || selected.as_deref() != expected_selected
            || durable != clock_cleared
        {
            mismatches.push(format!(
                "{}: {outcome:?}, {actual:?}, selected {selected:?}, durable {durable}",
                case.name
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// With abandoned-restore retirement disabled (the default until every
/// routing replica refuses to route to a `restoring` sandbox), a failed
/// fresh restore claim's rollback moves only the lifecycle, even with a
/// proven source: the execution the claim created stays active and the row
/// stays placed with its placement clock, so its pod isn't deleted under a
/// request an older replica may have routed to it.
pub(super) async fn without_retirement_a_failed_restore_revert_moves_only_the_lifecycle(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.12', 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        PRE_RESTORE_URI,
        -1,
        None,
    )
    .await?;
    let token = user_fresh_restore_token(
        fixture
            .store
            .begin_restore_lifecycle(sandbox_id, false)
            .await?,
    );
    insert_execution(
        &fixture.pool,
        sandbox_id,
        "0 seconds",
        SandboxActivationAttemptId::new(),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'on_node',
                active_node_id = $2,
                placement_changed_at = NOW()
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.pool)
    .await?;
    let before = reverted_row(&fixture.pool, sandbox_id).await?;
    let clock = || async {
        sqlx::query_scalar::<_, Option<OffsetDateTime>>(sql!(
            "SELECT placement_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&fixture.pool)
        .await
    };
    let clock_before = clock().await?;

    let outcome = fixture
        .store
        .revert_restore_claim_to_suspended(
            sandbox_id,
            Some(token),
            None,
            crate::controllers::substrate::AbandonedRestoreRetirement::Disabled,
        )
        .await?;

    assert_eq!(outcome, RestoreClaimRevert::ExecutionLeftActive);
    assert_eq!(
        reverted_row(&fixture.pool, sandbox_id).await?,
        RevertedRow {
            lifecycle: "suspended".to_string(),
            ..before
        }
    );
    assert_eq!(clock().await?, clock_before);
    Ok(())
}

/// One backup row of a full artifact.
struct Backup<'a> {
    artifact: LocalArtifactId,
    /// `suspend`, or `snapshot` for a user's named snapshot.
    origin: &'a str,
    state: &'a str,
    generation: Option<i64>,
    /// Registered this many minutes ago.
    minutes_ago: i32,
    uri: &'a str,
}

impl<'a> Backup<'a> {
    /// A ready suspend capture of a fresh artifact.
    fn suspend(generation: Option<i64>, minutes_ago: i32, uri: &'a str) -> Self {
        Self {
            artifact: LocalArtifactId::new(),
            origin: "suspend",
            state: "ready",
            generation,
            minutes_ago,
            uri,
        }
    }
}

async fn insert_backup(pool: &PgPool, sandbox_id: SandboxId, backup: &Backup<'_>) -> TestResult {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, artifact_snapshot_name, generation, org_id
         )
         VALUES ($1, $2, 'full', $3, NOW() - make_interval(mins => $4::INT), $5, 'full',
                 $6, CASE WHEN $6 = 'snapshot' THEN 'f-named' END, $7,
                 '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(backup.state)
    .bind(backup.minutes_ago)
    .bind(backup.artifact)
    .bind(backup.origin)
    .bind(backup.generation)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(snapshot_id)
    .bind(backup.uri)
    .execute(pool)
    .await?;
    Ok(())
}

/// A sandbox whose restore claimed it 15 minutes ago, under `token` when one
/// is given.
async fn insert_claimed_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    token: Option<Uuid>,
) -> TestResult {
    insert_sandbox(pool, sandbox_id, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - INTERVAL '15 minutes',
                restore_lease_token = $2,
                restore_lease_expires_at = CASE WHEN $2::uuid IS NOT NULL
                                                THEN NOW() + INTERVAL '5 minutes' END
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(token)
    .execute(pool)
    .await?;
    Ok(())
}

/// The revert's lifecycle, whether it kept a placement clock, and the capture
/// the row's next restore loads.
async fn reverted_restore(
    fixture: &PostgresStore,
    sandbox_id: SandboxId,
) -> TestResult<(String, bool, Option<String>)> {
    let (lifecycle, has_placement_clock): (String, bool) = sqlx::query_as(sql!(
        "SELECT lifecycle, placement_changed_at IS NOT NULL FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.pool)
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
        .await?
        .expect("sandbox row exists");
    let selected = fixture
        .store
        .selected_suspend_restore_storage_uri(&sandbox, RestoreSnapshotChoice::SourceNodeOrFresh)
        .await?;
    Ok((lifecycle, has_placement_clock, selected))
}

/// A failed restore's rollback keeps the row `restoring`, and the placement
/// clock that bounds its source window, only when a `suspended` row would
/// refuse the restore the retry makes. A capture without a generation that
/// registered before the last placement, after execution 5 started, leaves
/// the history unordered for a `suspended` row, while the retry's window
/// excludes it. Without that capture the rollback returns the row to
/// `suspended` and clears the clock. Either way the next restore loads the
/// capture the failed one started from.
///
/// A named snapshot from before the claim, or a failed suspend backup whose
/// retry lands after it, holds no window: the row returns to `suspended`,
/// where the backup that landed after the claim restores.
pub(super) async fn a_failed_restore_revert_keeps_a_window_only_the_retry_can_order(
    fixture: &PostgresStore,
) -> TestResult {
    const SOURCE_URI: &str = "s3://bucket/source?region=us-east-1";
    const POST_CLAIM_URI: &str = "s3://bucket/post-claim-suspend?region=us-east-1";
    let pool = &fixture.pool;
    let enabled = crate::controllers::substrate::AbandonedRestoreRetirement::Enabled;

    for with_unranked in [true, false] {
        let case = format!("unranked capture {with_unranked}");
        let sandbox_id = SandboxId::new();
        let token = Uuid::now_v7();
        let attempt = SandboxActivationAttemptId::new();
        insert_claimed_sandbox(pool, sandbox_id, Some(token)).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET placement_changed_at = NOW() - INTERVAL '30 minutes'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        // Execution 5 took the source capture; execution 6 is the claim's.
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                 (sandbox_id, generation, pod_name, pod_namespace, state, active, created_at,
                  activation_attempt_id, activation_attempt_expires_at,
                  activation_attempt_target_state)
             VALUES ($1, 5, $2, 'space', 'suspended', FALSE, NOW() - INTERVAL '60 minutes',
                     NULL, NULL, NULL),
                    ($1, 6, $3, 'space', 'created', TRUE, NOW() - INTERVAL '10 minutes',
                     $4, NOW() + INTERVAL '5 minutes', 'started')"
        ))
        .bind(sandbox_id)
        .bind(format!("space-sbx-{}-g5", sandbox_id.as_uuid().simple()))
        .bind(format!("space-sbx-{}-g6", sandbox_id.as_uuid().simple()))
        .bind(attempt)
        .execute(pool)
        .await?;
        if with_unranked {
            insert_backup(
                pool,
                sandbox_id,
                &Backup::suspend(None, 40, "s3://bucket/unranked?region=us-east-1"),
            )
            .await?;
        }
        insert_backup(pool, sandbox_id, &Backup::suspend(Some(5), 20, SOURCE_URI)).await?;

        let outcome = fixture
            .store
            .revert_restore_claim_to_suspended(sandbox_id, Some(token), Some((attempt, 6)), enabled)
            .await?;

        assert_eq!(
            outcome,
            RestoreClaimRevert::ExecutionRetired {
                pod_name: format!("space-sbx-{}-g6", sandbox_id.as_uuid().simple()),
            },
            "{case}"
        );
        let kept_lifecycle = if with_unranked {
            "restoring"
        } else {
            "suspended"
        };
        assert_eq!(
            reverted_restore(fixture, sandbox_id).await?,
            (
                kept_lifecycle.to_owned(),
                with_unranked,
                Some(SOURCE_URI.to_owned())
            ),
            "{case}"
        );
    }

    for case in ["named_pre_claim", "retried_pre_claim_suspend"] {
        let sandbox_id = SandboxId::new();
        insert_claimed_sandbox(pool, sandbox_id, None).await?;
        let artifact = LocalArtifactId::new();
        let pre_claim = match case {
            "named_pre_claim" => Backup {
                origin: "snapshot",
                ..Backup::suspend(None, 25, "s3://bucket/named?region=us-east-1")
            },
            _ => Backup {
                artifact,
                state: "failed",
                ..Backup::suspend(Some(5), 25, "s3://bucket/failed-attempt?region=us-east-1")
            },
        };
        insert_backup(pool, sandbox_id, &pre_claim).await?;
        insert_backup(
            pool,
            sandbox_id,
            &Backup {
                artifact,
                ..Backup::suspend(Some(5), 5, POST_CLAIM_URI)
            },
        )
        .await?;

        let outcome = fixture
            .store
            .revert_restore_claim_to_suspended(sandbox_id, None, None, enabled)
            .await?;

        assert_eq!(outcome, RestoreClaimRevert::LifecycleOnly, "{case}");
        assert_eq!(
            reverted_restore(fixture, sandbox_id).await?,
            (
                "suspended".to_owned(),
                false,
                Some(POST_CLAIM_URI.to_owned())
            ),
            "{case}"
        );
    }
    Ok(())
}
