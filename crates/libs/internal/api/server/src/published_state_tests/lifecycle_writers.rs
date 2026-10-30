//! Characterization of node publication's writes to Postgres
//! `sandboxes.lifecycle`, through [`publish_sandbox_states`] against the real
//! Postgres fixture: which stored lifecycles each projection moves, what it
//! moves them to, and the columns and rows that move with them (placement,
//! `active_node_id`/`target_node_id`, the `sandbox_k8s_executions` row, the
//! Postgres quarantine tables and open port exposures).
//!
//! Each projection gets one sweep over every stored Postgres
//! `sandboxes.lifecycle` value ([`StoredSandboxLifecycle::all`]: every
//! [`SandboxLifecycle`] variant plus the stored tombstones). The `failed`,
//! `quarantined`, activation-completion, activation-rollback,
//! `suspended`-report and ready-backup projections also get a report from a
//! node the row is not active on; the pause-failed
//! recovery's node refusal is pinned in `tests/published_state.rs`
//! (`node_inventory_only_recovers_sbx_manager_pause_failed_quarantines`). The
//! `started` and `stopped` convergence of `publish_sandbox_states` is swept
//! in `node_publication_converges_only_from_its_source_lifecycles`.
//! Publication runs in one transaction, as `publish_node_state` runs it, and
//! rows are read after that transaction commits.
//!
//! The node-local sbx-mgr SQLite store is not involved: these tests stand in
//! for the node by building the `PublishedSandboxState` it would send.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxQuarantineReasonCode, SnapshotId};
use pplx_space_db_test_fixtures::{
    insert_ready_worker_node, pg_test_db, pg_test_db_with_max_connections,
};
use pplx_space_placement::StoredSandboxLifecycle;
use pplx_space_placement::test_support::stored_lifecycle_matrix;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{
    insert_active_sandbox_with_managed_by, insert_k8s_execution, insert_ready_suspend_snapshot,
    published_sandbox_state,
};
use crate::proto::PublishedSandboxState;
use crate::publish_sandbox_states;

/// The Postgres rows a publication projection moves for one sandbox. The
/// execution columns read the sandbox's only `sandbox_k8s_executions` row.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    execution_active: Option<bool>,
    execution_state: Option<String>,
    activation_attempt_id: Option<Uuid>,
    quarantined_from: Option<String>,
    quarantine_history: Vec<String>,
}

async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, s.target_node_id,
                e.active AS execution_active, e.state AS execution_state,
                e.activation_attempt_id,
                q.entered_from_lifecycle AS quarantined_from,
                ARRAY(SELECT h.action FROM sandbox_quarantine_history h
                       WHERE h.sandbox_id = s.id ORDER BY h.occurred_at, h.action)
                    AS quarantine_history
           FROM sandboxes s
           LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
           LEFT JOIN sandbox_quarantine_state q ON q.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Publish `state` from `node_id` in one transaction and commit it.
async fn publish(pool: &PgPool, node_id: NodeId, state: PublishedSandboxState) -> TestResult {
    let mut tx = pool.begin().await?;
    publish_sandbox_states(&mut tx, node_id, &[state]).await?;
    tx.commit().await?;
    Ok(())
}

fn with_receipt(
    mut state: PublishedSandboxState,
    kind: &str,
    target_state: &str,
    execution_generation: i64,
    activation_attempt_id: Option<Uuid>,
    reason_code: &str,
) -> PublishedSandboxState {
    state.transition_receipt = Some(pplx_space_sbx_mgr_client::proto::TransitionReceipt {
        id: Uuid::now_v7().to_string(),
        kind: kind.to_owned(),
        target_state: target_state.to_owned(),
        execution_generation,
        reason_code: reason_code.to_owned(),
        activation_attempt_id: activation_attempt_id.map(|id| id.to_string()),
    });
    state
}

fn quarantined_report(sandbox_id: SandboxId) -> PublishedSandboxState {
    let mut report = published_sandbox_state(sandbox_id, "quarantined");
    report.failure_reason = Some("synthetic conflicting local artifacts".to_owned());
    report.quarantine_reason_code =
        Some(SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed.to_string());
    report
}

async fn set_activation_attempt(
    pool: &PgPool,
    sandbox_id: SandboxId,
    attempt: Uuid,
    target_state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = $2,
                route_activation_id = $2,
                activation_attempt_expires_at = NOW() + INTERVAL '5 minutes',
                activation_attempt_target_state = $3
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(attempt)
    .bind(target_state)
    .execute(pool)
    .await?;
    Ok(())
}

async fn clear_target_node(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn ready_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    Ok(node_id)
}

/// A `k8s` row placed on `node_id`, with one running execution.
async fn insert_running(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
) -> TestResult<(SandboxId, Row)> {
    let sandbox_id = SandboxId::new();
    insert_active_sandbox_with_managed_by(pool, sandbox_id, node_id, lifecycle, "k8s").await?;
    insert_k8s_execution(pool, sandbox_id, 7, "running", Some("100.64.0.7")).await?;
    let before = row(pool, sandbox_id).await?;
    Ok((sandbox_id, before))
}

/// Publish `report` for a row active on `node_id` from another ready node,
/// and assert the row is unchanged.
async fn assert_other_node_report_is_inert(
    pool: &PgPool,
    sandbox_id: SandboxId,
    report: PublishedSandboxState,
) -> TestResult {
    let before = row(pool, sandbox_id).await?;
    let other_node_id = ready_node(pool).await?;
    publish(pool, other_node_id, report).await?;
    assert_eq!(row(pool, sandbox_id).await?, before);
    Ok(())
}

// ---------------------------------------------------------------------------
// `failed` and `quarantined` projections.
// ---------------------------------------------------------------------------

/// A `failed` report moves only `k8s` rows on the reporting node to
/// `failed`, out of every lifecycle but `failed`, `quarantined`, `deleting`
/// and the stored non-variants. The guard compares the lifecycle trimmed and
/// lower-cased, so a padded `quarantined` is refused like the canonical one
/// and an unknown value is failed. Placement, both node columns and the
/// execution are left as they were.
#[tokio::test(flavor = "current_thread")]
async fn failed_report_moves_every_live_k8s_lifecycle_to_failed() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for managed_by in ["k8s", "space"] {
        for stored in StoredSandboxLifecycle::all_as_str().chain([" Quarantined ", "bogus"]) {
            let sandbox_id = SandboxId::new();
            insert_active_sandbox_with_managed_by(pool, sandbox_id, node_id, stored, managed_by)
                .await?;
            insert_k8s_execution(pool, sandbox_id, 7, "running", Some("100.64.0.7")).await?;
            let before = row(pool, sandbox_id).await?;

            let mut report = published_sandbox_state(sandbox_id, "failed");
            report.failure_reason = Some("synthetic guest failure".to_owned());
            publish(pool, node_id, report).await?;

            let fails = managed_by == "k8s"
                && !matches!(
                    stored,
                    "failed"
                        | "quarantined"
                        | "deleting"
                        | "deleted"
                        | "cleanup_required"
                        | " Quarantined "
                );
            let mut expected = before.clone();
            if fails {
                expected.lifecycle = "failed".to_owned();
            }
            let actual = row(pool, sandbox_id).await?;
            if actual != expected {
                mismatches.push(format!("{managed_by} {stored}: {actual:?}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    let (sandbox_id, _) = insert_running(pool, node_id, "started").await?;
    assert_other_node_report_is_inert(
        pool,
        sandbox_id,
        published_sandbox_state(sandbox_id, "failed"),
    )
    .await
}

/// A `quarantined` report moves a row on the reporting node to
/// `quarantined` from every lifecycle but `quarantined` and `deleted`, with
/// no substrate scope, and records a Postgres quarantine (a state row naming
/// the lifecycle it entered from, plus an `entered_quarantine` history row).
/// That includes `deleting`: a quarantine is a stronger hold than delete
/// intent, which a later delete re-claims. The claim compares the lifecycle
/// trimmed and lower-cased, so a padded `quarantined` is left as the
/// canonical one is, and an unknown value is quarantined and recorded as
/// stored. Placement, both node columns and the execution are left as they
/// were.
///
/// `cleanup_required` is not swept: no Postgres writer stores it.
#[tokio::test(flavor = "current_thread")]
async fn quarantined_report_moves_every_unquarantined_lifecycle_and_records_it() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for managed_by in ["k8s", "space"] {
        for stored in StoredSandboxLifecycle::all_as_str().chain([" Quarantined ", "bogus"]) {
            if stored == "cleanup_required" {
                continue;
            }
            let sandbox_id = SandboxId::new();
            insert_active_sandbox_with_managed_by(pool, sandbox_id, node_id, stored, managed_by)
                .await?;
            insert_k8s_execution(pool, sandbox_id, 7, "running", Some("100.64.0.7")).await?;
            let before = row(pool, sandbox_id).await?;

            publish(pool, node_id, quarantined_report(sandbox_id)).await?;

            let quarantines = !matches!(stored, "quarantined" | "deleted" | " Quarantined ");
            let mut expected = before.clone();
            if quarantines {
                expected.lifecycle = "quarantined".to_owned();
                expected.quarantined_from = Some(stored.to_owned());
                expected.quarantine_history = vec!["entered_quarantine".to_owned()];
            }
            let actual = row(pool, sandbox_id).await?;
            if actual != expected {
                mismatches.push(format!("{managed_by} {stored}: {actual:?}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    let (sandbox_id, _) = insert_running(pool, node_id, "started").await?;
    assert_other_node_report_is_inert(pool, sandbox_id, quarantined_report(sandbox_id)).await
}

// ---------------------------------------------------------------------------
// Transition receipts: activation completion and rollback.
// ---------------------------------------------------------------------------

/// An activation-completion receipt that matches the active execution's
/// generation and attempt releases that attempt whatever the row's
/// lifecycle, and moves the row to the receipt's `started` or `paused`
/// target, placed `on_node` (the fixture starts it unplaced), out of every
/// lifecycle but the suspend window (`suspending`, `suspended`),
/// `quarantined`, `deleting` and the stored non-variants. A `created` target
/// releases the attempt and keeps the row only when it is still `created`.
/// Both guards compare the lifecycle trimmed and lower-cased: a padded
/// `created` counts as `created` and a padded `suspended` as the suspend
/// window, while an unknown value is projected like any other lifecycle.
#[tokio::test(flavor = "current_thread")]
async fn activation_completion_receipt_projects_its_target_outside_the_suspend_window() -> TestResult
{
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for target in ["started", "paused", "created"] {
        let rows =
            StoredSandboxLifecycle::all_as_str().chain([" Created ", " Suspended ", "bogus"]);
        for stored in rows {
            let (sandbox_id, _) = insert_running(pool, node_id, stored).await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET placement = 'unplaced' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(pool)
            .await?;
            let attempt = Uuid::now_v7();
            set_activation_attempt(pool, sandbox_id, attempt, target).await?;
            let before = row(pool, sandbox_id).await?;

            let report = with_receipt(
                published_sandbox_state(sandbox_id, target),
                "activation_completed",
                target,
                7,
                Some(attempt),
                "",
            );
            publish(pool, node_id, report).await?;

            let mut expected = before.clone();
            if target == "created" {
                if matches!(stored, "created" | " Created ") {
                    // The release writes the receipt's target, so a padded
                    // `created` is stored canonically.
                    expected.activation_attempt_id = None;
                    expected.lifecycle = target.to_owned();
                }
            } else {
                expected.activation_attempt_id = None;
                let projects = !matches!(
                    stored,
                    "suspending"
                        | "suspended"
                        | "quarantined"
                        | "deleting"
                        | "deleted"
                        | "cleanup_required"
                        | " Suspended "
                );
                if projects {
                    expected.lifecycle = target.to_owned();
                    expected.placement = "on_node".to_owned();
                }
            }
            let actual = row(pool, sandbox_id).await?;
            if actual != expected {
                mismatches.push(format!("target {target}, stored {stored}: {actual:?}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // A receipt for another attempt or generation, or from another node,
    // neither releases the attempt nor moves the row.
    // (receipt generation, receipt names the execution's current attempt)
    for (generation, current_attempt) in [(7, false), (8, true)] {
        let (sandbox_id, _) = insert_running(pool, node_id, "stopped").await?;
        let current = Uuid::now_v7();
        set_activation_attempt(pool, sandbox_id, current, "started").await?;
        let attempt = if current_attempt {
            current
        } else {
            Uuid::now_v7()
        };
        let before = row(pool, sandbox_id).await?;
        let report = with_receipt(
            published_sandbox_state(sandbox_id, "started"),
            "activation_completed",
            "started",
            generation,
            Some(attempt),
            "",
        );
        publish(pool, node_id, report).await?;
        assert_eq!(
            row(pool, sandbox_id).await?,
            before,
            "generation {generation}"
        );
    }
    let (sandbox_id, _) = insert_running(pool, node_id, "stopped").await?;
    let attempt = Uuid::now_v7();
    set_activation_attempt(pool, sandbox_id, attempt, "started").await?;
    let report = with_receipt(
        published_sandbox_state(sandbox_id, "started"),
        "activation_completed",
        "started",
        7,
        Some(attempt),
        "",
    );
    assert_other_node_report_is_inert(pool, sandbox_id, report).await
}

/// One open and one closed port exposure on `sandbox_id`, both observed as
/// applied on `node_id`.
async fn insert_port_exposures(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult {
    for (guest_port, desired_state) in [(8080, "open"), (8081, "closed")] {
        sqlx::query(sql!(
            "INSERT INTO sandbox_port_exposures (
                 sandbox_id, protocol, guest_port, visibility, source,
                 desired_state, apply_state, last_applied_node_id, last_error
             )
             VALUES ($1, 'tcp', $2, 'private', 'api', $3, 'applied', $4, 'synthetic')"
        ))
        .bind(sandbox_id)
        .bind(guest_port)
        .bind(desired_state)
        .bind(node_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// `(desired_state, apply_state, last_applied_node_id, last_error)` per port,
/// in port order.
type PortExposure = (String, String, Option<NodeId>, Option<String>);

async fn port_exposures(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Vec<PortExposure>> {
    Ok(sqlx::query_as(sql!(
        "SELECT desired_state, apply_state, last_applied_node_id, last_error
           FROM sandbox_port_exposures
          WHERE sandbox_id = $1
          ORDER BY guest_port"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?)
}

/// An activation-rollback receipt that matches the active execution's
/// generation and attempt moves the row to the receipt's `created`,
/// `stopped` or `suspended` target out of every lifecycle but `quarantined`,
/// `deleting` and the stored non-variants. The move unplaces the row, clears
/// `active_node_id`, backfills an empty `target_node_id` from it, retires the
/// execution (`replaced`, or `suspended` for a suspended target, since this
/// publishes with abandoned-restore retirement disabled), and
/// resets the observation of open port exposures only. The candidate query
/// compares the lifecycle trimmed and lower-cased, so a padded `quarantined`
/// is refused like the canonical one. It also reads the normalized lifecycle
/// back as a `SandboxLifecycle`, so an unknown value fails the whole
/// publication, which rolls back and leaves the row as it was.
#[tokio::test(flavor = "current_thread")]
async fn activation_rollback_receipt_converges_its_target_out_of_every_live_lifecycle() -> TestResult
{
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (target, reason_code) in [
        ("created", SandboxQuarantineReasonCode::CreateInterrupted),
        (
            "stopped",
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
        ),
        (
            "suspended",
            SandboxQuarantineReasonCode::RestoreActivationInterrupted,
        ),
    ] {
        for stored in StoredSandboxLifecycle::all_as_str().chain([" Quarantined ", "bogus"]) {
            let (sandbox_id, _) = insert_running(pool, node_id, stored).await?;
            clear_target_node(pool, sandbox_id).await?;
            insert_port_exposures(pool, sandbox_id, node_id).await?;
            let attempt = Uuid::now_v7();
            set_activation_attempt(pool, sandbox_id, attempt, target).await?;
            let before = row(pool, sandbox_id).await?;
            let exposures_before = port_exposures(pool, sandbox_id).await?;

            let report = with_receipt(
                published_sandbox_state(sandbox_id, target),
                "activation_rollback",
                target,
                7,
                Some(attempt),
                reason_code.as_str(),
            );
            let published = publish(pool, node_id, report).await;

            let fails_publication = stored == "bogus";
            let converges = !fails_publication
                && !matches!(
                    stored,
                    "quarantined" | "deleting" | "deleted" | "cleanup_required" | " Quarantined "
                );
            let mut expected = before.clone();
            let mut expected_exposures = exposures_before.clone();
            if converges {
                expected.lifecycle = target.to_owned();
                expected.placement = "unplaced".to_owned();
                expected.active_node_id = None;
                expected.target_node_id = Some(node_id);
                expected.execution_active = Some(false);
                expected.execution_state = Some(
                    if target == "suspended" {
                        "suspended"
                    } else {
                        "replaced"
                    }
                    .to_owned(),
                );
                expected_exposures[0] = ("open".to_owned(), "pending".to_owned(), None, None);
            }
            let actual = row(pool, sandbox_id).await?;
            let actual_exposures = port_exposures(pool, sandbox_id).await?;
            if published.is_err() != fails_publication
                || actual != expected
                || actual_exposures != expected_exposures
            {
                mismatches.push(format!(
                    "target {target}, stored {stored:?}: published {:?}, {actual:?}, \
                     {actual_exposures:?}",
                    published.as_ref().map(|_| ())
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // A receipt for another execution generation, or from another node,
    // moves nothing.
    let rollback = |sandbox_id: SandboxId, generation: i64, attempt: Uuid| {
        with_receipt(
            published_sandbox_state(sandbox_id, "stopped"),
            "activation_rollback",
            "stopped",
            generation,
            Some(attempt),
            SandboxQuarantineReasonCode::ColdActivationInterrupted.as_str(),
        )
    };
    let (sandbox_id, before) = insert_running(pool, node_id, "starting").await?;
    let attempt = Uuid::now_v7();
    set_activation_attempt(pool, sandbox_id, attempt, "stopped").await?;
    let before = Row {
        activation_attempt_id: Some(attempt),
        ..before
    };
    publish(pool, node_id, rollback(sandbox_id, 8, attempt)).await?;
    assert_eq!(row(pool, sandbox_id).await?, before, "generation 8");
    assert_other_node_report_is_inert(pool, sandbox_id, rollback(sandbox_id, 7, attempt)).await
}

// ---------------------------------------------------------------------------
// `suspended` projections and quarantine recovery.
// ---------------------------------------------------------------------------

/// A `suspended` report with a durable snapshot of the current cycle moves
/// a row to `suspended` out of `started`, `paused` and `restoring` only, for
/// either substrate: it unplaces the row, clears `active_node_id`, backfills
/// an empty `target_node_id` from it, and retires a `k8s` row's execution as
/// `suspended`. A `suspending` row needs the node's commit evidence, which a
/// bare report does not carry. The guard compares the lifecycle trimmed and
/// lower-cased, so a padded `restoring` is projected like the canonical one;
/// an unknown value is not.
#[tokio::test(flavor = "current_thread")]
async fn suspended_report_projects_only_out_of_running_and_restoring_lifecycles() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for managed_by in ["k8s", "space"] {
        for stored in StoredSandboxLifecycle::all_as_str().chain([" Restoring ", "bogus"]) {
            let sandbox_id = SandboxId::new();
            insert_active_sandbox_with_managed_by(pool, sandbox_id, node_id, stored, managed_by)
                .await?;
            clear_target_node(pool, sandbox_id).await?;
            if managed_by == "k8s" {
                insert_k8s_execution(pool, sandbox_id, 7, "running", Some("100.64.0.7")).await?;
            }
            insert_ready_suspend_snapshot(pool, sandbox_id, node_id).await?;
            let before = row(pool, sandbox_id).await?;

            publish(
                pool,
                node_id,
                published_sandbox_state(sandbox_id, "suspended"),
            )
            .await?;

            let mut expected = before.clone();
            if matches!(stored, "started" | "paused" | "restoring" | " Restoring ") {
                expected.lifecycle = "suspended".to_owned();
                expected.placement = "unplaced".to_owned();
                expected.active_node_id = None;
                expected.target_node_id = Some(node_id);
                if managed_by == "k8s" {
                    expected.execution_active = Some(false);
                    expected.execution_state = Some("suspended".to_owned());
                }
            }
            let actual = row(pool, sandbox_id).await?;
            if actual != expected {
                mismatches.push(format!("{managed_by} {stored}: {actual:?}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // Without a snapshot of the current cycle the report moves nothing, and
    // neither does a report from another node.
    let (sandbox_id, before) = insert_running(pool, node_id, "started").await?;
    publish(
        pool,
        node_id,
        published_sandbox_state(sandbox_id, "suspended"),
    )
    .await?;
    assert_eq!(row(pool, sandbox_id).await?, before);
    insert_ready_suspend_snapshot(pool, sandbox_id, node_id).await?;
    assert_other_node_report_is_inert(
        pool,
        sandbox_id,
        published_sandbox_state(sandbox_id, "suspended"),
    )
    .await
}

async fn pin_snapshot_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "UPDATE sandboxes s
            SET current_local_artifact_id = sn.local_artifact_id,
                current_local_artifact_kind = sn.artifact_kind,
                current_local_artifact_origin = sn.artifact_origin
           FROM snapshots sn
          WHERE s.id = $1 AND sn.id = $2
      RETURNING sn.local_artifact_id"
    ))
    .bind(sandbox_id)
    .bind(snapshot_id)
    .fetch_one(pool)
    .await?)
}

/// Run the ready-backup projection for `snapshot_id` in one transaction, as
/// `finalize_artifact_backup` runs it, and commit.
async fn project_ready_backup(pool: &PgPool, snapshot_id: SnapshotId) -> TestResult<bool> {
    let mut tx = pool.begin().await?;
    let projected = super::super::project_ready_suspend_snapshot(&mut tx, snapshot_id)
        .await?
        .is_some();
    tx.commit().await?;
    Ok(projected)
}

/// A finalized ready backup projects `suspended` out of `started`, `paused`
/// and `restoring` only: it unplaces the row, clears `active_node_id`,
/// backfills an empty `target_node_id` from it, and retires the execution as
/// `suspended`. A `suspending` row is projected only once its own suspend
/// has committed: the execution is `suspended` and the row names the
/// backed-up artifact; either alone is refused. A backup taken on a node the
/// row is not active on projects nothing. The lifecycle gate is the same
/// trimmed and lower-cased one as the `suspended` report's, so a padded
/// `restoring` is projected and an unknown value is not.
#[tokio::test(flavor = "current_thread")]
async fn ready_backup_projects_suspended_only_out_of_running_and_restoring_lifecycles() -> TestResult
{
    #[derive(Debug, Clone, Copy)]
    enum Suspend {
        NotCommitted,
        Committed,
        ExecutionOnly,
        ArtifactOnly,
    }
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let sweep = StoredSandboxLifecycle::all_as_str().map(|stored| (stored, Suspend::NotCommitted));
    let commits = [
        ("suspending", Suspend::Committed),
        ("suspending", Suspend::ExecutionOnly),
        ("suspending", Suspend::ArtifactOnly),
        (" Restoring ", Suspend::NotCommitted),
        ("bogus", Suspend::NotCommitted),
    ];
    let mut mismatches = Vec::new();
    for (stored, suspend) in sweep.chain(commits) {
        let (sandbox_id, _) = insert_running(pool, node_id, stored).await?;
        clear_target_node(pool, sandbox_id).await?;
        let snapshot_id = insert_ready_suspend_snapshot(pool, sandbox_id, node_id).await?;
        if matches!(suspend, Suspend::Committed | Suspend::ArtifactOnly) {
            pin_snapshot_artifact(pool, sandbox_id, snapshot_id).await?;
        }
        if matches!(suspend, Suspend::Committed | Suspend::ExecutionOnly) {
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions SET state = 'suspended' WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .execute(pool)
            .await?;
        }
        let before = row(pool, sandbox_id).await?;

        let projected = project_ready_backup(pool, snapshot_id).await?;

        let projects = matches!(stored, "started" | "paused" | "restoring" | " Restoring ")
            || matches!(suspend, Suspend::Committed);
        let mut expected = before.clone();
        if projects {
            expected.lifecycle = "suspended".to_owned();
            expected.placement = "unplaced".to_owned();
            expected.active_node_id = None;
            expected.target_node_id = Some(node_id);
            expected.execution_active = Some(false);
            expected.execution_state = Some("suspended".to_owned());
        }
        let actual = row(pool, sandbox_id).await?;
        if projected != projects || actual != expected {
            mismatches.push(format!(
                "{stored}, {suspend:?}: projected {projected}, {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    let other_node_id = ready_node(pool).await?;
    let (sandbox_id, before) = insert_running(pool, node_id, "started").await?;
    let snapshot_id = insert_ready_suspend_snapshot(pool, sandbox_id, other_node_id).await?;
    assert!(!project_ready_backup(pool, snapshot_id).await?);
    assert_eq!(row(pool, sandbox_id).await?, before);
    Ok(())
}

/// A quarantine-recovery receipt for the node's own restore quarantine moves
/// the row to `suspended` only while it is still `quarantined`: the move
/// unplaces it, clears `active_node_id`, retires the execution as
/// `suspended`, deletes the Postgres quarantine state and records
/// `recovered_to_suspended`. Once another writer has moved the row out of
/// `quarantined`, the receipt changes nothing and the quarantine state
/// stays. The guard compares the lifecycle trimmed and lower-cased, so a
/// padded `quarantined` is recovered like the canonical one; an unknown value
/// is not.
#[tokio::test(flavor = "current_thread")]
async fn recovery_receipt_suspends_only_a_still_quarantined_row() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;
    let reason_code = SandboxQuarantineReasonCode::RestorePrepareFailed;

    let mut mismatches = Vec::new();
    for stored in StoredSandboxLifecycle::all_as_str().chain([" Quarantined ", "bogus"]) {
        let (sandbox_id, _) = insert_running(pool, node_id, "restoring").await?;
        let snapshot_id = insert_ready_suspend_snapshot(pool, sandbox_id, node_id).await?;
        let local_artifact_id = pin_snapshot_artifact(pool, sandbox_id, snapshot_id).await?;
        // The node quarantines the failed restore, then another writer may
        // move the row before the node's recovery receipt arrives.
        let mut quarantined = published_sandbox_state(sandbox_id, "quarantined");
        quarantined.quarantine_reason_code = Some(reason_code.to_string());
        publish(pool, node_id, quarantined).await?;
        let quarantine_id: Uuid = sqlx::query_scalar(sql!(
            "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        if stored != "quarantined" {
            sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                .bind(sandbox_id)
                .bind(stored)
                .execute(pool)
                .await?;
        }
        let before = row(pool, sandbox_id).await?;

        let mut receipt = with_receipt(
            published_sandbox_state(sandbox_id, "suspended"),
            "quarantine_recovery",
            "suspended",
            7,
            None,
            reason_code.as_str(),
        );
        if let Some(transition_receipt) = receipt.transition_receipt.as_mut() {
            transition_receipt.id = quarantine_id.to_string();
        }
        receipt.current_local_artifact_id = Some(local_artifact_id.to_string());
        receipt.current_local_artifact_kind =
            Some(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32);
        receipt.current_local_artifact_origin =
            Some(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32);
        publish(pool, node_id, receipt).await?;

        let mut expected = before.clone();
        if matches!(stored, "quarantined" | " Quarantined ") {
            expected.lifecycle = "suspended".to_owned();
            expected.placement = "unplaced".to_owned();
            expected.active_node_id = None;
            expected.execution_active = Some(false);
            expected.execution_state = Some("suspended".to_owned());
            expected.quarantined_from = None;
            expected
                .quarantine_history
                .push("recovered_to_suspended".to_owned());
        }
        let actual = row(pool, sandbox_id).await?;
        if actual != expected {
            mismatches.push(format!("{stored}: {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// A healthy `started` or `paused` report recovers only a `quarantined` row
/// whose Postgres quarantine is sbx-mgr's `pause_failed`, to the reported
/// lifecycle, deleting the quarantine state and recording the recovery in
/// the history. A row in any other lifecycle keeps its quarantine state. An
/// unknown value is not a candidate. A padded `quarantined` is: the candidate
/// query compares the lifecycle trimmed and lower-cased, but the recovery's
/// CAS compares it exactly, misses, and fails the whole publication, which
/// rolls back and leaves the row as it was.
#[tokio::test(flavor = "current_thread")]
async fn healthy_report_recovers_only_a_pause_failed_quarantine() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for reported in ["started", "paused"] {
        for stored in StoredSandboxLifecycle::all_as_str().chain([" Quarantined ", "bogus"]) {
            let (sandbox_id, _) = insert_running(pool, node_id, stored).await?;
            sqlx::query(sql!(
                "INSERT INTO sandbox_quarantine_state (
                     sandbox_id, quarantine_id, entered_from_lifecycle, source,
                     reason_code, next_action, last_observed_node_id
                 )
                 VALUES ($1, $2, 'started', 'sbx_manager', 'pause_failed',
                         'requires_operator_attention', $3)"
            ))
            .bind(sandbox_id)
            .bind(Uuid::now_v7())
            .bind(node_id)
            .execute(pool)
            .await?;
            let before = row(pool, sandbox_id).await?;

            let published =
                publish(pool, node_id, published_sandbox_state(sandbox_id, reported)).await;

            let fails_publication = stored == " Quarantined ";
            let mut expected = before.clone();
            if stored == "quarantined" {
                expected.lifecycle = reported.to_owned();
                expected.quarantined_from = None;
                expected.quarantine_history = vec![format!("recovered_to_{reported}")];
            } else if stored == "restoring" && reported == "started" {
                // The restore convergence, swept with the `started` report.
                expected.lifecycle = "started".to_owned();
            }
            let actual = row(pool, sandbox_id).await?;
            if published.is_err() != fails_publication || actual != expected {
                mismatches.push(format!(
                    "reported {reported}, stored {stored:?}: published {:?}, {actual:?}",
                    published.as_ref().map(|_| ())
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Absent reports.
// ---------------------------------------------------------------------------

/// A report that its node no longer holds a sandbox deletes the central row
/// only out of `stopped`, and only for a Direct (`space`) row; every other
/// stored lifecycle, and every `k8s` row, keeps its row and lifecycle. Each row
/// is active on the reporting node with no durable artifact. Columns are
/// `managed_by`; `A`: the row is deleted; `.`: it is kept unchanged.
#[tokio::test(flavor = "current_thread")]
async fn absent_report_deletes_only_a_stopped_direct_row() -> TestResult {
    const EXPECTED: &[&str] = &[
        //              space  k8s
        "created          .     .",
        "creating         .     .",
        "starting         .     .",
        "started          .     .",
        "restoring        .     .",
        "pausing          .     .",
        "paused           .     .",
        "resuming         .     .",
        "suspending       .     .",
        "suspended        .     .",
        "stopping         .     .",
        "stopped          A     .",
        "deleting         .     .",
        "checkpointing    .     .",
        "quarantined      .     .",
        "failed           .     .",
        "deleted          .     .",
        "cleanup_required .     .",
        "bogus            .     .",
        "_Suspended_      .     .",
    ];
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;

    let mut mismatches = Vec::new();
    for (stored, cells) in stored_lifecycle_matrix(["space", "k8s"], EXPECTED) {
        for (managed_by, deletes) in cells {
            let deletes = deletes.applies();
            let sandbox_id = SandboxId::new();
            insert_active_sandbox_with_managed_by(pool, sandbox_id, node_id, stored, managed_by)
                .await?;
            let before = row(pool, sandbox_id).await?;

            let absent = PublishedSandboxState {
                sandbox_id: sandbox_id.to_string(),
                present: false,
                ..PublishedSandboxState::default()
            };
            publish(pool, node_id, absent).await?;

            let after: Option<Row> = if sandbox_exists(pool, sandbox_id).await? {
                Some(row(pool, sandbox_id).await?)
            } else {
                None
            };
            let expected = (!deletes).then_some(before);
            if after != expected {
                mismatches.push(format!("{managed_by} {stored}: {after:?}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

async fn sandbox_exists(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

// ---------------------------------------------------------------------------
// Interleavings with a writer holding the row lock.
// ---------------------------------------------------------------------------

/// Bound for each interleaving step: waiting for the lock, and joining the
/// publication that waited.
const INTERLEAVING_BOUND: Duration = Duration::from_secs(10);

/// Poll `pg_blocking_pids` every 10 ms until backend `publication_pid` is
/// blocked by backend `writer_pid`, within [`INTERLEAVING_BOUND`].
async fn wait_until_blocked(pool: &PgPool, publication_pid: i32, writer_pid: i32) -> TestResult {
    let poll = async {
        loop {
            let blocked: bool = sqlx::query_scalar(sql!("SELECT $2 = ANY(pg_blocking_pids($1))"))
                .bind(publication_pid)
                .bind(writer_pid)
                .fetch_one(pool)
                .await?;
            if blocked {
                return TestResult::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(INTERLEAVING_BOUND, poll)
        .await
        .map_err(|_| format!("publication not blocked within {INTERLEAVING_BOUND:?}"))?
}

/// A publication that reaches a row while another writer holds it decides on
/// the row that writer leaves. Committed, a node-drain evacuation claim's
/// `suspending` (with its marker) stands against a `stopped` report and
/// against the activation-completion receipt of a `paused` row's resume,
/// whose attempt is still released. A `quarantined` report records no
/// quarantine behind a committed `stopped`, nor behind a committed move of
/// `active_node_id` to another node with the lifecycle unchanged, as a pod
/// placement write makes: its CAS is the only node check on the row version
/// it writes. Rolled back, the publication moves the row as it would alone.
#[tokio::test(flavor = "current_thread")]
async fn publication_waiting_on_a_row_lock_decides_on_the_committed_row() -> TestResult {
    #[derive(Clone, Copy, Debug)]
    enum Report {
        Stopped,
        Completion,
        Quarantined,
    }
    #[derive(Clone, Copy, Debug)]
    enum Concurrent {
        EvacuationClaim,
        Stop,
        MoveNode,
    }
    let cases = [
        (Report::Stopped, "started", Concurrent::EvacuationClaim),
        (Report::Completion, "paused", Concurrent::EvacuationClaim),
        (Report::Quarantined, "started", Concurrent::Stop),
        (Report::Quarantined, "started", Concurrent::MoveNode),
    ];

    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = ready_node(pool).await?;
    let other_node_id = ready_node(pool).await?;
    for (report_kind, stored, concurrent) in cases {
        for writer_commits in [true, false] {
            let (sandbox_id, _) = insert_running(pool, node_id, stored).await?;
            let attempt = Uuid::now_v7();
            let report = match report_kind {
                Report::Stopped => published_sandbox_state(sandbox_id, "stopped"),
                Report::Completion => {
                    set_activation_attempt(pool, sandbox_id, attempt, "started").await?;
                    with_receipt(
                        published_sandbox_state(sandbox_id, "started"),
                        "activation_completed",
                        "started",
                        7,
                        Some(attempt),
                        "",
                    )
                }
                Report::Quarantined => quarantined_report(sandbox_id),
            };
            let before = row(pool, sandbox_id).await?;

            let mut writer = pool.begin().await?;
            let writer_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
                .fetch_one(&mut *writer)
                .await?;
            match concurrent {
                // The evacuation claim's write: the listed lifecycle becomes
                // the restore target.
                Concurrent::EvacuationClaim => sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET evacuation_restore_target = lifecycle, lifecycle = 'suspending'
                      WHERE id = $1"
                ))
                .bind(sandbox_id),
                Concurrent::Stop => sqlx::query(sql!(
                    "UPDATE sandboxes SET lifecycle = 'stopped' WHERE id = $1"
                ))
                .bind(sandbox_id),
                Concurrent::MoveNode => sqlx::query(sql!(
                    "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
                ))
                .bind(sandbox_id)
                .bind(other_node_id),
            }
            .execute(&mut *writer)
            .await?;

            // The publication's own connection, so the backend that has to
            // block is known.
            let mut publication_conn = pool.acquire().await?;
            let publication_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
                .fetch_one(&mut *publication_conn)
                .await?;
            let publication = tokio::spawn(async move {
                let mut tx = sqlx::Connection::begin(&mut *publication_conn).await?;
                publish_sandbox_states(&mut tx, node_id, &[report]).await?;
                tx.commit().await?;
                TestResult::Ok(())
            });
            wait_until_blocked(pool, publication_pid, writer_pid).await?;
            if writer_commits {
                writer.commit().await?;
            } else {
                writer.rollback().await?;
            }
            tokio::time::timeout(INTERLEAVING_BOUND, publication)
                .await
                .map_err(|_| {
                    format!("publication did not finish within {INTERLEAVING_BOUND:?}")
                })???;

            let mut expected = before.clone();
            let mut expected_marker = None;
            match (report_kind, writer_commits) {
                (Report::Stopped | Report::Completion, true) => {
                    expected.lifecycle = "suspending".to_owned();
                    expected_marker = Some(stored.to_owned());
                }
                (Report::Stopped, false) => expected.lifecycle = "stopped".to_owned(),
                (Report::Completion, false) => expected.lifecycle = "started".to_owned(),
                (Report::Quarantined, true) => match concurrent {
                    Concurrent::MoveNode => expected.active_node_id = Some(other_node_id),
                    Concurrent::Stop | Concurrent::EvacuationClaim => {
                        expected.lifecycle = "stopped".to_owned();
                    }
                },
                (Report::Quarantined, false) => {
                    expected.lifecycle = "quarantined".to_owned();
                    expected.quarantined_from = Some(stored.to_owned());
                    expected.quarantine_history = vec!["entered_quarantine".to_owned()];
                }
            }
            if matches!(report_kind, Report::Completion) {
                expected.activation_attempt_id = None;
            }
            let case = format!(
                "{report_kind:?} on {stored} behind {concurrent:?}, writer commits {writer_commits}"
            );
            assert_eq!(row(pool, sandbox_id).await?, expected, "{case}");
            let marker: Option<String> = sqlx::query_scalar(sql!(
                "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
            ))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
            assert_eq!(marker, expected_marker, "{case}");
        }
    }
    Ok(())
}
