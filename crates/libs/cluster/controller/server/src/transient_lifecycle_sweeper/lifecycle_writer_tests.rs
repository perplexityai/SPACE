//! Characterization of every write the transient-lifecycle sweeper makes to
//! Postgres `sandboxes.lifecycle`, through the production
//! [`SubstrateSweepSink`] against the real Postgres fixture.
//!
//! Each writer gets one sweep over every stored Postgres `sandboxes.lifecycle`
//! value (all [`SandboxLifecycle`] variants plus the stored non-variants),
//! with the inputs production can supply, pinning its from-set and the
//! columns the `BEFORE UPDATE` triggers move with the lifecycle (the
//! evacuation marker, the restore lease, `preserve_local_artifact_on_restore_failure`
//! and `command_ready_pending`). Refusals already pinned at the query level
//! in `tests.rs` are not repeated here: the fail CAS's age (`r1`) and
//! substrate scope (`r4`), the marked-`restoring` exemption (`r6`), the
//! current-cycle snapshot exemption (`r7`), the restore lease and the
//! converge age (`r10`, `r13`), the revert's snapshot window (`r11`,
//! `r15`), and the `creating` fail CAS's activation-owner check
//! (`tests/crashed_create.rs`).
//!
//! The sink runs over the direct substrate, which labels its rows
//! `managed_by = 'space'` and needs no node or Kubernetes API for these
//! writes. Two guards are therefore out of reach here and stay covered at the
//! query level in `tests.rs`: the converge CAS's routability predicate (a
//! non-Kubernetes row is always routable) and the failed-active repair CAS,
//! which only ever matches `managed_by = 'k8s'` rows.
//!
//! Which writer runs is the reconciler's decision, pinned with a fake sink in
//! `tests.rs`; in particular it never undoes the lossy age-out of a
//! `suspending` or `restoring` row, so those undos are not characterized.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::Substrate;
use pplx_space_api::controllers::substrate::direct::DirectSubstrate;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, PositiveDuration, SandboxId, SandboxLifecycle};
use pplx_space_db_test_fixtures::{pg_test_db, pg_test_db_with_max_connections};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{SWEEPABLE_LIFECYCLES, SubstrateSweepSink, SweepSink};
use crate::lifecycle_writer_test_support::{
    ExecutionSeed, SandboxSeed, bounded, insert_execution, insert_sandbox, insert_snapshot,
    pin_local_artifact, single_backend_pool, stored_lifecycles, wait_until_blocked,
};

pub(super) fn age() -> PositiveDuration {
    PositiveDuration::new(Duration::from_secs(60)).expect("non-zero")
}

// Direct is only a placeholder: it supplies `managed_by`, and only the converge
// CAS and the failed-active repair CAS would decide differently for a
// Kubernetes label (see the module doc).
pub(super) fn direct_sink(pool: &PgPool) -> SubstrateSweepSink {
    SubstrateSweepSink::new(
        pool.clone(),
        Arc::new(Substrate::Direct(Box::new(DirectSubstrate::new(
            pool.clone(),
        )))),
    )
}

/// The Postgres `sandboxes` columns a sweeper write moves.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    lifecycle: String,
    evacuation_restore_target: Option<String>,
    restore_lease: bool,
    preserve_local_artifact: bool,
    command_ready_pending: bool,
}

impl Row {
    fn bare(lifecycle: &str) -> Self {
        Self {
            lifecycle: lifecycle.to_owned(),
            evacuation_restore_target: None,
            restore_lease: false,
            preserve_local_artifact: false,
            command_ready_pending: false,
        }
    }

    fn marked(mut self, target: &str) -> Self {
        self.evacuation_restore_target = Some(target.to_owned());
        self
    }

    fn leased(mut self) -> Self {
        self.restore_lease = true;
        self
    }

    fn preserving(mut self) -> Self {
        self.preserve_local_artifact = true;
        self
    }

    fn pending(mut self) -> Self {
        self.command_ready_pending = true;
        self
    }
}

async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT lifecycle,
                evacuation_restore_target,
                restore_lease_token IS NOT NULL AS restore_lease,
                preserve_local_artifact_on_restore_failure AS preserve_local_artifact,
                command_ready_pending
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Insert an hour-old `space` row carrying `row`'s columns. A lease is
/// written already expired an hour ago, well past every age window.
async fn insert_row(pool: &PgPool, sandbox_id: SandboxId, row: &Row) -> TestResult {
    insert_sandbox(pool, sandbox_id, &SandboxSeed::new(&row.lifecycle, "space")).await?;
    // Same lifecycle, so no lifecycle trigger fires.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = $2,
                restore_lease_expires_at = CASE WHEN $3 THEN NOW() - INTERVAL '1 hour' END,
                restore_lease_token = CASE WHEN $3 THEN space_uuid_v7() END,
                preserve_local_artifact_on_restore_failure = $4,
                command_ready_pending = $5
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(row.evacuation_restore_target.as_deref())
    .bind(row.restore_lease)
    .bind(row.preserve_local_artifact)
    .bind(row.command_ready_pending)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_lifecycle(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(pool)
        .await?;
    Ok(())
}

/// Move a row to `lifecycle` and then put `lifecycle_changed_at` back, so a
/// write token taken before the move still names the row's last write. The
/// undo CASes bind that token; rewinding the clock isolates their lifecycle
/// predicate from their token predicate.
async fn relabel_keeping_write_token(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
) -> TestResult {
    let changed_at: String = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at::text FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    set_lifecycle(pool, sandbox_id, lifecycle).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = $2::timestamptz WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(changed_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Land a later write on the same lifecycle: away and back, so the row's
/// last write is no longer the one a token names.
async fn rewrite_lifecycle(pool: &PgPool, sandbox_id: SandboxId, via: &str) -> TestResult {
    let lifecycle = row(pool, sandbox_id).await?.lifecycle;
    set_lifecycle(pool, sandbox_id, via).await?;
    set_lifecycle(pool, sandbox_id, &lifecycle).await
}

async fn set_managed_by(pool: &PgPool, sandbox_id: SandboxId, managed_by: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET managed_by = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(managed_by)
        .execute(pool)
        .await?;
    Ok(())
}

async fn set_node_columns(
    pool: &PgPool,
    sandbox_id: SandboxId,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2, target_node_id = $3 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(active_node_id)
    .bind(target_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Aged-transient fail CAS and its undo.
// ---------------------------------------------------------------------------

/// The fail CAS moves a row to `failed` only out of the lifecycle the sweep
/// listed it under, for every sweepable lifecycle: an aged row stored in any
/// other value, including the stored non-variants, is left alone.
#[tokio::test(flavor = "current_thread")]
async fn fail_cas_claims_only_the_lifecycle_it_was_listed_under() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        for listed in SWEEPABLE_LIFECYCLES.iter().copied() {
            let sandbox_id = SandboxId::new();
            insert_row(pool, sandbox_id, &Row::bare(stored)).await?;

            let claimed = sink
                .fail_aged_candidate(sandbox_id, listed, age())
                .await?
                .is_some();

            let accepts = stored == listed.as_str();
            let expected = Row::bare(if accepts { "failed" } else { stored });
            let actual = row(pool, sandbox_id).await?;
            if claimed != accepts || actual != expected {
                mismatches.push(format!(
                    "stored {stored}, listed {listed}: claimed {claimed}, row {actual:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// What the fail CAS clears on its way to `failed`, and the published-artifact
/// half of the marked-`suspending` exemption: a marked suspend whose node
/// published its local artifact belongs to node drain's redrive, while the
/// same row without the marker, or with the marker and nothing published,
/// ages out.
#[tokio::test(flavor = "current_thread")]
async fn fail_cas_clears_the_moved_columns_and_spares_a_published_evacuation() -> TestResult {
    let restoring = SandboxLifecycle::Restoring;
    let suspending = SandboxLifecycle::Suspending;
    // (case, listed lifecycle, row before, local artifact published, fails)
    let cases = [
        (
            "restoring row with a long-expired lease and fork preservation",
            restoring,
            Row::bare("restoring").leased().preserving(),
            false,
            true,
        ),
        (
            "marked suspending row with nothing published",
            suspending,
            Row::bare("suspending").marked("paused"),
            false,
            true,
        ),
        (
            "marked suspending row with a published local artifact",
            suspending,
            Row::bare("suspending").marked("paused"),
            true,
            false,
        ),
        (
            "unmarked suspending row with a published local artifact",
            suspending,
            Row::bare("suspending"),
            true,
            true,
        ),
    ];

    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);
    for (case, listed, before, published, fails) in cases {
        let sandbox_id = SandboxId::new();
        insert_row(pool, sandbox_id, &before).await?;
        if published {
            pin_local_artifact(pool, sandbox_id).await?;
        }

        let claimed = sink
            .fail_aged_candidate(sandbox_id, listed, age())
            .await?
            .is_some();

        assert_eq!(claimed, fails, "{case}");
        let expected = if fails { Row::bare("failed") } else { before };
        assert_eq!(row(pool, sandbox_id).await?, expected, "{case}");
    }
    Ok(())
}

/// The age-out undo moves a row back only out of `failed`, and only while its
/// last lifecycle write is the one the fail returned. It restores the
/// lifecycle it is handed.
#[tokio::test(flavor = "current_thread")]
async fn fail_undo_reverts_only_its_own_failed_write() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        let sandbox_id = SandboxId::new();
        insert_row(pool, sandbox_id, &Row::bare("pausing")).await?;
        let token = sink
            .fail_aged_candidate(sandbox_id, SandboxLifecycle::Pausing, age())
            .await?
            .expect("the accepting fail CAS claims");
        relabel_keeping_write_token(pool, sandbox_id, stored).await?;

        let restored = sink
            .undo_fail_aged_candidate(sandbox_id, SandboxLifecycle::Pausing, &token)
            .await?;

        let accepts = stored == "failed";
        let expected = Row::bare(if accepts { "pausing" } else { stored });
        let actual = row(pool, sandbox_id).await?;
        if restored != accepts || actual != expected {
            mismatches.push(format!(
                "stored {stored}: restored {restored}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // A later write that lands on `failed` again carries another token.
    let sandbox_id = SandboxId::new();
    insert_row(pool, sandbox_id, &Row::bare("pausing")).await?;
    let token = sink
        .fail_aged_candidate(sandbox_id, SandboxLifecycle::Pausing, age())
        .await?
        .expect("the accepting fail CAS claims");
    rewrite_lifecycle(pool, sandbox_id, "started").await?;
    assert!(
        !sink
            .undo_fail_aged_candidate(sandbox_id, SandboxLifecycle::Pausing, &token)
            .await?
    );
    assert_eq!(row(pool, sandbox_id).await?, Row::bare("failed"));
    Ok(())
}

/// The fail CAS re-checks its guards on the row version it is blocked
/// behind. A writer holding the row lock decides the outcome: committing a
/// lifecycle move or a fresh restore lease makes the waiting CAS claim
/// nothing, and rolling back lets it claim.
#[tokio::test(flavor = "current_thread")]
async fn fail_cas_waiting_on_a_row_lock_decides_on_the_committed_row() -> TestResult {
    enum Concurrent {
        CommitLifecycle(&'static str),
        CommitFreshLease,
        Rollback,
    }
    let cases = [
        (Concurrent::CommitLifecycle("started"), Row::bare("started")),
        (
            Concurrent::CommitFreshLease,
            Row::bare("restoring").leased(),
        ),
        (Concurrent::Rollback, Row::bare("failed")),
    ];

    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    for (concurrent, expected) in cases {
        let sandbox_id = SandboxId::new();
        insert_row(pool, sandbox_id, &Row::bare("restoring")).await?;

        let mut writer = pool.begin().await?;
        let writer_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *writer)
            .await?;
        match concurrent {
            Concurrent::CommitLifecycle(lifecycle) => {
                sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                    .bind(sandbox_id)
                    .bind(lifecycle)
                    .execute(&mut *writer)
                    .await?;
            }
            Concurrent::CommitFreshLease | Concurrent::Rollback => {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET restore_lease_expires_at = NOW() + INTERVAL '5 minutes',
                            restore_lease_token = space_uuid_v7()
                      WHERE id = $1"
                ))
                .bind(sandbox_id)
                .execute(&mut *writer)
                .await?;
            }
        }

        // The sweep runs on its own one-connection pool, so the backend that
        // has to block is known.
        let (sweep_pool, sweep_pid) = single_backend_pool(&db).await?;
        let sweep = tokio::spawn(async move {
            direct_sink(&sweep_pool)
                .fail_aged_candidate(sandbox_id, SandboxLifecycle::Restoring, age())
                .await
        });
        bounded(wait_until_blocked(pool, sweep_pid, writer_pid)).await?;
        match concurrent {
            Concurrent::Rollback => writer.rollback().await?,
            Concurrent::CommitLifecycle(_) | Concurrent::CommitFreshLease => {
                writer.commit().await?;
            }
        }

        let claimed = bounded(async { Ok(sweep.await??) }).await?.is_some();
        assert_eq!(claimed, expected.lifecycle == "failed", "{expected:?}");
        assert_eq!(row(pool, sandbox_id).await?, expected);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Restoring converge, revert-to-suspended and its undo, unrestorable fail.
// ---------------------------------------------------------------------------

/// The converge CAS moves only `restoring` rows, to whichever wake target it
/// is handed, and leaving `restoring` clears the restore lease and fork
/// preservation.
#[tokio::test(flavor = "current_thread")]
async fn converge_cas_moves_only_restoring_rows() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        for target in [SandboxLifecycle::Started, SandboxLifecycle::Paused] {
            let sandbox_id = SandboxId::new();
            let before = if stored == "restoring" {
                Row::bare(stored).leased().preserving()
            } else {
                Row::bare(stored)
            };
            insert_row(pool, sandbox_id, &before).await?;

            let claimed = sink.converge_restoring(sandbox_id, target, age()).await?;

            let accepts = stored == "restoring";
            let expected = if accepts {
                Row::bare(target.as_str())
            } else {
                before
            };
            let actual = row(pool, sandbox_id).await?;
            if claimed != accepts || actual != expected {
                mismatches.push(format!(
                    "stored {stored}, target {target}: claimed {claimed}, row {actual:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // Another substrate's `restoring` row is not this sink's to converge,
    // even with the routable execution a `k8s` row needs.
    let sandbox_id = SandboxId::new();
    insert_row(pool, sandbox_id, &Row::bare("restoring")).await?;
    set_managed_by(pool, sandbox_id, "k8s").await?;
    insert_execution(pool, sandbox_id, ExecutionSeed::RUNNING).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_ip = '100.64.0.9' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    assert!(
        !sink
            .converge_restoring(sandbox_id, SandboxLifecycle::Started, age())
            .await?
    );
    assert_eq!(row(pool, sandbox_id).await?, Row::bare("restoring"));
    Ok(())
}

/// The revert CAS moves a `restoring` row with a pre-restore snapshot to
/// `suspended` and no other stored lifecycle. Reverting clears the restore
/// lease and fork preservation and keeps an evacuation marker, because
/// `suspended` is one of the lifecycles the marker survives.
#[tokio::test(flavor = "current_thread")]
async fn revert_cas_moves_only_restoring_rows() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    let markers = stored_lifecycles().map(|stored| (stored, None));
    for (stored, marker) in markers.chain([("restoring", Some("started"))]) {
        let sandbox_id = SandboxId::new();
        let mut before = if stored == "restoring" {
            Row::bare(stored).leased().preserving()
        } else {
            Row::bare(stored)
        };
        if let Some(marker) = marker {
            before = before.marked(marker);
        }
        insert_row(pool, sandbox_id, &before).await?;
        insert_snapshot(pool, sandbox_id, "ready", -60).await?;

        let claimed = sink
            .revert_to_suspended(sandbox_id, SandboxLifecycle::Restoring, age())
            .await?
            .is_some();

        let accepts = stored == "restoring";
        let expected = if accepts {
            let suspended = Row::bare("suspended");
            match marker {
                Some(marker) => suspended.marked(marker),
                None => suspended,
            }
        } else {
            before
        };
        let actual = row(pool, sandbox_id).await?;
        if claimed != accepts || actual != expected {
            mismatches.push(format!(
                "stored {stored}, marker {marker:?}: claimed {claimed}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // A `restoring` row younger than the age window, or of another
    // substrate, is not reverted even with its pre-restore snapshot.
    for case in ["younger than the age window", "another substrate"] {
        let sandbox_id = SandboxId::new();
        insert_row(pool, sandbox_id, &Row::bare("restoring")).await?;
        insert_snapshot(pool, sandbox_id, "ready", -60).await?;
        if case == "another substrate" {
            set_managed_by(pool, sandbox_id, "k8s").await?;
        } else {
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '10 seconds'
                  WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(pool)
            .await?;
        }
        let claimed = sink
            .revert_to_suspended(sandbox_id, SandboxLifecycle::Restoring, age())
            .await?;
        assert!(claimed.is_none(), "{case}");
        assert_eq!(
            row(pool, sandbox_id).await?,
            Row::bare("restoring"),
            "{case}"
        );
    }
    Ok(())
}

/// The revert undo moves a row back only out of `suspended` and only while
/// the revert's write is the row's last, restoring the lifecycle and the
/// fork-preservation flag it is handed but not the lease.
#[tokio::test(flavor = "current_thread")]
async fn revert_undo_reverts_only_its_own_suspended_write() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        let sandbox_id = SandboxId::new();
        insert_row(
            pool,
            sandbox_id,
            &Row::bare("restoring").leased().preserving(),
        )
        .await?;
        insert_snapshot(pool, sandbox_id, "ready", -60).await?;
        let token = sink
            .revert_to_suspended(sandbox_id, SandboxLifecycle::Restoring, age())
            .await?
            .expect("the accepting revert CAS claims");
        relabel_keeping_write_token(pool, sandbox_id, stored).await?;

        let restored = sink
            .undo_revert_to_suspended(sandbox_id, SandboxLifecycle::Restoring, &token, true)
            .await?;

        let accepts = stored == "suspended";
        let expected = if accepts {
            Row::bare("restoring").preserving()
        } else {
            Row::bare(stored)
        };
        let actual = row(pool, sandbox_id).await?;
        if restored != accepts || actual != expected {
            mismatches.push(format!(
                "stored {stored}: restored {restored}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The unrestorable-`restoring` fail CAS moves only `restoring` rows of its
/// own substrate, and only while neither node column names a node: a row
/// with an active node or a target node has a restore source.
#[tokio::test(flavor = "current_thread")]
async fn unrestorable_fail_cas_moves_only_unplaced_restoring_rows_of_its_substrate() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        let sandbox_id = SandboxId::new();
        // Unplaced, no execution, no snapshot, no marker, placed long ago:
        // the shape the planner provably cannot restore.
        insert_row(pool, sandbox_id, &Row::bare(stored)).await?;

        let claimed = sink.fail_unrestorable_restoring(sandbox_id, age()).await?;

        let accepts = stored == "restoring";
        let expected = Row::bare(if accepts { "failed" } else { stored });
        let actual = row(pool, sandbox_id).await?;
        if claimed != accepts || actual != expected {
            mismatches.push(format!(
                "stored {stored}: claimed {claimed}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    let node_id = NodeId::new();
    // (case, active node, target node, managed_by)
    let refusals = [
        ("active node only", Some(node_id), None, "space"),
        ("target node only", None, Some(node_id), "space"),
        ("another substrate", None, None, "k8s"),
    ];
    for (case, active_node_id, target_node_id, managed_by) in refusals {
        let sandbox_id = SandboxId::new();
        insert_row(pool, sandbox_id, &Row::bare("restoring")).await?;
        set_node_columns(pool, sandbox_id, active_node_id, target_node_id).await?;
        set_managed_by(pool, sandbox_id, managed_by).await?;

        assert!(
            !sink.fail_unrestorable_restoring(sandbox_id, age()).await?,
            "{case}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            Row::bare("restoring"),
            "{case}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Command-ready-pending reap, its undo, and heal.
// ---------------------------------------------------------------------------

/// The reap moves a row to `failed` only out of `started` and only while it
/// still carries `command_ready_pending`; the move clears the marker. Heal
/// clears the marker on any lifecycle and never writes the lifecycle. Both
/// are scoped to the sink's substrate.
#[tokio::test(flavor = "current_thread")]
async fn reap_moves_only_marked_started_rows_and_heal_never_moves_a_lifecycle() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        let reaped = SandboxId::new();
        insert_row(pool, reaped, &Row::bare(stored).pending()).await?;
        let claimed = sink.reap_command_ready_pending(reaped).await?.is_some();
        let accepts = stored == "started";
        let expected = if accepts {
            Row::bare("failed")
        } else {
            Row::bare(stored).pending()
        };
        let actual = row(pool, reaped).await?;
        if claimed != accepts || actual != expected {
            mismatches.push(format!("reap {stored}: claimed {claimed}, row {actual:?}"));
        }

        let healed = SandboxId::new();
        insert_row(pool, healed, &Row::bare(stored).pending()).await?;
        sink.heal_command_ready_pending(healed).await?;
        let actual = row(pool, healed).await?;
        if actual != Row::bare(stored) {
            mismatches.push(format!("heal {stored}: row {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // The data-plane access path clears the marker of a `started` row it has
    // confirmed ready, racing the reaper; the reap then claims nothing.
    let confirmed = SandboxId::new();
    insert_row(pool, confirmed, &Row::bare("started")).await?;
    assert!(sink.reap_command_ready_pending(confirmed).await?.is_none());
    assert_eq!(row(pool, confirmed).await?, Row::bare("started"));

    for write in ["reap", "heal"] {
        let sandbox_id = SandboxId::new();
        insert_row(pool, sandbox_id, &Row::bare("started").pending()).await?;
        set_managed_by(pool, sandbox_id, "k8s").await?;
        if write == "reap" {
            assert!(sink.reap_command_ready_pending(sandbox_id).await?.is_none());
        } else {
            sink.heal_command_ready_pending(sandbox_id).await?;
        }
        assert_eq!(
            row(pool, sandbox_id).await?,
            Row::bare("started").pending(),
            "{write} of another substrate's row"
        );
    }
    Ok(())
}

/// The reap undo moves a row back to `started` only out of `failed` and only
/// while the reap's write is the row's last, and puts the marker back.
#[tokio::test(flavor = "current_thread")]
async fn reap_undo_reverts_only_its_own_failed_write() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        let sandbox_id = SandboxId::new();
        insert_row(pool, sandbox_id, &Row::bare("started").pending()).await?;
        let token = sink
            .reap_command_ready_pending(sandbox_id)
            .await?
            .expect("the accepting reap claims");
        relabel_keeping_write_token(pool, sandbox_id, stored).await?;

        let restored = sink
            .undo_reap_command_ready_pending(sandbox_id, &token)
            .await?;

        let accepts = stored == "failed";
        let expected = if accepts {
            Row::bare("started").pending()
        } else {
            Row::bare(stored)
        };
        let actual = row(pool, sandbox_id).await?;
        if restored != accepts || actual != expected {
            mismatches.push(format!(
                "stored {stored}: restored {restored}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // A later write that lands on `failed` again carries another token.
    let sandbox_id = SandboxId::new();
    insert_row(pool, sandbox_id, &Row::bare("started").pending()).await?;
    let token = sink
        .reap_command_ready_pending(sandbox_id)
        .await?
        .expect("the accepting reap claims");
    rewrite_lifecycle(pool, sandbox_id, "started").await?;
    assert!(
        !sink
            .undo_reap_command_ready_pending(sandbox_id, &token)
            .await?
    );
    assert_eq!(row(pool, sandbox_id).await?, Row::bare("failed"));
    Ok(())
}
