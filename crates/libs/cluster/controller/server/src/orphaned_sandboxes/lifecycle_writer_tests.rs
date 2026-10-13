//! Characterization of the orphaned-sandbox reconciler's writes to Postgres
//! `sandboxes.lifecycle` and what moves with them (placement, the node
//! columns, the `sandbox_k8s_executions` row and the Postgres quarantine
//! tables), through [`SubstrateActionSink::classify_orphan`] against the real
//! Postgres fixture, for every stored lifecycle and each classification guard.
//!
//! The sink runs without a pod cleaner, which reports every pod already gone.
//! Deferral on an in-flight upload is pinned in `tests.rs`
//! (`creating_snapshot_defers_disposition`).

use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;
use crate::lifecycle_writer_test_support::{
    ExecutionSeed, SandboxSeed, insert_execution, insert_node, insert_sandbox, insert_snapshot,
    pod_name, pod_uid, stored_lifecycles,
};

/// The Postgres rows a classification moves for one sandbox. The execution
/// columns read generation 1, the execution every case seeds.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    execution_state: String,
    execution_active: bool,
    quarantined_from: Option<String>,
    quarantine_history: i64,
}

async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<Row>> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, s.target_node_id,
                e.state AS execution_state, e.active AS execution_active,
                q.entered_from_lifecycle AS quarantined_from,
                (SELECT COUNT(*) FROM sandbox_quarantine_history h
                  WHERE h.sandbox_id = s.id AND h.action = 'entered_quarantine')
                    AS quarantine_history
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.generation = 1
           LEFT JOIN sandbox_quarantine_state q ON q.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?)
}

/// How the reconciler found the orphan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Guard {
    /// The active execution's pod is missing.
    PodName,
    /// The latest execution was forgotten and there is no active one.
    Forgotten,
    /// A legacy `deleting` row.
    LegacyDeleting,
}

fn classify_guard(guard: Guard, sandbox_id: SandboxId) -> TestResult<ClassifyGuard> {
    Ok(match guard {
        Guard::PodName => ClassifyGuard::PodName {
            name: pod_name(sandbox_id, 1),
            uid: Some(PodUid::parse(pod_uid(sandbox_id, 1))?),
        },
        Guard::Forgotten => ClassifyGuard::Forgotten,
        Guard::LegacyDeleting => ClassifyGuard::LegacyDeleting { pod_name: None },
    })
}

/// A `k8s` row resident on `node_id` for an hour, with one generation-1
/// execution an hour old: active and running for [`Guard::PodName`],
/// inactive and forgotten otherwise.
async fn insert_orphan(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    lifecycle: &str,
    guard: Guard,
) -> TestResult {
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new(lifecycle, "k8s").on_node(node_id),
    )
    .await?;
    let execution = match guard {
        Guard::PodName => ExecutionSeed::RUNNING,
        Guard::Forgotten | Guard::LegacyDeleting => ExecutionSeed {
            generation: 1,
            state: "forgotten",
            active: false,
        },
    };
    insert_execution(pool, sandbox_id, execution).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expected {
    Skipped,
    Quarantined,
    Recovered,
    Deleted,
}

/// What classification does to an hour-old orphan stored in `lifecycle`.
fn expected(guard: Guard, lifecycle: &str, recoverable: bool) -> Expected {
    let resolve = |lifecycle: &str| match (lifecycle, recoverable) {
        (_, true) => Expected::Recovered,
        ("creating", false) => Expected::Deleted,
        (_, false) => Expected::Quarantined,
    };
    match guard {
        Guard::PodName => match lifecycle {
            "started" | "paused" | "resuming" | "pausing" | "stopping" | "creating"
            | "starting" => resolve(lifecycle),
            _ => Expected::Skipped,
        },
        Guard::Forgotten => match lifecycle {
            "creating" | "starting" | "started" | "paused" | "resuming" | "pausing"
            | "stopping" => resolve(lifecycle),
            _ => Expected::Skipped,
        },
        Guard::LegacyDeleting => match lifecycle {
            "deleting" => Expected::Deleted,
            _ => Expected::Skipped,
        },
    }
}

/// Classification moves an orphan only out of the lifecycles its guard owns:
/// to `suspended` when a current snapshot exists, otherwise to `quarantined`
/// with a Postgres quarantine record, except that an abandoned `creating`
/// row and a legacy `deleting` row are deleted. Both moves unplace the row,
/// clear both node columns and retire an active execution into the new
/// lifecycle. Every other stored lifecycle, including the stored
/// non-variants, is left as it was.
#[tokio::test(flavor = "current_thread")]
async fn classification_moves_only_the_lifecycles_its_guard_owns() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = SubstrateActionSink::new_without_substrate(pool.clone());
    let node_id = insert_node(pool, None, None).await?;

    let mut mismatches = Vec::new();
    for guard in [Guard::PodName, Guard::Forgotten, Guard::LegacyDeleting] {
        for stored in stored_lifecycles() {
            for recoverable in [false, true] {
                let sandbox_id = SandboxId::new();
                insert_orphan(pool, sandbox_id, node_id, stored, guard).await?;
                if recoverable {
                    insert_snapshot(pool, sandbox_id, "ready", 60).await?;
                }
                let before = row(pool, sandbox_id).await?;

                let outcome = sink
                    .classify_orphan(sandbox_id, classify_guard(guard, sandbox_id)?)
                    .await?;

                let expectation = expected(guard, stored, recoverable);
                let retired = |lifecycle: &str| {
                    let execution_state = if guard == Guard::PodName {
                        lifecycle.to_owned()
                    } else {
                        "forgotten".to_owned()
                    };
                    Some(Row {
                        lifecycle: lifecycle.to_owned(),
                        placement: "unplaced".to_owned(),
                        active_node_id: None,
                        target_node_id: None,
                        execution_state,
                        execution_active: false,
                        quarantined_from: (lifecycle == "quarantined").then(|| stored.to_owned()),
                        quarantine_history: i64::from(lifecycle == "quarantined"),
                    })
                };
                let (outcome_matches, expected_row) = match expectation {
                    Expected::Skipped => {
                        (outcome == ClassifyOutcome::SkippedTerminal, before.clone())
                    }
                    Expected::Quarantined => (
                        matches!(outcome, ClassifyOutcome::Quarantined { from_lifecycle }
                            if from_lifecycle.as_str() == stored),
                        retired("quarantined"),
                    ),
                    Expected::Recovered => (
                        matches!(outcome, ClassifyOutcome::RecoveredSuspended { from_lifecycle }
                            if from_lifecycle.as_str() == stored),
                        retired("suspended"),
                    ),
                    Expected::Deleted => (
                        matches!(outcome, ClassifyOutcome::Deleted { from_lifecycle, .. }
                            if from_lifecycle.as_str() == stored),
                        None,
                    ),
                };
                let actual = row(pool, sandbox_id).await?;
                if !outcome_matches || actual != expected_row {
                    mismatches.push(format!(
                        "{guard:?}, stored {stored}, recoverable {recoverable}: \
                         expected {expectation:?}, got {outcome:?} and {actual:?}"
                    ));
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// Each guard acts only on the execution that defines it: the forgotten
/// guard on a row whose latest execution is the forgotten one, the pod guard
/// on the active execution with the observed pod's name, and for a pending
/// create only once that execution is past the create grace window. Every
/// refusal sits beside a resolving control of the same lifecycle.
#[tokio::test(flavor = "current_thread")]
async fn classification_acts_only_on_the_execution_its_guard_names() -> TestResult {
    #[derive(Debug, Clone, Copy)]
    enum Shape {
        /// The guard's own execution and nothing newer.
        Defining,
        /// A newer generation, active and running.
        NewerRunning,
        /// A newer generation, inactive but not forgotten.
        NewerRetired,
        /// The active execution runs a pod of another name.
        OtherPodName,
        /// The active execution was updated within the create grace window.
        Young,
    }
    let cases = [
        (Guard::Forgotten, "started", Shape::Defining, true),
        (Guard::Forgotten, "started", Shape::NewerRunning, false),
        (Guard::Forgotten, "started", Shape::NewerRetired, false),
        (Guard::PodName, "started", Shape::Defining, true),
        (Guard::PodName, "started", Shape::OtherPodName, false),
        (Guard::PodName, "starting", Shape::Defining, true),
        (Guard::PodName, "starting", Shape::Young, false),
    ];

    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = SubstrateActionSink::new_without_substrate(pool.clone());
    let node_id = insert_node(pool, None, None).await?;
    for (guard, lifecycle, shape, resolves) in cases {
        let sandbox_id = SandboxId::new();
        insert_orphan(pool, sandbox_id, node_id, lifecycle, guard).await?;
        match shape {
            Shape::Defining => {}
            Shape::NewerRunning => {
                let newer = ExecutionSeed {
                    generation: 2,
                    ..ExecutionSeed::RUNNING
                };
                insert_execution(pool, sandbox_id, newer).await?;
            }
            Shape::NewerRetired => {
                let newer = ExecutionSeed {
                    generation: 2,
                    state: "replaced",
                    active: false,
                };
                insert_execution(pool, sandbox_id, newer).await?;
            }
            Shape::Young => {
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions SET updated_at = NOW() WHERE sandbox_id = $1"
                ))
                .bind(sandbox_id)
                .execute(pool)
                .await?;
            }
            Shape::OtherPodName => {
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions SET pod_name = $2 WHERE sandbox_id = $1"
                ))
                .bind(sandbox_id)
                .bind(format!("{}-other", pod_name(sandbox_id, 1)))
                .execute(pool)
                .await?;
            }
        }
        let before = row(pool, sandbox_id).await?;

        let outcome = sink
            .classify_orphan(sandbox_id, classify_guard(guard, sandbox_id)?)
            .await?;

        let case = format!("{guard:?}, {lifecycle}, {shape:?}");
        let after = row(pool, sandbox_id).await?;
        if resolves {
            assert!(
                matches!(outcome, ClassifyOutcome::Quarantined { .. }),
                "{case}: {outcome:?}"
            );
            assert_eq!(
                after.map(|row| row.lifecycle).as_deref(),
                Some("quarantined"),
                "{case}"
            );
        } else {
            assert_eq!(outcome, ClassifyOutcome::SkippedTerminal, "{case}");
            assert_eq!(after, before, "{case}");
        }
    }
    Ok(())
}
