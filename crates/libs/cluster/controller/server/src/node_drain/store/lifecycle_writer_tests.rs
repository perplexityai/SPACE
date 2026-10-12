//! Characterization of node drain's writes to Postgres `sandboxes.lifecycle`
//! and the columns that move with it (`evacuation_restore_target`,
//! `active_node_id`, placement), through [`NodeDrainSink`] on the production
//! [`SubstrateSink`] against the real Postgres fixture.
//!
//! Evacuation runs with the sink's in-process suspend
//! ([`SubstrateSink::with_suspend_execution`]). The suspend stands in for the
//! Kubernetes substrate: it can write what a concurrent actor would write
//! while the RPC is in flight, then fail before the node commits, or succeed
//! with the persisted handoff `finish_suspended_execution` makes. Every other
//! sink method used here touches only Postgres.
//!
//! Refusals already pinned at the query level are not repeated here: the
//! claim's forced-drain mode (`gentle_escalation_tests`); the redrive's
//! marker, retirement and snapshot gates, the in-flight-upload guard of
//! mark-lost, and the gone-node identity, recoverable-suspend and
//! marked-restore guards (`durable_upload_gate_tests`).
//!
//! Not reachable without a Kubernetes substrate, and so not characterized
//! here: the restore finalize (`restoring` → target, after
//! `restore_evacuated_execution`), and the claims of
//! `mark_sandbox_failed_on_node` and `abandon_stale_resident_sandboxes`,
//! which delete the execution through Kubernetes as soon as they claim a row.
//! Their SQL is covered at the query level in `durable_upload_gate_tests`.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId, SandboxLifecycle};
use pplx_space_db_test_fixtures::{pg_test_db, pg_test_db_with_max_connections};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;
use crate::lifecycle_writer_test_support::{
    ExecutionSeed, SandboxSeed, bounded, insert_execution, insert_node, insert_sandbox,
    insert_snapshot, keeps_marker, pin_local_artifact, single_backend_pool, stored_lifecycles,
    wait_until_blocked,
};

/// The lifecycles node drain lists for evacuation (`EVACUABLE_LIFECYCLES`).
const LISTED: [SandboxLifecycle; 2] = [SandboxLifecycle::Started, SandboxLifecycle::Paused];

/// The Postgres `sandboxes` columns a drain write moves.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    lifecycle: String,
    evacuation_restore_target: Option<String>,
    active_node_id: Option<NodeId>,
    placement: String,
}

impl Row {
    /// A row is `on_node` exactly when it has an active node, in every
    /// fixture here and after every write here.
    fn new(lifecycle: &str, marker: Option<&str>, active_node_id: Option<NodeId>) -> Self {
        Self {
            lifecycle: lifecycle.to_owned(),
            evacuation_restore_target: marker.map(str::to_owned),
            active_node_id,
            placement: if active_node_id.is_some() {
                "on_node"
            } else {
                "unplaced"
            }
            .to_owned(),
        }
    }
}

async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT lifecycle, evacuation_restore_target, active_node_id, placement
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn insert_forced_draining_node(pool: &PgPool) -> TestResult<NodeId> {
    insert_node(pool, Some("draining"), Some("forced")).await
}

/// A `k8s` row in `lifecycle`, resident on `node_id` with a running
/// execution, carrying `marker`.
async fn insert_resident(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
    marker: Option<&'static str>,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let mut seed = SandboxSeed::new(lifecycle, "k8s").on_node(node_id);
    seed.marker = marker;
    insert_sandbox(pool, sandbox_id, &seed).await?;
    insert_execution(pool, sandbox_id, ExecutionSeed::RUNNING).await?;
    Ok(sandbox_id)
}

/// What the in-process suspend does once evacuation has claimed the row.
#[derive(Clone, Copy)]
enum Suspend {
    /// Fail before the node commits, after another writer lands `lifecycle`,
    /// or after the node commits the active execution `suspended` when
    /// `commits_execution`.
    Fails {
        concurrent_lifecycle: Option<&'static str>,
        commits_execution: bool,
    },
    /// Succeed with the Kubernetes substrate's persisted handoff, then let
    /// another writer land `lifecycle` before node drain continues.
    HandsOff {
        concurrent_lifecycle: Option<&'static str>,
    },
}

impl Suspend {
    const FAILS: Self = Self::Fails {
        concurrent_lifecycle: None,
        commits_execution: false,
    };
}

/// A sink whose suspend records the row it saw, then acts as `suspend` says.
///
/// The successful handoff reproduces `finish_suspended_execution`'s
/// finalization: the row is `suspended`, unplaced, without an active node,
/// with the published artifact recorded, and its execution is retired
/// `suspended`. It also removes the source node's `nodes` row: evacuation
/// enqueues the artifact backup after the suspend and swallows the outcome,
/// and with no node row that enqueue resolves no endpoint and returns before
/// any node-api or Kubernetes call. The backup enqueue is not characterized.
fn evacuation_sink(
    sink_pool: &PgPool,
    pool: &PgPool,
    node_id: NodeId,
    suspend: Suspend,
) -> (SubstrateSink, Arc<Mutex<Vec<Row>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let suspend_pool = pool.clone();
    let sink = SubstrateSink::with_suspend_execution(sink_pool.clone(), move |sandbox_id| {
        let pool = suspend_pool.clone();
        let recorded = Arc::clone(&recorded);
        async move {
            let to_substrate = |error: sqlx::Error| SubstrateError::Failed {
                message: error.to_string(),
            };
            let observed = sqlx::query_as::<_, Row>(sql!(
                "SELECT lifecycle, evacuation_restore_target, active_node_id, placement
                   FROM sandboxes WHERE id = $1"
            ))
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await
            .map_err(to_substrate)?;
            recorded.lock().expect("recorder lock").push(observed);
            let (concurrent_lifecycle, outcome) = match suspend {
                Suspend::Fails {
                    concurrent_lifecycle,
                    commits_execution,
                } => {
                    if commits_execution {
                        sqlx::query(sql!(
                            "UPDATE sandbox_k8s_executions SET state = 'suspended'
                              WHERE sandbox_id = $1 AND active"
                        ))
                        .bind(sandbox_id)
                        .execute(&pool)
                        .await
                        .map_err(to_substrate)?;
                    }
                    (
                        concurrent_lifecycle,
                        Err(SubstrateError::Unavailable {
                            message: "synthetic suspend failure before the node commit".to_owned(),
                        }),
                    )
                }
                Suspend::HandsOff {
                    concurrent_lifecycle,
                } => {
                    let artifact_id = LocalArtifactId::new();
                    let mut tx = pool.begin().await.map_err(to_substrate)?;
                    sqlx::query(sql!(
                        "UPDATE sandbox_k8s_executions
                            SET active = FALSE, state = 'suspended', pod_ip = NULL,
                                terminated_at = COALESCE(terminated_at, NOW()),
                                updated_at = NOW()
                          WHERE sandbox_id = $1 AND active"
                    ))
                    .bind(sandbox_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(to_substrate)?;
                    sqlx::query(sql!(
                        "UPDATE sandboxes
                            SET lifecycle = 'suspended', placement = 'unplaced',
                                active_node_id = NULL, command_ready_pending = FALSE,
                                current_local_artifact_id = $2,
                                current_local_artifact_kind = 'full',
                                current_local_artifact_origin = 'suspend'
                          WHERE id = $1"
                    ))
                    .bind(sandbox_id)
                    .bind(artifact_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(to_substrate)?;
                    tx.commit().await.map_err(to_substrate)?;
                    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
                        .bind(node_id)
                        .execute(&pool)
                        .await
                        .map_err(to_substrate)?;
                    (
                        concurrent_lifecycle,
                        Ok(Some(PublishedLocalArtifact {
                            id: artifact_id,
                            kind: ArtifactKind::Full,
                        })),
                    )
                }
            };
            if let Some(lifecycle) = concurrent_lifecycle {
                sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                    .bind(sandbox_id)
                    .bind(lifecycle)
                    .execute(&pool)
                    .await
                    .map_err(to_substrate)?;
            }
            outcome
        }
    });
    (sink, seen)
}

fn seen(recorder: &Arc<Mutex<Vec<Row>>>) -> Vec<Row> {
    recorder.lock().expect("recorder lock").clone()
}

/// Evacuation failed, and not with the terminal failed-on-node verdict.
fn retryable_failure(result: &Result<(), EvacuateError>) -> bool {
    matches!(result, Err(error) if !matches!(error, EvacuateError::SandboxFailedOnNode(_)))
}

// ---------------------------------------------------------------------------
// Evacuation: claim, revert and the suspend handoff.
// ---------------------------------------------------------------------------

/// The evacuation claim moves a row to `suspending` with the listed lifecycle
/// as its restore target only when the row is still in that lifecycle, or is
/// already `suspending` toward that same target. A suspend that fails before
/// the node commits reverts the claim to the listed lifecycle and clears the
/// marker.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claims_only_the_listed_lifecycle_or_its_own_marked_suspend() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = insert_forced_draining_node(pool).await?;

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        for listed in LISTED {
            for marker in [None, Some("started"), Some("paused")] {
                let sandbox_id = insert_resident(pool, node_id, stored, marker).await?;
                let before = row(pool, sandbox_id).await?;
                let (sink, recorder) = evacuation_sink(pool, pool, node_id, Suspend::FAILS);

                let result = sink.evacuate_sandbox(sandbox_id, node_id, listed).await;

                let claims = stored == listed.as_str()
                    || (stored == "suspending" && marker == Some(listed.as_str()));
                let (expected_seen, expected_row) = if claims {
                    (
                        vec![Row::new("suspending", Some(listed.as_str()), Some(node_id))],
                        Row::new(listed.as_str(), None, Some(node_id)),
                    )
                } else {
                    (Vec::new(), before)
                };
                let actual = row(pool, sandbox_id).await?;
                let actual_seen = seen(&recorder);
                if !retryable_failure(&result)
                    || actual_seen != expected_seen
                    || actual != expected_row
                {
                    mismatches.push(format!(
                        "stored {stored}, marker {marker:?}, listed {listed}: \
                         {result:?}, suspend saw {actual_seen:?}, row {actual:?}"
                    ));
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The claim re-asserts residency and the node's drain phase. Each refusal
/// sits beside the accepting control, differing in that one column.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_requires_residency_on_a_draining_node() -> TestResult {
    enum Residency {
        OnNode,
        OnOtherNode,
        Unplaced,
    }
    let cases = [
        ("draining node", Some("draining"), Residency::OnNode, true),
        (
            "resident on another node",
            Some("draining"),
            Residency::OnOtherNode,
            false,
        ),
        (
            "no active node",
            Some("draining"),
            Residency::Unplaced,
            false,
        ),
        (
            "node already drained",
            Some("drained"),
            Residency::OnNode,
            false,
        ),
        ("node not draining", None, Residency::OnNode, false),
    ];

    let db = pg_test_db().await?;
    let pool = &db.pool;
    for (case, phase, residency, claims) in cases {
        let node_id = insert_node(pool, phase, Some("forced")).await?;
        let sandbox_id = insert_resident(pool, node_id, "started", None).await?;
        let active_node_id = match residency {
            Residency::OnNode => Some(node_id),
            Residency::OnOtherNode => Some(NodeId::new()),
            Residency::Unplaced => None,
        };
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET active_node_id = $2,
                    placement = CASE WHEN $2::uuid IS NULL THEN 'unplaced' ELSE 'on_node' END
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(active_node_id)
        .execute(pool)
        .await?;
        let before = row(pool, sandbox_id).await?;
        let (sink, recorder) = evacuation_sink(pool, pool, node_id, Suspend::FAILS);

        let result = sink
            .evacuate_sandbox(sandbox_id, node_id, SandboxLifecycle::Started)
            .await;

        assert!(retryable_failure(&result), "{case}: {result:?}");
        let expected_seen = if claims {
            vec![Row::new("suspending", Some("started"), Some(node_id))]
        } else {
            Vec::new()
        };
        assert_eq!(seen(&recorder), expected_seen, "{case}");
        let expected = if claims {
            Row::new("started", None, Some(node_id))
        } else {
            before
        };
        assert_eq!(row(pool, sandbox_id).await?, expected, "{case}");
    }
    Ok(())
}

/// After a failed suspend the revert moves the row back only while it is
/// still `suspending` and the active execution has not committed its
/// suspend. Whatever another writer landed during the suspend stands.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_revert_moves_only_a_claim_still_suspending_and_uncommitted() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = insert_forced_draining_node(pool).await?;

    let mut mismatches = Vec::new();
    let landed = stored_lifecycles().map(|lifecycle| (Some(lifecycle), false));
    for (concurrent_lifecycle, commits_execution) in landed.chain([(None, true)]) {
        let sandbox_id = insert_resident(pool, node_id, "paused", None).await?;
        let suspend = Suspend::Fails {
            concurrent_lifecycle,
            commits_execution,
        };
        let (sink, _) = evacuation_sink(pool, pool, node_id, suspend);

        let result = sink
            .evacuate_sandbox(sandbox_id, node_id, SandboxLifecycle::Paused)
            .await;

        let during = concurrent_lifecycle.unwrap_or("suspending");
        let reverts = during == "suspending" && !commits_execution;
        let expected = if reverts {
            Row::new("paused", None, Some(node_id))
        } else {
            let marker = keeps_marker(during).then_some("paused");
            Row::new(during, marker, Some(node_id))
        };
        let actual = row(pool, sandbox_id).await?;
        if !retryable_failure(&result) || actual != expected {
            mismatches.push(format!(
                "landed {concurrent_lifecycle:?}, execution committed {commits_execution}: \
                 {result:?}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// After the substrate's suspend handoff (row `suspended`, unplaced, no
/// active node), node drain's own `suspending` → `suspended` update finds
/// nothing to do: the row stays as the handoff left it, with the evacuation
/// marker naming the lifecycle to restore, and evacuation succeeds. A write
/// another actor lands between the handoff and that update stands, unless it
/// writes `suspending` again, which the update moves to `suspended`.
///
/// Evacuation reports success in every case; for a row that is no longer
/// `suspended` that is listed as a finding rather than pinned, so only the
/// handoff case asserts the result.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_after_the_suspend_handoff_leaves_the_handed_off_row() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let mut mismatches = Vec::new();
    let landed = stored_lifecycles().map(Some).chain([None]);
    for concurrent_lifecycle in landed {
        let node_id = insert_forced_draining_node(pool).await?;
        let sandbox_id = insert_resident(pool, node_id, "started", None).await?;
        let suspend = Suspend::HandsOff {
            concurrent_lifecycle,
        };
        let (sink, _) = evacuation_sink(pool, pool, node_id, suspend);

        let result = sink
            .evacuate_sandbox(sandbox_id, node_id, SandboxLifecycle::Started)
            .await;

        let after_handoff = match concurrent_lifecycle {
            None | Some("suspending") => "suspended",
            Some(lifecycle) => lifecycle,
        };
        let marker = keeps_marker(after_handoff).then_some("started");
        let expected = Row::new(after_handoff, marker, None);
        let actual = row(pool, sandbox_id).await?;
        if (concurrent_lifecycle.is_none() && result.is_err()) || actual != expected {
            mismatches.push(format!(
                "landed {concurrent_lifecycle:?}: {result:?}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The claim waits on a writer holding the row lock and decides on the row
/// that writer leaves: a committed lifecycle move or residency move refuses
/// the claim without calling the suspend, and a rollback lets it claim.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_waiting_on_a_row_lock_decides_on_the_committed_row() -> TestResult {
    enum Concurrent {
        CommitLifecycle(&'static str),
        CommitResidencyElsewhere,
        Rollback,
    }
    let cases = [
        Concurrent::CommitLifecycle("stopped"),
        Concurrent::CommitResidencyElsewhere,
        Concurrent::Rollback,
    ];

    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    for concurrent in cases {
        let node_id = insert_forced_draining_node(pool).await?;
        let other_node_id = NodeId::new();
        let sandbox_id = insert_resident(pool, node_id, "started", None).await?;

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
            Concurrent::CommitResidencyElsewhere | Concurrent::Rollback => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
                ))
                .bind(sandbox_id)
                .bind(other_node_id)
                .execute(&mut *writer)
                .await?;
            }
        }

        // The claim runs on the sink's own one-connection pool, so the
        // backend that has to block is known.
        let (claim_pool, claim_pid) = single_backend_pool(&db).await?;
        let (sink, recorder) = evacuation_sink(&claim_pool, pool, node_id, Suspend::FAILS);
        let evacuation = tokio::spawn(async move {
            sink.evacuate_sandbox(sandbox_id, node_id, SandboxLifecycle::Started)
                .await
        });
        bounded(wait_until_blocked(pool, claim_pid, writer_pid)).await?;
        let expected = match concurrent {
            Concurrent::CommitLifecycle(lifecycle) => {
                writer.commit().await?;
                Row::new(lifecycle, None, Some(node_id))
            }
            Concurrent::CommitResidencyElsewhere => {
                writer.commit().await?;
                Row::new("started", None, Some(other_node_id))
            }
            Concurrent::Rollback => {
                writer.rollback().await?;
                Row::new("started", None, Some(node_id))
            }
        };

        let result = bounded(async { Ok(evacuation.await?) }).await?;
        assert!(retryable_failure(&result), "{result:?}");
        let expected_seen = if matches!(concurrent, Concurrent::Rollback) {
            vec![Row::new("suspending", Some("started"), Some(node_id))]
        } else {
            Vec::new()
        };
        assert_eq!(seen(&recorder), expected_seen);
        assert_eq!(row(pool, sandbox_id).await?, expected);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Restore sweep: stranded-suspend redrive and the cohort restore's refusals.
// ---------------------------------------------------------------------------

fn sink_without_suspend(pool: &PgPool) -> SubstrateSink {
    SubstrateSink::with_suspend_execution(pool.clone(), |_| async {
        unreachable!("this write never suspends")
    })
}

/// The stranded-suspend redrive moves a marked row whose source execution is
/// retired and whose local artifact is published to `suspended`, keeping the
/// marker, only out of `suspending`.
#[tokio::test(flavor = "current_thread")]
async fn redrive_moves_only_marked_suspending_rows() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let mut ids = Vec::new();
    for stored in stored_lifecycles() {
        let sandbox_id = SandboxId::new();
        insert_sandbox(
            pool,
            sandbox_id,
            &SandboxSeed::new(stored, "k8s").marked("started"),
        )
        .await?;
        pin_local_artifact(pool, sandbox_id).await?;
        ids.push((stored, sandbox_id));
    }

    let advanced: std::collections::BTreeSet<SandboxId> = sink_without_suspend(pool)
        .redrive_stranded_suspending_evacuations()
        .await?
        .into_iter()
        .collect();

    let mut mismatches = Vec::new();
    for (stored, sandbox_id) in ids {
        let advances = stored == "suspending";
        let lifecycle = if advances { "suspended" } else { stored };
        let expected = Row::new(lifecycle, Some("started"), None);
        let actual = row(pool, sandbox_id).await?;
        if advanced.contains(&sandbox_id) != advances || actual != expected {
            mismatches.push(format!("{stored}: row {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The cohort restore defers without writing when the row has lost its
/// marker, its newest full snapshot is not ready, or it has no snapshot. A
/// marked `suspended` row with a ready snapshot, the accepting control, is
/// what the restore sweep lists. (Restoring it needs the Kubernetes
/// substrate.)
#[tokio::test(flavor = "current_thread")]
async fn cohort_restore_defers_rows_the_restore_sweep_would_not_list() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink_without_suspend(pool);
    let timeout = PositiveDuration::new(Duration::from_secs(60)).expect("non-zero");

    let eligible = SandboxId::new();
    insert_sandbox(
        pool,
        eligible,
        &SandboxSeed::new("suspended", "k8s").marked("started"),
    )
    .await?;
    insert_snapshot(pool, eligible, "ready", -600).await?;

    // (case, seed, snapshot states oldest first)
    let cases = [
        (
            "marker cleared by a takeover",
            SandboxSeed::new("suspended", "k8s"),
            &["ready"][..],
        ),
        (
            "newest full snapshot still uploading",
            SandboxSeed::new("suspended", "k8s").marked("started"),
            &["ready", "creating"][..],
        ),
        (
            "no snapshot at all",
            SandboxSeed::new("restoring", "k8s").marked("paused"),
            &[][..],
        ),
    ];
    let mut deferred_ids = Vec::new();
    for (case, seed, snapshots) in cases {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, &seed).await?;
        for (offset, state) in (0_i64..).zip(snapshots) {
            insert_snapshot(pool, sandbox_id, state, offset - 600).await?;
        }
        let before = row(pool, sandbox_id).await?;

        let result = sink
            .restore_evacuated_sandbox(sandbox_id, SandboxLifecycle::Started, timeout, None)
            .await;

        assert!(matches!(result, Ok(RestoreOutcome::Deferred)), "{case}");
        assert_eq!(row(pool, sandbox_id).await?, before, "{case}");
        deferred_ids.push(sandbox_id);
    }

    let listed: Vec<SandboxId> = sink
        .list_restore_pending_evacuations(false)
        .await?
        .into_iter()
        .map(|(sandbox_id, _)| sandbox_id)
        .collect();
    assert_eq!(listed, vec![eligible]);
    Ok(())
}

// ---------------------------------------------------------------------------
// Give-up writes reachable without Kubernetes.
// ---------------------------------------------------------------------------

async fn set_target_node(pool: &PgPool, sandbox_id: SandboxId, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark-lost fails only `suspended` rows targeted at the node, and only while
/// no copy is durable for releasing the source node.
#[tokio::test(flavor = "current_thread")]
async fn mark_lost_fails_only_suspended_rows_without_a_durable_copy() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let sink = sink_without_suspend(pool);

    let mut mismatches = Vec::new();
    for stored in stored_lifecycles() {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, &SandboxSeed::new(stored, "k8s")).await?;
        set_target_node(pool, sandbox_id, node_id).await?;

        let flipped = sink.mark_sandbox_lost(sandbox_id, node_id).await?;

        let accepts = stored == "suspended";
        let expected = Row::new(if accepts { "failed" } else { stored }, None, None);
        let actual = row(pool, sandbox_id).await?;
        if flipped != u64::from(accepts) || actual != expected {
            mismatches.push(format!(
                "stored {stored}: flipped {flipped}, row {actual:?}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");

    // (case, target node is this node, a ready snapshot since placement)
    let cases = [
        ("targeted at another node", false, false),
        ("durable copy since placement", true, true),
    ];
    for (case, targeted_here, durable) in cases {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, &SandboxSeed::new("suspended", "k8s")).await?;
        let target = if targeted_here {
            node_id
        } else {
            NodeId::new()
        };
        set_target_node(pool, sandbox_id, target).await?;
        if durable {
            insert_snapshot(pool, sandbox_id, "ready", 0).await?;
        }

        assert_eq!(
            sink.mark_sandbox_lost(sandbox_id, node_id).await?,
            0,
            "{case}"
        );
        assert_eq!(
            row(pool, sandbox_id).await?,
            Row::new("suspended", None, None),
            "{case}"
        );
    }
    Ok(())
}

/// The gone-node bulk give-up fails, and unmarks, every unmarked row
/// resident on the node in a resident lifecycle, and no row resident
/// elsewhere.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_give_up_fails_only_resident_rows_of_the_node() -> TestResult {
    fn resident(lifecycle: &str) -> bool {
        matches!(
            lifecycle,
            "creating"
                | "starting"
                | "started"
                | "pausing"
                | "paused"
                | "resuming"
                | "stopping"
                | "suspending"
                | "restoring"
                | "checkpointing"
        )
    }

    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink_without_suspend(pool);
    let node_id = insert_forced_draining_node(pool).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, 'uid-observed')"
    ))
    .bind(node_id)
    .bind(format!("synthetic-node-{node_id}"))
    .execute(pool)
    .await?;

    let mut cases: Vec<(SandboxId, Row, bool)> = Vec::new();
    for stored in stored_lifecycles() {
        let sandbox_id = insert_resident(pool, node_id, stored, None).await?;
        cases.push((sandbox_id, row(pool, sandbox_id).await?, resident(stored)));
    }
    let elsewhere = insert_resident(pool, NodeId::new(), "started", None).await?;
    cases.push((elsewhere, row(pool, elsewhere).await?, false));

    let flipped = sink
        .fail_resident_sandboxes_on_gone_node(node_id, Some("uid-observed"))
        .await?;

    let mut mismatches = Vec::new();
    let mut expected_flips = 0_u64;
    for (sandbox_id, before, fails) in cases {
        let expected = if fails {
            expected_flips += 1;
            Row::new("failed", None, before.active_node_id)
        } else {
            before.clone()
        };
        let actual = row(pool, sandbox_id).await?;
        if actual != expected {
            mismatches.push(format!("{before:?}: row {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    assert_eq!(flipped, expected_flips);
    Ok(())
}
