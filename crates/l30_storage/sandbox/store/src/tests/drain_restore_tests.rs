//! The drain restore's claim and commit, and the v2 drain's release of a
//! stopped sandbox ([`crate::drain_restore`]).
//!
//! A refused claim must leave the row exactly as it was: each refusal compares
//! the whole row before and after, beside a control row that the same claim
//! takes.

use std::time::Duration;

use pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections;

use super::*;
use crate::delete::mark_deleting;
use crate::drain_restore::{
    DrainRestoreClaim, DrainRestoreCommit, DrainRestoreRefusal, V2StoppedEvacuation,
    claim_drain_restore, complete_drain_restore, evacuate_v2_stopped,
};
use crate::sweeper::EvacuationRestoreTarget;

const LEASE_TTL_SECS: i64 = 60;
const EXECUTION_OWNERSHIP_TTL_SECS: i64 = 300;
const STARTED: EvacuationRestoreTarget = EvacuationRestoreTarget::Started;
const PAUSED: EvacuationRestoreTarget = EvacuationRestoreTarget::Paused;

/// When S, the drain's suspend snapshot, was taken in these fixtures.
const S_TAKEN: &str = "2026-01-01T00:00:00Z";
const NEWER: &str = "2026-01-02T00:00:00Z";
const OLDER: &str = "2025-12-01T00:00:00Z";

/// A full snapshot of `sandbox` in `state`, taken at `taken`, carrying
/// `artifact` when given, stored at a location that is orphaned when asked.
async fn snapshot(
    pool: &PgPool,
    sandbox: SandboxId,
    artifact: Option<Uuid>,
    state: &str,
    taken: &str,
    orphaned: bool,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, org_id
         )
         VALUES ($1, $2, 'full', $3, $4::TIMESTAMPTZ, $5,
                 CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'full' END,
                 CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'suspend' END, $6)"
    ))
    .bind(id)
    .bind(sandbox)
    .bind(state)
    .bind(taken)
    .bind(artifact)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string, org_id, orphaned_at
         ) VALUES (
             $1, $2, 's3://test-bucket/snapshots/drained?region=us-east-1', $3,
             CASE WHEN $4 THEN NOW() END
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(id)
    .bind(SEED_ORG)
    .bind(orphaned)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Point the row's current local artifact at `artifact`, as the suspend that
/// published it does.
async fn mark_artifact(pool: &PgPool, sandbox: SandboxId, artifact: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether the row names its suspend artifact, as a current suspend does, or
/// is a legacy row that names none.
#[derive(Clone, Copy, Debug)]
enum Artifact {
    Marked,
    Legacy,
}

/// A drained sandbox: `lifecycle`, with S, a ready and stored full snapshot,
/// which the row's artifact marker names when `artifact` is `Marked`.
async fn drained(
    pool: &PgPool,
    lifecycle: &str,
    artifact: Artifact,
) -> TestResult<(SandboxId, Uuid, Uuid)> {
    let id = SandboxId::from_uuid(insert_sandbox(pool, None, lifecycle).await?);
    let s_artifact = Uuid::now_v7();
    let s = snapshot(pool, id, Some(s_artifact), "ready", S_TAKEN, false).await?;
    if matches!(artifact, Artifact::Marked) {
        mark_artifact(pool, id, s_artifact).await?;
    }
    Ok((id, s, s_artifact))
}

/// The whole row as JSON text: equal before and after a statement iff it
/// wrote nothing.
async fn row_image(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT row_to_json(s)::TEXT FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Wait until some session of this database waits on a lock.
async fn wait_for_lock_waiter(pool: &PgPool) -> TestResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database() AND wait_event_type = 'Lock'
             )"
        ))
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the second claim never waited on the row lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// An active execution of `id` at `generation` in `state`, running under
/// activation attempt `attempt` toward `attempt_target` when given, routed on
/// a pod IP so a completion receipt can release it.
async fn execution(
    pool: &PgPool,
    id: SandboxId,
    generation: i64,
    state: &str,
    attempt: Option<(Uuid, &str)>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, pod_ip,
              activation_attempt_id, activation_attempt_target_state, route_activation_id)
         VALUES ($1, $2, $3, 'space', $4, '10.0.0.1', $5, $6, $5)"
    ))
    .bind(id)
    .bind(generation)
    .bind(format!("pod-{id}-g{generation}"))
    .bind(state)
    .bind(attempt.map(|(attempt, _)| attempt))
    .bind(attempt.map(|(_, target)| target))
    .execute(pool)
    .await?;
    Ok(())
}

/// The active execution's attempt, as `(attempt, target state)`.
async fn active_attempt(
    pool: &PgPool,
    id: SandboxId,
) -> TestResult<(Option<Uuid>, Option<String>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT activation_attempt_id, activation_attempt_target_state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Every execution of `id` as JSON text, beside [`row_image`].
async fn executions_image(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.generation)::TEXT, '[]')
           FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// The node's activation receipt for `attempt` on generation 1 of `id`,
/// toward `target`, through the ingest path that projects it.
async fn receipt(
    executor: impl sqlx::PgExecutor<'_>,
    node: NodeId,
    id: SandboxId,
    attempt: Uuid,
    target: EvacuationRestoreTarget,
) -> TestResult<Vec<SandboxId>> {
    let rows = sqlx::types::Json(serde_json::json!([{
        "sandbox_id": id,
        "published_lifecycle": target.as_str(),
        "transition_receipt_kind": pplx_space_common_core::types::SandboxTransitionReceiptKind::ActivationCompleted.as_str(),
        "transition_receipt_target_state": target.as_str(),
        "transition_receipt_execution_generation": 1,
        "transition_receipt_activation_attempt_id": attempt,
    }]));
    Ok(crate::ingest::release_completed_activations(executor, node, rows).await?)
}

/// A drained row claimed under `token`, on `node`, restored onto
/// generation 1 under activation attempt `attempt` toward `target`.
async fn restored(
    pool: &PgPool,
    node: NodeId,
    target: EvacuationRestoreTarget,
    token: Uuid,
    attempt: Uuid,
) -> TestResult<SandboxId> {
    let (id, s, _) = drained(pool, "suspended", Artifact::Marked).await?;
    assert_eq!(claim(pool, id, s, token).await?, DrainRestoreClaim::Claimed);
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2, target_node_id = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(node)
    .execute(pool)
    .await?;
    execution(pool, id, 1, "running", Some((attempt, target.as_str()))).await?;
    Ok(id)
}

async fn claim(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
    snapshot: Uuid,
    token: Uuid,
) -> TestResult<DrainRestoreClaim> {
    Ok(claim_drain_restore(
        executor,
        id,
        snapshot,
        LEASE_TTL_SECS,
        token,
        EXECUTION_OWNERSHIP_TTL_SECS,
    )
    .await?)
}

/// The restore columns: lifecycle, lease token, and whether the lease is live.
async fn lease(pool: &PgPool, id: SandboxId) -> TestResult<(String, Option<Uuid>, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, restore_lease_token,
                COALESCE(restore_lease_expires_at > NOW(), FALSE)
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// One way to make an otherwise claimable `suspended` row refuse the claim,
/// and the refusal it names.
#[derive(Clone, Copy, Debug)]
enum Case {
    /// No refusal: the control row.
    Control,
    /// The v1 drain's marker is set.
    EvacuationMarker,
    /// A full snapshot newer than S, in `state`, on a legacy row.
    NewerLegacy(&'static str),
    /// A later suspend's artifact is the row's current one.
    NewerArtifact,
    /// S is in `state`.
    SourceState(&'static str),
    /// S's only location is orphaned.
    SourceOrphaned,
    /// The operation names an older ready snapshot, not S.
    OlderNamed,
    /// The operation names another sandbox's snapshot.
    ForeignNamed,
    /// An active execution did work just now.
    FreshExecution,
    /// A restore holds the row `restoring`.
    Restoring,
    /// The sandbox runs again.
    Started,
    /// The sandbox is being deleted.
    Deleting,
}

impl Case {
    const ALL: [Self; 14] = [
        Self::Control,
        Self::EvacuationMarker,
        Self::NewerLegacy("creating"),
        Self::NewerLegacy("failed"),
        Self::NewerArtifact,
        Self::SourceState("creating"),
        Self::SourceState("failed"),
        Self::SourceOrphaned,
        Self::OlderNamed,
        Self::ForeignNamed,
        Self::FreshExecution,
        Self::Restoring,
        Self::Started,
        Self::Deleting,
    ];

    fn expected(self) -> DrainRestoreClaim {
        use DrainRestoreRefusal as R;
        DrainRestoreClaim::Refused(match self {
            Self::Control => return DrainRestoreClaim::Claimed,
            Self::EvacuationMarker
            | Self::NewerLegacy(_)
            | Self::NewerArtifact
            | Self::OlderNamed
            | Self::ForeignNamed
            | Self::Started => R::Superseded,
            Self::SourceState("creating") => R::SourcePending,
            Self::SourceState(_) | Self::SourceOrphaned => R::SourceLost,
            Self::FreshExecution | Self::Restoring => R::InFlight,
            Self::Deleting => R::Deleted,
        })
    }

    /// A drained `suspended` row with this case applied, and the snapshot the
    /// operation names.
    async fn apply(self, pool: &PgPool) -> TestResult<(SandboxId, Uuid)> {
        let artifact = if matches!(self, Self::NewerLegacy(_)) {
            Artifact::Legacy
        } else {
            Artifact::Marked
        };
        let (id, s, s_artifact) = drained(pool, "suspended", artifact).await?;
        let mut named = s;
        let set = |statement: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query(sqlx::AssertSqlSafe(statement))
                    .bind(id)
                    .execute(&pool)
                    .await
            }
        };
        match self {
            Self::Control => {}
            Self::EvacuationMarker => {
                set("UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1")
                    .await?;
            }
            Self::NewerLegacy(state) => {
                snapshot(pool, id, None, state, NEWER, false).await?;
            }
            Self::NewerArtifact => {
                let newer = Uuid::now_v7();
                snapshot(pool, id, Some(newer), "ready", NEWER, false).await?;
                mark_artifact(pool, id, newer).await?;
            }
            Self::SourceState(state) => {
                sqlx::query(sql!("UPDATE snapshots SET state = $2 WHERE id = $1"))
                    .bind(s)
                    .bind(state)
                    .execute(pool)
                    .await?;
            }
            Self::SourceOrphaned => {
                sqlx::query(sql!(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NOW()
                      WHERE snapshot_id = $1"
                ))
                .bind(s)
                .execute(pool)
                .await?;
            }
            Self::OlderNamed => {
                named = snapshot(pool, id, Some(s_artifact), "ready", OLDER, false).await?;
            }
            Self::ForeignNamed => {
                let (_, foreign, _) = drained(pool, "suspended", Artifact::Marked).await?;
                named = foreign;
            }
            Self::FreshExecution => {
                execution(pool, id, 1, "running", None).await?;
            }
            Self::Restoring => {
                set("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1").await?;
            }
            Self::Started => {
                set("UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1").await?;
            }
            Self::Deleting => {
                let mut tx = pool.begin().await?;
                mark_deleting(&mut tx, id).await?;
                tx.commit().await?;
            }
        }
        Ok((id, named))
    }
}

/// A `suspended` row whose restore selector loads S is claimed under the
/// operation's lease; every other row is refused with the reason the
/// operation decides from, and left exactly as it was. A missing row reads
/// deleted.
#[tokio::test(flavor = "current_thread")]
async fn the_claim_takes_only_a_suspended_row_loading_s_and_names_every_refusal() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for case in Case::ALL {
        let (id, named) = case.apply(pool).await?;
        let before = row_image(pool, id).await?;
        let token = Uuid::now_v7();
        assert_eq!(
            claim(pool, id, named, token).await?,
            case.expected(),
            "{case:?}"
        );
        if matches!(case, Case::Control) {
            assert_eq!(
                lease(pool, id).await?,
                ("restoring".to_owned(), Some(token), true)
            );
        } else {
            assert_eq!(row_image(pool, id).await?, before, "{case:?}: row changed");
        }
    }
    assert_eq!(
        claim(pool, SandboxId::new(), Uuid::now_v7(), Uuid::now_v7()).await?,
        DrainRestoreClaim::Refused(DrainRestoreRefusal::Deleted)
    );
    Ok(())
}

/// Two claims of one row at once take it once. The second waits on the
/// first's row lock, then finds the row restoring.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_claims_take_the_row_once() -> TestResult {
    // The open claim, the waiting claim and the lock probe each hold a
    // connection.
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let pool = db.pool.clone();
    let (id, s, _) = drained(&pool, "suspended", Artifact::Marked).await?;
    let first_token = Uuid::now_v7();
    let mut first = pool.begin().await?;
    assert_eq!(
        claim(&mut *first, id, s, first_token).await?,
        DrainRestoreClaim::Claimed
    );
    let second = tokio::spawn({
        let pool = pool.clone();
        async move { claim(&pool, id, s, Uuid::now_v7()).await }
    });
    wait_for_lock_waiter(&pool).await?;
    first.commit().await?;
    assert_eq!(
        second.await??,
        DrainRestoreClaim::Refused(DrainRestoreRefusal::InFlight)
    );
    assert_eq!(lease(&pool, id).await?.1, Some(first_token));
    Ok(())
}

/// The commit lands the target only under the claim's token and execution,
/// on its node, clears the lease and releases the restore's attempt.
#[tokio::test(flavor = "current_thread")]
async fn the_commit_lands_its_own_restore() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for target in [STARTED, PAUSED] {
        let token = Uuid::now_v7();
        let attempt = Uuid::now_v7();
        let id = restored(pool, NodeId::new(), STARTED, token, attempt).await?;
        let before = (
            row_image(pool, id).await?,
            executions_image(pool, id).await?,
        );
        for (case, token, generation) in [
            ("another lease", Uuid::now_v7(), 1),
            ("another execution", token, 2),
        ] {
            assert_eq!(
                complete_drain_restore(pool, id, token, target, generation, Some(attempt)).await?,
                DrainRestoreCommit::Lost,
                "{target}: {case}"
            );
            assert_eq!(
                (
                    row_image(pool, id).await?,
                    executions_image(pool, id).await?
                ),
                before,
                "{target}: {case} wrote"
            );
        }
        assert_eq!(
            complete_drain_restore(pool, id, token, target, 1, Some(attempt)).await?,
            DrainRestoreCommit::Landed
        );
        let landed: (String, String, Option<Uuid>, bool) = sqlx::query_as(sql!(
            "SELECT lifecycle, placement, restore_lease_token, restore_lease_expires_at IS NULL
               FROM sandboxes
              WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            landed,
            (target.as_str().to_owned(), "on_node".to_owned(), None, true),
            "{target}"
        );
        assert_eq!(active_attempt(pool, id).await?, (None, None), "{target}");
    }
    Ok(())
}

/// The node's activation receipt can land the row before the restore's own
/// commit runs. The commit then reads the row in its target on the restore's
/// execution as landed, and writes nothing more.
#[tokio::test(flavor = "current_thread")]
async fn a_receipt_that_landed_first_commits_as_already_landed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let token = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    let id = restored(pool, node, STARTED, token, attempt).await?;

    assert_eq!(receipt(pool, node, id, attempt, STARTED).await?, [id]);
    let landed_by_receipt = row_image(pool, id).await?;
    assert_eq!(
        complete_drain_restore(pool, id, token, STARTED, 1, Some(attempt)).await?,
        DrainRestoreCommit::AlreadyLanded
    );
    assert_eq!(
        row_image(pool, id).await?,
        landed_by_receipt,
        "the commit wrote the row"
    );
    assert_eq!(active_attempt(pool, id).await?, (None, None));
    Ok(())
}

/// A `paused` target wakes its VM `started` and pauses it before the commit,
/// so the receipt may land the row `started` first: the commit lands that
/// row `paused`, on the restore's running execution only.
#[tokio::test(flavor = "current_thread")]
async fn a_paused_target_lands_paused_from_the_started_its_receipt_landed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for running in [true, false] {
        let node = NodeId::new();
        let token = Uuid::now_v7();
        let attempt = Uuid::now_v7();
        let id = restored(pool, node, STARTED, token, attempt).await?;
        assert_eq!(receipt(pool, node, id, attempt, STARTED).await?, [id]);
        if !running {
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions SET state = 'terminating'
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(id)
            .execute(pool)
            .await?;
        }
        let commit = complete_drain_restore(pool, id, token, PAUSED, 1, Some(attempt)).await?;
        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(id)
                .fetch_one(pool)
                .await?;
        if running {
            assert_eq!(
                (commit, lifecycle.as_str()),
                (DrainRestoreCommit::Landed, "paused")
            );
        } else {
            assert_eq!(
                (commit, lifecycle.as_str()),
                (DrainRestoreCommit::Lost, "started")
            );
        }
    }
    Ok(())
}

/// The same race with the receipt's transaction still open when the commit
/// arrives: the commit waits on the row lock, then reads the receipt's
/// landing, not the `restoring` row it would have seen without the lock.
#[tokio::test(flavor = "current_thread")]
async fn a_receipt_committing_while_the_commit_waits_reads_as_already_landed() -> TestResult {
    // The receipt's transaction, the waiting commit and the lock probe each
    // hold a connection.
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let pool = db.pool.clone();
    let node = NodeId::new();
    let token = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    let id = restored(&pool, node, STARTED, token, attempt).await?;

    let mut receipt_tx = pool.begin().await?;
    assert_eq!(
        receipt(&mut *receipt_tx, node, id, attempt, STARTED).await?,
        [id]
    );
    let commit = tokio::spawn({
        let pool = pool.clone();
        async move { complete_drain_restore(&pool, id, token, STARTED, 1, Some(attempt)).await }
    });
    wait_for_lock_waiter(&pool).await?;
    receipt_tx.commit().await?;
    assert_eq!(commit.await??, DrainRestoreCommit::AlreadyLanded);
    Ok(())
}

/// A row in the target that isn't this restore's landing is lost and left
/// exactly as it was: a user restore on a new execution, a user's pause, an
/// execution that stopped running, the v1 marker, another lease's restore, a
/// delete. The control row, landed by the receipt, reads as landed.
#[tokio::test(flavor = "current_thread")]
async fn a_row_moved_on_is_lost_unchanged() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for case in [
        "control",
        "user restore on a new execution",
        "user pause",
        "execution not running",
        "v1 marker",
        "another lease's restore",
        "delete",
    ] {
        let node = NodeId::new();
        let token = Uuid::now_v7();
        let attempt = Uuid::now_v7();
        let id = restored(pool, node, STARTED, token, attempt).await?;
        if case != "another lease's restore" && case != "delete" {
            assert_eq!(
                receipt(pool, node, id, attempt, STARTED).await?,
                [id],
                "{case}"
            );
        }
        let set = |statement: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query(sqlx::AssertSqlSafe(statement))
                    .bind(id)
                    .execute(&pool)
                    .await
            }
        };
        match case {
            "control" => {}
            "user restore on a new execution" => execution(pool, id, 2, "running", None).await?,
            "user pause" => {
                set("UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1").await?;
            }
            "execution not running" => {
                set("UPDATE sandbox_k8s_executions SET state = 'terminating'
                      WHERE sandbox_id = $1 AND active")
                .await?;
            }
            "v1 marker" => {
                set("UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1")
                    .await?;
            }
            "another lease's restore" => {
                set("UPDATE sandboxes
                        SET restore_lease_token = gen_random_uuid(),
                            restore_lease_expires_at = NOW() + INTERVAL '1 minute'
                      WHERE id = $1")
                .await?;
            }
            "delete" => {
                let mut tx = pool.begin().await?;
                mark_deleting(&mut tx, id).await?;
                tx.commit().await?;
            }
            other => unreachable!("{other}"),
        }
        let before = (
            row_image(pool, id).await?,
            executions_image(pool, id).await?,
        );
        let commit = complete_drain_restore(pool, id, token, STARTED, 1, Some(attempt)).await?;
        if case == "control" {
            assert_eq!(commit, DrainRestoreCommit::AlreadyLanded, "{case}");
        } else {
            assert_eq!(commit, DrainRestoreCommit::Lost, "{case}");
            assert_eq!(
                (
                    row_image(pool, id).await?,
                    executions_image(pool, id).await?
                ),
                before,
                "{case}: wrote"
            );
        }
    }
    Ok(())
}

/// The commit releases the restore's attempt only with a landed row: a lost
/// commit leaves it, so the attempt still fences the execution the
/// operation will retry.
#[tokio::test(flavor = "current_thread")]
async fn a_lost_commit_keeps_the_attempt() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let token = Uuid::now_v7();
    let attempt = Uuid::now_v7();
    let id = restored(pool, NodeId::new(), STARTED, token, attempt).await?;
    assert_eq!(
        complete_drain_restore(pool, id, Uuid::now_v7(), STARTED, 1, Some(attempt)).await?,
        DrainRestoreCommit::Lost
    );
    assert_eq!(
        active_attempt(pool, id).await?,
        (Some(attempt), Some("started".to_owned()))
    );
    Ok(())
}

/// The drained node's Kubernetes name in the stopped fixtures.
const NODE_NAME: &str = "worker-drained.synthetic.internal";

/// A stopped sandbox at home on `node`: stopped at `stopped_at`, its
/// pre-stop execution on [`NODE_NAME`] created before that, and a ready,
/// stored disk of `artifact` published at `published_at`.
async fn stopped_at_home(
    pool: &PgPool,
    node: NodeId,
    artifact: Uuid,
    published_at: &str,
) -> TestResult<SandboxId> {
    let id = SandboxId::from_uuid(insert_sandbox(pool, None, "stopped").await?);
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, placement = 'on_node',
                lifecycle_changed_at = $3::TIMESTAMPTZ
          WHERE id = $1"
    ))
    .bind(id)
    .bind(node)
    .bind(S_TAKEN)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, k8s_node_name, created_at)
         VALUES ($1, 1, $2, 'space', 'running', $3, $4::TIMESTAMPTZ)"
    ))
    .bind(id)
    .bind(format!("pod-{id}-g1"))
    .bind(NODE_NAME)
    .bind(OLDER)
    .execute(pool)
    .await?;
    let disk = snapshot(pool, id, Some(artifact), "ready", published_at, false).await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET type = 'disk', artifact_kind = 'disk', artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(disk)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn evacuate(
    pool: &PgPool,
    id: SandboxId,
    node: NodeId,
    artifact: Uuid,
) -> TestResult<V2StoppedEvacuation> {
    let mut tx = pool.begin().await?;
    let done = evacuate_v2_stopped(&mut tx, id, node, NODE_NAME, artifact).await?;
    tx.commit().await?;
    Ok(done)
}

/// A stopped sandbox the node reported evacuated is pinned to the reported
/// disk and released from its home, once, and answers released again after.
/// A report it can't take, from another stop or with another pin, changes
/// nothing and is refused, never settled; a row that left the node moved on.
/// A v1 marker an undrain abandoned is taken over.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_sandbox_is_released_only_on_its_homes_report_of_this_stops_disk() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let artifact = Uuid::now_v7();

    let id = stopped_at_home(pool, node, artifact, NEWER).await?;
    assert_eq!(
        evacuate(pool, id, node, artifact).await?,
        V2StoppedEvacuation::Released
    );
    let released: (
        String,
        Option<NodeId>,
        String,
        Option<Uuid>,
        Option<String>,
        bool,
    ) = sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.active_node_id, s.placement, s.current_local_artifact_id,
                    s.current_local_artifact_kind,
                    EXISTS (SELECT 1 FROM sandbox_k8s_executions e
                             WHERE e.sandbox_id = s.id AND e.active)
               FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        released,
        (
            "stopped".to_owned(),
            None,
            "unplaced".to_owned(),
            Some(artifact),
            Some("disk".to_owned()),
            false
        )
    );
    let after = row_image(pool, id).await?;
    assert_eq!(
        evacuate(pool, id, node, artifact).await?,
        V2StoppedEvacuation::Released
    );
    assert_eq!(row_image(pool, id).await?, after, "released twice");

    type Setup = fn(SandboxId, NodeId) -> String;
    let cases: [(&str, &str, Setup, bool); 7] = [
        (
            "disk from before the stop",
            OLDER,
            |_, _| String::new(),
            true,
        ),
        (
            "pins another disk",
            NEWER,
            |id, _| {
                format!(
                    "UPDATE sandboxes SET current_local_artifact_id = gen_random_uuid(), current_local_artifact_kind = 'disk', current_local_artifact_origin = 'stop' WHERE id = '{id}'"
                )
            },
            true,
        ),
        (
            "execution since the stop",
            NEWER,
            |id, _| {
                format!(
                    "UPDATE sandbox_k8s_executions SET created_at = NOW() WHERE sandbox_id = '{id}'"
                )
            },
            true,
        ),
        (
            "execution on another node",
            NEWER,
            |id, _| {
                format!(
                    "UPDATE sandbox_k8s_executions SET k8s_node_name = 'elsewhere' WHERE sandbox_id = '{id}'"
                )
            },
            true,
        ),
        (
            "disk not ready",
            NEWER,
            |id, _| format!("UPDATE snapshots SET state = 'creating' WHERE sandbox_id = '{id}'"),
            true,
        ),
        (
            "home elsewhere",
            NEWER,
            |id, _| {
                format!("UPDATE sandboxes SET active_node_id = gen_random_uuid() WHERE id = '{id}'")
            },
            false,
        ),
        (
            "started again",
            NEWER,
            |id, _| format!("UPDATE sandboxes SET lifecycle = 'started' WHERE id = '{id}'"),
            false,
        ),
    ];
    for (case, published_at, setup, refused) in cases {
        let id = stopped_at_home(pool, node, artifact, published_at).await?;
        let statement = setup(id, node);
        if !statement.is_empty() {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(pool)
                .await?;
        }
        let before = (
            row_image(pool, id).await?,
            executions_image(pool, id).await?,
        );
        let done = evacuate(pool, id, node, artifact).await?;
        assert_eq!(
            matches!(done, V2StoppedEvacuation::Refused(_)),
            refused,
            "{case}: {done:?}"
        );
        assert_eq!(
            matches!(done, V2StoppedEvacuation::MovedOn),
            !refused,
            "{case}: {done:?}"
        );
        assert_eq!(
            (
                row_image(pool, id).await?,
                executions_image(pool, id).await?
            ),
            before,
            "{case}: wrote"
        );
    }
    assert_eq!(
        evacuate(pool, SandboxId::new(), node, artifact).await?,
        V2StoppedEvacuation::MovedOn
    );

    // A v1 stopped evacuation an undrain abandoned before its claim is taken
    // over: released, its marker cleared.
    let abandoned = stopped_at_home(pool, node, artifact, NEWER).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'stopped' WHERE id = $1"
    ))
    .bind(abandoned)
    .execute(pool)
    .await?;
    assert_eq!(
        evacuate(pool, abandoned, node, artifact).await?,
        V2StoppedEvacuation::Released
    );
    let marker: Option<String> = sqlx::query_scalar(sql!(
        "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(abandoned)
    .fetch_one(pool)
    .await?;
    assert_eq!(marker, None);
    Ok(())
}

/// A stopped sandbox a re-placement landed on the node, still pinning the
/// disk it was materialized from, is released on that disk's durability,
/// though the disk was published before the landing; unpinned, the same
/// disk is refused as another stop's.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placed_stopped_sandbox_is_released_on_its_pinned_disk() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let artifact = Uuid::now_v7();
    for (pinned, expected) in [
        (true, V2StoppedEvacuation::Released),
        (
            false,
            V2StoppedEvacuation::Refused("the disk isn't a ready, stored disk of this stop"),
        ),
    ] {
        let id = stopped_at_home(pool, node, artifact, OLDER).await?;
        if pinned {
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET current_local_artifact_id = $2, current_local_artifact_kind = 'disk',
                        current_local_artifact_origin = 'stop'
                  WHERE id = $1"
            ))
            .bind(id)
            .bind(artifact)
            .execute(pool)
            .await?;
        }
        assert_eq!(
            evacuate(pool, id, node, artifact).await?,
            expected,
            "pinned={pinned}"
        );
    }
    Ok(())
}

/// A start materializes only an unplaced stopped row whose pinned disk is
/// durable and that runs nowhere: the row a drain's release leaves is marked
/// once, and taken back; a placed row, one with an active execution, or one
/// whose disk isn't ready is not.
#[tokio::test(flavor = "current_thread")]
async fn only_an_unplaced_stopped_row_with_a_durable_pin_is_marked_for_materialization()
-> TestResult {
    use crate::drain_restore::{
        mark_unplaced_stopped_for_materialization, unmark_unplaced_stopped,
    };
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    let released = |pool: PgPool| async move {
        let artifact = Uuid::now_v7();
        let id = stopped_at_home(&pool, node, artifact, NEWER).await?;
        assert_eq!(
            evacuate(&pool, id, node, artifact).await?,
            V2StoppedEvacuation::Released
        );
        TestResult::Ok(id)
    };
    let marker = |pool: PgPool, id: SandboxId| async move {
        let marker: Option<String> = sqlx::query_scalar(sql!(
            "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(&pool)
        .await?;
        TestResult::Ok(marker)
    };

    let id = released(pool.clone()).await?;
    assert!(mark_unplaced_stopped_for_materialization(pool, id).await?);
    assert_eq!(marker(pool.clone(), id).await?.as_deref(), Some("stopped"));
    assert!(
        !mark_unplaced_stopped_for_materialization(pool, id).await?,
        "marked twice"
    );
    assert!(unmark_unplaced_stopped(pool, id).await?);
    assert_eq!(marker(pool.clone(), id).await?, None);

    for (case, statement) in [
        (
            "placed",
            "UPDATE sandboxes SET active_node_id = gen_random_uuid() WHERE id = $1",
        ),
        (
            "an active execution",
            "UPDATE sandbox_k8s_executions SET active = TRUE WHERE sandbox_id = $1",
        ),
        (
            "disk not ready",
            "UPDATE snapshots SET state = 'creating' WHERE sandbox_id = $1",
        ),
    ] {
        let id = released(pool.clone()).await?;
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .bind(id)
            .execute(pool)
            .await?;
        assert!(
            !mark_unplaced_stopped_for_materialization(pool, id).await?,
            "{case}"
        );
        assert_eq!(marker(pool.clone(), id).await?, None, "{case}");
    }
    Ok(())
}
