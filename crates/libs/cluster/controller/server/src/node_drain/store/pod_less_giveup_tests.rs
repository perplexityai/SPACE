//! The pod-less give-up against the real Postgres fixture: the evacuation
//! suspend's classification and lifecycle flap, the guard pre-check, and the
//! revert transaction. The authoritative pod read is the
//! [`PodAbsenceProber`] seam, tested here with a scripted prober.

use std::collections::HashMap;
use std::time::Duration;

use pplx_space_api::controllers::substrate::kubernetes::GenerationTeardownEvidence;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeId, PositiveDuration, SandboxId, SandboxLifecycle,
};
use pplx_space_db_test_fixtures::{pg_test_db, pg_test_db_with_max_connections};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::pod_less_giveup::{
    GenerationTeardownObserver, POD_READ_CONCURRENCY, PodAbsenceProber, PodLessCandidate, PodRef,
    is_deadlock, node_vouches, pod_refs, pods_proven_absent, precheck, recover_pod_less_sandbox_tx,
};
use super::*;
use crate::lifecycle_writer_test_support::{
    SandboxSeed, bounded, insert_node, insert_sandbox, insert_snapshot, pin_local_artifact,
    pod_name, single_backend_pool, wait_until_blocked,
};
use crate::orphaned_sandboxes::PodPresenceStatus;

const STALE: Duration = Duration::from_secs(600);

/// An execution row the pod-less give-up reads.
#[derive(Debug, Clone, Copy)]
struct Execution {
    generation: i64,
    state: &'static str,
    active: bool,
    /// Whether the pod watcher persisted the pod's UID.
    pod_uid: bool,
    /// Seconds since `updated_at` last advanced.
    idle_secs: i64,
}

impl Execution {
    /// The shape that wedged a drain: a started-recovery generation that
    /// never bound a pod and stopped advancing an hour ago.
    const STALLED_RECOVERY: Self = Self {
        generation: 2,
        state: "recovering",
        active: true,
        pod_uid: false,
        idle_secs: 3600,
    };
    /// The generation before it, retired when recovery replaced it.
    const REPLACED: Self = Self {
        generation: 1,
        state: "replaced",
        active: false,
        pod_uid: true,
        idle_secs: 7200,
    };
}

async fn insert_pod_less_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    row: Execution,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid,
             state, active, created_at, updated_at)
         VALUES ($1, $2, $3, 'sandboxes', $4, $5, $6,
                 NOW() - make_interval(secs => $7::BIGINT + 60),
                 NOW() - make_interval(secs => $7::BIGINT))"
    ))
    .bind(sandbox_id)
    .bind(row.generation)
    .bind(pod_name(sandbox_id, row.generation))
    .bind(row.pod_uid.then(|| format!("uid-{}", row.generation)))
    .bind(row.state)
    .bind(row.active)
    .bind(row.idle_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// A `started` resident of a forced-draining node with `executions`.
async fn resident_with(pool: &PgPool, executions: &[Execution]) -> TestResult<(NodeId, SandboxId)> {
    let node_id = insert_node(pool, Some("draining"), Some("forced")).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new("started", "k8s").on_node(node_id),
    )
    .await?;
    for execution in executions {
        insert_pod_less_execution(pool, sandbox_id, *execution).await?;
    }
    Ok((node_id, sandbox_id))
}

/// The resident with the two execution rows that reproduce the livelock.
async fn wedged_resident(pool: &PgPool) -> TestResult<(NodeId, SandboxId)> {
    resident_with(pool, &[Execution::REPLACED, Execution::STALLED_RECOVERY]).await
}

fn candidate(sandbox_id: SandboxId, source_node_id: NodeId) -> PodLessCandidate {
    PodLessCandidate {
        sandbox_id,
        source_node_id,
        pre_lifecycle: SandboxLifecycle::Started,
        stale: PositiveDuration::new_panic(STALE),
    }
}

/// The pre-check, then the transaction against the generation it saw, with
/// the node reporting that generation deleted locally: the sink's path minus
/// the pod reads and the node read.
async fn give_up(pool: &PgPool, candidate: PodLessCandidate) -> TestResult<PodLessRecovery> {
    give_up_with(pool, candidate, Ok(GenerationTeardownEvidence::Deleted)).await
}

/// A node that gives every generation the same answer.
struct Uniform(Result<GenerationTeardownEvidence, SubstrateError>);

impl GenerationTeardownObserver for Uniform {
    async fn observe(
        &self,
        _generation: ExecutionGeneration,
    ) -> Result<GenerationTeardownEvidence, SubstrateError> {
        self.0.clone()
    }
}

/// [`give_up`] with `evidence` as the node's answer for every generation.
async fn give_up_with(
    pool: &PgPool,
    candidate: PodLessCandidate,
    evidence: Result<GenerationTeardownEvidence, SubstrateError>,
) -> TestResult<PodLessRecovery> {
    give_up_asking(pool, candidate, &Uniform(evidence)).await
}

/// [`give_up`] with `node` answering for each generation.
async fn give_up_asking<O: GenerationTeardownObserver>(
    pool: &PgPool,
    candidate: PodLessCandidate,
    node: &O,
) -> TestResult<PodLessRecovery> {
    let generation = match precheck(pool, candidate).await? {
        Ok(generation) => generation,
        Err(hold) => return Ok(PodLessRecovery::NotEligible(hold)),
    };
    Ok(match node_vouches(node, generation).await {
        Ok(()) => recover_pod_less_sandbox_tx(pool, candidate, generation).await?,
        Err(hold) => PodLessRecovery::NotEligible(hold),
    })
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct SandboxRow {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
}

async fn sandbox_row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<SandboxRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, active_node_id, target_node_id
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// `(generation, state, active)` for every execution row, oldest first.
async fn executions(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Vec<(i64, String, bool)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT generation, state, active
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?)
}

fn unplaced(lifecycle: &str) -> SandboxRow {
    SandboxRow {
        lifecycle: lifecycle.to_owned(),
        placement: "unplaced".to_owned(),
        active_node_id: None,
        target_node_id: None,
    }
}

fn resident(node_id: NodeId) -> SandboxRow {
    SandboxRow {
        lifecycle: "started".to_owned(),
        placement: "on_node".to_owned(),
        active_node_id: Some(node_id),
        target_node_id: Some(node_id),
    }
}

async fn insert_observed(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    status: &str,
    pid: Option<i32>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, pid)
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(status)
    .bind(pid)
    .execute(pool)
    .await?;
    Ok(())
}

async fn observed_rows(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

// -----------------------------------------------------------------
// The livelock
// -----------------------------------------------------------------

/// The reproduction. A suspend that finds no pod used to come back as a
/// generic retryable failure, and the claim and revert around it stamped
/// `lifecycle_changed_at` every time, so no repair loop keyed on that clock
/// could see the row age. The suspend now classifies as the terminal
/// [`EvacuateError::SandboxNoPodOnNode`]; the flap itself is unchanged, which
/// is why the give-up measures progress on the execution rows instead.
#[tokio::test]
async fn pod_less_suspend_classifies_terminal_and_the_flap_resets_the_lifecycle_clock() -> TestResult
{
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    let sink = SubstrateSink::with_suspend_execution(pool.clone(), |_| async {
        Err(SubstrateError::SandboxNoPod {
            message: "sandbox pod has no Kubernetes UID for suspend retirement".to_owned(),
        })
    });
    let lifecycle_age = || async {
        sqlx::query_scalar::<_, f64>(sql!(
            "SELECT EXTRACT(EPOCH FROM NOW() - lifecycle_changed_at)::FLOAT8
               FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&pool)
        .await
    };
    assert!(lifecycle_age().await? >= 3600.0);

    let result = sink
        .evacuate_sandbox(sandbox_id, node_id, SandboxLifecycle::Started)
        .await;

    assert!(
        matches!(result, Err(EvacuateError::SandboxNoPodOnNode(_))),
        "{result:?}"
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    assert!(
        lifecycle_age().await? < 60.0,
        "the claim and revert restamp lifecycle_changed_at"
    );
    // The give-up's clock is untouched by the flap, and the snapshot the row
    // was suspended to still restores it.
    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::RevertedToSuspended
    );
    Ok(())
}

// -----------------------------------------------------------------
// Revert or park
// -----------------------------------------------------------------

#[tokio::test]
async fn recoverable_resident_reverts_to_suspended_off_the_node() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::RevertedToSuspended
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, unplaced("suspended"));
    assert_eq!(
        executions(&pool, sandbox_id).await?,
        vec![
            (1, "replaced".to_owned(), false),
            (2, "suspended".to_owned(), false),
        ]
    );
    Ok(())
}

/// The give-up never fails a row. With nothing to restore from, here only a
/// failed upload, the resident stays and parks the drain in the precheck,
/// before the node is asked. Its executions are left as they were.
#[tokio::test]
async fn unrecoverable_resident_parks() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "failed", -60).await?;
    let before = executions(&pool, sandbox_id).await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::NotEligible(PodLessHold::NoRestoreSource)
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    assert_eq!(executions(&pool, sandbox_id).await?, before);
    Ok(())
}

/// The give-up never fails a row. With no restore source, a resident parks
/// in the precheck, before the node is asked, whether the node publishes no
/// record for it or the `failed`, pid-less record of a kept row, and any
/// record is kept. (What a node that lost the row answers is covered by
/// `an_older_ready_snapshot_does_not_release_a_surviving_rootfs`.)
#[tokio::test]
async fn a_rootfs_that_may_survive_parks_the_row() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    for record in [None, Some("failed")] {
        let (node_id, sandbox_id) = wedged_resident(&pool).await?;
        if let Some(status) = record {
            insert_observed(&pool, sandbox_id, node_id, status, None).await?;
        }

        assert_eq!(
            give_up(&pool, candidate(sandbox_id, node_id)).await?,
            PodLessRecovery::NotEligible(PodLessHold::NoRestoreSource),
            "{record:?}"
        );
        assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
        assert_eq!(
            observed_rows(&pool, sandbox_id).await?,
            i64::from(record.is_some()),
            "{record:?}"
        );
    }
    Ok(())
}

/// The surviving-rootfs case with a restore source: a ready snapshot older
/// than the lifecycle transition, no marker, no observed row. The snapshot is
/// database evidence only. A rootfs that outlived its node-local row may hold
/// writes the snapshot doesn't, and that node has no row and no delete
/// receipt to vouch with, so it answers pending. Neither that, a superseded
/// generation, nor a node that can't be read releases the resident.
#[tokio::test]
async fn an_older_ready_snapshot_does_not_release_a_surviving_rootfs() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let answers: [(
        Result<GenerationTeardownEvidence, SubstrateError>,
        PodLessHold,
    ); 3] = [
        (
            Ok(GenerationTeardownEvidence::Pending),
            PodLessHold::NodePending,
        ),
        (
            Ok(GenerationTeardownEvidence::Superseded),
            PodLessHold::NodeSuperseded,
        ),
        (
            Err(SubstrateError::Unavailable {
                message: "node-api unreachable".to_owned(),
            }),
            PodLessHold::NodeUnreadable,
        ),
    ];
    for (answer, reason) in answers {
        let (node_id, sandbox_id) = wedged_resident(&pool).await?;
        insert_snapshot(&pool, sandbox_id, "ready", -60).await?;

        assert_eq!(
            give_up_with(&pool, candidate(sandbox_id, node_id), answer).await?,
            PodLessRecovery::NotEligible(reason)
        );
        assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
        assert_eq!(observed_rows(&pool, sandbox_id).await?, 0);
    }
    Ok(())
}

/// The deleted path restores whatever the timestamp order selects, so that
/// choice must be unambiguous. A newer full snapshot still being deleted names
/// bytes the ready-only eligibility never compares against, and two ready
/// snapshots at the same timestamp leave the restore to pick either. Both
/// hold the revert; a lone ready snapshot is the control.
#[tokio::test]
async fn an_ambiguous_restore_source_holds_the_revert() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    for (second, offset_secs, expected) in [
        (
            Some("deleting"),
            -30,
            PodLessRecovery::NotEligible(PodLessHold::HeldForDataSafety),
        ),
        (
            Some("ready"),
            -60,
            PodLessRecovery::NotEligible(PodLessHold::HeldForDataSafety),
        ),
        (None, 0, PodLessRecovery::RevertedToSuspended),
    ] {
        let (node_id, sandbox_id) = wedged_resident(&pool).await?;
        insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
        if let Some(state) = second {
            insert_snapshot(&pool, sandbox_id, state, offset_secs).await?;
        }

        assert_eq!(
            give_up(&pool, candidate(sandbox_id, node_id)).await?,
            expected,
            "{second:?}"
        );
    }
    Ok(())
}

/// No active execution at all: the suspend's other no-pod case. The retired
/// rows still name every pod the sandbox had, so absence can be proven.
#[tokio::test]
async fn resident_without_an_active_execution_is_covered() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = resident_with(&pool, &[Execution::REPLACED]).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::RevertedToSuspended
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, unplaced("suspended"));
    Ok(())
}

// -----------------------------------------------------------------
// Guards
// -----------------------------------------------------------------

/// Each case seeds the wedged resident, applies one disqualifying write, and
/// expects the give-up to leave the row resident with that guard's reason.
#[tokio::test]
async fn each_guard_leaves_the_row_resident() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let cases: [(PodLessHold, &str); 6] = [
        // Started recovery's claim, or a pod-metadata write, advanced it.
        (
            PodLessHold::RecentProgress,
            "UPDATE sandbox_k8s_executions SET updated_at = NOW()
              WHERE sandbox_id = $1 AND active",
        ),
        // The pod watcher bound a pod.
        (
            PodLessHold::PodBound,
            "UPDATE sandbox_k8s_executions SET pod_uid = 'uid-live'
              WHERE sandbox_id = $1 AND active",
        ),
        (
            PodLessHold::PodBound,
            "UPDATE sandbox_k8s_executions SET pod_ip = '10.1.2.3'::inet
              WHERE sandbox_id = $1 AND generation = 1",
        ),
        // A node still publishes a live VM, whose pod may have been
        // force-deleted under it.
        (
            PodLessHold::VmObserved,
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, pid)
             SELECT id, active_node_id, 'started', 4242 FROM sandboxes WHERE id = $1",
        ),
        // A `failed` record that still carries a pid may be a live process.
        (
            PodLessHold::VmObserved,
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, pid)
             SELECT id, active_node_id, 'failed', 4242 FROM sandboxes WHERE id = $1",
        ),
        // A user moved the row since the suspend failed.
        (
            PodLessHold::RowDrifted,
            "UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1",
        ),
    ];
    for (reason, disqualify) in cases {
        let (node_id, sandbox_id) = wedged_resident(&pool).await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(disqualify)))
            .bind(sandbox_id)
            .execute(&pool)
            .await?;
        let before = sandbox_row(&pool, sandbox_id).await?;

        assert_eq!(
            give_up(&pool, candidate(sandbox_id, node_id)).await?,
            PodLessRecovery::NotEligible(reason),
            "{disqualify}"
        );
        assert_eq!(
            sandbox_row(&pool, sandbox_id).await?,
            before,
            "{disqualify}"
        );
    }
    Ok(())
}

/// Gentle mode promises not to touch residents, and an undrain ends the
/// drain; either between the failed suspend and the give-up refuses it.
#[tokio::test]
async fn give_up_requires_the_node_to_still_be_force_draining() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    for (phase, mode) in [(Some("draining"), Some("gentle")), (None, None)] {
        let (node_id, sandbox_id) = wedged_resident(&pool).await?;
        sqlx::query(sql!(
            "UPDATE nodes SET drain_phase = $2, drain_mode = $3 WHERE id = $1"
        ))
        .bind(node_id)
        .bind(phase)
        .bind(mode)
        .execute(&pool)
        .await?;

        assert_eq!(
            give_up(&pool, candidate(sandbox_id, node_id)).await?,
            PodLessRecovery::NotEligible(PodLessHold::RowDrifted),
            "{phase:?}/{mode:?}"
        );
        assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    }
    Ok(())
}

/// An `:undrain` that commits while the give-up is mid-transaction is seen:
/// the transaction waits on the node row, then re-reads the guards against
/// the committed undrain and refuses. A plain read of the node row wouldn't
/// wait, and would authorize the write on the drain state it saw.
#[tokio::test]
async fn undrain_racing_the_transaction_is_seen() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let (node_id, sandbox_id) = wedged_resident(pool).await?;
    insert_snapshot(pool, sandbox_id, "ready", -60).await?;
    let candidate = candidate(sandbox_id, node_id);
    let Ok(generation) = precheck(pool, candidate).await? else {
        panic!("the wedged resident passes the pre-check");
    };

    let mut undrain = pool.begin().await?;
    let undrain_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *undrain)
        .await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = NULL, drain_mode = NULL WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&mut *undrain)
    .await?;

    let (give_up_pool, give_up_pid) = single_backend_pool(&db).await?;
    let give_up = tokio::spawn(async move {
        recover_pod_less_sandbox_tx(&give_up_pool, candidate, generation).await
    });
    bounded(wait_until_blocked(pool, give_up_pid, undrain_pid)).await?;
    undrain.commit().await?;

    assert_eq!(
        bounded(async { Ok(give_up.await??) }).await?,
        PodLessRecovery::NotEligible(PodLessHold::RowDrifted)
    );
    assert_eq!(sandbox_row(pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// While the transaction waits on the sandbox lock it holds the node row
/// shared, so the wait is bounded; past the bound it backs out as contention
/// and the next tick retries.
#[tokio::test]
async fn a_held_sandbox_lock_backs_out_as_contention() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let (node_id, sandbox_id) = wedged_resident(pool).await?;
    insert_snapshot(pool, sandbox_id, "ready", -60).await?;
    let candidate = candidate(sandbox_id, node_id);
    let Ok(generation) = precheck(pool, candidate).await? else {
        panic!("the wedged resident passes the pre-check");
    };
    let mut holder = pool.begin().await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .execute(&mut *holder)
        .await?;

    assert_eq!(
        bounded(async { Ok(recover_pod_less_sandbox_tx(pool, candidate, generation).await?,) })
            .await?,
        PodLessRecovery::NotEligible(PodLessHold::LockContended)
    );
    holder.rollback().await?;
    assert_eq!(sandbox_row(pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// Both contention codes back the give-up out as `lock_contended`; any other
/// database error still surfaces as a failure.
#[tokio::test]
async fn contention_codes_are_classified() -> TestResult {
    let db = pg_test_db().await?;
    let raise = |code: &'static str| {
        let pool = db.pool.clone();
        async move {
            sqlx::query(sqlx::AssertSqlSafe(sql!(
                format!("DO $$ BEGIN RAISE EXCEPTION 'synthetic' USING ERRCODE = '{code}'; END $$")
                    .as_str()
            )))
            .execute(&pool)
            .await
            .expect_err("the statement raises")
        }
    };
    let deadlock = raise("40P01").await;
    assert!(is_deadlock(&deadlock) && !is_lock_timeout(&deadlock));
    let timeout = raise("55P03").await;
    assert!(is_lock_timeout(&timeout) && !is_deadlock(&timeout));
    let other = raise("23505").await;
    assert!(!is_lock_timeout(&other) && !is_deadlock(&other));
    Ok(())
}

/// `k8s_node_name` survives re-materialization and a recovery re-claim of an
/// old generation, so a node name left on a pod-less execution isn't pod
/// evidence and doesn't park the row.
#[tokio::test]
async fn stale_node_name_does_not_block() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = 'worker-a'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::RevertedToSuspended
    );
    Ok(())
}

/// The revert holds a non-durable marker. Here an older snapshot would pass
/// the activity-anchored eligibility, but the marked artifact may hold newer
/// bytes than it; reverting would clear the marker and let the node terminate
/// with them.
#[tokio::test]
async fn local_only_marker_holds_the_revert_arm() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    pin_local_artifact(&pool, sandbox_id).await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::NotEligible(PodLessHold::HeldForDataSafety)
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// A row the revert can't take is refused before the pod reads, so a resident
/// parked for good costs one guard query per tick, not an apiserver read per
/// generation. The suspend-only sink panics on any Kubernetes access, so the
/// clean refusal is the proof.
#[tokio::test]
async fn a_held_resident_takes_no_pod_read() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let sink = SubstrateSink::with_suspend_execution(pool.clone(), |_| async {
        unreachable!("the give-up doesn't suspend")
    });
    let recover = |sandbox_id, node_id| {
        sink.recover_pod_less_sandbox(
            sandbox_id,
            node_id,
            SandboxLifecycle::Started,
            PositiveDuration::new_panic(STALE),
        )
    };

    // Nothing to restore from.
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    assert_eq!(
        recover(sandbox_id, node_id).await?,
        PodLessRecovery::NotEligible(PodLessHold::NoRestoreSource)
    );
    // Restorable, but held: the marked artifact has no durable copy.
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    pin_local_artifact(&pool, sandbox_id).await?;
    assert_eq!(
        recover(sandbox_id, node_id).await?,
        PodLessRecovery::NotEligible(PodLessHold::HeldForDataSafety)
    );
    Ok(())
}

/// A suspend upload in flight may be newer than the durable snapshot the
/// revert would restore, so the revert waits for it.
#[tokio::test]
async fn in_flight_upload_holds_the_revert_arm() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    insert_snapshot(&pool, sandbox_id, "creating", 0).await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::NotEligible(PodLessHold::HeldForDataSafety)
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// An upload of a newer artifact than the marked one is still in flight: the
/// marker names an older durable artifact, and a later suspend began
/// uploading another before it could move the marker. The revert waits for
/// it, since it may hold bytes newer than the marked snapshot.
#[tokio::test]
async fn a_newer_artifacts_upload_holds_the_revert() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_marker_snapshot(&pool, sandbox_id).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, type, state, source_node_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'full', 'creating', $3, NOW(), $4, 'full', 'suspend')"
    ))
    .bind(uuid::Uuid::now_v7())
    .bind(sandbox_id)
    .bind(node_id)
    .bind(uuid::Uuid::now_v7())
    .execute(&pool)
    .await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::NotEligible(PodLessHold::HeldForDataSafety)
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// A suspend whose upload failed after the newest ready snapshot may have left
/// newer bytes on the node, and the markerless eligibility only reads ready
/// snapshots, so the revert holds. Two controls revert: a failure older than
/// the ready snapshot says nothing newer exists, and a failed retry of the
/// ready artifact itself carries no newer bytes.
#[tokio::test]
async fn a_newer_failed_upload_holds_the_revert() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    for (failed_offset, retry_of_ready, expected) in [
        (
            0,
            false,
            PodLessRecovery::NotEligible(PodLessHold::HeldForDataSafety),
        ),
        (-120, false, PodLessRecovery::RevertedToSuspended),
        (0, true, PodLessRecovery::RevertedToSuspended),
    ] {
        let (node_id, sandbox_id) = wedged_resident(&pool).await?;
        insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
        insert_snapshot(&pool, sandbox_id, "failed", failed_offset).await?;
        if retry_of_ready {
            sqlx::query(sql!(
                "UPDATE snapshots
                    SET local_artifact_id = $2,
                        artifact_kind = 'full',
                        artifact_origin = 'suspend'
                  WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .bind(uuid::Uuid::now_v7())
            .execute(&pool)
            .await?;
        }

        assert_eq!(
            give_up(&pool, candidate(sandbox_id, node_id)).await?,
            expected,
            "failed upload at {failed_offset}s, retry of ready: {retry_of_ready}"
        );
    }
    Ok(())
}

/// A generation created after the pod reads may have a pod nobody read.
#[tokio::test]
async fn generation_created_after_the_pod_reads_backs_out() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    let candidate = candidate(sandbox_id, node_id);
    let Ok(generation) = precheck(&pool, candidate).await? else {
        panic!("the wedged resident passes the pre-check");
    };
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'replaced'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    insert_pod_less_execution(
        &pool,
        sandbox_id,
        Execution {
            generation: 3,
            ..Execution::STALLED_RECOVERY
        },
    )
    .await?;

    assert_eq!(
        recover_pod_less_sandbox_tx(&pool, candidate, generation).await?,
        PodLessRecovery::NotEligible(PodLessHold::GenerationChanged)
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

// -----------------------------------------------------------------
// Residents with no execution row
// -----------------------------------------------------------------

/// Pin a suspend marker on the row and register its exact artifact as a
/// durable, ready full snapshot taken a minute before the row's last
/// lifecycle change: the marker-backed proof, which survives the drain's
/// flap restamping `lifecycle_changed_at`.
async fn insert_marker_snapshot(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    pin_local_artifact(pool, sandbox_id).await?;
    let snapshot_id = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, type, state, source_node_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin)
         SELECT $1, s.id, 'full', 'ready', s.active_node_id,
                s.lifecycle_changed_at - INTERVAL '1 minute',
                s.current_local_artifact_id, 'full', 'suspend'
           FROM sandboxes s
          WHERE s.id = $2"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://synthetic-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

/// Run one failed evacuation suspend against the row, as every drain tick
/// does, so the test sees `lifecycle_changed_at` after the flap.
async fn flap(pool: &PgPool, sandbox_id: SandboxId, node_id: NodeId) -> TestResult {
    let sink = SubstrateSink::with_suspend_execution(pool.clone(), |_| async {
        Err(SubstrateError::SandboxNoPod {
            message: "sandbox is not assigned to a pod".to_owned(),
        })
    });
    let result = sink
        .evacuate_sandbox(sandbox_id, node_id, SandboxLifecycle::Started)
        .await;
    assert!(
        matches!(result, Err(EvacuateError::SandboxNoPodOnNode(_))),
        "{result:?}"
    );
    Ok(())
}

/// No execution row means no pod name to read and no generation to ask the
/// node about, so even a marker-backed snapshot that survives the drain's
/// flap doesn't release the resident. Runs through the production sink,
/// which reaches neither the apiserver nor a node for it.
#[tokio::test]
async fn resident_without_an_execution_row_parks() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = resident_with(&pool, &[]).await?;
    insert_marker_snapshot(&pool, sandbox_id).await?;
    flap(&pool, sandbox_id, node_id).await?;

    let sink = SubstrateSink::with_suspend_execution(pool.clone(), |_| async {
        unreachable!("the give-up doesn't suspend")
    });
    assert_eq!(
        sink.recover_pod_less_sandbox(
            sandbox_id,
            node_id,
            SandboxLifecycle::Started,
            PositiveDuration::new_panic(STALE),
        )
        .await?,
        PodLessRecovery::NotEligible(PodLessHold::NoExecutionRow)
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// A `failed`, pid-less observed record describes no process. It doesn't
/// block, and the give-up deletes it so a reverted row doesn't keep reading
/// `failed` through the observed-state overlay.
#[tokio::test]
async fn failed_pid_less_observed_record_is_purged_not_blocking() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    insert_observed(&pool, sandbox_id, node_id, "failed", None).await?;

    assert_eq!(
        give_up(&pool, candidate(sandbox_id, node_id)).await?,
        PodLessRecovery::RevertedToSuspended
    );
    assert_eq!(observed_rows(&pool, sandbox_id).await?, 0);
    Ok(())
}

// -----------------------------------------------------------------
// The pod reads
// -----------------------------------------------------------------

struct ScriptedProber(HashMap<String, PodPresenceStatus>);

impl PodAbsenceProber for ScriptedProber {
    async fn probe(&self, pod_namespace: &str, pod_name: &str) -> PodPresenceStatus {
        assert_eq!(pod_namespace, "sandboxes");
        self.0
            .get(pod_name)
            .copied()
            .unwrap_or(PodPresenceStatus::Missing)
    }
}

fn pod(name: &str) -> PodRef {
    PodRef {
        generation: ExecutionGeneration::new(1).expect("a valid generation"),
        pod_namespace: "sandboxes".to_owned(),
        pod_name: name.to_owned(),
    }
}

#[tokio::test]
async fn absence_is_proven_only_when_every_pod_name_reads_absent() {
    let refs = [pod("g1"), pod("g2")];
    for (g1, expected) in [
        (PodPresenceStatus::Missing, Ok(())),
        (PodPresenceStatus::TerminalSucceeded, Ok(())),
        (PodPresenceStatus::TerminalFailed, Ok(())),
        // A lingering older-generation pod blocks as surely as the newest.
        (PodPresenceStatus::Present, Err(PodLessHold::PodPresent)),
        (PodPresenceStatus::Unready, Err(PodLessHold::PodPresent)),
        // An unreadable pod fails safe.
        (PodPresenceStatus::Unknown, Err(PodLessHold::PodUnreadable)),
    ] {
        let prober = ScriptedProber(HashMap::from([("g1".to_owned(), g1)]));
        assert_eq!(pods_proven_absent(&prober, &refs).await, expected, "{g1:?}");
    }
}

/// Counts the reads in flight, holding each one briefly so overlap shows.
#[derive(Default)]
struct OverlapProber {
    in_flight: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

impl PodAbsenceProber for OverlapProber {
    async fn probe(&self, _pod_namespace: &str, _pod_name: &str) -> PodPresenceStatus {
        use std::sync::atomic::Ordering;
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        PodPresenceStatus::Missing
    }
}

/// The reads overlap, so a sandbox with many generations doesn't pay their
/// latencies end to end, and never more than the bound run at once.
#[tokio::test]
async fn pod_reads_overlap_within_the_bound() {
    let refs: Vec<PodRef> = (0..3 * POD_READ_CONCURRENCY)
        .map(|generation| pod(&format!("g{generation}")))
        .collect();
    let prober = OverlapProber::default();

    assert_eq!(pods_proven_absent(&prober, &refs).await, Ok(()));
    let peak = prober.peak.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        peak > 1 && peak <= POD_READ_CONCURRENCY,
        "peak in-flight reads {peak}"
    );
}

#[tokio::test]
async fn an_empty_pod_name_set_never_proves_absence() {
    let prober = ScriptedProber(HashMap::new());
    assert_eq!(
        pods_proven_absent(&prober, &[]).await,
        Err(PodLessHold::NoPodName)
    );
}

/// A stored generation that isn't a valid one fails the database read, so
/// the guard read and the pod refs error out rather than reaching the node,
/// where it would read as a node that couldn't be asked.
#[tokio::test]
async fn an_invalid_stored_generation_fails_the_read() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = resident_with(
        &pool,
        &[Execution {
            generation: 0,
            ..Execution::STALLED_RECOVERY
        }],
    )
    .await?;

    precheck(&pool, candidate(sandbox_id, node_id))
        .await
        .expect_err("the guard read decodes the generation");
    pod_refs(&pool, sandbox_id)
        .await
        .expect_err("the pod refs decode the generation");
    Ok(())
}

#[tokio::test]
async fn pod_refs_name_every_generation_in_its_recorded_namespace() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (_node_id, sandbox_id) = wedged_resident(&pool).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_namespace = 'sandboxes-previous'
          WHERE sandbox_id = $1 AND generation = 1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;

    assert_eq!(
        pod_refs(&pool, sandbox_id).await?,
        vec![
            PodRef {
                generation: ExecutionGeneration::new(2)?,
                pod_namespace: "sandboxes".to_owned(),
                pod_name: pod_name(sandbox_id, 2),
            },
            PodRef {
                generation: ExecutionGeneration::new(1)?,
                pod_namespace: "sandboxes-previous".to_owned(),
                pod_name: pod_name(sandbox_id, 1),
            },
        ]
    );
    Ok(())
}

// -----------------------------------------------------------------
// The node's answer
// -----------------------------------------------------------------

type Answer = Result<GenerationTeardownEvidence, SubstrateError>;

/// Answers per generation, recording what was asked.
struct ScriptedNode {
    answers: HashMap<i64, Answer>,
    asked: std::sync::Mutex<Vec<i64>>,
}

impl ScriptedNode {
    fn new(answers: Vec<(i64, Answer)>) -> Self {
        Self {
            answers: answers.into_iter().collect(),
            asked: std::sync::Mutex::default(),
        }
    }

    fn asked(&self) -> Vec<i64> {
        self.asked.lock().expect("asked lock").clone()
    }
}

impl GenerationTeardownObserver for ScriptedNode {
    async fn observe(
        &self,
        generation: ExecutionGeneration,
    ) -> Result<GenerationTeardownEvidence, SubstrateError> {
        let generation = generation.get();
        self.asked.lock().expect("asked lock").push(generation);
        self.answers
            .get(&generation)
            .cloned()
            .unwrap_or(Ok(GenerationTeardownEvidence::Pending))
    }
}

/// The livelock's own shape parks: a stalled recovery's generation 2 never
/// got a pod, so it never reached the node, no delete receipt can cover it,
/// and the node answers pending for it. The resident stays placed even when
/// the node holds a receipt for generation 1, and only generation 2 is asked.
#[tokio::test]
async fn a_stalled_recovery_parks_and_is_not_released() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = wedged_resident(&pool).await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    let node = ScriptedNode::new(vec![(1, Ok(GenerationTeardownEvidence::Deleted))]);

    assert_eq!(
        give_up_asking(&pool, candidate(sandbox_id, node_id), &node).await?,
        PodLessRecovery::NotEligible(PodLessHold::NodePending)
    );
    assert_eq!(node.asked(), vec![2]);
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// Missing pod history isn't proof a generation never ran. Generation 2 has
/// no recorded pod (it may have run before its history was recorded, or an
/// older writer cleared its UID), generation 3 is the stalled recovery, and
/// the node answers pending for both and deleted only for generation 1. An
/// older generation's receipt doesn't cover a newer one, so the resident
/// stays placed.
#[tokio::test]
async fn an_older_generations_delete_does_not_cover_a_newer_pending_one() -> TestResult {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let (node_id, sandbox_id) = resident_with(
        &pool,
        &[
            Execution::REPLACED,
            Execution {
                generation: 2,
                state: "replaced",
                active: false,
                pod_uid: false,
                idle_secs: 5400,
            },
            Execution {
                generation: 3,
                ..Execution::STALLED_RECOVERY
            },
        ],
    )
    .await?;
    insert_snapshot(&pool, sandbox_id, "ready", -60).await?;
    let node = ScriptedNode::new(vec![(1, Ok(GenerationTeardownEvidence::Deleted))]);

    assert_eq!(
        give_up_asking(&pool, candidate(sandbox_id, node_id), &node).await?,
        PodLessRecovery::NotEligible(PodLessHold::NodePending)
    );
    assert_eq!(sandbox_row(&pool, sandbox_id).await?, resident(node_id));
    Ok(())
}

/// Only a delete covering the newest generation vouches. Every other answer
/// is its hold, and only the newest generation is asked.
#[tokio::test]
async fn only_a_delete_covering_the_newest_generation_vouches() {
    let newest = ExecutionGeneration::new(3).expect("a valid generation");
    let unreachable = || {
        Err(SubstrateError::Unavailable {
            message: "node-api unreachable".to_owned(),
        })
    };
    let cases: Vec<(Answer, Result<(), PodLessHold>)> = vec![
        (Ok(GenerationTeardownEvidence::Deleted), Ok(())),
        (
            Ok(GenerationTeardownEvidence::Pending),
            Err(PodLessHold::NodePending),
        ),
        (
            Ok(GenerationTeardownEvidence::Durable),
            Err(PodLessHold::NodeSuspended),
        ),
        (
            Ok(GenerationTeardownEvidence::Superseded),
            Err(PodLessHold::NodeSuperseded),
        ),
        (
            Ok(GenerationTeardownEvidence::Refused),
            Err(PodLessHold::NodeRefused),
        ),
        (unreachable(), Err(PodLessHold::NodeUnreadable)),
    ];
    for (answer, expected) in cases {
        let node = ScriptedNode::new(vec![(3, answer)]);
        assert_eq!(node_vouches(&node, newest).await, expected);
        assert_eq!(node.asked(), vec![3]);
    }
}

/// Each hold's classification, variant by variant: a row aging in, leaving
/// the candidate set, or losing a race clears on its own; everything else
/// can keep the drain parked and WARNs. Tags are distinct.
#[test]
fn each_hold_is_classified_by_whether_it_clears_on_its_own() {
    use PodLessHold::*;
    // An exhaustive match, so a new variant fails to compile here until
    // it's added to the table below.
    fn listed(hold: PodLessHold) {
        match hold {
            RecentProgress | RowDrifted | NoPodName | GenerationChanged | SnapshotChanged
            | LockContended | VmObserved | PodBound | PodPresent | PodUnreadable
            | NoRestoreSource | HeldForDataSafety | NoExecutionRow | NodePending
            | NodeSuspended | NodeSuperseded | NodeRefused | NodeUnreadable => {}
        }
    }
    let holds = [
        (RecentProgress, true),
        (RowDrifted, true),
        (NoPodName, true),
        (GenerationChanged, true),
        (SnapshotChanged, true),
        (LockContended, true),
        (VmObserved, false),
        (PodBound, false),
        (PodPresent, false),
        (PodUnreadable, false),
        (NoRestoreSource, false),
        (HeldForDataSafety, false),
        (NoExecutionRow, false),
        (NodePending, false),
        (NodeSuspended, false),
        (NodeSuperseded, false),
        (NodeRefused, false),
        (NodeUnreadable, false),
    ];
    let tags: std::collections::HashSet<&str> = holds.iter().map(|(hold, _)| hold.tag()).collect();
    assert_eq!(tags.len(), holds.len(), "tags must be distinct");
    assert_eq!(holds.len(), 18, "every variant is in the table");
    for (hold, clears) in holds {
        listed(hold);
        assert_eq!(hold.clears_on_its_own(), clears, "{hold:?}");
    }
}
