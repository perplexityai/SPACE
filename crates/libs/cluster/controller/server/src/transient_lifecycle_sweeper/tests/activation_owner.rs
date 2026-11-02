//! The owner check on every activation-owned lifecycle (`creating`,
//! `starting`, `resuming`, `restoring`, `pausing`, `checkpointing`): a row whose active execution's
//! activation-attempt lease is live, or ended less than an age window ago
//! (expired, or given up by its owner), is neither listed nor failed,
//! converged or reverted, however far past its age threshold it is. Once the
//! lease ended a full window ago, the row takes its usual sweep action.
//!
//! Every test runs against the production SQL on the Postgres fixture. The
//! direct substrate supplies `managed_by = 'space'`; the owner check reads the
//! execution row directly, so it decides the same way for either label.

use std::collections::HashMap;

use pplx_space_api::controllers::substrate::{AbandonedRestoreRetirement, RetireFence};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    PodUid, SandboxActivationAttemptId, SandboxId, SandboxLifecycle, SubstrateKind,
};
use pplx_space_db_test_fixtures::{pg_test_db, pg_test_db_with_max_connections};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::super::lifecycle_writer_tests::{age, direct_sink};
use super::super::store::{
    FIRST_ACTIVATION_SQL, LIST_FAILED_ACTIVE_EXECUTIONS_SQL, LOCK_SANDBOX_ROW_SQL,
    REPAIR_FAILED_ACTIVE_EXECUTION_SQL, candidate_pod_fence_with_observation,
};
use super::super::{FailGuard, SWEEPABLE_LIFECYCLES, SubstrateSweepSink, SweepSink};
use crate::lifecycle_writer_test_support::{
    ExecutionSeed, SandboxSeed, bounded, insert_execution, insert_node, insert_sandbox,
    insert_snapshot, single_backend_pool, wait_until_blocked,
};

/// The lifecycles whose owner holds an activation attempt, spelled out here
/// rather than read from `is_activation_owned`, so a change
/// to that set fails these tests instead of moving their expectations along
/// with it.
const ACTIVATION_OWNED: [SandboxLifecycle; 6] = [
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Checkpointing,
];

/// The active execution's activation-attempt lease, relative to now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lease {
    /// Held, and expires in five minutes: the owner is within its lease.
    Live,
    /// Held, and expired ten seconds ago: inside the age window after
    /// expiry, where an owner that outran its lease may still be finishing.
    JustExpired,
    /// Held, and expired an hour ago, well past the age window: a crashed
    /// owner.
    Expired,
    /// Held, with no expiry. No writer produces it; the sweep must still
    /// treat the attempt as owned.
    NoExpiry,
    /// Given up just now by an owner that never dispatched its activation:
    /// no attempt, and the expiry cut to the time it gave up.
    JustGivenUp,
    /// Given up the same way an hour ago.
    GivenUpLongAgo,
    /// Neither an attempt nor an expiry: the activation finished, or the
    /// execution never took an attempt.
    None,
}

impl Lease {
    const ALL: [Self; 7] = [
        Self::Live,
        Self::JustExpired,
        Self::Expired,
        Self::NoExpiry,
        Self::JustGivenUp,
        Self::GivenUpLongAgo,
        Self::None,
    ];

    /// Whether an attempt holds the lease, and when the lease ends, in
    /// seconds from now.
    fn columns(self) -> (bool, Option<i64>) {
        match self {
            Self::Live => (true, Some(300)),
            Self::JustExpired => (true, Some(-10)),
            Self::Expired => (true, Some(-3600)),
            Self::NoExpiry => (true, None),
            Self::JustGivenUp => (false, Some(0)),
            Self::GivenUpLongAgo => (false, Some(-3600)),
            Self::None => (false, None),
        }
    }

    /// Whether the owner may still be running, so an activation-owned row
    /// must be left alone.
    fn owner_may_be_alive(self) -> bool {
        matches!(
            self,
            Self::Live | Self::JustExpired | Self::NoExpiry | Self::JustGivenUp
        )
    }

    /// Whether the lease ended a full age window ago, which the select
    /// reports as an abandoned activation.
    fn ended_long_ago(self) -> bool {
        matches!(self, Self::Expired | Self::GivenUpLongAgo)
    }
}

/// Insert an hour-old, placed row in `lifecycle` with a running active
/// execution carrying `lease`. A held attempt is also the route's first, as
/// a create's is.
async fn insert_row(pool: &PgPool, lifecycle: &str, lease: Lease) -> TestResult<SandboxId> {
    insert_row_with(pool, lifecycle, "space", "running", lease).await
}

/// [`insert_row`] for a row of `managed_by` whose execution is in
/// `execution_state`.
async fn insert_row_with(
    pool: &PgPool,
    lifecycle: &str,
    managed_by: &'static str,
    execution_state: &'static str,
    lease: Lease,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let node_id = insert_node(pool, None, None).await?;
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new(lifecycle, managed_by).on_node(node_id),
    )
    .await?;
    insert_execution(
        pool,
        sandbox_id,
        ExecutionSeed {
            state: execution_state,
            ..ExecutionSeed::RUNNING
        },
    )
    .await?;
    let (held, ends_in_secs) = lease.columns();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = CASE WHEN $2 THEN attempt.id END,
                route_activation_id = CASE WHEN $2 THEN attempt.id END,
                activation_attempt_expires_at = NOW() + make_interval(secs => $3::BIGINT),
                activation_attempt_target_state = CASE WHEN $2 THEN 'started' END
           FROM (SELECT space_uuid_v7() AS id) attempt
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(held)
    .bind(ends_in_secs)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

pub(super) async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Whether the candidate select lists `sandbox_id` under `lifecycle`, and if
/// so whether it reports the activation abandoned.
async fn listed(
    sink: &SubstrateSweepSink,
    lifecycle: SandboxLifecycle,
    sandbox_id: SandboxId,
) -> TestResult<Option<bool>> {
    Ok(sink
        .list_aged_transient(lifecycle, age())
        .await?
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .map(|candidate| candidate.activation_abandoned))
}

/// A sandbox in `creating`, `starting`, `resuming`, `restoring`, `pausing`
/// or `checkpointing`, an hour past its age threshold, is left alone while
/// its owner's lease is live, expired less than an age window ago, or was
/// given up just now: the sweep neither lists it nor fails it. Once the lease ended a full window ago,
/// whether it expired or was given up, the same row is listed as abandoned
/// and failed; with no lease at all it is listed, not abandoned. The other
/// sweepable lifecycles (`suspending`, `stopping`) ignore the lease: no
/// request takes an attempt while the row is in either (a start is refused on
/// `suspending` and can't materialize an execution on `stopping`), so a lease
/// found on the execution there can't make the row sweep-proof.
#[tokio::test(flavor = "current_thread")]
async fn an_activation_owned_row_is_swept_only_once_its_owner_is_gone() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for &lifecycle in SWEEPABLE_LIFECYCLES {
        for lease in Lease::ALL {
            let id = insert_row(pool, lifecycle.as_str(), lease).await?;
            let swept = !ACTIVATION_OWNED.contains(&lifecycle) || !lease.owner_may_be_alive();

            let listing = listed(&sink, lifecycle, id).await?;
            let claimed = sink
                .fail_aged_candidate(id, lifecycle, age(), FailGuard::Aged)
                .await?
                .is_some();
            let stored = lifecycle_of(pool, id).await?;

            let expected_listing = swept.then_some(lease.ended_long_ago());
            let expected_lifecycle = if swept { "failed" } else { lifecycle.as_str() };
            if listing != expected_listing || claimed != swept || stored != expected_lifecycle {
                mismatches.push(format!(
                    "{lifecycle}, {lease:?}: listed {listing:?}, fail claimed {claimed}, \
                     lifecycle {stored}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// A `restoring` sandbox whose VM came up, or whose pre-restore snapshot is
/// ready, is converged to `started` or reverted to `suspended` only once the
/// wake driving it is provably gone. While the wake's lease is live, expired
/// less than an age window ago, or was given up just now, neither happens,
/// even with its restore lease expired an hour ago: the wake would otherwise
/// lose its own transition.
#[tokio::test(flavor = "current_thread")]
async fn a_restoring_row_is_converged_or_reverted_only_once_its_wake_is_gone() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool);

    let mut mismatches = Vec::new();
    for lease in Lease::ALL {
        let converge = insert_row(pool, "restoring", lease).await?;
        expire_restore_lease(pool, converge).await?;
        let converged = sink
            .converge_restoring(converge, SandboxLifecycle::Started, age())
            .await?;

        let acts = !lease.owner_may_be_alive();
        let converged_to = lifecycle_of(pool, converge).await?;
        let expected_converged = if acts { "started" } else { "restoring" };
        if converged != acts || converged_to != expected_converged {
            mismatches.push(format!(
                "{lease:?}: converge claimed {converged} ({converged_to})"
            ));
        }

        for retirement in RETIREMENT_MODES {
            let revert = insert_row(pool, "restoring", lease).await?;
            expire_restore_lease(pool, revert).await?;
            insert_snapshot(pool, revert, "ready", -60).await?;
            let fence = recorded_fence(pool, revert).await?;
            let reverted = revert_sink(pool, retirement)
                .revert_to_suspended(revert, SandboxLifecycle::Restoring, age(), &fence)
                .await?
                .is_some();
            let reverted_to = lifecycle_of(pool, revert).await?;
            let expected_reverted = if acts { "suspended" } else { "restoring" };
            if reverted != acts || reverted_to != expected_reverted {
                mismatches.push(format!(
                    "{lease:?}, retirement {retirement:?}: revert claimed {reverted} \
                     ({reverted_to})"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// Both revert variants carry the owner check: the one that only moves the
/// lifecycle (the default) and the one that also retires the restore's
/// execution.
pub(super) const RETIREMENT_MODES: [AbandonedRestoreRetirement; 2] = [
    AbandonedRestoreRetirement::Disabled,
    AbandonedRestoreRetirement::Enabled,
];

/// A sink whose revert retires the restore's execution or not.
fn revert_sink(pool: &PgPool, retirement: AbandonedRestoreRetirement) -> SubstrateSweepSink {
    direct_sink(pool).with_abandoned_restore_retirement(retirement)
}

/// The retire fence the candidate select would record for the row's active
/// execution, read straight from it, so a revert that retires the execution
/// matches it and only the owner check can refuse.
pub(super) async fn recorded_fence(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<RetireFence> {
    let row: RecordedExecutionRow = sqlx::query_as(sql!(
        "SELECT pod_name, activation_attempt_id, generation,
                NULLIF(BTRIM(pod_uid), '') AS pod_uid, route_activation_id
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    Ok(candidate_pod_fence_with_observation(
        sandbox_id,
        row.pod_name,
        row.activation_attempt_id,
        row.generation,
        row.pod_uid,
        row.route_activation_id,
        &HashMap::new(),
    ))
}

/// The active execution's identity columns [`recorded_fence`] reads.
#[derive(sqlx::FromRow)]
struct RecordedExecutionRow {
    pod_name: Option<String>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    generation: Option<i64>,
    pod_uid: Option<PodUid>,
    route_activation_id: Option<SandboxActivationAttemptId>,
}

/// Give the row a restore lease that expired an hour ago, so only the
/// activation lease stands between the row and the sweep.
async fn expire_restore_lease(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 hour',
                restore_lease_token = space_uuid_v7()
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// The sweep write raced against a new owner in
/// [`a_new_owner_that_claims_first_keeps_the_row`].
#[derive(Debug, Clone, Copy)]
enum Write {
    Fail(SandboxLifecycle),
    Converge,
    Revert,
}

impl Write {
    fn lifecycle(self) -> SandboxLifecycle {
        match self {
            Self::Fail(lifecycle) => lifecycle,
            Self::Converge | Self::Revert => SandboxLifecycle::Restoring,
        }
    }

    fn lands_on(self) -> &'static str {
        match self {
            Self::Fail(_) => "failed",
            Self::Converge => "started",
            Self::Revert => "suspended",
        }
    }

    async fn run(self, sink: &SubstrateSweepSink, sandbox_id: SandboxId) -> TestResult<bool> {
        Ok(match self {
            Self::Fail(lifecycle) => sink
                .fail_aged_candidate(sandbox_id, lifecycle, age(), FailGuard::Aged)
                .await?
                .is_some(),
            Self::Converge => {
                sink.converge_restoring(sandbox_id, SandboxLifecycle::Started, age())
                    .await?
            }
            Self::Revert => sink
                .revert_to_suspended(
                    sandbox_id,
                    SandboxLifecycle::Restoring,
                    age(),
                    &RetireFence::NoExecution,
                )
                .await?
                .is_some(),
        })
    }
}

/// A new activation attempt claims a crashed owner's row by locking the
/// sandbox row and then writing a fresh lease on the execution
/// (`assign_activation_attempt`). A sweep write that arrives in between waits
/// for the claim and then refuses, so the new owner keeps the row; a claim
/// that rolls back leaves the crashed owner's row to the sweep. That holds
/// for the fail CAS on every activation-owned lifecycle, and for the
/// `restoring` converge and revert CASes.
#[tokio::test(flavor = "current_thread")]
async fn a_new_owner_that_claims_first_keeps_the_row() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;

    let writes = ACTIVATION_OWNED
        .into_iter()
        .map(Write::Fail)
        .chain([Write::Converge, Write::Revert]);
    for write in writes {
        for commit in [true, false] {
            let lifecycle = write.lifecycle();
            let id = insert_row(pool, lifecycle.as_str(), Lease::Expired).await?;
            expire_restore_lease(pool, id).await?;
            insert_snapshot(pool, id, "ready", -60).await?;

            // The claim's first step: lock the sandbox, as the sweep's CASes
            // also do first.
            let mut owner = pool.begin().await?;
            let owner_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
                .fetch_one(&mut *owner)
                .await?;
            sqlx::query(LOCK_SANDBOX_ROW_SQL)
                .bind(id)
                .execute(&mut *owner)
                .await?;

            let (sweep_pool, sweep_pid) = single_backend_pool(&db).await?;
            // The revert that only moves the lifecycle: the one that also
            // retires the execution would refuse the new owner's claim on its
            // fence alone, owner check or not.
            let sweep = tokio::spawn(async move {
                let sink = revert_sink(&sweep_pool, AbandonedRestoreRetirement::Disabled);
                write.run(&sink, id).await
            });
            bounded(wait_until_blocked(pool, sweep_pid, owner_pid)).await?;

            // Its second step, with the sweep already waiting: the new lease.
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                    SET activation_attempt_id = space_uuid_v7(),
                        activation_attempt_expires_at = NOW() + INTERVAL '5 minutes'
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(id)
            .execute(&mut *owner)
            .await?;
            if commit {
                owner.commit().await?;
            } else {
                owner.rollback().await?;
            }

            let claimed = bounded(async { sweep.await? }).await?;
            let case = format!("{write:?}, owner commits: {commit}");
            assert_eq!(claimed, !commit, "{case}");
            let expected = if commit {
                lifecycle.as_str()
            } else {
                write.lands_on()
            };
            assert_eq!(lifecycle_of(pool, id).await?, expected, "{case}");
        }
    }
    Ok(())
}

/// The two failed-cleanup listing columns this test reads.
#[derive(sqlx::FromRow)]
struct FailedListing {
    id: SandboxId,
    first_activation_abandoned: bool,
}

/// A `failed` Kubernetes sandbox whose execution never reached `running` and
/// whose last owner gave its activation up (a start that adopted a crashed
/// create's row, say, which the sweep then failed with the VM alive) is
/// reported to failed cleanup as an activation that never finished, and is
/// never repaired back to `started`, the same as a create that crashed
/// holding its attempt.
/// With no lease at all, or once the execution got as far as `running`, the
/// row is reported finished and repaired, as any stale `failed` row with a
/// started VM is.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_row_its_owner_gave_up_on_is_never_repaired() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let kubernetes = SubstrateKind::Kubernetes;
    // (execution state, lease, reported never finished, repaired)
    let cases = [
        ("creating", Lease::GivenUpLongAgo, true, false),
        ("created", Lease::JustGivenUp, true, false),
        ("creating", Lease::Expired, true, false),
        ("creating", Lease::None, false, true),
        ("running", Lease::GivenUpLongAgo, false, true),
    ];
    let mut mismatches = Vec::new();
    for (execution_state, lease, never_finished, repaired) in cases {
        let id =
            insert_row_with(pool, "failed", kubernetes.as_str(), execution_state, lease).await?;

        let listed: Vec<FailedListing> = sqlx::query_as(LIST_FAILED_ACTIVE_EXECUTIONS_SQL)
            .bind(kubernetes)
            .bind(10_000_i64)
            .fetch_all(pool)
            .await?;
        let reported = listed
            .iter()
            .find(|row| row.id == id)
            .map(|row| row.first_activation_abandoned);
        // The re-read just before an alive retire must agree with the listing.
        let reread: Option<(bool, bool)> = sqlx::query_as(FIRST_ACTIVATION_SQL)
            .bind(id)
            .bind(kubernetes)
            .fetch_optional(pool)
            .await?;
        let reread = reread.map(|(_unfinished, abandoned)| abandoned);
        let claimed: Option<SandboxId> = sqlx::query_scalar(REPAIR_FAILED_ACTIVE_EXECUTION_SQL)
            .bind(id)
            .bind(kubernetes)
            .fetch_optional(pool)
            .await?;
        let stored = lifecycle_of(pool, id).await?;

        let expected_lifecycle = if repaired { "started" } else { "failed" };
        if reported != Some(never_finished)
            || reread != reported
            || claimed.is_some() != repaired
            || stored != expected_lifecycle
        {
            mismatches.push(format!(
                "{execution_state}, {lease:?}: reported {reported:?}, re-read {reread:?}, \
                 repaired {}, lifecycle {stored}",
                claimed.is_some()
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
