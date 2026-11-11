//! The restoring-episode deadline: a `restoring` row a client keeps retrying
//! renews its restore lease and its wake's activation attempt on every retry,
//! which the ordinary owner guards read as a live owner forever. Past the
//! deadline, measured from the claim that started the episode, a row whose
//! active execution has no pod IP is listed and reverted anyway. A row whose
//! pod has an IP, a row still inside the deadline, and every row
//! while the deadline is off keep today's guards, and so does a row of any
//! substrate but Kubernetes, which has no pod IP to wait for.
//!
//! Every fixture is the retry loop's own shape, so the guards the deadline
//! waives are present, not vacuously absent: a restore lease a retry renewed
//! moments ago, a live activation attempt on the reused pending execution,
//! and a ready pre-restore snapshot the revert can prove.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::{AbandonedRestoreRetirement, Substrate};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{PositiveDuration, SandboxId, SandboxLifecycle};
use pplx_space_db_test_fixtures::{pg_test_db, pg_test_db_with_max_connections};
use pplx_space_test_env::{TestEnv, TestResult};
use sqlx::PgPool;

use super::super::lifecycle_writer_tests::{age, direct_sink};
use super::super::{
    DEFAULT_RESTORING_EPISODE_DEADLINE_SECS, FailGuard, ReconcilerConfig, SubstrateSweepSink,
    SweepSink,
};
use super::activation_owner::{RETIREMENT_MODES, lifecycle_of, recorded_fence};
use crate::lifecycle_writer_test_support::{
    ExecutionSeed, SandboxSeed, bounded, insert_execution, insert_node, insert_sandbox,
    insert_snapshot, kubernetes_substrate,
};

/// The deadline the fixtures bracket. Their ages derive from it, so they
/// stay on the side of it they're named for.
const DEADLINE_SECS: i64 = 1800;

fn deadline() -> PositiveDuration {
    PositiveDuration::new(Duration::from_secs(DEADLINE_SECS.cast_unsigned())).expect("non-zero")
}

/// A `restoring` row the retry loop holds.
#[derive(Debug, Clone, Copy)]
struct Retried {
    /// Seconds since the claim that started the episode.
    episode_secs: i64,
    /// Whether the active execution recorded a pod IP.
    pod_ip: bool,
    /// Whether a ready snapshot from before the episode exists.
    snapshot: bool,
    /// The row's `managed_by`.
    managed_by: &'static str,
}

impl Retried {
    /// Past the deadline, no pod IP, revertible.
    const OVERDUE: Self = Self {
        episode_secs: 2 * DEADLINE_SECS,
        pod_ip: false,
        snapshot: true,
        managed_by: "k8s",
    };
}

/// Insert `shape`: a placed `restoring` row whose restore lease a retry just
/// renewed, and whose `creating` execution carries that retry's live
/// activation attempt.
async fn insert_retried(pool: &PgPool, shape: Retried) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let node_id = insert_node(pool, None, None).await?;
    let mut seed = SandboxSeed::new("restoring", shape.managed_by).on_node(node_id);
    seed.lifecycle_age_secs = shape.episode_secs;
    insert_sandbox(pool, sandbox_id, &seed).await?;
    insert_execution(
        pool,
        sandbox_id,
        ExecutionSeed {
            state: "creating",
            ..ExecutionSeed::RUNNING
        },
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = attempt.id,
                route_activation_id = attempt.id,
                activation_attempt_expires_at = NOW() + INTERVAL '300 seconds',
                activation_attempt_target_state = 'started',
                k8s_node_name = 'node-a',
                pod_ip = CASE WHEN $2 THEN '100.64.0.9'::inet END
           FROM (SELECT space_uuid_v7() AS id) attempt
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(shape.pod_ip)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + INTERVAL '150 seconds',
                restore_lease_token = space_uuid_v7()
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    if shape.snapshot {
        insert_snapshot(pool, sandbox_id, "ready", -60).await?;
    }
    Ok(sandbox_id)
}

/// A Kubernetes sink whose deadline is `deadline` and whose revert retires or
/// not. Its apiserver holds no pods, which only the candidate select's pod
/// observation reads.
fn sink(
    pool: &PgPool,
    deadline: Option<PositiveDuration>,
    retirement: AbandonedRestoreRetirement,
) -> TestResult<SubstrateSweepSink> {
    let substrate = Substrate::from_kubernetes(kubernetes_substrate(pool)?);
    Ok(SubstrateSweepSink::new(pool.clone(), Arc::new(substrate))
        .with_abandoned_restore_retirement(retirement)
        .with_restoring_episode_deadline(deadline))
}

async fn execution_active(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Whether the select lists `sandbox_id`, and if so whether as overdue.
async fn listed(sink: &SubstrateSweepSink, sandbox_id: SandboxId) -> TestResult<Option<bool>> {
    Ok(sink
        .list_aged_transient(SandboxLifecycle::Restoring, age())
        .await?
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .map(|candidate| candidate.episode_overdue))
}

/// A retried `restoring` row past the deadline whose execution has no pod IP
/// is listed as overdue and reverted to `suspended` under the retry's live
/// restore lease and activation attempt, in both revert modes. With
/// retirement on, the revert also retires the execution, which is what the
/// cleanup's pod delete then frees; off, the execution stays for the cleanup
/// to retire under the fence, as for any revert.
#[tokio::test(flavor = "current_thread")]
async fn an_overdue_retried_restore_without_a_pod_ip_is_reverted() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    for retirement in RETIREMENT_MODES {
        let sink = sink(pool, Some(deadline()), retirement)?;
        let id = insert_retried(pool, Retried::OVERDUE).await?;

        assert_eq!(listed(&sink, id).await?, Some(true), "{retirement:?}");
        let fence = recorded_fence(pool, id).await?;
        let reverted = sink
            .revert_to_suspended(id, SandboxLifecycle::Restoring, age(), &fence)
            .await?;
        assert!(reverted.is_some(), "{retirement:?}: the revert claims it");
        assert_eq!(lifecycle_of(pool, id).await?, "suspended");
        assert!(
            !restore_lease_held(pool, id).await?,
            "{retirement:?}: leaving `restoring` drops the retry's restore lease, so the next \
             wake can claim the row at once"
        );
        assert_eq!(
            execution_active(pool, id).await?,
            !retirement.is_enabled(),
            "{retirement:?}"
        );
    }
    Ok(())
}

/// The deadline spares every row it isn't about. A row whose execution
/// recorded a pod IP got past scheduling and may be past the snapshot load;
/// a row inside the deadline may still be a slow, healthy restore; a row
/// with no revertible snapshot can't be reverted, so the deadline doesn't
/// lift its guards for anything else; a Direct row has no Kubernetes
/// execution, which must not read as one without a pod IP; and with the
/// deadline off, an overdue row keeps today's guards. Each stays `restoring`
/// under the retry's renewed leases, so the retry converges it as it does
/// today.
#[tokio::test(flavor = "current_thread")]
async fn a_retried_restore_the_deadline_is_not_about_keeps_its_guards() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let cases = [
        (
            "pod IP recorded",
            Retried {
                pod_ip: true,
                ..Retried::OVERDUE
            },
            Some(deadline()),
        ),
        (
            "inside the deadline",
            Retried {
                episode_secs: DEADLINE_SECS / 2,
                ..Retried::OVERDUE
            },
            Some(deadline()),
        ),
        (
            "no revertible snapshot",
            Retried {
                snapshot: false,
                ..Retried::OVERDUE
            },
            Some(deadline()),
        ),
        ("deadline off", Retried::OVERDUE, None),
        (
            "a Direct row",
            Retried {
                managed_by: "space",
                ..Retried::OVERDUE
            },
            Some(deadline()),
        ),
    ];
    let mut mismatches = Vec::new();
    for (name, shape, deadline) in cases {
        for retirement in RETIREMENT_MODES {
            let sink = match shape.managed_by {
                "k8s" => sink(pool, deadline, retirement)?,
                _ => direct_sink(pool)
                    .with_abandoned_restore_retirement(retirement)
                    .with_restoring_episode_deadline(deadline),
            };
            let id = insert_retried(pool, shape).await?;
            let listing = listed(&sink, id).await?;
            let fence = recorded_fence(pool, id).await?;
            let reverted = sink
                .revert_to_suspended(id, SandboxLifecycle::Restoring, age(), &fence)
                .await?
                .is_some();
            let stored = lifecycle_of(pool, id).await?;
            if listing.is_some() || reverted || stored != "restoring" {
                mismatches.push(format!(
                    "{name}, {retirement:?}: listed {listing:?}, reverted {reverted}, \
                     lifecycle {stored}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// The deadline only ever reverts. An overdue row the select lists over a
/// live owner is still refused by the fail CAS, which keeps its owner guards:
/// failing it would take the sandbox away from a retry that could still
/// restore it from the same snapshot.
#[tokio::test(flavor = "current_thread")]
async fn an_overdue_restore_is_never_failed_under_a_live_owner() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink(pool, Some(deadline()), AbandonedRestoreRetirement::Enabled)?;

    let id = insert_retried(pool, Retried::OVERDUE).await?;
    assert_eq!(listed(&sink, id).await?, Some(true));
    for guard in [FailGuard::Aged, FailGuard::NoPendingReceipt] {
        let failed = sink
            .fail_aged_candidate(id, SandboxLifecycle::Restoring, age(), guard)
            .await?;
        assert_eq!(
            failed, None,
            "{guard:?}: the fail CAS keeps its owner guards"
        );
    }
    assert_eq!(lifecycle_of(pool, id).await?, "restoring");
    Ok(())
}

/// An overdue row whose restore lease and activation attempt both ended long
/// ago would be listed without the deadline, so it isn't reported as one the
/// deadline let through: its revert overrides no live retry.
#[tokio::test(flavor = "current_thread")]
async fn an_overdue_restore_nobody_owns_is_not_reported_as_overdue() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink(pool, Some(deadline()), AbandonedRestoreRetirement::Enabled)?;

    let id = insert_retried(pool, Retried::OVERDUE).await?;
    end_owners(pool, id).await?;
    assert_eq!(listed(&sink, id).await?, Some(false));
    Ok(())
}

/// The control for the Direct case in
/// [`a_retried_restore_the_deadline_is_not_about_keeps_its_guards`]: the same
/// Direct row, once its restore lease and activation attempt ended long ago,
/// is listed and reverted through the ordinary guards. So it's the deadline,
/// not the Direct listing, that the case shows sparing it.
#[tokio::test(flavor = "current_thread")]
async fn a_direct_row_nobody_owns_is_still_reverted() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = direct_sink(pool).with_restoring_episode_deadline(Some(deadline()));

    let id = insert_retried(
        pool,
        Retried {
            managed_by: "space",
            ..Retried::OVERDUE
        },
    )
    .await?;
    end_owners(pool, id).await?;
    assert_eq!(listed(&sink, id).await?, Some(false));
    let fence = recorded_fence(pool, id).await?;
    let reverted = sink
        .revert_to_suspended(id, SandboxLifecycle::Restoring, age(), &fence)
        .await?;
    assert!(reverted.is_some());
    assert_eq!(lifecycle_of(pool, id).await?, "suspended");
    Ok(())
}

/// End the row's restore lease and its activation attempt an hour ago, well
/// past any age window: whoever owned the restore is gone.
async fn end_owners(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 hour'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether the row still carries a restore lease or its token.
async fn restore_lease_held(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT restore_lease_expires_at IS NOT NULL OR restore_lease_token IS NOT NULL
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// A pod-IP writer locks the active execution, not the sandbox, so the revert
/// takes that lock too before it decides the episode has no pod IP. It
/// doesn't wait for it: while a writer holds it, the revert claims nothing
/// and returns at once. Afterwards, an IP that committed keeps the row
/// `restoring`, and a write that rolled back leaves it to the revert. Both
/// revert modes.
#[tokio::test(flavor = "current_thread")]
async fn a_revert_defers_while_a_pod_ip_writer_holds_the_execution() -> TestResult {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;

    for retirement in RETIREMENT_MODES {
        for commit in [true, false] {
            let id = insert_retried(pool, Retried::OVERDUE).await?;
            let fence = recorded_fence(pool, id).await?;
            let sink = sink(pool, Some(deadline()), retirement)?;
            let case = format!("{retirement:?}, pod IP commits: {commit}");

            let mut writer = pool.begin().await?;
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                    SET pod_ip = '100.64.0.9'::inet
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(id)
            .execute(&mut *writer)
            .await?;

            let held = bounded(async {
                Ok(sink
                    .revert_to_suspended(id, SandboxLifecycle::Restoring, age(), &fence)
                    .await?)
            })
            .await?;
            assert!(held.is_none(), "{case}: deferred while the writer holds it");
            assert_eq!(lifecycle_of(pool, id).await?, "restoring", "{case}");

            if commit {
                writer.commit().await?;
            } else {
                writer.rollback().await?;
            }
            let reverted = sink
                .revert_to_suspended(id, SandboxLifecycle::Restoring, age(), &fence)
                .await?
                .is_some();
            assert_eq!(reverted, !commit, "{case}");
            let expected = if commit { "restoring" } else { "suspended" };
            assert_eq!(lifecycle_of(pool, id).await?, expected, "{case}");
        }
    }
    Ok(())
}

/// Unset keeps the default deadline, a positive number of seconds replaces
/// it, `0` turns it off, and a value that isn't a number keeps the default.
#[test]
fn the_restoring_episode_deadline_parses_from_the_environment() {
    const VAR: &str = "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_RESTORING_EPISODE_DEADLINE_SECS";
    let parsed = |env: &TestEnv| ReconcilerConfig::from_environment(env).restoring_episode_deadline;
    let secs = |secs| PositiveDuration::new(Duration::from_secs(secs));
    let default = secs(DEFAULT_RESTORING_EPISODE_DEADLINE_SECS);
    let mut env = TestEnv::default();
    assert_eq!(parsed(&env), default);
    env.set(VAR, "2400");
    assert_eq!(parsed(&env), secs(2400));
    env.set(VAR, "0");
    assert_eq!(parsed(&env), None);
    for invalid in ["abc", "", "-5"] {
        env.set(VAR, invalid);
        assert_eq!(parsed(&env), default, "{invalid:?}");
    }
}
