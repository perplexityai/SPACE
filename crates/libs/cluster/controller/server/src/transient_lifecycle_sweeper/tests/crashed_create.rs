//! A create whose API process died after its pod was placed: the VM came up,
//! the row stayed `creating`, and the active execution holds the crashed
//! request's activation-attempt lease. Once that lease has been expired for a
//! full age window the sweep fails and retires the row, alive VM or not; while
//! the lease is live, or expired more recently, it leaves the row alone.
//!
//! The fake-sink tests pin the reconciler's choice of action. The
//! Postgres-backed tests pin the candidate select and the fail CAS that carry
//! the owner check.

use super::*;

/// A `creating` candidate whose active execution's activation lease expired
/// a full age window ago.
fn abandoned_create() -> SweepCandidate {
    SweepCandidate {
        activation_abandoned: true,
        ..candidate(SandboxLifecycle::Creating)
    }
}

/// A crashed create whose VM is alive is failed and retired, whether or not
/// the DB shows its pod routable. It's never converged and never hydrated on
/// the way: the client that asked for it usually never got its id.
#[tokio::test(flavor = "current_thread")]
async fn a_crashed_create_with_a_live_vm_fails_and_retires() {
    for routable_execution in [true, false] {
        let create = SweepCandidate {
            routable_execution,
            ..abandoned_create()
        };
        let id = create.sandbox_id;
        let sink = FakeSink::new();
        sink.set_probe_verdict(id, ProbeVerdict::AliveOnNode);
        sink.set_routability(id, ExecutionRoutability::Routable);
        sink.enqueue_candidates(SandboxLifecycle::Creating, vec![create]);
        let reconciler = Reconciler::new(sweep_config(), sink);

        let report = reconciler.tick().await;

        let case = format!("routable {routable_execution}");
        assert_eq!(reconciler.sink.fail_calls(), vec![id], "{case}");
        assert_eq!(reconciler.sink.cleanup_calls(), vec![id], "{case}");
        assert_eq!(report.aged_out, 1, "{case}");
        assert!(reconciler.sink.converge_calls().is_empty(), "{case}");
        assert!(reconciler.sink.routability_calls().is_empty(), "{case}");
        assert_eq!(report.converged + report.converged_hydrated, 0, "{case}");
    }
}

/// An alive create the sweep can't show was abandoned keeps its VM: no fail,
/// no retire. The candidate select keeps a create with a live or recently
/// expired lease out entirely; this is the case it lets through, an execution
/// whose attempt was already released. The same tick fails an abandoned
/// create, so the sweep was able to act.
#[tokio::test(flavor = "current_thread")]
async fn a_create_without_an_abandoned_activation_keeps_its_live_vm() {
    let unproven = candidate(SandboxLifecycle::Creating);
    let unproven_id = unproven.sandbox_id;
    let abandoned = abandoned_create();
    let abandoned_id = abandoned.sandbox_id;
    let sink = FakeSink::new();
    sink.set_probe_verdict(unproven_id, ProbeVerdict::AliveOnNode);
    sink.set_probe_verdict(abandoned_id, ProbeVerdict::AliveOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Creating, vec![unproven, abandoned]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.deferred, 1);
    assert_eq!(reconciler.sink.fail_calls(), vec![abandoned_id]);
    assert_eq!(reconciler.sink.cleanup_calls(), vec![abandoned_id]);
}

/// When the fail CAS claims nothing (a new owner took the execution, or the
/// create finished), the sweep tears nothing down.
#[tokio::test(flavor = "current_thread")]
async fn a_crashed_create_whose_fail_cas_loses_is_left_alone() {
    let create = abandoned_create();
    let id = create.sandbox_id;
    let sink = FakeSink::new();
    sink.set_probe_verdict(id, ProbeVerdict::AliveOnNode);
    sink.set_cas_unclaimed(id);
    sink.enqueue_candidates(SandboxLifecycle::Creating, vec![create]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(reconciler.sink.fail_calls(), vec![id]);
    assert!(reconciler.sink.cleanup_calls().is_empty());
    assert_eq!(report.aged_out, 0);
}

/// A crashed create whose VM is dead or missing keeps the fail-and-retire
/// path it always had, abandoned activation or not.
#[tokio::test(flavor = "current_thread")]
async fn a_crashed_create_whose_vm_is_gone_still_fails_and_retires() {
    for verdict in [ProbeVerdict::DeadOnNode, ProbeVerdict::MissingOnNode] {
        for activation_abandoned in [true, false] {
            let create = SweepCandidate {
                activation_abandoned,
                ..candidate(SandboxLifecycle::Creating)
            };
            let id = create.sandbox_id;
            let sink = FakeSink::new();
            sink.set_probe_verdict(id, verdict);
            sink.enqueue_candidates(SandboxLifecycle::Creating, vec![create]);
            let reconciler = Reconciler::new(sweep_config(), sink);

            let report = reconciler.tick().await;

            let case = format!("verdict {verdict}, abandoned {activation_abandoned}");
            assert_eq!(reconciler.sink.fail_calls(), vec![id], "{case}");
            assert_eq!(reconciler.sink.cleanup_calls(), vec![id], "{case}");
            assert_eq!(report.aged_out, 1, "{case}");
        }
    }
}

/// A crashed create the sweep failed with its VM alive, whose retire then
/// didn't go through, is retired by the failed-cleanup pass on a later try
/// rather than repaired to `started`, even though its node reports the VM
/// `started`. The same row without an abandoned create is repaired, as any
/// stale `failed` row with a started VM is.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_crashed_create_with_a_live_vm_is_retired_not_repaired() {
    for first_activation_abandoned in [true, false] {
        let sink = FakeSink::new();
        let id = SandboxId::new();
        sink.enqueue_failed_cleanup(vec![FailedCleanupCandidate {
            first_activation_abandoned,
            ..failed_candidate(id)
        }]);
        sink.set_probe_observation(
            id,
            ProbeObservation {
                verdict: ProbeVerdict::AliveOnNode,
                node_status: Some(SandboxLifecycle::Started),
            },
        );
        let reconciler = Reconciler::new(
            ReconcilerConfig {
                failed_cleanup_enabled: true,
                ..ReconcilerConfig::default()
            },
            sink,
        );

        let report = reconciler.tick().await;

        let case = format!("create abandoned: {first_activation_abandoned}");
        let (retired, repaired) = if first_activation_abandoned {
            (vec![id], Vec::new())
        } else {
            (Vec::new(), vec![id])
        };
        assert_eq!(
            reconciler.sink.failed_execution_cleanup_calls(),
            retired,
            "{case}"
        );
        assert_eq!(
            reconciler.sink.failed_active_repair_calls(),
            repaired,
            "{case}"
        );
        assert_eq!(report.failed_cleanup_deferred, 0, "{case}");
    }
}

/// `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_CREATING_AGE_SECS` sets how long a
/// `creating` row waits before the sweep looks at it, independently of the
/// shared default. Unset or unparseable keeps the default, which is what
/// `creating` used before the knob existed, and `0` turns `creating` off.
#[test]
fn the_creating_age_knob_overrides_the_default_age() {
    const KNOB: &str = "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_CREATING_AGE_SECS";
    let year = Duration::from_secs(365 * 24 * 60 * 60);
    let config_for = |value: Option<&str>| {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set(
            "SPACE_TRANSIENT_LIFECYCLE_SWEEPER_DEFAULT_AGE_SECS",
            year.as_secs().to_string(),
        );
        if let Some(value) = value {
            env.set(KNOB, value);
        }
        ReconcilerConfig::from_environment(&env)
    };
    let creating_age = |config: &ReconcilerConfig| {
        config
            .resolve_age(SandboxLifecycle::Creating)
            .map(PositiveDuration::get)
    };

    let unset = config_for(None);
    assert_eq!(unset.creating_age, LifecycleAge::UseDefault);
    assert_eq!(creating_age(&unset), Some(year));

    let set = config_for(Some("900"));
    assert_eq!(creating_age(&set), Some(Duration::from_secs(900)));
    assert_eq!(
        set.resolve_age(SandboxLifecycle::Starting)
            .map(PositiveDuration::get),
        Some(year),
        "the knob moves only creating"
    );

    assert_eq!(creating_age(&config_for(Some("0"))), None);
    assert_eq!(creating_age(&config_for(Some("soon"))), Some(year));
}

mod postgres_backed {
    use pplx_space_common_core::sql;
    use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
    use pplx_space_db_test_fixtures::{pg_test_db, pg_test_db_with_max_connections};
    use pplx_space_test_env::TestResult;
    use sqlx::PgPool;

    // The direct substrate supplies `managed_by = 'space'` and needs no
    // Kubernetes API for these statements. The owner check reads the
    // execution row directly, so it decides the same way for either label.
    use super::super::super::lifecycle_writer_tests::{age, direct_sink};
    use super::super::super::store::{
        LIST_FAILED_ACTIVE_EXECUTIONS_SQL, REPAIR_FAILED_ACTIVE_EXECUTION_SQL,
    };
    use super::super::super::{SubstrateSweepSink, SweepSink};
    use crate::lifecycle_writer_test_support::{
        ExecutionSeed, SandboxSeed, bounded, insert_execution, insert_node, insert_sandbox,
        single_backend_pool, wait_until_blocked,
    };

    /// The activation-attempt columns of the active execution.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Lease {
        /// Expired an hour ago, well past the age window.
        Expired,
        /// Expired ten seconds ago: inside the age window after expiry, where
        /// a create that outran its lease may still be finishing.
        JustExpired,
        /// Expires in five minutes: the owner can still finalize.
        Live,
        /// An attempt id with no expiry. No writer produces it; the sweep
        /// must still treat it as owned.
        NoExpiry,
        /// No attempt at all: released.
        Released,
    }

    /// Insert an hour-old, placed row in `lifecycle` whose execution, in
    /// `execution_state` (`running` for [`insert_row`]), carries `lease`. The
    /// attempt is the execution's first, so it also published the route, as
    /// a create's does.
    async fn insert_row(pool: &PgPool, lifecycle: &str, lease: Lease) -> TestResult<SandboxId> {
        insert_row_managed_by(pool, lifecycle, "space", "running", lease).await
    }

    async fn insert_row_managed_by(
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
        let (attempt, expires_in_secs) = match lease {
            Lease::Expired => (true, Some(-3600_i64)),
            Lease::JustExpired => (true, Some(-10)),
            Lease::Live => (true, Some(300)),
            Lease::NoExpiry => (true, None),
            Lease::Released => (false, None),
        };
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = CASE WHEN $2 THEN attempt.id END,
                    route_activation_id = CASE WHEN $2 THEN attempt.id END,
                    activation_attempt_expires_at =
                        NOW() + make_interval(secs => $3::BIGINT),
                    activation_attempt_target_state = CASE WHEN $2 THEN 'started' END
               FROM (SELECT space_uuid_v7() AS id) attempt
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .bind(attempt)
        .bind(expires_in_secs)
        .execute(pool)
        .await?;
        Ok(sandbox_id)
    }

    async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
        Ok(
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(pool)
                .await?,
        )
    }

    /// Whether the candidate select lists `sandbox_id` under `lifecycle`,
    /// and if so whether it reports the activation abandoned.
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

    /// The candidate select hands the sweep a `creating` row only once its
    /// execution's lease has been expired for a full age window, and reports
    /// that as an abandoned activation. A live lease, one that expired inside
    /// the window, or an attempt with no expiry keeps the row out of the
    /// sweep, and the fail CAS refuses it too.
    #[tokio::test(flavor = "current_thread")]
    async fn only_a_create_without_a_live_owner_is_listed_or_failed() -> TestResult {
        let db = pg_test_db().await?;
        let pool = &db.pool;
        let sink = direct_sink(pool);

        // (lease, listed with this abandoned fact, fail CAS claims)
        let cases = [
            (Lease::Expired, Some(true), true),
            (Lease::Released, Some(false), true),
            (Lease::JustExpired, None, false),
            (Lease::Live, None, false),
            (Lease::NoExpiry, None, false),
        ];
        for (lease, expected_listing, fails) in cases {
            let id = insert_row(pool, "creating", lease).await?;

            assert_eq!(
                listed(&sink, SandboxLifecycle::Creating, id).await?,
                expected_listing,
                "{lease:?}"
            );
            let claimed = sink
                .fail_aged_candidate(id, SandboxLifecycle::Creating, age())
                .await?
                .is_some();
            assert_eq!(claimed, fails, "{lease:?}");
            let expected = if fails { "failed" } else { "creating" };
            assert_eq!(lifecycle_of(pool, id).await?, expected, "{lease:?}");
        }

        // Every lifecycle reports the activation abandoned only off a lease
        // expired a full window ago, even where, unlike `creating`, the
        // select lists a row whose lease is still live.
        for (lease, expected) in [
            (Lease::Expired, true),
            (Lease::JustExpired, false),
            (Lease::Live, false),
            (Lease::NoExpiry, false),
        ] {
            let id = insert_row(pool, "starting", lease).await?;
            assert_eq!(
                listed(&sink, SandboxLifecycle::Starting, id).await?,
                Some(expected),
                "starting row, {lease:?}"
            );
        }
        Ok(())
    }

    /// An activation-attempt assignment on the row (`assign_activation_attempt`)
    /// locks the sandbox row and then writes a fresh lease on the execution. A fail that arrives in between
    /// waits for the claim and then refuses, so the new owner keeps the
    /// create; a claim that rolls back leaves the crashed create to fail.
    #[tokio::test(flavor = "current_thread")]
    async fn a_new_owner_that_claims_first_keeps_the_create() -> TestResult {
        let db = pg_test_db_with_max_connections(3).await?;
        let pool = &db.pool;

        for commit in [true, false] {
            let id = insert_row(pool, "creating", Lease::Expired).await?;

            // The claim's first step, as `assign_activation_attempt` takes
            // it: lock the sandbox.
            let mut owner = pool.begin().await?;
            let owner_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
                .fetch_one(&mut *owner)
                .await?;
            sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
                .bind(id)
                .execute(&mut *owner)
                .await?;

            let (sweep_pool, sweep_pid) = single_backend_pool(&db).await?;
            let sweep = tokio::spawn(async move {
                direct_sink(&sweep_pool)
                    .fail_aged_candidate(id, SandboxLifecycle::Creating, age())
                    .await
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

            let claimed = bounded(async { Ok(sweep.await??) }).await?.is_some();
            assert_eq!(claimed, !commit, "owner commits: {commit}");
            let expected = if commit { "creating" } else { "failed" };
            assert_eq!(
                lifecycle_of(pool, id).await?,
                expected,
                "owner commits: {commit}"
            );
        }
        Ok(())
    }

    /// The two failed-cleanup listing columns this file reads.
    #[derive(sqlx::FromRow)]
    struct Listed {
        id: SandboxId,
        first_activation_abandoned: bool,
    }

    /// A `failed` Kubernetes row whose execution never got a pod IP (still
    /// `creating`, or `created` once the pod UID was written) and still holds
    /// the expired attempt that published its route is a create that never
    /// finished. The failed-cleanup listing says so, and the repair
    /// CAS won't put it back to `started`. Everything else is still listed as
    /// finished and still repaired: a released first attempt, a later attempt,
    /// and an execution that got as far as `running` or `recovering` even
    /// though its attempt was never released.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_create_that_never_finished_is_never_repaired() -> TestResult {
        let db = pg_test_db().await?;
        let pool = &db.pool;

        // (case, execution state, lease, a later attempt replaced the first,
        //  abandoned, repaired)
        let cases = [
            (
                "crashed create, no pod UID",
                "creating",
                Lease::Expired,
                false,
                true,
                false,
            ),
            (
                "crashed create, pod UID",
                "created",
                Lease::Expired,
                false,
                true,
                false,
            ),
            (
                "create in flight",
                "creating",
                Lease::Live,
                false,
                false,
                false,
            ),
            (
                "first attempt released",
                "creating",
                Lease::Released,
                false,
                false,
                true,
            ),
            (
                "later attempt",
                "creating",
                Lease::Expired,
                true,
                false,
                true,
            ),
            (
                "finished create, release missed",
                "running",
                Lease::Expired,
                false,
                false,
                true,
            ),
            (
                "recovery, release missed",
                "recovering",
                Lease::Expired,
                false,
                false,
                true,
            ),
        ];
        for (case, execution_state, lease, later_attempt, abandoned, repaired) in cases {
            let id = insert_row_managed_by(pool, "failed", "k8s", execution_state, lease).await?;
            if later_attempt {
                sqlx::query(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = space_uuid_v7()
                      WHERE sandbox_id = $1"
                ))
                .bind(id)
                .execute(pool)
                .await?;
            }

            let listed: Vec<Listed> = sqlx::query_as(LIST_FAILED_ACTIVE_EXECUTIONS_SQL)
                .bind("k8s")
                .bind(10_000_i64)
                .fetch_all(pool)
                .await?;
            let reported = listed
                .iter()
                .find(|row| row.id == id)
                .map(|row| row.first_activation_abandoned);
            assert_eq!(reported, Some(abandoned), "{case}");

            let claimed: Option<SandboxId> = sqlx::query_scalar(REPAIR_FAILED_ACTIVE_EXECUTION_SQL)
                .bind(id)
                .bind("k8s")
                .fetch_optional(pool)
                .await?;
            assert_eq!(claimed.is_some(), repaired, "{case}");
            let expected = if repaired { "started" } else { "failed" };
            assert_eq!(lifecycle_of(pool, id).await?, expected, "{case}");
        }
        Ok(())
    }
}
