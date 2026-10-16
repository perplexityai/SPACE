//! Postgres-backed coverage for the `draining → drained` gate's fourth term:
//! the rows the drained-node terminator's resident guard would refuse the node
//! for.
//!
//! The two gates count residency from different lifecycle sets — node-drain
//! from an include-list, the terminator from an exclusion list — so four
//! lifecycles (`created`, `stopped`, `deleting`, `quarantined`) were resident to
//! the second and not the first. A node carrying only those rows cleared the
//! first gate, reached `drained`, and was then refused on every terminator tick
//! with node-drain no longer looking at it.
//!
//! These tests assert the two gates against a live database *together*: it is
//! not enough that this count is nonzero, it has to be nonzero exactly when the
//! terminator's own UPDATE matches no row. Sharing the predicate is what makes
//! that true, and running both statements over one fixture is what proves the
//! sharing is real rather than two agreeing restatements.

use pplx_space_common_core::types::{NodeId, SandboxId};

use pplx_space_common_svc::sql::sandbox_residency::CENSUS_PLACEMENT_SETTLE;

use crate::drained_node_terminator::store::mark_terminating_query;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Which of the two node-release gates counts a row in a given lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CountedBy {
    /// The resident include-list *and* the terminator's exclusion-list guard.
    /// The evacuation pipeline services these, so a drain converges on its own.
    BothGates,
    /// The terminator's guard only. This is the divergence: node-drain used to
    /// release the node while the terminator held it. `deleting` resolves on
    /// its own; the other three hold until an operator deletes the sandbox or
    /// quarantine resolution releases it.
    TerminatorOnly,
    /// Neither. `suspended` pins through `target_node_id` instead and is the
    /// snapshot gate's business; `failed` is terminal.
    NeitherGate,
}

/// Exhaustive on purpose: adding a `SandboxLifecycle` variant makes this match
/// fail to compile, which forces whoever adds it to say which gates count it.
/// The bug this term fixes is exactly two lifecycle lists drifting apart with
/// nothing in the build to notice — `quarantined` joined the divergence in
/// 2026-07 and no test failed.
fn counted_by(lifecycle: SandboxLifecycle) -> CountedBy {
    use SandboxLifecycle as L;
    match lifecycle {
        L::Creating
        | L::Starting
        | L::Started
        | L::Pausing
        | L::Paused
        | L::Resuming
        | L::Stopping
        | L::Suspending
        | L::Restoring
        | L::Checkpointing => CountedBy::BothGates,
        L::Created | L::Stopped | L::Deleting | L::Quarantined => CountedBy::TerminatorOnly,
        L::Suspended | L::Failed => CountedBy::NeitherGate,
    }
}

/// Lifecycle strings `sandboxes.lifecycle` can hold that the
/// `SandboxLifecycle` enum does not model, so `SandboxLifecycle::all()` cannot
/// reach them:
///
/// * `deleted` — a real tombstone value, written by raw SQL in the internal
///   API's status handler and pruned later by `tombstone_retention`. It has to
///   stay outside this gate: tombstones outlive the sandbox by design, so
///   counting one would hold its node in `draining` until retention ran.
/// * `cleanup_required` — a node-local sandbox-manager state. Nothing writes it
///   to this column, but the terminator's guard excludes it, so the exclusion
///   is exercised rather than argued.
/// * `cleanup_failed` — stands in for *any* value neither list knows. An
///   exclusion list is fail-closed for those: it counts them, so an unmodelled
///   lifecycle holds the node instead of silently releasing it.
const UNMODELLED_LIFECYCLES: &[(&str, CountedBy)] = &[
    ("deleted", CountedBy::NeitherGate),
    ("cleanup_required", CountedBy::NeitherGate),
    ("cleanup_failed", CountedBy::TerminatorOnly),
];

/// A node in `draining`. `heartbeat` decides whether the control plane can see
/// it, which is what decides whether the census discount is even available —
/// the fail-closed cases need a node with no `node_status_snapshots` row, so the
/// heartbeat is a parameter rather than a second copy of this INSERT.
///
/// Heartbeat freshness is only **one** of two independent things the discount
/// requires; the other is that the node's last accepted sandbox census is itself
/// recent (`sandbox_inventory_pruned_at`, gated by `node_census_fresh`). The
/// `Fresh` arm stamps a current census for the same reason the sibling
/// `live_residents_tests` fixture does: otherwise the stale-heartbeat cases
/// would withhold the discount for two reasons at once and prove neither, and
/// the discount-expected cases would fail for a reason the test is not about.
pub(super) async fn insert_draining_node(
    pool: &PgPool,
    heartbeat: Heartbeat,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.42'::inet, 'worker', 'k8s', 'registered', $2, NOW())"
    ))
    .bind(node_id)
    .bind(PHASE_DRAINING)
    .execute(pool)
    .await?;
    if matches!(heartbeat, Heartbeat::Fresh) {
        sqlx::query(sql!(
            "INSERT INTO node_status_snapshots (
                 node_id, observed_at, stale_after, reachable, role,
                 scheduling_status, services, sandbox_inventory_pruned_at,
                 updated_at
             )
             VALUES ($1, NOW(), NOW() + make_interval(secs => 30), TRUE,
                     'worker', 'ready', '[]'::jsonb, NOW(), NOW())"
        ))
        .bind(node_id)
        .execute(pool)
        .await?;
    }
    Ok(node_id)
}

/// Whether the node has published a status heartbeat at all. Only the two states
/// these tests need; `residency_tests` carries the full four-way version for the
/// resident scan, and the unreachable/expired variants are its business.
#[derive(Debug, Clone, Copy)]
pub(super) enum Heartbeat {
    /// Reachable and unexpired: the node's census answer counts.
    Fresh,
    /// No `node_status_snapshots` row — the control plane cannot see the node,
    /// so nothing it does or doesn't report can discount a row.
    Absent,
}

/// Pinned through `active_node_id` (the column both residency definitions
/// read) and placed well outside [`CENSUS_PLACEMENT_SETTLE`], so a missing
/// census row would be readable as absence if the mode allowed it.
pub(super) async fn insert_sandbox(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, network_policy_generation, placement_changed_at
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $3, 'on_node', 'k8s', $2, 1,
             NOW() - make_interval(secs => $4)
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(lifecycle)
    .bind((CENSUS_PLACEMENT_SETTLE * 2).as_secs() as f64)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Publish a census row: the node reporting that it holds `sandbox_id` in
/// `status`. Same signature as the sibling `residency_tests` helper — `status`
/// is a parameter rather than a hard-coded `'started'` because
/// `node_observes_sandbox` deliberately does not match it against the cluster
/// lifecycle, and the case that turns on that is a `stopped` sandbox reported as
/// `stopped` (see `a_stopped_row_its_node_reports_is_never_discounted`).
async fn observe_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    status: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, observed_at, updated_at)
         VALUES ($1, $2, $3, NOW(), NOW())"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) fn sink(pool: PgPool, residency: ResidencyEvidence) -> SubstrateSink {
    sink_with_gate(pool, residency, true)
}

pub(super) fn sink_with_gate(
    pool: PgPool,
    residency: ResidencyEvidence,
    gate: bool,
) -> SubstrateSink {
    let mut sink = SubstrateSink::with_suspend_execution(pool, |_| async move {
        unreachable!("the gate scans never suspend");
    });
    sink.residency_evidence = residency;
    sink.termination_blocking_gate = gate;
    sink
}

/// With the gate off, the *write* must let a divergent-lifecycle node through.
///
/// This is the test the gate-off claim actually needed. `tb4` asserts the same
/// property against `FakeSink`, which has no SQL, so it passed while the real
/// UPDATE carried the wide guard unconditionally and held every such node in
/// `draining` — the flag off, the behaviour on, and no WARN to explain it
/// because the reconciler's hold was the half that *was* gated.
///
/// The two halves are asserted together on one fixture, because the risk is not
/// "does the guard exist" but "does its scope follow the flag": a gate-off run
/// must release the four divergent lifecycles and must still refuse a genuinely
/// resident one. Asserting only the first would pass if the guard were dropped
/// altogether, which reopens the race this guard was added to close.
#[tokio::test(flavor = "current_thread")]
async fn the_gate_off_write_releases_divergent_lifecycles_but_still_guards_residents() -> TestResult
{
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    for residency in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        let off = sink_with_gate(db.pool.clone(), residency, false);

        // Every lifecycle that diverges between the two gates. `stopped` is the
        // one that matters operationally: an ordinary user sandbox nothing
        // sweeps, which under the wide-always guard stalled an MNG rollout on a
        // cluster whose flag said the term was disabled.
        for lifecycle in ["stopped", "created", "deleting", "quarantined"] {
            let node = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
            let sandbox = insert_sandbox(&db.pool, node, lifecycle).await?;
            observe_sandbox(&db.pool, sandbox, node, "started").await?;
            assert!(
                NodeDrainSink::set_drain_phase(&off, node, PHASE_DRAINING, PHASE_DRAINED, None)
                    .await?,
                "{residency}/{lifecycle}: gate off must advance; the flag withholds this term"
            );
            assert_eq!(
                drain_phase(&db.pool, node).await?.as_deref(),
                Some(PHASE_DRAINED),
                "{residency}/{lifecycle}: gate off must actually commit the phase"
            );
        }

        // ...and the resident term is still enforced, so turning the gate off
        // did not simply delete the guard.
        let resident_node = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
        let resident_sandbox = insert_sandbox(&db.pool, resident_node, "started").await?;
        observe_sandbox(&db.pool, resident_sandbox, resident_node, "started").await?;
        assert!(
            !NodeDrainSink::set_drain_phase(
                &off,
                resident_node,
                PHASE_DRAINING,
                PHASE_DRAINED,
                None
            )
            .await?,
            "{residency}: gate off must still refuse a genuinely resident sandbox — that race is not what the flag gates"
        );

        // And with the gate on, the same divergent lifecycle is refused, so the
        // two scopes are demonstrably different rather than both permissive.
        let on = sink_with_gate(db.pool.clone(), residency, true);
        let gated = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
        let gated_sandbox = insert_sandbox(&db.pool, gated, "stopped").await?;
        observe_sandbox(&db.pool, gated_sandbox, gated, "started").await?;
        assert!(
            !NodeDrainSink::set_drain_phase(&on, gated, PHASE_DRAINING, PHASE_DRAINED, None)
                .await?,
            "{residency}: gate on must refuse `stopped` — otherwise this test proves nothing about the scope following the flag"
        );
    }
    Ok(())
}

/// Run the terminator's own `drained → terminating` flip against a node parked
/// in `drained`, returning whether it advanced. This is the statement that used
/// to match zero rows forever.
async fn terminator_would_advance(
    pool: &PgPool,
    node_id: NodeId,
    residency: ResidencyEvidence,
) -> TestResult<bool> {
    let query = mark_terminating_query(residency);
    let updated = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(PHASE_TERMINATING)
        .bind(PHASE_DRAINED)
        .execute(pool)
        .await?;
    Ok(updated.rows_affected() == 1)
}

/// The full lifecycle matrix against a live database, in one pass: for every
/// `SandboxLifecycle`, what the resident scan counts, what the new term counts,
/// and — the claim that matters — whether the terminator actually refuses the
/// node. A term that merely *looked* like the terminator's guard would pass the
/// first two assertions and fail the third.
///
/// The four `TerminatorOnly` rows are the bug: the resident scan reports the
/// node empty, so it converged to `drained`, where the terminator refused it on
/// every tick with node-drain no longer watching.
#[tokio::test(flavor = "current_thread")]
async fn the_two_gates_agree_across_every_lifecycle() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = sink(db.pool.clone(), ResidencyEvidence::LifecycleOnly);

    let matrix = SandboxLifecycle::all()
        .iter()
        .map(|lifecycle| (lifecycle.as_str(), counted_by(*lifecycle)))
        .chain(UNMODELLED_LIFECYCLES.iter().copied());

    for (lifecycle, expected) in matrix {
        let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
        insert_sandbox(&db.pool, node_id, lifecycle).await?;

        let resident_occupying = u64::from(expected == CountedBy::BothGates);
        assert_eq!(
            NodeDrainSink::count_resident_sandboxes_on_node(&sink, node_id).await?,
            ResidentSandboxCounts {
                occupying: resident_occupying,
                discounted: 0,
            },
            "{lifecycle}: the resident include-list must keep counting exactly \
             what it counted — widening it would enroll these rows in the \
             stale-abandon sweep, which flips them to `failed`"
        );

        let holds = expected != CountedBy::NeitherGate;
        let blocking =
            NodeDrainSink::count_termination_blocking_residents_on_node(&sink, node_id).await?;
        assert_eq!(
            blocking,
            if holds {
                TerminationBlockingResidents {
                    by_lifecycle: vec![(lifecycle.to_string(), 1)],
                }
            } else {
                TerminationBlockingResidents::default()
            },
            "{lifecycle}: expected {expected:?} to hold the drain gate = {holds}"
        );

        // Same fixture parked in `drained`, running the terminator's own flip:
        // it must refuse exactly when the term above holds. This is the
        // equivalence the shared predicate is for — with the term absent, every
        // `TerminatorOnly` row below is a node parked here forever.
        sqlx::query(sql!("UPDATE nodes SET drain_phase = $2 WHERE id = $1"))
            .bind(node_id)
            .bind(PHASE_DRAINED)
            .execute(&db.pool)
            .await?;
        assert_eq!(
            terminator_would_advance(&db.pool, node_id, ResidencyEvidence::LifecycleOnly).await?,
            !holds,
            "{lifecycle}: the drain gate and the terminator's resident guard must answer identically, or a node clears one and parks at the other"
        );
    }
    Ok(())
}

/// Several lifecycles on one node are reported per lifecycle and summed, so an
/// operator reading the warning sees what is actually there rather than a bare
/// total.
#[tokio::test(flavor = "current_thread")]
async fn the_breakdown_is_per_lifecycle_and_the_total_is_derived() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    insert_sandbox(&db.pool, node_id, "quarantined").await?;
    insert_sandbox(&db.pool, node_id, "quarantined").await?;
    insert_sandbox(&db.pool, node_id, "stopped").await?;
    // A `started` row is resident to *both* definitions; it belongs in this
    // count too, since the terminator would refuse the node for it.
    insert_sandbox(&db.pool, node_id, "started").await?;

    let sink = sink(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
    let blocking =
        NodeDrainSink::count_termination_blocking_residents_on_node(&sink, node_id).await?;
    assert_eq!(
        blocking,
        TerminationBlockingResidents {
            by_lifecycle: vec![
                ("quarantined".to_string(), 2),
                ("started".to_string(), 1),
                ("stopped".to_string(), 1),
            ],
        },
        "lifecycle-ordered so the warning text is stable tick to tick"
    );
    assert_eq!(blocking.total(), 4);
    Ok(())
}

/// The race, exercised rather than argued: call the phase write *directly* with
/// a resident sandbox present. That is precisely the state the reconciler cannot
/// see — its four gate terms are separate reads, so a sandbox attaching after
/// the last one and before this UPDATE is invisible to the pre-check. The
/// statement has to refuse on its own, or the node commits `drained` and the
/// terminator refuses it on every tick thereafter with nothing watching.
///
/// Asserting through `NodeDrainSink::set_drain_phase` rather than the query
/// string is the point: a `contains("NOT EXISTS")` assertion passes whether or
/// not the guard is wired to anything.
#[tokio::test(flavor = "current_thread")]
async fn the_phase_write_refuses_a_node_that_gained_a_sandbox_after_the_pre_check() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    // Both evidence modes: the census-aware branch renders a different predicate
    // (the discount subqueries instead of a literal TRUE) and it is the mode
    // production flips to, so the statement has to be executed in it rather than
    // string-asserted. A census-absent row would be *discounted*, so the row
    // here is observed — the guard must still refuse for it.
    for residency in [
        ResidencyEvidence::LifecycleOnly,
        ResidencyEvidence::NodeCensusAware,
    ] {
        let sink = sink(db.pool.clone(), residency);

        // A quarantined row — resident to the terminator's guard, invisible to
        // the resident include-list, so nothing else in the gate would stop
        // this write.
        let held = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
        let sandbox = insert_sandbox(&db.pool, held, "quarantined").await?;
        observe_sandbox(&db.pool, sandbox, held, "started").await?;
        assert!(
            !NodeDrainSink::set_drain_phase(&sink, held, PHASE_DRAINING, PHASE_DRAINED, None)
                .await?,
            "{residency}: the UPDATE must refuse while a terminator-resident \
             sandbox is attached"
        );
        assert_eq!(
            drain_phase(&db.pool, held).await?.as_deref(),
            Some(PHASE_DRAINING),
            "{residency}: a refused write must leave the phase alone, not \
             half-apply it"
        );

        // Same write on an empty node still goes through — the guard must not
        // be a blanket refusal, or no node ever drains.
        let empty = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
        assert!(
            NodeDrainSink::set_drain_phase(&sink, empty, PHASE_DRAINING, PHASE_DRAINED, None)
                .await?,
            "{residency}: an empty node must still advance"
        );
        assert_eq!(
            drain_phase(&db.pool, empty).await?.as_deref(),
            Some(PHASE_DRAINED)
        );
    }
    let sink = sink(db.pool.clone(), ResidencyEvidence::LifecycleOnly);

    // And the guard rides only on this edge: leaving the drain must not be
    // blocked by a sandbox that is still on the node.
    let undrained = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    insert_sandbox(&db.pool, undrained, "quarantined").await?;
    assert!(
        NodeDrainSink::set_drain_phase(&sink, undrained, PHASE_DRAINING, PHASE_TERMINATING, None)
            .await?,
        "only the drain-completion edge carries the guard"
    );
    Ok(())
}

/// Poll `pg_blocking_pids` until some backend is blocked by the transaction on
/// `holder`, bounded so a wiring mistake fails the test instead of hanging it.
///
/// The bound is half [`DRAIN_COMPLETION_LOCK_TIMEOUT`] — 2.5 s of sleeping plus
/// this many round trips, against the racing write's 5 s. Deliberately derived
/// from that constant rather than written as a number: if the poll could outlast
/// it, the racing write would hit `55P03` and the test would fail on a `?`
/// instead of on its assertion, which is a confusing failure for a test whose
/// whole value is failing loudly for the right reason. The margin is a factor of
/// two, not an order of magnitude, so lower the divisor rather than raising the
/// step if this ever gets close.
///
/// Any backend whose `pg_blocking_pids()` contains the holder's pid is waiting on
/// a lock this transaction owns, which for this fixture can only be the `nodes`
/// row. Polled **on the holder's own connection**, not the pool:
/// `try_pg_test_db` caps the pool at 2, the holder has one and the racing task
/// needs the other, so a pool query here would starve and take the racing task's
/// slot with it.
pub(super) async fn wait_until_blocked(
    holder: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> TestResult<bool> {
    let deadline = DRAIN_COMPLETION_LOCK_TIMEOUT / 2;
    let step = std::time::Duration::from_millis(50);
    for _ in 0..(deadline.as_millis() / step.as_millis()) {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE pg_backend_pid() = ANY(pg_blocking_pids(pid))
             )"
        ))
        .fetch_one(&mut **holder)
        .await?;
        if waiting {
            return Ok(true);
        }
        tokio::time::sleep(step).await;
    }
    Ok(false)
}

pub(super) async fn drain_phase(pool: &PgPool, node_id: NodeId) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT drain_phase FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_one(pool)
        .await?,
    )
}

/// The race itself, with two real transactions interleaved — the only shape that
/// can decide this, because the failure is a snapshot-visibility rule rather
/// than a missing predicate.
///
/// Under READ COMMITTED an UPDATE that blocks on a row lock re-checks the row it
/// is updating, but Postgres documents that it "does not see effects of those
/// commands on other rows in the database". The guard is a `NOT EXISTS` over
/// `sandboxes` — other rows — so a guarded UPDATE alone commits `drained` even
/// though it waited for the attaching transaction. The `SELECT … FOR UPDATE`
/// that precedes it is what makes the guard's snapshot fresh.
///
/// The interleaving here is the production one: the live attach writer upserts
/// the same `nodes` row inside its transaction, so it holds the lock this write
/// wants while the sandbox row is still uncommitted.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_attaching_concurrently_still_refuses_the_phase_write() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = sink(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;

    // The attaching transaction: takes the `nodes` row lock the way the pod
    // placement transaction does (its `INSERT … ON CONFLICT (id) DO UPDATE` on
    // `nodes` holds this same lock), then attaches a sandbox — and holds both
    // uncommitted.
    let mut attach = db.pool.begin().await?;
    // The production shape, and the `WHERE` is the load-bearing part. The
    // placement transaction reaches this row through `INSERT … ON CONFLICT (id)
    // DO UPDATE … WHERE <something changed>`, which locks the conflicting tuple
    // *before* evaluating that `WHERE` — and normally the `WHERE` excludes, so it
    // holds the lock without writing a new tuple version. This fixture inserts
    // the same address, so its `WHERE` also excludes: the lock-only path, which
    // is the one `set_drain_phase_query`'s comment rests on. Without the `WHERE`
    // this would always write a new version and exercise the plain-`UPDATE`
    // waiter path instead — passing, but not for the documented reason.
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.42'::inet, 'worker', 'k8s', 'registered')
         ON CONFLICT (id) DO UPDATE SET address = EXCLUDED.address
             WHERE nodes.address IS DISTINCT FROM EXCLUDED.address"
    ))
    .bind(node_id)
    .execute(&mut *attach)
    .await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, network_policy_generation, placement_changed_at
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started', 'on_node', 'k8s', $2, 1, NOW()
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&mut *attach)
    .await?;

    // Pre-warm the connection the racing task will lease. `try_pg_test_db` caps
    // the pool at 2 and this fixture holds one, so if the task paid TCP + auth +
    // after-connect setup inside the race window it could arrive *after* the
    // commit below and then refuse for the trivial already-committed reason —
    // green, having proven nothing about snapshot freshness.
    NodeDrainSink::count_termination_blocking_residents_on_node(&sink, node_id).await?;

    // The drain-completion write, racing it. It must block on the `nodes` lock,
    // so drive it concurrently and commit the attach underneath it.
    let racing = tokio::spawn(async move {
        NodeDrainSink::set_drain_phase(&sink, node_id, PHASE_DRAINING, PHASE_DRAINED, None).await
    });

    // Wait for the racing write to actually be *blocked on this row's lock*, by
    // asking Postgres rather than guessing at a duration. `!is_finished()` is not
    // enough: it cannot tell "parked on the lock" from "still acquiring a pool
    // connection or hasn't sent the SELECT yet", and in that second state the
    // commit below lands first and the racing statement then refuses for the
    // trivial already-committed reason — green, having measured nothing.
    let blocked = wait_until_blocked(&mut attach).await?;
    assert!(
        blocked,
        "the racing write never blocked on the `nodes` row lock, so this test \
         would pass with the lock removed"
    );
    attach.commit().await?;

    assert!(
        !racing
            .await
            .expect("the racing phase write must not panic")?,
        "a sandbox that committed while the phase write was in flight must still \
         refuse it — otherwise the node reaches `drained` holding a resident \
         sandbox and the terminator refuses it on every tick thereafter"
    );
    assert_eq!(
        drain_phase(&db.pool, node_id).await?.as_deref(),
        Some(PHASE_DRAINING)
    );
    Ok(())
}

/// The term composes with the census discount instead of bypassing it. This is
/// the property that keeps adding the term from *creating* a new park: if this
/// count kept a phantom row that the resident scan discounted, a node whose
/// drain converged only because of the discount would now be held in `draining`
/// forever by the same row — the mirror image of the bug being fixed.
#[tokio::test(flavor = "current_thread")]
async fn a_census_absent_row_is_discounted_here_too() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    insert_sandbox(&db.pool, node_id, "quarantined").await?;
    let live = insert_sandbox(&db.pool, node_id, "quarantined").await?;
    observe_sandbox(&db.pool, live, node_id, "started").await?;

    let census_aware = sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);
    assert_eq!(
        NodeDrainSink::count_termination_blocking_residents_on_node(&census_aware, node_id).await?,
        TerminationBlockingResidents {
            by_lifecycle: vec![("quarantined".to_string(), 1)],
        },
        "a row the node's own census omits must not hold the drain gate, or the \
         discount would release one gate and not the other"
    );

    // The kill switch restores the pre-census answer: both rows count.
    let lifecycle_only = sink(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
    assert_eq!(
        NodeDrainSink::count_termination_blocking_residents_on_node(&lifecycle_only, node_id)
            .await?
            .total(),
        2
    );
    Ok(())
}

/// The property the whole "is this data loss?" question turns on: **a `stopped`
/// sandbox on a healthy node is never discounted, so no gate can release its
/// node.**
///
/// Its disk lives only on that node — sandbox-manager's stop keeps the
/// filesystem and leaves the local row in `Stopped`, which is what makes
/// `stop` → `start` a recovery — and the node's own full census is a `List` over
/// those local rows, so the node keeps reporting the sandbox for as long as it
/// holds the bytes. `node_observes_sandbox` tests presence without matching
/// `status`, so the census row's `stopped` status is presence just like a
/// `started` one.
///
/// **What this adds over its neighbours.** The *count* half is already covered:
/// `a_census_absent_row_is_discounted_here_too` asserts
/// `count_termination_blocking_residents_on_node` under
/// [`ResidencyEvidence::NodeCensusAware`] with a census-present `quarantined`
/// row, and `the_gate_off_write_releases_divergent_lifecycles_but_still_guards_residents`
/// covers the phase write. What is **not** covered elsewhere is the terminator's
/// own flip: nothing else drives `terminator_would_advance` under
/// `NodeCensusAware` against a census-present fourth-term row. That is the
/// assertion to keep if this test is ever trimmed.
///
/// It does **not** catch a narrowing of `node_observes_sandbox` to rows whose
/// status agrees with the cluster lifecycle: this fixture's lifecycle and status
/// both read `stopped`, so such a narrowing would leave it green.
/// `residency_tests::a_status_mismatch_is_presence_not_absence` owns that
/// property and asserts it with a genuine mismatch. The agreeing pair is
/// deliberate here — it is the shape production actually produces for a stopped
/// sandbox, and therefore the shape the node-release decision turns on.
///
/// Two producer-layer regressions would falsify the same guarantee from
/// underneath and are **not** covered anywhere: sandbox-manager dropping the
/// local row on stop (it keeps it — `CleanupCompletion::MarkStopped` with
/// `keep_filesystem = true`), and node-ctl's full-inventory facet filtering
/// non-running sandboxes out of the `List` it publishes. Both live in crates
/// whose fixtures need a real sandbox filesystem. Naming the gap is deliberate:
/// a reader who assumes this test guards the whole chain would not go looking.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_row_its_node_reports_is_never_discounted() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    let stopped = insert_sandbox(&db.pool, node_id, "stopped").await?;
    observe_sandbox(&db.pool, stopped, node_id, "stopped").await?;

    let census_aware = sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);
    assert_eq!(
        NodeDrainSink::count_termination_blocking_residents_on_node(&census_aware, node_id).await?,
        TerminationBlockingResidents {
            by_lifecycle: vec![("stopped".to_string(), 1)],
        },
        "the most permissive residency mode must still count a `stopped` row its \
         node reports; discounting it would release the node and destroy the \
         sandbox's only copy"
    );

    // The other end of the same predicate: the terminator's own flip must refuse
    // the node too. The count above is node-drain's read; this is the statement
    // that would actually authorize the ASG call.
    sqlx::query(sql!("UPDATE nodes SET drain_phase = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(PHASE_DRAINED)
        .execute(&db.pool)
        .await?;
    assert!(
        !terminator_would_advance(&db.pool, node_id, ResidencyEvidence::NodeCensusAware).await?,
        "the drained-node terminator must refuse a node holding a reported \
         `stopped` sandbox under the census-aware mode as well"
    );

    // The negative control this assertion needs: with the census row gone the
    // discount *does* fire, so the assertions above are about the census row and
    // not about `stopped` being unconditionally undroppable.
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(stopped)
    .execute(&db.pool)
    .await?;
    assert!(
        terminator_would_advance(&db.pool, node_id, ResidencyEvidence::NodeCensusAware).await?,
        "a `stopped` row the node's census omits is a phantom placement and must \
         be discounted, or the discount would release one gate and not the other"
    );
    Ok(())
}

/// Fail-closed, matching the resident scan: a node the control plane cannot see
/// keeps every row counting. Over-counting holds a drain open; under-counting
/// releases a node that the terminator then destroys with a live sandbox on it.
#[tokio::test(flavor = "current_thread")]
async fn a_node_that_cannot_be_seen_keeps_every_row_counting() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    // No `node_status_snapshots` row at all: the shape that is discounted on a
    // fresh node, minus the heartbeat that makes the census meaningful.
    let node_id = insert_draining_node(&db.pool, Heartbeat::Absent).await?;
    insert_sandbox(&db.pool, node_id, "quarantined").await?;

    let census_aware = sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);
    assert_eq!(
        NodeDrainSink::count_termination_blocking_residents_on_node(&census_aware, node_id)
            .await?
            .total(),
        1,
        "a blind node's rows must keep holding the drain gate"
    );
    Ok(())
}
