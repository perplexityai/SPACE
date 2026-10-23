//! Postgres-backed coverage for the termination fence: the `nodes` row lock that
//! orders a placement writer against the provider-terminate path.
//!
//! The fence has two halves in two crates — `fence_node_for_placement` in
//! `pplx-space-common-svc`, taken by the API's placement writers, and the
//! `SELECT … FOR UPDATE` that `set_drain_phase` and `mark_terminating` take
//! before writing a phase. Neither half means anything alone, so the tests live
//! here, next to the phase writers, and drive both sides as two real concurrent
//! transactions.
//!
//! **Why the interleaving is the only shape that can decide this.** Every
//! failure the fence closes is a snapshot-visibility rule rather than a missing
//! predicate, and each of the two orders fails differently:
//!
//! * *Placement commits first.* Under READ COMMITTED an UPDATE that blocks on a
//!   row lock re-checks the row it is updating but, in Postgres's own words,
//!   "does not see effects of those commands on other rows in the database". The
//!   phase writers' guards are `NOT EXISTS` over `sandboxes` — other rows — so a
//!   guarded UPDATE alone advances the phase even though it waited. The
//!   preceding `FOR UPDATE`, in its own statement, is what makes the guard's
//!   snapshot fresh.
//! * *Phase write commits first.* A locking `SELECT` re-reads the row after
//!   acquiring the lock, so the placement observes the committed phase rather
//!   than the one that was current when it started. A fence that read the column
//!   *without* `FOR KEY SHARE` would read the pre-write value and let the
//!   placement through.
//!
//! Both tests therefore start the node in a phase the fence *accepts*
//! (`draining`) and move it under the racing transaction. A test that started at
//! `drained` would pass with the lock clause deleted, because the pre-write value
//! already refuses — green, having measured nothing.
//!
//! Fixtures come from the sibling `termination_gate_tests` rather than a fourth
//! copy of the node INSERT; `wait_until_blocked` in particular has to stay
//! bounded inside [`DRAIN_COMPLETION_LOCK_TIMEOUT`], which is a property of that
//! file's constant, not of this one.

use pplx_space_common_svc::sql::node_placement_fence::{
    PlacementFence, Reaffirmation, fence_node_for_placement, lock_node_for_phase_write,
};

use super::termination_gate_tests::{
    Heartbeat, drain_phase, insert_draining_node, insert_sandbox, sink, sink_with_gate,
    wait_until_blocked,
};
use super::*;

use crate::drained_node_terminator::DrainedNodeTerminatorSink;
use crate::drained_node_terminator::store::NodeDbStore;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The placement half, run as its own transaction: take the fence, and only if
/// it is open attach a sandbox and commit. Returns the verdict.
///
/// Written as one function because the two things have to share a transaction —
/// the lock is released at commit, so a fence taken in a separate transaction
/// orders nothing. This mirrors what every API writer does, which is why the
/// verdict rather than the row count is what the tests assert on.
async fn placement_attempt(pool: &PgPool, node_id: NodeId) -> TestResult<PlacementFence> {
    let mut tx = pool.begin().await?;
    let fence = fence_node_for_placement(&mut tx, node_id.as_uuid(), None, true).await?;
    if fence.refuses() {
        return Ok(fence);
    }
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
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(fence)
}

async fn resident_count(pool: &PgPool, node_id: NodeId) -> TestResult<i64> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)::BIGINT FROM sandboxes WHERE active_node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn the_phase_lock_restores_the_callers_lock_timeout() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    let mut tx = db.pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '17s'")
        .execute(&mut *tx)
        .await?;

    lock_node_for_phase_write(&mut tx, node_id.as_uuid(), Duration::from_millis(250)).await?;

    let lock_timeout = sqlx::query_scalar::<_, String>("SELECT current_setting('lock_timeout')")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(lock_timeout, "17s");
    tx.rollback().await?;
    Ok(())
}

/// Take the `nodes` row lock and advance `draining → drained` with the
/// production guard, leaving the transaction open so a racing placement blocks
/// on it.
///
/// Uses `set_drain_phase_query` rather than a hand-written UPDATE so the guard
/// under test is the shipped one; the caller supplies the transaction because the
/// whole point is to hold the lock across the race.
async fn lock_and_advance_to_drained(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
) -> TestResult<u64> {
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(DRAIN_COMPLETION_LOCK_TIMEOUT.as_millis().to_string())
        .execute(&mut **tx)
        .await?;
    sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE"))
        .bind(node_id)
        .execute(&mut **tx)
        .await?;
    let query = set_drain_phase_query(ResidencyEvidence::LifecycleOnly, GuardScope::Resident);
    let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(PHASE_DRAINED)
        .bind(Option::<&str>::None)
        .bind(PHASE_DRAINING)
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

/// **The reverse order, and the one that lets a sandbox die with its instance.** A phase write that
/// commits while a placement is in flight must make that placement refuse.
///
/// The node starts in `draining`, which the fence accepts, so the *only* way the
/// racing placement can see `drained` is by having waited on the row lock and
/// re-read the row afterwards. Delete `FOR KEY SHARE` from
/// `fence_node_for_placement` and this fails with `Open`: the placement
/// reads the pre-write `draining`, attaches, and the sandbox is on a node one
/// terminator tick from the provider call.
#[tokio::test(flavor = "current_thread")]
async fn a_phase_write_committing_mid_placement_refuses_the_placement() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;

    // Pre-warm the connection the racing task will lease. `try_pg_test_db` caps
    // the pool at 2 and this test holds one for the whole race, so if the task
    // paid TCP + auth + after-connect setup inside the window it could arrive
    // after the commit below and then refuse for the trivial
    // already-committed reason — green, having proven nothing about the lock.
    placement_attempt(&db.pool, NodeId::new()).await?;

    let mut advance = db.pool.begin().await?;
    assert_eq!(
        lock_and_advance_to_drained(&mut advance, node_id).await?,
        1,
        "the fixture node must actually advance; a zero here would make the race \
         below vacuous"
    );

    let pool = db.pool.clone();
    let racing = tokio::spawn(async move { placement_attempt(&pool, node_id).await });

    // Wait for the placement to be *blocked on this row's lock*, by asking
    // Postgres rather than guessing at a duration. `!is_finished()` cannot tell
    // "parked on the lock" from "still leasing a connection", and in that second
    // state the commit below lands first.
    assert!(
        wait_until_blocked(&mut advance).await?,
        "the racing placement never blocked on the `nodes` row lock, so this test \
         would pass with `FOR KEY SHARE` removed from the fence"
    );
    advance.commit().await?;

    assert_eq!(
        racing.await.expect("the racing placement must not panic")?,
        PlacementFence::Drained,
        "a placement that was in flight when the drain completed must observe the \
         committed phase, not the one that was current when it started"
    );
    assert_eq!(
        resident_count(&db.pool, node_id).await?,
        0,
        "the refused placement must not have written `active_node_id`"
    );
    Ok(())
}

/// The forward order: a placement that commits while the phase write is in
/// flight must make the phase write refuse.
///
/// This is `a_sandbox_attaching_concurrently_still_refuses_the_phase_write` with
/// the fence supplying the lock instead of a `nodes` upsert standing in for it.
/// That difference is the point — that test could only be written for the one
/// writer that already upserted `nodes`, and the four that do not are exactly the
/// ones this fence exists for.
#[tokio::test(flavor = "current_thread")]
async fn a_fenced_placement_committing_mid_phase_write_refuses_the_phase_write() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = sink(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;

    // The placement transaction, holding the fence's `FOR KEY SHARE` and its
    // uncommitted sandbox row — no `nodes` upsert anywhere, which is the shape
    // the four unordered writers have.
    let mut attach = db.pool.begin().await?;
    let fence = fence_node_for_placement(&mut attach, node_id.as_uuid(), None, true).await?;
    assert_eq!(
        fence,
        PlacementFence::Open,
        "a `draining` node must accept the placement, or the race below never starts"
    );
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

    // Pre-warm the racing task's connection; see the sibling test.
    NodeDrainSink::count_termination_blocking_residents_on_node(&sink, node_id).await?;

    let racing = tokio::spawn(async move {
        NodeDrainSink::set_drain_phase(&sink, node_id, PHASE_DRAINING, PHASE_DRAINED, None).await
    });

    assert!(
        wait_until_blocked(&mut attach).await?,
        "the phase write never blocked on the `nodes` row lock, so this test would \
         pass with the fence's lock removed"
    );
    attach.commit().await?;

    assert!(
        !racing
            .await
            .expect("the racing phase write must not panic")?,
        "a fenced placement that committed while the phase write was in flight must \
         still refuse it"
    );
    assert_eq!(
        drain_phase(&db.pool, node_id).await?.as_deref(),
        Some(PHASE_DRAINING)
    );
    Ok(())
}

/// The same forward order against the *other* route into `terminating`.
///
/// `mark_terminating` used to run as a bare pooled `UPDATE nodes`, which takes
/// `FOR NO KEY UPDATE` — a mode that conflicts with neither the fence's
/// `FOR KEY SHARE` nor nothing at all, and whose `NOT EXISTS` guards go stale
/// exactly as the sibling tests describe. Remove the `SELECT … FOR UPDATE` from
/// `mark_terminating` and this fails by flipping the node to `terminating` with a
/// live sandbox on it, which is the destroy the fence exists to prevent.
#[tokio::test(flavor = "current_thread")]
async fn a_fenced_placement_committing_mid_terminate_flip_refuses_the_flip() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    // `mark_terminating` flips `drained → terminating`, so put the node there
    // first. Done as a plain UPDATE rather than through the drain gate because
    // the gate is the sibling tests' subject, not this one's.
    sqlx::query(sql!("UPDATE nodes SET drain_phase = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(PHASE_DRAINED)
        .execute(&db.pool)
        .await?;

    // The placement holds the fence's lock. The node is `drained`, so the fence
    // itself refuses here — which is why this test drives the placement's SQL
    // directly: the property under test is the *flip's* behaviour when a
    // placement commits underneath it, and a fence-refusing placement would
    // never create that situation.
    let mut attach = db.pool.begin().await?;
    sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR KEY SHARE"))
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

    // The real reconciler sink, so the `FOR UPDATE` under test is the shipped
    // one. Pre-warm its connection first: the pool caps at 2, `attach` holds
    // one, and a task that paid connection setup inside the race window could
    // arrive after the commit and refuse for the trivial already-committed
    // reason.
    let terminator = NodeDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
    DrainedNodeTerminatorSink::count_local_snapshots(&terminator, node_id).await?;
    let racing = tokio::spawn(async move {
        DrainedNodeTerminatorSink::mark_terminating(&terminator, node_id).await
    });

    assert!(
        wait_until_blocked(&mut attach).await?,
        "the terminate flip never blocked on the `nodes` row lock, so this test \
         would pass with `mark_terminating`'s `FOR UPDATE` removed"
    );
    attach.commit().await?;

    assert!(
        !racing.await.expect("the racing flip must not panic")?,
        "a sandbox that committed while the flip was in flight must refuse it — \
         otherwise the node reaches `terminating` and the provider destroys the \
         instance with the sandbox on it"
    );
    assert_eq!(
        drain_phase(&db.pool, node_id).await?.as_deref(),
        Some(PHASE_DRAINED)
    );
    Ok(())
}

/// The negative direction for both races above: with nothing racing, the fence
/// is open on a node that is merely `draining`, and open on a node with no drain
/// at all.
///
/// Without this, deleting `classify_phase`'s `Draining` arm — refusing
/// everything with a non-NULL phase — would pass every test in this file, and
/// gentle drains would start failing every placement onto their residents' node.
#[tokio::test(flavor = "current_thread")]
async fn an_undrained_or_gently_draining_node_is_open() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let draining = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    assert_eq!(
        placement_attempt(&db.pool, draining).await?,
        PlacementFence::Open
    );
    assert_eq!(
        resident_count(&db.pool, draining).await?,
        1,
        "an open fence must let the placement through, not just report `Open`"
    );

    let undrained = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    sqlx::query(sql!("UPDATE nodes SET drain_phase = NULL WHERE id = $1"))
        .bind(undrained)
        .execute(&db.pool)
        .await?;
    assert_eq!(
        placement_attempt(&db.pool, undrained).await?,
        PlacementFence::Open
    );
    Ok(())
}

/// `terminating` and a node whose row is already gone both refuse.
///
/// Provider termination returns after API acceptance, before the underlying
/// compute is necessarily gone. A placement that started earlier can therefore
/// arrive after the final node-row delete; reopening the fence then recreates
/// the same destroy window under a missing row.
#[tokio::test(flavor = "current_thread")]
async fn a_terminating_or_deleted_node_refuses() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    sqlx::query(sql!("UPDATE nodes SET drain_phase = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(PHASE_TERMINATING)
        .execute(&db.pool)
        .await?;
    assert_eq!(
        placement_attempt(&db.pool, node_id).await?,
        PlacementFence::Terminating
    );
    assert_eq!(resident_count(&db.pool, node_id).await?, 0);

    let gone = NodeId::new();
    assert_eq!(
        placement_attempt(&db.pool, gone).await?,
        PlacementFence::MissingNode,
        "a placement onto a node whose row is gone must fail closed"
    );
    assert_eq!(
        resident_count(&db.pool, gone).await?,
        0,
        "the missing-node refusal must not write a phantom active_node_id"
    );
    Ok(())
}

/// A placement already in flight when the terminal node-row delete commits
/// must observe absence and refuse. Provider APIs return on accepted
/// termination, so deleting the control-plane row is not proof that no late
/// external create/restore can still report completion on that machine.
#[tokio::test(flavor = "current_thread")]
async fn a_node_delete_committing_mid_placement_refuses_the_placement() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    sqlx::query(sql!("UPDATE nodes SET drain_phase = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(PHASE_TERMINATING)
        .execute(&db.pool)
        .await?;

    let mut delete = db.pool.begin().await?;
    assert_eq!(
        sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
            .bind(node_id)
            .execute(&mut *delete)
            .await?
            .rows_affected(),
        1
    );
    let pool = db.pool.clone();
    let racing = tokio::spawn(async move { placement_attempt(&pool, node_id).await });
    assert!(
        wait_until_blocked(&mut delete).await?,
        "the placement must reach the node lock before its row is deleted"
    );
    delete.commit().await?;

    assert_eq!(
        racing.await.expect("the racing placement must not panic")?,
        PlacementFence::MissingNode
    );
    assert_eq!(resident_count(&db.pool, node_id).await?, 0);
    Ok(())
}

/// The kill switch removes the check *and* the lock, restoring the exact prior
/// behaviour.
///
/// Asserted against a `terminating` node — the one the enforcing fence refuses —
/// so this cannot pass by disabling nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_kill_switch_restores_the_unfenced_write() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    sqlx::query(sql!("UPDATE nodes SET drain_phase = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(PHASE_TERMINATING)
        .execute(&db.pool)
        .await?;

    let mut tx = db.pool.begin().await?;
    assert_eq!(
        fence_node_for_placement(&mut tx, node_id.as_uuid(), None, false).await?,
        PlacementFence::Open,
        "a disabled fence must accept a node the enabled one refuses"
    );
    // Nothing is holding this row: the disabled fence issued no statement, so a
    // concurrent `FOR UPDATE` proceeds immediately. That is what "removes the
    // lock as well as the check" means, and it is the half a verdict assertion
    // alone cannot see.
    let contended = async {
        let mut probe = db.pool.begin().await?;
        sqlx::query(sql!("SELECT set_config('lock_timeout', '250', TRUE)"))
            .execute(&mut *probe)
            .await?;
        sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE"))
            .bind(node_id)
            .execute(&mut *probe)
            .await?;
        probe.rollback().await
    }
    .await;
    assert!(
        contended.is_ok(),
        "the disabled fence must hold no lock on the nodes row; a 55P03 here means it \
         locked anyway: {contended:?}"
    );
    tx.rollback().await?;

    // And the sandbox goes on, unfenced, exactly as before this change.
    insert_sandbox(&db.pool, node_id, "started").await?;
    assert_eq!(resident_count(&db.pool, node_id).await?, 1);
    Ok(())
}

/// **The carve-out must not answer from a snapshot taken before it waited.**
///
/// This is the test that catches a merge of the fence's two statements into one.
/// A single locking `SELECT` carrying a correlated `EXISTS` over `sandboxes` looks
/// strictly better — one round trip, and the two facts "share a snapshot" — and it
/// is wrong in exactly the case the fence exists for. `FOR KEY SHARE` triggers
/// EvalPlanQual on the `nodes` tuple, so the *phase* is re-read from the newest
/// committed version; EPQ runs the rest of the plan against the snapshot taken
/// **before the wait**, so the subquery is not. Merged, the fence sees a fresh
/// `drained` and a stale "already attached", takes the re-affirmation carve-out,
/// and returns `Open` for what is by then a *first* attach onto a node one
/// terminator tick from the provider call.
///
/// The interleaving below is the reachable one, not a contrived one. Several
/// production paths clear `active_node_id` on a node being drained — the
/// gone-node reap, suspend and markerless-restoring recovery, the
/// suspended-execution reaper, quarantine rollback — so a detach landing while a
/// placement waits on the phase writer's lock is ordinary.
///
/// `pod_placement` is the writer that realises it: its `UPDATE` guard is
/// `active_node_id IS DISTINCT FROM $1 OR …`, a "something differs" predicate, so
/// a now-NULL column makes it match and it writes the placement back.
///
/// The create finalizer is *partly* spared, and the earlier version of this
/// comment overstated it. Its `active_node_id IS NOT DISTINCT FROM $3` conjunct is
/// only the **third** disjunct of the `WHERE` — the already-`started` idempotent
/// arm. The two arms that perform a first attach (`$6 IS NULL AND
/// lifecycle = 'creating'`, and the restoring arm) carry no such comparison, so a
/// stale carve-out there is a bad write, not a lost CAS. Which is the point:
/// testing only one writer would have missed this, and reasoning about only one
/// arm of one writer nearly did.
#[tokio::test(flavor = "current_thread")]
async fn the_carve_out_sees_a_detach_that_landed_while_the_fence_waited() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(3).await?
    else {
        return Ok(());
    };
    let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
    let sandbox_id = insert_sandbox(&db.pool, node_id, "started").await?;

    // The phase writer, holding the row lock across its own commit.
    let mut advance = db.pool.begin().await?;
    assert_eq!(
        lock_and_advance_to_drained(&mut advance, node_id).await?,
        0,
        "the resident sandbox must hold the guarded advance; a 1 here means the \
         fixture is not exercising a contended row"
    );
    // The guard refused the advance, so drive the phase directly — the subject
    // here is the fence's snapshot, not the advance gate.
    sqlx::query(sql!("SELECT set_config('lock_timeout', '5000', TRUE)"))
        .execute(&mut *advance)
        .await?;
    sqlx::query(sql!("UPDATE nodes SET drain_phase = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(PHASE_DRAINED)
        .execute(&mut *advance)
        .await?;

    // **Acquire the racing task's connection before spawning it, and BEGIN on it
    // by hand.** A `pool.begin()` inside the task was a 50% flake: on a
    // `current_thread` runtime the task only advances when this one awaits, and
    // establishing a fresh pooled connection (TCP, auth, the after-connect type
    // cache) has to finish *and* the locking `SELECT` has to register as blocked
    // in `pg_locks` before `wait_until_blocked`'s bounded poll gives up. Half the
    // time it did not, and the test failed on plumbing with
    // "never blocked on the row lock" — an assertion about the harness, wearing
    // the words of an assertion about the product.
    //
    // Handing the task a live connection makes its first act the locking
    // statement itself. `wait_until_blocked`'s deadline stays as it is, because it
    // is bounded by the lock timeout for a reason.
    let mut racing_conn = db.pool.acquire().await?;
    sqlx::query(sql!("BEGIN"))
        .execute(&mut *racing_conn)
        .await?;
    let racing = tokio::spawn(async move {
        let fence = fence_node_for_placement(
            &mut racing_conn,
            node_id.as_uuid(),
            // No execution name: this test is the direct-substrate shape of
            // the carve-out, where the node match is the whole evidence.
            Some(Reaffirmation {
                sandbox_id: sandbox_id.as_uuid(),
                active_k8s_node_name: None,
            }),
            true,
        )
        .await?;
        sqlx::query(sql!("ROLLBACK"))
            .execute(&mut *racing_conn)
            .await?;
        Ok::<_, sqlx::Error>(fence)
    });

    assert!(
        wait_until_blocked(&mut advance).await?,
        "the fence never blocked on the `nodes` row lock, so this test cannot \
         observe the snapshot it exists to check"
    );

    // The detach lands *while* the fence is parked on the lock. A third
    // connection, hence the raised pool cap.
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    advance.commit().await?;

    assert_eq!(
        racing.await.expect("the racing fence must not panic")?,
        PlacementFence::Drained,
        "the fence unblocked to a committed `drained` and a sandbox that is no \
         longer attached, so this is a first attach and must be refused — an \
         `Open` here means the carve-out answered from the pre-wait snapshot"
    );
    Ok(())
}

/// The interlock that makes [`Writer::CreateFinalize`]'s re-affirmation carve-out
/// safe on the Kubernetes restore path.
///
/// A review round argued the carve-out opens that path: `persist_pod_node_placement`
/// writes `active_node_id` during the pod-ready wait, so by the time
/// `finalize_created_sandbox` fences, the row already names the node, the carve-out
/// fires, and the finalizer — the only *controller-level* writer left on a restore —
/// cannot refuse. Every step of that chain is real. The conclusion needs one more
/// thing: for the finalizer to meet a refusing node, the node has to *reach* a
/// refusing phase while the sandbox is attached. It cannot, and this is the test of
/// that rather than an argument for it, because the argument was the author's and
/// crosses two subsystems no other test in this change touches.
///
/// **Both guard scopes, because the mechanism differs between them and production
/// can run either.** `draining → drained` is guarded by an include-list under
/// [`GuardScope::Resident`] (`restoring` ∈ `RESIDENT_LIFECYCLES`) and by an
/// exclusion-list under [`GuardScope::Wide`] (`restoring` ∉
/// `NON_RESIDENT_LIFECYCLES`, which is fail-closed for anything unclassified).
/// `ReconcilerConfig`'s struct default is `false` and the env knob can set either,
/// so covering one scope would leave the other's mechanism unasserted. An earlier
/// revision of this test asserted only the `Wide` path while its own docstring
/// credited the include-list — the mutation check caught that, since removing
/// `Restoring` from `RESIDENT_LIFECYCLES` left the test green.
///
/// The reverse ordering is covered by a different writer: if the node drained
/// first, `PodPlacement` refuses, because a Kubernetes suspend NULLs
/// `active_node_id` and so *its* carve-out cannot fire either.
///
/// The `deleted` case is the control. Without it, the refusal could come from
/// anything in the fixture — the assertion needs a lifecycle that reaches the same
/// code and lets the edge through.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_attached_by_pod_placement_holds_the_node_out_of_drained() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    for gate in [false, true] {
        for (lifecycle, expected_advance) in [("restoring", false), ("deleted", true)] {
            let node_id = insert_draining_node(&db.pool, Heartbeat::Fresh).await?;
            // Stand in for `persist_pod_node_placement`: the pod-ready wait has
            // already attached the sandbox, before the finalizer runs.
            insert_sandbox(&db.pool, node_id, lifecycle).await?;

            let advanced = NodeDrainSink::set_drain_phase(
                &sink_with_gate(db.pool.clone(), ResidencyEvidence::LifecycleOnly, gate),
                node_id,
                PHASE_DRAINING,
                PHASE_DRAINED,
                None,
            )
            .await?;

            assert_eq!(
                advanced,
                expected_advance,
                "gate={gate}, `{lifecycle}` sandbox attached: expected the \
                 `draining -> drained` edge to {} advance",
                if expected_advance { "" } else { "not" }
            );
            assert_eq!(
                drain_phase(&db.pool, node_id).await?.as_deref(),
                Some(if expected_advance {
                    PHASE_DRAINED
                } else {
                    PHASE_DRAINING
                }),
                "gate={gate}: committed phase disagrees with the reported outcome \
                 for `{lifecycle}`"
            );
        }
    }
    Ok(())
}
