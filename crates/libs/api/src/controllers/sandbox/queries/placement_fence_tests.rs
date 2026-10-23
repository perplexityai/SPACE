//! Postgres-backed proof that the sandbox controller's placement writers
//! actually consult the termination fence.
//!
//! The *ordering* the fence provides is proved in the cluster-controller, next
//! to the phase writers it has to be ordered against
//! (`node_drain/store/placement_fence_tests.rs`) — that is where two live
//! transactions can be interleaved. These tests answer the other, duller half,
//! and it is the half the fence's enumeration risk lives in: **does this
//! particular writer call it at all.** Deleting a `check_placement_fence` call
//! site leaves the interleaving tests green, because they drive the fence
//! directly.
//!
//! So each test pre-commits a fenced phase and asserts the writer refuses, and
//! each is paired with an open-node case. Without the pair, a writer that failed
//! for any other reason would look fenced.
//!
//! **Four of the six call sites are pinned this way, not all six.** `pooled_create`
//! and `pooled_post_wake` are here, `pod_placement` is in the k8s store's Postgres
//! tests, and `create_finalize` is exercised by `create_postgres_tests` (whose
//! fixtures all run through the fence with an open node, so a fence that refused
//! wrongly would break them). `post_create_placement` and `direct_restore` sit
//! inside `create_sandbox` and the direct substrate's restore-receipt path, which
//! need a substrate adapter to reach; their fence call is the same one-line shape
//! and the ordering it relies on is pinned in the cluster-controller. Recorded so
//! nobody reads the four as six.

use super::create_postgres_tests::{
    insert_actor, insert_node, pooled_claim, pooled_create_input, pooled_create_request,
};
use super::*;

use pplx_space_common_core::types::{NodeDrainPhase, TemplateName, TemplateReference};
use pplx_space_common_svc::sql::node_placement_fence::PlacementFence;

use crate::controllers::placement_fence::{
    Writer, check_placement_fence, check_placement_fence_for_execution,
};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

/// A node in a phase the fence refuses. `insert_node` writes a live `ready`
/// worker (`drain_phase IS NULL`), which is what the open-node halves need, so
/// the phase is applied on top rather than parameterized into the fixture.
async fn drain_node(pool: &PgPool, node_id: NodeId, phase: NodeDrainPhase) -> TestResult {
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = $2, drain_started_at = NOW() WHERE id = $1"
    ))
    .bind(node_id)
    .bind(phase.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

async fn attached_node(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<NodeId>> {
    Ok(sqlx::query_scalar::<_, Option<NodeId>>(sql!(
        "SELECT active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// `insert_pooled_sandbox` must refuse a claim whose node has been committed to
/// `terminating`, and must leave no row behind — the pooled create's whole
/// refusal story is that the transaction rolls back and the caller hands the
/// warm slot back.
#[tokio::test(flavor = "current_thread")]
async fn the_pooled_create_refuses_a_terminating_node() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let req = pooled_create_request("fenced-pooled-sandbox", None);
    let template_network_policy = NetworkPolicyV1::deny_all();
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let create_owner = SandboxCreateOwner::local(&actor);
    let input = pooled_create_input(
        &db.pool,
        &req,
        &requested_template,
        &auto_lifecycle,
        &template_network_policy,
        &create_owner,
    );

    let fenced_node = NodeId::new();
    insert_node(&db.pool, fenced_node).await?;
    drain_node(&db.pool, fenced_node, NodeDrainPhase::Terminating).await?;
    let fenced_sandbox = SandboxId::new();
    let mut tx = db.pool.begin().await?;
    let refused = insert_pooled_sandbox(
        &mut tx,
        &input,
        &create_owner,
        &pooled_claim(fenced_sandbox, fenced_node),
        "space",
    )
    .await;
    assert!(
        refused.is_err(),
        "a pooled create onto a `terminating` node must be refused"
    );
    drop(tx);
    let exists: bool =
        sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"))
            .bind(fenced_sandbox)
            .fetch_one(&db.pool)
            .await?;
    assert!(
        !exists,
        "the refused pooled create must leave no sandbox row"
    );

    // The pair: an ordinary live node still takes the create. Without this the
    // test above would pass on a writer that had simply stopped working.
    let open_node = NodeId::new();
    insert_node(&db.pool, open_node).await?;
    let open_sandbox = SandboxId::new();
    let mut tx = db.pool.begin().await?;
    insert_pooled_sandbox(
        &mut tx,
        &input,
        &create_owner,
        &pooled_claim(open_sandbox, open_node),
        "space",
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        attached_node(&db.pool, open_sandbox).await?,
        Some(open_node)
    );
    Ok(())
}

/// The pooled post-wake writer must refuse a `drained` node and leave the row's
/// lifecycle alone.
///
/// `drained` rather than `terminating` on purpose: it is the phase a node reaches
/// by converging on its own, so it is the one an operator never requested. It
/// refuses for the same reason — a sandbox published as `started` there is
/// refused by the terminator on every tick from then on.
///
/// **The fixture attaches from NULL, and today's only production caller does
/// not.** The pooled create commits the same `active_node_id` at
/// `insert_pooled_sandbox` a few steps earlier (fenced there), so by the time
/// this writer runs the row is already on the node and the already-attached
/// carve-out makes this check a no-op on that path. What this pins is the check
/// itself, for the next caller of a `pub` helper that may attach from NULL — not
/// a refusal the pooled path can currently reach. Recorded so the test is not
/// read as evidence of the latter.
#[tokio::test(flavor = "current_thread")]
async fn the_pooled_post_wake_refuses_a_drained_node() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    drain_node(&db.pool, node_id, NodeDrainPhase::Drained).await?;

    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'paused', 'unplaced', 'k8s', 1
         )"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let refused = update_and_fetch_post_wake_sandbox(&db.pool, sandbox_id, Some(node_id)).await;
    assert!(
        refused.is_err(),
        "a pooled post-wake onto a `drained` node must be refused"
    );
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        lifecycle, "paused",
        "the refused wake must not publish the sandbox as started"
    );
    assert_eq!(attached_node(&db.pool, sandbox_id).await?, None);

    // The pair: undrain the same node and the same call succeeds. Same row, so a
    // fixture problem cannot explain the refusal above.
    sqlx::query(sql!("UPDATE nodes SET drain_phase = NULL WHERE id = $1"))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
    let woken = update_and_fetch_post_wake_sandbox(&db.pool, sandbox_id, Some(node_id)).await?;
    assert_eq!(woken.lifecycle, "started");
    assert_eq!(attached_node(&db.pool, sandbox_id).await?, Some(node_id));
    Ok(())
}

/// A gentle drain must keep working. The fence stops at `drained`, so a wake
/// onto a node that is merely `draining` still lands — that mode exists so
/// residents keep serving until they leave on their own, and refusing here would
/// manufacture the disruption the operator declined.
#[tokio::test(flavor = "current_thread")]
async fn a_draining_node_still_takes_the_pooled_post_wake() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    drain_node(&db.pool, node_id, NodeDrainPhase::Draining).await?;

    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'paused', 'unplaced', 'k8s', 1
         )"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;

    let woken = update_and_fetch_post_wake_sandbox(&db.pool, sandbox_id, Some(node_id)).await?;
    assert_eq!(woken.lifecycle, "started");
    assert_eq!(attached_node(&db.pool, sandbox_id).await?, Some(node_id));
    Ok(())
}

/// **The carve-out that keeps the fence from destroying what it protects.** A
/// sandbox already attached to the node is re-stated, not attached, so the fence
/// must stay open even when the node is `terminating`.
///
/// This is not a nicety. `finalize_created_sandbox` has an idempotent arm that
/// preserves an existing `started`/`on_node` placement, and its caller responds
/// to a refusal by calling `rollback_created_sandbox` — which deletes the
/// execution *and* the sandbox row. So a fence that refused here would delete a
/// live, already-placed user sandbox the moment its node reached `drained`, which
/// is a park state for an operator to resolve by hand. The fence would become the thing it exists to prevent.
///
/// Asserted on a `terminating` node — the strongest verdict — so it cannot pass
/// by the fence being open for some milder reason.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_already_on_the_node_is_re_stated_not_refused() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;

    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, network_policy_generation, placement_changed_at
         ) VALUES ($1, $2, $3, 'started', 'on_node', 'k8s', $4, 1, NOW())"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    drain_node(&db.pool, node_id, NodeDrainPhase::Terminating).await?;

    let mut tx = db.pool.begin().await?;
    assert_eq!(
        check_placement_fence(&mut tx, node_id, sandbox_id, Writer::CreateFinalize).await?,
        PlacementFence::Open,
        "a row already naming this node must not be refused — its caller's refusal \
         path deletes the sandbox"
    );
    tx.rollback().await?;

    // The pair, on the same terminating node: a *different* sandbox, not yet
    // attached, is refused. Without this the assertion above would pass on a
    // fence that had stopped working.
    let newcomer = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_generation
         ) VALUES ($1, $2, $3, 'creating', 'unplaced', 'k8s', 1)"
    ))
    .bind(newcomer)
    .bind(actor.id)
    .bind(actor.org_id)
    .execute(&db.pool)
    .await?;
    let mut tx = db.pool.begin().await?;
    assert!(
        check_placement_fence(&mut tx, node_id, newcomer, Writer::CreateFinalize)
            .await?
            .refuses(),
        "a sandbox not yet on this node must still be refused"
    );
    tx.rollback().await?;
    Ok(())
}

/// The restore path's fence, end to end through the writer that owns it.
///
/// `finalize_created_sandbox` is the **only** `active_node_id` write on a restore
/// (`restore_operation_id.is_some()` skips the placement-only `UPDATE`), so
/// fencing the placement update and not this one would have left restores
/// unfenced — the gap that a count of "four writers" hid.
#[tokio::test(flavor = "current_thread")]
async fn the_create_finalizer_refuses_a_terminating_node() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;
    drain_node(&db.pool, node_id, NodeDrainPhase::Terminating).await?;

    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_generation
         ) VALUES ($1, $2, $3, 'creating', 'unplaced', 'k8s', 1)"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .execute(&db.pool)
    .await?;

    let refused = finalize_created_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        "on_node",
        Some(node_id),
        true,
        None,
    )
    .await?;
    assert_eq!(
        refused,
        FinalizeOutcome::Fenced(PlacementFence::Terminating),
        "the finalizer must report the fence, not a lost CAS — the two take the \
         same rollback but say different things to the operator"
    );
    assert_eq!(attached_node(&db.pool, sandbox_id).await?, None);
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        lifecycle, "creating",
        "the refused finalize must not publish the sandbox as started"
    );

    // The pair: undrain and the same call finalizes.
    sqlx::query(sql!("UPDATE nodes SET drain_phase = NULL WHERE id = $1"))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
    assert!(
        finalize_created_sandbox(
            &db.pool,
            sandbox_id,
            "started",
            "on_node",
            Some(node_id),
            true,
            None,
        )
        .await?
        .is_finalized()
    );
    assert_eq!(attached_node(&db.pool, sandbox_id).await?, Some(node_id));
    Ok(())
}

/// **A writer denied the re-affirmation carve-out must still be refused**, even
/// on a row that already names the node.
///
/// This is the assertion whose absence let a live hole ship. The carve-out was
/// scoped by `Writer::takes_reaffirmation_carve_out` in one commit, but the call
/// site kept passing the sandbox id unconditionally, so the predicate was dead
/// code and every writer still got the carve-out. Nothing failed: the existing
/// tests all assert either a refusal on an unattached row or the carve-out
/// *firing*, and neither shape can see a carve-out that fires too widely.
///
/// `direct_restore` is the writer that matters. `active_node_id` survives a
/// suspend on the direct substrate, so a restore aimed back at the source node
/// meets a row that still names it — indistinguishable from a re-affirmation, and
/// not one: the guest is not running there, this is a fresh attach, and admitting
/// it publishes a newly restored sandbox on an instance whose ASG terminate may
/// already be in flight. It loses nothing by being refused, because its refusal
/// is `RestoreInFlight` — the snapshot is kept and the restore retries elsewhere.
#[tokio::test(flavor = "current_thread")]
async fn a_writer_without_the_carve_out_is_refused_on_its_own_node() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id).await?;

    // Suspended, and still naming the node — the direct-suspend shape.
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, network_policy_generation, placement_changed_at
         ) VALUES ($1, $2, $3, 'suspended', 'on_node', 'space', $4, 1, NOW())"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    drain_node(&db.pool, node_id, NodeDrainPhase::Terminating).await?;

    let mut tx = db.pool.begin().await?;
    assert!(
        check_placement_fence(&mut tx, node_id, sandbox_id, Writer::DirectRestore)
            .await?
            .refuses(),
        "a writer without the carve-out must be refused even on a row that already \
         names this node — its `active_node_id` is a historical placement, not a \
         live one, so admitting it is a first attach onto a terminating node"
    );
    tx.rollback().await?;

    // The pair, on the same row and the same node: a writer that *does* hold the
    // carve-out is let through. Without this the assertion above would pass on a
    // fence that had simply stopped carving out for anyone.
    //
    // `PooledPostWake` rather than `PodPlacement`, and the difference is not
    // cosmetic. `check_placement_fence` forwards `active_k8s_node_name: None`,
    // which is the real production shape for this writer and for
    // `CreateFinalize` — but *not* for `PodPlacement`, whose only production
    // caller always passes `Some(node_name)` through
    // `check_placement_fence_for_execution`. Asserting `Open` for
    // `PodPlacement` here would pin a contract its production caller cannot
    // obtain: with this fixture's row carrying no `sandbox_k8s_executions` row,
    // the execution probe fails and the real verdict is `Terminating`.
    let mut tx = db.pool.begin().await?;
    assert_eq!(
        check_placement_fence(&mut tx, node_id, sandbox_id, Writer::PooledPostWake).await?,
        PlacementFence::Open,
        "a writer whose refusal tears the sandbox down must keep the carve-out"
    );
    tx.rollback().await?;

    // And the third case, which is what `PodPlacement` actually does in
    // production: the same row, the same node, but the *execution* named too.
    // The row carries no `sandbox_k8s_executions` row, so the execution probe
    // finds no match and the carve-out correctly does not fire — an inherited
    // `active_node_id` is not proof that *this* execution belongs here.
    //
    // This is the assertion that keeps the case above sound. Passing
    // `Writer::PodPlacement` through the `None` entry point would report `Open`,
    // which is why the control uses a writer whose production shape really is
    // `None`; without this pair, a reader could conclude pod placement takes the
    // node-only carve-out, which is the weakness the execution probe closed.
    let mut tx = db.pool.begin().await?;
    assert_eq!(
        check_placement_fence_for_execution(
            &mut tx,
            node_id,
            sandbox_id,
            Writer::PodPlacement,
            Some("space-test-some-other-node"),
        )
        .await?,
        PlacementFence::Terminating,
        "pod placement must not inherit the carve-out from a row that names the          node when no active execution does"
    );
    tx.rollback().await?;
    Ok(())
}

async fn insert_lifecycle_row(
    pool: &PgPool,
    actor: &User,
    lifecycle: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_override_json, network_policy_generation
         ) VALUES ($1, $2, $3, $4, 'on_node', 'k8s', $5, 1)"
    ))
    .bind(sandbox_id)
    .bind(actor.id)
    .bind(actor.org_id)
    .bind(lifecycle)
    .bind(sqlx::types::Json(&NetworkPolicyV1::default()))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn insert_execution(pool: &PgPool, sandbox_id: SandboxId, active: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, active, state)
         VALUES ($1, 1, 'test-pod-' || $1::text, 'space', $2,
                 CASE WHEN $2 THEN 'creating' ELSE 'forgotten' END)"
    ))
    .bind(sandbox_id)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

async fn row_exists(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1)"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// A create that fails after its pod exists, and whose cancel guard has already
/// claimed the row for durable cleanup, must keep the row: that cleanup finds
/// the pod through the row's execution record, so deleting the row first leaves
/// the pod live with nothing tracking it. The claim is what decides, not the
/// error variant — a plain `Failed` is the case that used to lose the row.
///
/// The pair: a `creating` row with no such claim is still removed, so the guard
/// on the `DELETE` is not simply keeping every row.
#[tokio::test(flavor = "current_thread")]
async fn create_failure_cleanup_leaves_a_row_claimed_for_durable_cleanup() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let failed = SubstrateError::Failed {
        message: "placement failed after the pod existed".to_string(),
    };

    let claimed = insert_lifecycle_row(&db.pool, &actor, "deleting").await?;
    clean_up_substrate_create_failure(&db.pool, claimed, &failed).await?;
    assert!(
        row_exists(&db.pool, claimed).await?,
        "a row the cancel guard claimed must survive for its cleanup to find the pod"
    );

    let unclaimed = insert_lifecycle_row(&db.pool, &actor, "creating").await?;
    clean_up_substrate_create_failure(&db.pool, unclaimed, &failed).await?;
    assert!(
        !row_exists(&db.pool, unclaimed).await?,
        "an unclaimed create's row is still removed on failure"
    );
    Ok(())
}

/// A `RestoreInFlight` failure keeps the row only while substrate work still
/// owns it.
///
/// - A row the cancel guard already claimed (`deleting`) is about to disappear,
///   so the caller isn't offered a retry of it.
/// - A `creating` or `restoring` shell with no active execution is dead:
///   nothing is left for a retry to converge, and its pod is the readiness
///   wait's or the cancel guard's to delete. It's deleted, as any other failed
///   create's row is.
/// - A `creating` row that still has an active execution stays for retry, so
///   the shell case isn't just deleting every row.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_in_flight_failure_keeps_the_row_only_while_substrate_work_owns_it() -> TestResult
{
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let in_flight = SubstrateError::restore_in_flight("placement refused");

    let claimed = insert_lifecycle_row(&db.pool, &actor, "deleting").await?;
    assert_eq!(
        clean_up_substrate_create_failure(&db.pool, claimed, &in_flight).await?,
        SubstrateCreateFailureDisposition::Deleted
    );
    assert!(
        row_exists(&db.pool, claimed).await?,
        "the claimed row is the guard's to delete, after its pod"
    );

    // Each shell keeps the retired execution a real one does, so dropping the
    // `active` term from the delete's `NOT EXISTS` would keep it and fail here.
    let shell = insert_lifecycle_row(&db.pool, &actor, "creating").await?;
    insert_execution(&db.pool, shell, false).await?;
    assert_eq!(
        clean_up_substrate_create_failure(&db.pool, shell, &in_flight).await?,
        SubstrateCreateFailureDisposition::Deleted
    );
    assert!(
        !row_exists(&db.pool, shell).await?,
        "the dead shell is deleted"
    );

    // A fork restored as a new sandbox starts `restoring`; its dead shell goes
    // the same way, or the fork's executor would wait on it instead of
    // re-creating the target.
    let fork_shell = insert_lifecycle_row(&db.pool, &actor, "restoring").await?;
    insert_execution(&db.pool, fork_shell, false).await?;
    assert_eq!(
        clean_up_substrate_create_failure(&db.pool, fork_shell, &in_flight).await?,
        SubstrateCreateFailureDisposition::Deleted
    );
    assert!(
        !row_exists(&db.pool, fork_shell).await?,
        "the dead fork shell is deleted"
    );

    let live = insert_lifecycle_row(&db.pool, &actor, "creating").await?;
    insert_execution(&db.pool, live, true).await?;
    assert_eq!(
        clean_up_substrate_create_failure(&db.pool, live, &in_flight).await?,
        SubstrateCreateFailureDisposition::RetainedForRetry
    );
    assert!(row_exists(&db.pool, live).await?);
    Ok(())
}

/// An execution committed while the dead-shell delete waits on the sandbox row
/// must keep the row. Execution creation takes that row lock, and a `NOT EXISTS`
/// folded into the waiting `DELETE` would be evaluated against its pre-wait
/// snapshot, missing the new execution and cascading it away with the row.
///
/// Two real transactions: the holder locks the row and inserts an active
/// execution; cleanup starts and blocks on the lock; the holder commits.
#[tokio::test(flavor = "current_thread")]
async fn a_shell_delete_waiting_on_the_row_sees_an_execution_committed_meanwhile() -> TestResult {
    // Three connections at once: the holder, the cleanup, and the poller.
    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = insert_lifecycle_row(&db.pool, &actor, "creating").await?;

    let mut holder = db.pool.begin().await?;
    let holder_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *holder)
        .await?;
    sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .execute(&mut *holder)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, active, state)
         VALUES ($1, 1, 'test-pod-' || $1::text, 'space', TRUE, 'creating')"
    ))
    .bind(sandbox_id)
    .execute(&mut *holder)
    .await?;

    let pool = db.pool.clone();
    let cleanup = tokio::spawn(async move {
        clean_up_substrate_create_failure(
            &pool,
            sandbox_id,
            &SubstrateError::restore_in_flight("placement refused"),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                      WHERE datname = current_database()
                        AND $1 = ANY(pg_blocking_pids(pid))
                 )"
            ))
            .bind(holder_pid)
            .fetch_one(&db.pool)
            .await?;
            if blocked {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    holder.commit().await?;

    assert_eq!(
        cleanup.await??,
        SubstrateCreateFailureDisposition::RetainedForRetry
    );
    assert!(
        row_exists(&db.pool, sandbox_id).await?,
        "the row and the execution committed during the wait survive"
    );
    Ok(())
}

/// Property: if anyone claims the row for deletion while create-failure cleanup
/// runs, the client is never told to retry that sandbox.
///
/// Two real transactions: the holder locks the row and moves it to `deleting`
/// (a concurrent delete's claim), cleanup starts and blocks on the lock, then the
/// holder commits. A lifecycle read taken before the lock would still say
/// `creating`, and the later shell delete would then match nothing and report
/// `RetainedForRetry` for a row on its way out.
#[tokio::test(flavor = "current_thread")]
async fn a_delete_claimed_while_cleanup_waits_is_not_offered_for_retry() -> TestResult {
    // Three connections at once: the holder, the cleanup, and the poller.
    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox_id = insert_lifecycle_row(&db.pool, &actor, "creating").await?;

    let mut holder = db.pool.begin().await?;
    let holder_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *holder)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *holder)
    .await?;

    let pool = db.pool.clone();
    let cleanup = tokio::spawn(async move {
        clean_up_substrate_create_failure(
            &pool,
            sandbox_id,
            &SubstrateError::restore_in_flight("placement refused"),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                      WHERE datname = current_database()
                        AND $1 = ANY(pg_blocking_pids(pid))
                 )"
            ))
            .bind(holder_pid)
            .fetch_one(&db.pool)
            .await?;
            if blocked {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    holder.commit().await?;

    assert_eq!(
        cleanup.await??,
        SubstrateCreateFailureDisposition::Deleted,
        "a row claimed for deletion during the wait must not be offered for retry"
    );
    assert!(
        row_exists(&db.pool, sandbox_id).await?,
        "the row is its claimant's to delete, not this cleanup's"
    );
    Ok(())
}
