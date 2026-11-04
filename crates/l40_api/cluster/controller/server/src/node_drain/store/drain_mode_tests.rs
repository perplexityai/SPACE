//! Postgres-backed coverage for how this reconciler reads and re-checks
//! `nodes.drain_mode`.
//!
//! What is pinned here:
//!
//! - The **draining listing** keeps returning a node whatever its column says,
//!   and resolves NULL and unrecognized values to gentle. A typed decode would
//!   fail the row and drop the node out of the work list entirely, so a typo
//!   would stop the node being drained at all rather than draining it gently.
//! - The listing is scoped to Kubernetes-managed nodes: a direct node in the
//!   same phase is never handed to this pipeline.
//! - The **evacuation claim** re-asserts `drain_phase = 'draining' AND
//!   drain_mode = 'forced'` inside its own UPDATE, so a tick holding a stale
//!   `Forced` cannot suspend a resident of a drain that has since gone gentle.
//!
//! Scoped to the *suspend* claim deliberately: it is not a general mid-tick
//! guarantee for the tick. `abandon_stale_resident_sandboxes_query` runs earlier
//! in the same `handle_draining_node` and is gated only on the tick's in-memory
//! `mode.evacuates_residents()` — its SQL re-asserts `drain_phase = 'draining'`
//! with no `drain_mode` conjunct — so a stale `Forced` can still flip an aged
//! transient to `lifecycle = 'failed'` on a node that is now gentle. In-place
//! escalation does not widen that gap: reaching `forced → gentle` still takes
//! `:undrain` plus a fresh `:drain`, because the admin endpoint refuses the
//! in-place downgrade. It is called out here so the claim above is not read as
//! covering the whole tick.

use pplx_space_common_core::types::{NodeId, SandboxId};

use pplx_space_db_test_fixtures::try_pg_test_db;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Insert a node in `drain_phase`, with the given mode, whose drain started
/// `started_secs_ago` ago. `mode = None` writes SQL NULL — the shape of a row
/// whose writer set no mode.
async fn insert_drained_node(
    pool: &PgPool,
    node_id: NodeId,
    managed_by: &str,
    drain_phase: Option<&str>,
    mode: Option<NodeDrainMode>,
    started_secs_ago: Option<i64>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (
             id, address, role, managed_by, state,
             drain_phase, drain_mode, drain_started_at
         )
         VALUES (
             $1, '10.0.0.77'::inet, 'worker', $2, 'registered',
             $3, $4,
             CASE WHEN $5::BIGINT IS NULL
                  THEN NULL
                  ELSE NOW() - make_interval(secs => $5::BIGINT)
             END
         )"
    ))
    .bind(node_id)
    .bind(managed_by)
    .bind(drain_phase)
    .bind(mode)
    .bind(started_secs_ago)
    .execute(pool)
    .await?;
    Ok(())
}

/// Store an arbitrary string in `drain_mode`, bypassing [`NodeDrainMode`]
/// entirely. Possible because the column carries no `CHECK` constraint — the
/// closed set is enforced in application code — which is exactly the condition
/// the lenient column read exists for.
async fn write_raw_mode(pool: &PgPool, node_id: NodeId, raw: &str) -> TestResult {
    sqlx::query(sql!("UPDATE nodes SET drain_mode = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(raw)
        .execute(pool)
        .await?;
    Ok(())
}

/// The evacuation claim refuses a node that is no longer force-draining.
///
/// The reconciler resolves a node's mode when it lists draining nodes and then
/// works through that node's residents. An operator who lands `:undrain` followed
/// by a gentle `:drain` inside that window leaves the tick holding a stale
/// `Forced`, and without the claim re-asserting the node's state it would suspend
/// and migrate residents of a drain that is now explicitly gentle. Checking it in
/// the claim makes the decision atomic with the write.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_claim_refuses_a_node_that_turned_gentle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_drained_node(
        &db.pool,
        node_id,
        "k8s",
        Some("draining"),
        Some(NodeDrainMode::Forced),
        Some(60),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, network_policy_generation
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'started', 'on_node', 'k8s', $2, $2, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    // While the node is still forced, the claim takes the sandbox.
    assert!(
        claim_evacuation_suspend(&db.pool, sandbox_id, node_id, SandboxLifecycle::Started,).await?,
        "a forced drain must be claimable"
    );

    // Reset the sandbox, then model the operator's undrain + gentle re-drain.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started', evacuation_restore_target = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!("UPDATE nodes SET drain_mode = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(NodeDrainMode::Gentle)
        .execute(&db.pool)
        .await?;

    assert!(
        !claim_evacuation_suspend(&db.pool, sandbox_id, node_id, SandboxLifecycle::Started,)
            .await?,
        "a gentle drain must not be claimable, even from a tick that listed it as forced"
    );
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        lifecycle, "started",
        "and the sandbox must be left running, not moved to suspending"
    );
    Ok(())
}

/// A typo stored in `drain_mode` must not break the drain work list.
///
/// With no `CHECK` on the column, `'gently'` is a value Postgres will accept
/// from any writer, so this is reachable in production rather than hypothetical.
/// The listing has to keep returning the node (a typed `Option<NodeDrainMode>`
/// decode would fail the row and drop it out of the reconciler's work list
/// entirely, so the node would stop being drained at all) and has to resolve it
/// to gentle rather than to the evacuating mode.
#[tokio::test(flavor = "current_thread")]
async fn unrecognized_column_value_still_lists_the_node_as_a_gentle_drain() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_drained_node(
        &db.pool,
        node_id,
        "k8s",
        Some("draining"),
        Some(NodeDrainMode::Gentle),
        Some(30),
    )
    .await?;
    write_raw_mode(&db.pool, node_id, "gently").await?;

    let rows: Vec<(NodeId, Option<String>)> = sqlx::query_as(draining_nodes_query())
        .bind(PHASE_DRAINING)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&db.pool)
        .await?;
    let raw = rows
        .iter()
        .find(|(id, _)| *id == node_id)
        .map(|(_, mode)| mode.clone())
        .ok_or("the node must still appear in the draining listing")?;
    assert_eq!(raw.as_deref(), Some("gently"));
    assert_eq!(
        NodeDrainMode::from_column_str(raw.as_deref()),
        NodeDrainMode::Gentle,
        "an unrecognized mode must not select the evacuating branch"
    );
    Ok(())
}

/// The listing carries the mode, and a NULL column reads as gentle — the
/// reconciler must not evacuate a node's residents on the strength of a mode
/// nobody set.
#[tokio::test(flavor = "current_thread")]
async fn draining_listing_reports_mode_with_null_reading_as_gentle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let gentle = NodeId::new();
    let forced = NodeId::new();
    let null_mode = NodeId::new();
    insert_drained_node(
        &db.pool,
        gentle,
        "k8s",
        Some("draining"),
        Some(NodeDrainMode::Gentle),
        Some(30),
    )
    .await?;
    insert_drained_node(
        &db.pool,
        forced,
        "k8s",
        Some("draining"),
        Some(NodeDrainMode::Forced),
        Some(30),
    )
    .await?;
    insert_drained_node(&db.pool, null_mode, "k8s", Some("draining"), None, Some(30)).await?;

    let rows: Vec<(NodeId, Option<String>)> = sqlx::query_as(draining_nodes_query())
        .bind(PHASE_DRAINING)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&db.pool)
        .await?;
    let observed: HashMap<NodeId, NodeDrainMode> = rows
        .into_iter()
        .map(|(id, mode)| (id, NodeDrainMode::from_column_str(mode.as_deref())))
        .collect();

    assert_eq!(observed.get(&gentle), Some(&NodeDrainMode::Gentle));
    assert_eq!(observed.get(&forced), Some(&NodeDrainMode::Forced));
    assert_eq!(observed.get(&null_mode), Some(&NodeDrainMode::Gentle));
    Ok(())
}

/// The draining work list binds the Kubernetes substrate: a direct node in
/// the same phase is never handed to the Kubernetes drain pipeline.
#[tokio::test(flavor = "current_thread")]
async fn draining_listing_skips_direct_nodes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let k8s_node = NodeId::new();
    let direct_node = NodeId::new();
    for (node_id, managed_by) in [(k8s_node, "k8s"), (direct_node, "space")] {
        insert_drained_node(
            &db.pool,
            node_id,
            managed_by,
            Some("draining"),
            None,
            Some(30),
        )
        .await?;
    }

    let rows: Vec<(NodeId, Option<String>)> = sqlx::query_as(draining_nodes_query())
        .bind(PHASE_DRAINING)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&db.pool)
        .await?;
    let listed: Vec<NodeId> = rows.into_iter().map(|(id, _)| id).collect();
    assert_eq!(listed, vec![k8s_node]);
    Ok(())
}
