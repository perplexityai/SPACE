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

/// Read the column back the way production does: as a raw string, resolved by
/// [`NodeDrainMode::from_column_str`]. `NodeDrainMode` has no sqlx `Decode`
/// precisely so this cannot be short-circuited into a typed read.
///
/// Returns `None` only for a genuine SQL NULL, so a test can still tell "no mode
/// written" from "gentle" — which `from_column_str` alone collapses.
async fn mode_of(pool: &PgPool, node_id: NodeId) -> TestResult<Option<NodeDrainMode>> {
    let raw: Option<String> =
        sqlx::query_scalar(sql!("SELECT drain_mode FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_one(pool)
            .await?;
    Ok(raw
        .as_deref()
        .map(|value| NodeDrainMode::from_column_str(Some(value))))
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

fn secs(value: u64) -> PositiveDuration {
    PositiveDuration::new_panic(Duration::from_secs(value))
}

/// The escalation itself: a gentle drain older than the deadline is rewritten
/// to forced, and the node id comes back so the caller can log and count it.
#[tokio::test(flavor = "current_thread")]
async fn escalates_a_gentle_drain_past_the_deadline() -> TestResult {
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
        Some(7200),
    )
    .await?;

    let escalated = escalate_stale_gentle_drains(&db.pool, &[node_id], secs(3600)).await?;
    assert_eq!(escalated, vec![node_id]);
    assert_eq!(
        mode_of(&db.pool, node_id).await?,
        Some(NodeDrainMode::Forced)
    );
    Ok(())
}

/// A gentle drain younger than the deadline is left alone. Waiting is the mode
/// working as asked; escalating early would evacuate sandboxes the operator
/// expected to be left running.
#[tokio::test(flavor = "current_thread")]
async fn leaves_a_young_gentle_drain_alone() -> TestResult {
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
        Some(60),
    )
    .await?;

    assert!(
        escalate_stale_gentle_drains(&db.pool, &[node_id], secs(3600))
            .await?
            .is_empty()
    );
    assert_eq!(
        mode_of(&db.pool, node_id).await?,
        Some(NodeDrainMode::Gentle)
    );
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

/// An unrecognized value escalates on the deadline like any other gentle drain,
/// and the write normalizes the column back to a value this build understands.
///
/// "Unrecognized reads as gentle" is meant uniformly: a node carrying `'gently'`
/// is not evacuated, and it is subject to the same escalation deadline. Carving
/// out an exception would put mode-reasoning back into the escalation path, and
/// would park such a node in `draining` forever — the failure the deadline
/// exists to prevent.
#[tokio::test(flavor = "current_thread")]
async fn escalation_normalizes_an_unrecognized_mode() -> TestResult {
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
        Some(7200),
    )
    .await?;
    write_raw_mode(&db.pool, node_id, "gently").await?;
    // The caller's classification is what makes it a candidate at all.
    assert_eq!(
        NodeDrainMode::from_column_str(Some("gently")),
        NodeDrainMode::Gentle,
        "this test's premise: an unrecognized value is gentle to every reader"
    );

    let escalated = escalate_stale_gentle_drains(&db.pool, &[node_id], secs(600)).await?;
    assert_eq!(escalated, vec![node_id]);
    assert_eq!(
        mode_of(&db.pool, node_id).await?,
        Some(NodeDrainMode::Forced),
        "escalation replaces the uninterpretable value with a known one"
    );
    Ok(())
}

/// A NULL-mode drain escalates too, and it must: every reader resolves NULL to
/// gentle (`NodeDrainMode::from_column_str`), so the reconciler has stopped
/// evacuating that node, and without an escalation path it would park in
/// `draining` forever with the deadline silently not covering it.
///
/// The UPDATE itself no longer knows what "gentle" means — it escalates the ids
/// it is handed. This test therefore pins the whole chain: the reader calls NULL
/// gentle (asserted as the premise), so the listing yields it as a candidate,
/// so the deadline applies to it.
#[tokio::test(flavor = "current_thread")]
async fn escalates_a_null_mode_drain_because_readers_call_it_gentle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    assert_eq!(
        NodeDrainMode::from_column_str(None),
        NodeDrainMode::Gentle,
        "this test's premise: a NULL column is a gentle drain to every reader"
    );
    let node_id = NodeId::new();
    insert_drained_node(&db.pool, node_id, "k8s", Some("draining"), None, Some(7200)).await?;

    let escalated = escalate_stale_gentle_drains(&db.pool, &[node_id], secs(600)).await?;
    assert_eq!(escalated, vec![node_id]);
    assert_eq!(
        mode_of(&db.pool, node_id).await?,
        Some(NodeDrainMode::Forced)
    );
    Ok(())
}

/// Every guard the UPDATE still owns, one row each. All are old enough to
/// escalate on age alone, all are handed in as candidates, and none may be
/// touched.
///
/// Mode is deliberately absent from this list: an already-`forced` node is
/// excluded by the caller, which only classifies gentle nodes as candidates, not
/// by this statement. `gm6_escalation_counts_once_per_drain` covers that half.
#[tokio::test(flavor = "current_thread")]
async fn skips_rows_every_guard_excludes() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // No start time: the age is unknown, so the deadline cannot be evaluated.
    // Fail closed rather than escalate on an assumed age.
    let no_start_time = NodeId::new();
    // Not draining any more — `drained` is past the point where evacuating
    // residents means anything.
    let drained = NodeId::new();
    // Direct/admin node: this reconciler is the EKS drain path only.
    let space_managed = NodeId::new();

    insert_drained_node(
        &db.pool,
        no_start_time,
        "k8s",
        Some("draining"),
        Some(NodeDrainMode::Gentle),
        None,
    )
    .await?;
    insert_drained_node(
        &db.pool,
        drained,
        "k8s",
        Some("drained"),
        Some(NodeDrainMode::Gentle),
        Some(7200),
    )
    .await?;
    insert_drained_node(
        &db.pool,
        space_managed,
        "space",
        Some("draining"),
        Some(NodeDrainMode::Gentle),
        Some(7200),
    )
    .await?;

    // Every row is handed in as a candidate: the point is that the SQL's own
    // guards refuse them even when the caller asks.
    let candidates = [no_start_time, drained, space_managed];
    let escalated = escalate_stale_gentle_drains(&db.pool, &candidates, secs(600)).await?;
    assert!(
        escalated.is_empty(),
        "no guarded row may be escalated, got {escalated:?}"
    );
    for node_id in [no_start_time, drained, space_managed] {
        assert_eq!(
            mode_of(&db.pool, node_id).await?,
            Some(NodeDrainMode::Gentle),
            "node {node_id} must keep its gentle mode"
        );
    }
    Ok(())
}

/// The phase re-assertion is a compare-and-swap, not decoration.
///
/// Candidate ids come from a listing taken earlier in the tick, so a node that
/// was undrained in between is a stale read. Escalating it would write a
/// `drain_mode` onto a node with no drain — which the next `:drain` would then
/// inherit if it failed to write its own mode.
///
/// (Per-drain idempotency is not this statement's job: it comes from the caller
/// no longer classifying a `forced` node as a candidate, covered by
/// `gm6_escalation_counts_once_per_drain`.)
#[tokio::test(flavor = "current_thread")]
async fn escalation_refuses_a_candidate_that_left_draining() -> TestResult {
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
        Some(7200),
    )
    .await?;
    // The undrain that landed between the listing and the escalation.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = NULL, drain_mode = NULL WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    assert!(
        escalate_stale_gentle_drains(&db.pool, &[node_id], secs(600))
            .await?
            .is_empty(),
        "a node that left `draining` must not be escalated on a stale candidate id"
    );
    assert_eq!(
        mode_of(&db.pool, node_id).await?,
        None,
        "and no mode may be written onto a node with no drain"
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
