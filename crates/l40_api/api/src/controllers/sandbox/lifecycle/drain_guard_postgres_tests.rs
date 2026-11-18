use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;

use super::*;
use axum::{http::StatusCode, response::IntoResponse};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_node_model::{NodeDrainMode, NodeDrainPhase};

use pplx_space_test_env::TestResult;

/// Insert (or re-phase) a node draining in **forced** mode, which is the mode
/// every gate assertion in this file is about: the 409s exist to stop users
/// racing the evacuation pipeline, and only a forced drain has one. The gentle
/// and NULL cases state their mode through [`insert_node_with_mode`].
async fn insert_node(pool: &PgPool, node_id: NodeId, drain_phase: Option<&str>) -> TestResult {
    insert_node_with_mode(pool, node_id, drain_phase, Some(NodeDrainMode::Forced)).await
}

async fn insert_node_with_mode(
    pool: &PgPool,
    node_id: NodeId,
    drain_phase: Option<&str>,
    drain_mode: Option<NodeDrainMode>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'registered', $2, $3)
         ON CONFLICT (id) DO UPDATE
         SET drain_phase = EXCLUDED.drain_phase,
             drain_mode = EXCLUDED.drain_mode"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(drain_mode)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id,
             network_policy_override_json, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'unplaced', 'k8s',
             $3, $4,
             $5, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(active_node_id)
    .bind(target_node_id)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ensure_resolved_node_not_draining_blocks_via_active_node_id() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, Some("draining")).await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "suspended",
        Some(node_id),
        Some(node_id),
    )
    .await?;

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake)
        .await
        .expect_err("expected drain to block placement");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    assert!(err.to_string().contains("draining"));
    Ok(())
}

// Failing test for the iter-2 codex finding: when `finish_suspended_execution`
// clears `active_node_id` but leaves `target_node_id` pointing at the
// draining node, a guard that only inspects `active_node_id` would let the
// doomed restore through. The query must `COALESCE(target_node_id,
// active_node_id)` and still see the drain phase.
#[tokio::test(flavor = "current_thread")]
async fn ensure_resolved_node_not_draining_blocks_via_target_node_id_when_active_is_null()
-> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, Some("draining")).await?;
    // `active_node_id = NULL` mirrors `finish_suspended_execution`'s state;
    // `target_node_id` still pins the restore pod to the source node.
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, Some(node_id)).await?;

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Restore)
        .await
        .expect_err("post-suspend restore must still 409 because target_node_id is draining");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ensure_resolved_node_not_draining_passes_when_both_columns_null() -> TestResult {
    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, None).await?;

    ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Start)
        .await
        .expect("truly unplaced sandbox has no draining node to race");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ensure_resolved_node_not_draining_passes_when_node_drain_phase_is_null() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, None).await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "suspended",
        Some(node_id),
        Some(node_id),
    )
    .await?;

    ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake)
        .await
        .expect("non-draining node must pass the guard");
    Ok(())
}

// Pairing dormant lifecycle with a draining target_node_id is the
// post-suspend "restore via auto-wake on connect" race: ECW envd connects
// to a paused sandbox, substrate auto-wakes, and `target_node_id` pins
// the new pod back to the cordoned source node.
#[tokio::test(flavor = "current_thread")]
async fn drain_gate_blocks_dormant_sandbox_on_draining_target_node() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, Some("draining")).await?;
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, Some(node_id)).await?;

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("inserted sandbox must be visible");
    assert_eq!(sandbox.lifecycle, "suspended");

    let err = ensure_access_path_not_blocked_by_drain(&db.pool, &sandbox, SandboxAction::Spaced)
        .await
        .expect_err("dormant + draining-target should 409");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

// Regression: a *gentle* primary must not mask a blocking fallback.
//
// `preferred_restore_node_name` falls back to the latest execution's k8s node
// when the primary placement column has no `node_k8s` mapping, so that fallback
// is the node placement actually lands on. A non-blocking primary therefore
// cannot end the search: returning it would skip a fallback that is
// mid-evacuation, and the sandbox would be re-placed onto the node being
// evacuated.
#[tokio::test(flavor = "current_thread")]
async fn gentle_primary_does_not_mask_a_forced_fallback() -> TestResult {
    let db = pg_test_db().await?;
    let primary_id = NodeId::new();
    let fallback_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let fallback_k8s_name = format!("forced-fallback-{}", sandbox_id.as_uuid().simple());
    // Primary: gentle-draining, deliberately WITHOUT a `node_k8s` mapping, which
    // is what makes the substrate fall back to the executions node.
    insert_node_with_mode(
        &db.pool,
        primary_id,
        Some("draining"),
        Some(NodeDrainMode::Gentle),
    )
    .await?;
    insert_node_with_mode(
        &db.pool,
        fallback_id,
        Some("draining"),
        Some(NodeDrainMode::Forced),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(fallback_id)
    .bind(&fallback_k8s_name)
    .execute(&db.pool)
    .await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "suspended",
        Some(primary_id),
        Some(primary_id),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-gentle-mask', 'space', $2, 'terminated', false)"
    ))
    .bind(sandbox_id)
    .bind(&fallback_k8s_name)
    .execute(&db.pool)
    .await?;

    let state = fetch_active_node_drain_state(&db.pool, sandbox_id).await?;
    assert_eq!(
        state.and_then(NodeDrain::mode),
        Some(NodeDrainMode::Forced),
        "the blocking fallback must win over the non-blocking primary"
    );

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Restore)
        .await
        .expect_err("a forced-draining fallback must 409 even behind a gentle primary");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

// The other side of the masking fix: a *mapped* gentle primary is the whole
// answer, and a stale execution row must not 409 against it.
//
// The substrate consults the executions fallback only when the primary can't be
// resolved to a k8s node. So when the primary is mapped and gently draining,
// that is where placement lands — an older execution naming some since-drained
// node is history, and blocking on it would deny a sandbox sitting on a
// perfectly serviceable node.
#[tokio::test(flavor = "current_thread")]
async fn mapped_gentle_primary_ignores_a_stale_blocking_execution() -> TestResult {
    let db = pg_test_db().await?;
    let primary_id = NodeId::new();
    let stale_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let primary_k8s_name = format!("mapped-gentle-{}", sandbox_id.as_uuid().simple());
    let stale_k8s_name = format!("stale-drained-{}", sandbox_id.as_uuid().simple());
    insert_node_with_mode(
        &db.pool,
        primary_id,
        Some("draining"),
        Some(NodeDrainMode::Gentle),
    )
    .await?;
    // The primary IS mapped — that is what makes the fallback unreachable.
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(primary_id)
    .bind(&primary_k8s_name)
    .execute(&db.pool)
    .await?;
    // A node the sandbox used to run on, now terminating.
    insert_node_with_mode(
        &db.pool,
        stale_id,
        Some("terminating"),
        Some(NodeDrainMode::Forced),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(stale_id)
    .bind(&stale_k8s_name)
    .execute(&db.pool)
    .await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        Some(primary_id),
        Some(primary_id),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-stale-history', 'space', $2, 'terminated', false)"
    ))
    .bind(sandbox_id)
    .bind(&stale_k8s_name)
    .execute(&db.pool)
    .await?;

    let state = fetch_active_node_drain_state(&db.pool, sandbox_id).await?;
    assert_eq!(
        state.and_then(NodeDrain::mode),
        Some(NodeDrainMode::Gentle),
        "the mapped primary is the answer; the stale execution must not win"
    );
    ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake).await?;
    Ok(())
}

// Same shape among multi-role siblings: one physical k8s node can map to several
// SPACE `nodes` rows, and they can now differ in mode. An arbitrary `LIMIT 1`
// pick would decide whether the gate fires, so every sibling is resolved and any
// blocking one wins.
#[tokio::test(flavor = "current_thread")]
async fn forced_sibling_wins_over_gentle_sibling_on_the_same_k8s_node() -> TestResult {
    let db = pg_test_db().await?;
    let gentle_id = NodeId::new();
    let forced_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let shared_k8s_name = format!("shared-mixed-{}", sandbox_id.as_uuid().simple());
    insert_node_with_mode(
        &db.pool,
        gentle_id,
        Some("draining"),
        Some(NodeDrainMode::Gentle),
    )
    .await?;
    insert_node_with_mode(
        &db.pool,
        forced_id,
        Some("draining"),
        Some(NodeDrainMode::Forced),
    )
    .await?;
    for node_id in [gentle_id, forced_id] {
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(&shared_k8s_name)
        .execute(&db.pool)
        .await?;
    }
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, None).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-mixed-siblings', 'space', $2, 'terminated', false)"
    ))
    .bind(sandbox_id)
    .bind(&shared_k8s_name)
    .execute(&db.pool)
    .await?;

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Restore)
        .await
        .expect_err("a forced sibling must 409 regardless of sibling ordering");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

// A gentle drain is the mode where a resident sandbox keeps working. Both
// gates must stay open for the whole `draining` window — the reconciler is not
// migrating anything, so there is no pipeline for the user to race, and a 409
// here would be the disruption the operator declined by choosing gentle.
#[tokio::test(flavor = "current_thread")]
async fn gentle_drain_does_not_block_access_or_placement_while_draining() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node_with_mode(
        &db.pool,
        node_id,
        Some("draining"),
        Some(NodeDrainMode::Gentle),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        Some(node_id),
        Some(node_id),
    )
    .await?;

    let state = fetch_active_node_drain_state(&db.pool, sandbox_id).await?;
    assert_eq!(
        state,
        Some(NodeDrain::V1 {
            phase: NodeDrainPhase::Draining,
            mode: NodeDrainMode::Gentle,
        }),
        "the gate must read the mode off the node row, not assume forced"
    );

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("inserted sandbox must be visible");
    ensure_access_path_not_blocked_by_drain(&db.pool, &sandbox, SandboxAction::Spaced).await?;
    ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake).await?;
    Ok(())
}

// Gentle mode opens the gate for `draining` only. Once the node reaches
// `drained` it is waiting to be terminated, so re-placing a sandbox on it is
// wrong in either mode — this is the phase where the same node that just
// allowed traffic has to start refusing it.
#[tokio::test(flavor = "current_thread")]
async fn gentle_drain_still_blocks_once_the_node_is_drained() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node_with_mode(
        &db.pool,
        node_id,
        Some("drained"),
        Some(NodeDrainMode::Gentle),
    )
    .await?;
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, Some(node_id)).await?;

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Restore)
        .await
        .expect_err("a drained node must 409 regardless of drain mode");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    assert!(err.to_string().contains("drained"));
    Ok(())
}

// The operator-visible property: once a gently-draining node's `drain_mode`
// becomes `forced`, the gate treats it exactly as one drained forced from the
// start. Nothing on this path reads how the mode came to be `forced` — the gate
// re-reads the column per request — so the same sandbox that was served a moment
// ago starts 409ing with no api-server state to invalidate and no coordination
// with whoever wrote the column.
//
// The mode is moved here with a direct UPDATE rather than through the admin
// endpoint: this file's subject is the gate, and driving `:drain` would need the
// whole handler. The endpoint-to-column wiring is covered elsewhere rather than
// here — `node_store::in_flight_drain_tests` covers the write,
// `routes::admin::nodes::tests` covers the transition table, and
// `routes::admin::nodes::drain_handler_tests` drives a real `:drain` against an
// already-draining row. So the direct UPDATE is a scoping choice, not a gap.
#[tokio::test(flavor = "current_thread")]
async fn escalating_a_gentle_drain_to_forced_closes_the_gate() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node_with_mode(
        &db.pool,
        node_id,
        Some("draining"),
        Some(NodeDrainMode::Gentle),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        Some(node_id),
        Some(node_id),
    )
    .await?;
    ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake).await?;

    sqlx::query(sql!("UPDATE nodes SET drain_mode = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(NodeDrainMode::Forced)
        .execute(&db.pool)
        .await?;

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake)
        .await
        .expect_err("an escalated drain must start blocking immediately");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

// The substrate's `access_runtime_client` calls `ensure_pod_exists` for
// **any** recoverable-live lifecycle when the pod IP disappears or the
// node-api is unreachable; that recovery re-pins the pod to
// `target_node_id`. So a `started` sandbox on a draining node whose
// runtime momentarily goes unreachable will be re-placed on the cordoned
// source node unless the gate fires unconditionally. The earlier
// dormant-only gate let this case through; this test pins the contract
// down at "any drain phase, any lifecycle, blocks the access path".
#[tokio::test(flavor = "current_thread")]
async fn drain_gate_blocks_running_sandbox_on_draining_node() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, Some("draining")).await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        Some(node_id),
        Some(node_id),
    )
    .await?;

    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("inserted sandbox must be visible");
    assert_eq!(sandbox.lifecycle, "started");

    let err = ensure_access_path_not_blocked_by_drain(&db.pool, &sandbox, SandboxAction::Spaced)
        .await
        .expect_err(
            "running sandbox on draining node must 409 — substrate recovery would re-pin \
         the pod to target_node_id otherwise",
        );
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

// Companion to `..._via_sandbox_k8s_executions_fallback` below: the
// substrate also falls back to the executions table when **primary**
// placement (`target_node_id` / `active_node_id`) is set but the
// corresponding `node_k8s` mapping is missing. In that case
// `preferred_restore_node_name` logs a warning and uses the executions
// node, so the guard has to consult the fallback path even when the
// primary node row exists.
#[tokio::test(flavor = "current_thread")]
async fn drain_gate_blocks_when_primary_node_lacks_k8s_mapping_and_fallback_is_draining()
-> TestResult {
    let db = pg_test_db().await?;
    let primary_id = NodeId::new();
    let fallback_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let fallback_k8s_name = format!("draining-fallback-{}", sandbox_id.as_uuid().simple());
    // Primary node row exists with NO drain phase and (critically) no
    // `node_k8s` mapping — the substrate cannot use it for the restore
    // pod's affinity, so it falls back to the executions table.
    insert_node(&db.pool, primary_id, None).await?;
    insert_node(&db.pool, fallback_id, Some("draining")).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(fallback_id)
    .bind(&fallback_k8s_name)
    .execute(&db.pool)
    .await?;
    // Sandbox placement points at the unmapped primary; the executions
    // fallback row points at a different, draining node.
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "suspended",
        Some(primary_id),
        Some(primary_id),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-B', 'space', $2, 'terminated', false)"
    ))
    .bind(sandbox_id)
    .bind(&fallback_k8s_name)
    .execute(&db.pool)
    .await?;

    let state = fetch_active_node_drain_state(&db.pool, sandbox_id).await?;
    assert_eq!(
        state.and_then(NodeDrain::phase),
        Some(NodeDrainPhase::Draining),
        "fallback executions node must surface the drain when primary's node_k8s mapping is missing"
    );

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Restore)
        .await
        .expect_err("primary-unmapped + draining-fallback must 409");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

// `node_k8s.k8s_node_name` has no unique constraint, so a single
// physical k8s node can have multiple SPACE `nodes` rows mapped to it
// (e.g. worker + builder role rows sharing the same node name). The
// fallback path must check every such row for a non-NULL drain_phase,
// otherwise a sibling non-draining row could mask a draining sibling
// and let the gate pass on a node that's actually being drained.
#[tokio::test(flavor = "current_thread")]
async fn drain_gate_fallback_finds_draining_row_among_multi_role_node_k8s_siblings() -> TestResult {
    let db = pg_test_db().await?;
    let healthy_id = NodeId::new();
    let draining_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let shared_k8s_name = format!("shared-node-{}", sandbox_id.as_uuid().simple());
    insert_node(&db.pool, healthy_id, None).await?;
    insert_node(&db.pool, draining_id, Some("draining")).await?;
    // Two `node_k8s` rows pointing at the same physical k8s node name —
    // the same shape worker + builder duplication produces. The healthy
    // row would be picked first by the old `LIMIT 1`-before-filter SQL.
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(healthy_id)
    .bind(&shared_k8s_name)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(draining_id)
    .bind(&shared_k8s_name)
    .execute(&db.pool)
    .await?;
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, None).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-multi', 'space', $2, 'terminated', false)"
    ))
    .bind(sandbox_id)
    .bind(&shared_k8s_name)
    .execute(&db.pool)
    .await?;

    let state = fetch_active_node_drain_state(&db.pool, sandbox_id).await?;
    assert_eq!(
        state.and_then(NodeDrain::phase),
        Some(NodeDrainPhase::Draining),
        "fallback must surface the draining sibling even when a healthy sibling shares the k8s node name"
    );

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Restore)
        .await
        .expect_err("multi-role-row fallback with a draining sibling must 409");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

// `forget_pod_tracking` clears both `active_node_id` and `target_node_id`
// after a tracked pod disappears but leaves the most recent
// `sandbox_k8s_executions.k8s_node_name` behind, and
// `preferred_restore_node_name` falls back to that name. A guard that
// only joined the two placement columns would pass here and let the
// substrate create a pod pinned to the draining source node. This test
// pins that fallback into the drain SQL.
#[tokio::test(flavor = "current_thread")]
async fn drain_gate_blocks_via_sandbox_k8s_executions_fallback() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let k8s_node_name = format!("draining-node-{}", sandbox_id.as_uuid().simple());
    insert_node(&db.pool, node_id, Some("draining")).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(&k8s_node_name)
    .execute(&db.pool)
    .await?;
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, None).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-A', 'space', $2, 'terminated', false)"
    ))
    .bind(sandbox_id)
    .bind(&k8s_node_name)
    .execute(&db.pool)
    .await?;

    let state = fetch_active_node_drain_state(&db.pool, sandbox_id).await?;
    assert_eq!(
        state.and_then(NodeDrain::phase),
        Some(NodeDrainPhase::Draining),
        "executions fallback must surface the draining node when both placement columns are NULL"
    );

    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Restore)
        .await
        .expect_err("executions-fallback to a draining node must 409");
    assert_eq!(err.clone().into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

/// A fresh ready full snapshot with a storage location, which makes a
/// `suspended` / `restoring` row portable for restore affinity.
async fn insert_fresh_ready_snapshot(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    let snapshot_id = pplx_space_snapshot_model::SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp)
         VALUES ($1, $2, 'full', 'ready', NOW())"
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
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

/// Keep `sqlx::query` events enabled for the whole test process.
///
/// Scoped subscribers on other test threads come and go concurrently, and a
/// callsite-interest rebuild racing ours can compute the global max level (and
/// cached interest) from a dispatcher set that no longer includes ours, which
/// silently drops every event we meant to count. One process-lifetime
/// dispatcher that accepts everything keeps each rebuild's answer "enabled";
/// events still reach only the subscriber in scope on this thread.
fn keep_query_events_enabled() {
    static ALWAYS_ENABLED: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    ALWAYS_ENABLED.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
    tracing::callsite::rebuild_interest_cache();
}

/// The `sql!` locators of every statement `fetch_active_node_drain_state`
/// sends for `sandbox_id`.
async fn drain_gate_statements(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(Option<NodeDrain>, Vec<String>)> {
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt;

    let layer = CapturedEventsLayer::new().with_target("sqlx::query");
    let events = layer.events();
    let state = {
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        keep_query_events_enabled();
        fetch_active_node_drain_state(pool, sandbox_id).await?
    };
    let statements = events
        .events()
        .iter()
        .map(|event| {
            let summary = field_value(event, "summary").unwrap_or_default();
            let statement = field_value(event, "db.statement").unwrap_or_default();
            let sql = if statement.is_empty() {
                summary
            } else {
                statement
            };
            sql.trim_start()
                .strip_prefix("/*")
                .and_then(|rest| rest.split_once("*/"))
                .map_or_else(|| sql.to_string(), |(locator, _)| locator.to_string())
        })
        .collect();
    Ok((state, statements))
}

#[tokio::test(flavor = "current_thread")]
async fn portable_suspended_sandbox_bypasses_a_forced_drain_in_one_read() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, Some("terminating")).await?;
    insert_sandbox(&db.pool, sandbox_id, "suspended", None, Some(node_id)).await?;
    insert_fresh_ready_snapshot(&db.pool, sandbox_id).await?;

    let (state, statements) = drain_gate_statements(&db.pool, sandbox_id).await?;
    assert_eq!(state, None);
    assert_eq!(statements.len(), 1, "{statements:#?}");
    ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake).await?;

    // The same snapshot does not release a live sandbox: its VM memory on the
    // node, not the snapshot, is canonical.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&db.pool)
    .await?;
    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake)
        .await
        .expect_err("a live sandbox on a terminating node is blocked");
    assert_eq!(err.into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mapped_primary_drain_state_is_one_read() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, None).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'worker-a')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    insert_sandbox(&db.pool, sandbox_id, "started", Some(node_id), None).await?;

    let (state, statements) = drain_gate_statements(&db.pool, sandbox_id).await?;
    assert_eq!(state, None);
    assert_eq!(statements.len(), 1, "{statements:#?}");

    insert_node(&db.pool, node_id, Some("drained")).await?;
    let (state, statements) = drain_gate_statements(&db.pool, sandbox_id).await?;
    assert_eq!(
        state.and_then(NodeDrain::phase),
        Some(NodeDrainPhase::Drained)
    );
    assert_eq!(statements.len(), 1, "{statements:#?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn missing_sandbox_has_no_drain_state() -> TestResult {
    let db = pg_test_db().await?;
    let (state, statements) = drain_gate_statements(&db.pool, SandboxId::new()).await?;
    assert_eq!(state, None);
    // No row, so no primary; the executions fallback still runs, as before.
    assert_eq!(statements.len(), 2, "{statements:#?}");
    Ok(())
}

/// A node named `name` whose drain is `engine` in `phase`.
async fn named_node(
    pool: &PgPool,
    name: &str,
    engine: Option<&str>,
    phase: Option<&str>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    insert_node_with_mode(pool, node_id, phase, None).await?;
    sqlx::query(sql!("UPDATE nodes SET drain_engine = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(engine)
        .execute(pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A sandbox stopped an hour ago at home on `node_id`, named `name`, its
/// pre-stop execution there, and a disk of this stop in `disk_state`,
/// stored.
async fn stopped_at_home(
    pool: &PgPool,
    node_id: NodeId,
    name: &str,
    disk_state: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, "stopped", Some(node_id), None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active, created_at
         )
         VALUES ($1, 1, 'pod-' || $1::TEXT, 'space', $2, 'running', true,
                 NOW() - INTERVAL '2 hours')"
    ))
    .bind(sandbox_id)
    .bind(name)
    .execute(pool)
    .await?;
    let snapshot_id = pplx_space_snapshot_model::SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin
         )
         VALUES ($1, $2, 'disk', $3, NOW(), $4, 'disk', 'stop')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(disk_state)
    .bind(uuid::Uuid::now_v7())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// The status a start of `sandbox_id` gets from the gate: `None` passes.
async fn start_gate(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<StatusCode>> {
    Ok(
        ensure_resolved_node_not_draining(pool, sandbox_id, SandboxAction::Start)
            .await
            .err()
            .map(|err| err.into_response().status()),
    )
}

/// The portability exemption's stopped twin, by the evidence: a stopped
/// row at home on a drained or terminating v2 node whose disk is durable
/// since its stop starts (its start moves it off the node). Without a
/// durable disk, on a v2 node still draining, or on a v1 drained node, the
/// node's drain still holds it.
#[tokio::test(flavor = "current_thread")]
async fn the_stopped_twin_passes_a_start_of_a_stopped_row_on_a_drained_v2_node() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for (case, engine, phase, disk, expected) in [
        ("v2 drained", Some("v2"), Some("drained"), "ready", None),
        (
            "v2 terminating",
            Some("v2"),
            Some("terminating"),
            "ready",
            None,
        ),
        (
            "v2 drained, disk not durable",
            Some("v2"),
            Some("drained"),
            "creating",
            Some(StatusCode::SERVICE_UNAVAILABLE),
        ),
        (
            "v2 draining",
            Some("v2"),
            Some("draining"),
            "ready",
            Some(StatusCode::SERVICE_UNAVAILABLE),
        ),
        (
            "v1 drained",
            None,
            Some("drained"),
            "ready",
            Some(StatusCode::CONFLICT),
        ),
    ] {
        let name = format!("worker-{}", NodeId::new());
        let node_id = named_node(pool, &name, engine, phase).await?;
        let sandbox_id = stopped_at_home(pool, node_id, &name, disk).await?;
        assert_eq!(start_gate(pool, sandbox_id).await?, expected, "{case}");
    }
    Ok(())
}

/// The stopped twin for a handed-off row, marked and unplaced, claimed or
/// not: one that ran on a v2-drained node starts, so its start resumes it.
/// The same shape v1's drain released keeps v1's answer from the node it
/// last ran on, as on main.
#[tokio::test(flavor = "current_thread")]
async fn the_stopped_twin_passes_a_start_of_a_handed_off_row_from_a_v2_node() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for (case, engine, lifecycle, expected) in [
        ("v2, handed off", Some("v2"), "stopped", None),
        ("v2, claimed", Some("v2"), "restoring", None),
        ("v1, released", None, "stopped", Some(StatusCode::CONFLICT)),
    ] {
        let name = format!("worker-{}", NodeId::new());
        let node_id = named_node(pool, &name, engine, Some("drained")).await?;
        let sandbox_id = stopped_at_home(pool, node_id, &name, "ready").await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET evacuation_restore_target = 'stopped', active_node_id = NULL,
                    target_node_id = NULL, placement = 'unplaced', lifecycle = $2
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'forgotten'
              WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        assert_eq!(start_gate(pool, sandbox_id).await?, expected, "{case}");
    }
    Ok(())
}

/// A stopped row main's own paths left unplaced with its stored disk pinned
/// (a v1 re-placement landed it, then its pod went) isn't a stopped twin:
/// the node it last ran on still holds it while that node force-drains,
/// through the executions fallback, as on main.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_row_main_unplaced_keeps_its_last_nodes_drain_hold() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(pool, node_id, Some("draining")).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'worker-re-placed')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    insert_sandbox(pool, sandbox_id, "stopped", Some(node_id), None).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace,
             k8s_node_name, state, active
         )
         VALUES ($1, 1, 'pod-of-the-re-placement', 'space', 'worker-re-placed', 'running', true)"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    // The re-placement's pod bind stamps the placement clock; the pin is
    // the stored disk it re-placed the sandbox from.
    pplx_space_sandbox_store::wake::record_pod_node_placement(pool, sandbox_id, node_id).await?;
    let artifact = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2, current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact)
    .execute(pool)
    .await?;
    let snapshot_id = pplx_space_snapshot_model::SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin
         )
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, 'disk', 'stop')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(artifact)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    // Its pod went: the pod teardown retires the execution and unplaces it.
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE, state = 'forgotten'
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *tx)
    .await?;
    pplx_space_sandbox_store::pod_tracking::unplace_after_pod_teardown(&mut tx, sandbox_id).await?;
    tx.commit().await?;

    let state = fetch_active_node_drain_state(pool, sandbox_id).await?;
    assert_eq!(
        state.and_then(NodeDrain::phase),
        Some(NodeDrainPhase::Draining),
        "the executions fallback must surface the node the row last ran on"
    );
    let err = ensure_resolved_node_not_draining(pool, sandbox_id, SandboxAction::Start)
        .await
        .expect_err("a row main unplaced keeps its last node's drain hold");
    assert_eq!(err.into_response().status(), StatusCode::CONFLICT);
    Ok(())
}

/// Make `node_id`'s drain a v2 one in `phase` (`None`: its cancel pending),
/// as the admin route writes it: no mode, and `drain_status` with a deadline
/// `deadline` seconds from now and, as a node would have reported, every
/// sandbox of `pending` pending. The gates read none of the status.
async fn set_v2_drain(
    pool: &PgPool,
    node_id: NodeId,
    phase: Option<&str>,
    deadline: i64,
    pending: &[SandboxId],
) -> TestResult {
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let status = serde_json::json!({
        "generation": 1,
        "deadline_at": now + deadline,
        "report": {
            "reported_at": now,
            "evacuated_grew_at": now,
            "pending": pending.iter().map(|sandbox_id| serde_json::json!({
                "sandbox_id": sandbox_id.to_string(), "wait": "moving", "running": true,
            })).collect::<Vec<_>>(),
        },
    });
    sqlx::query(sql!(
        "UPDATE nodes SET drain_engine = 'v2', drain_phase = $2, drain_mode = NULL,
                          drain_status = $3
          WHERE id = $1"
    ))
    .bind(node_id)
    .bind(phase)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

/// The refusal a v2 drain gives: a 503 with `Retry-After`.
fn assert_retryable(err: ControllerError, case: &str) {
    let response = err.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{case}");
    assert!(
        response
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER),
        "{case}"
    );
}

/// The sandboxes of one node, in each lifecycle the v2 tests ask about.
struct V2Residents {
    running: SandboxId,
    paused: SandboxId,
    stopped: SandboxId,
}

async fn v2_residents(pool: &PgPool, node_id: NodeId) -> TestResult<V2Residents> {
    let residents = V2Residents {
        running: SandboxId::new(),
        paused: SandboxId::new(),
        stopped: SandboxId::new(),
    };
    for (sandbox_id, lifecycle) in [
        (residents.running, "started"),
        (residents.paused, "paused"),
        (residents.stopped, "stopped"),
    ] {
        insert_sandbox(pool, sandbox_id, lifecycle, Some(node_id), Some(node_id)).await?;
    }
    Ok(residents)
}

/// While a v2 drain is `draining`, before its deadline or past it, and with
/// its cancel pending, its node is sealed: a started sandbox's calls go
/// through, its wake included, and so does every sandbox's suspend, stop,
/// pause, kill and stats read; every activation of a sandbox that isn't
/// running is refused with a 503 and `Retry-After`: a start, wake, restore,
/// resume or implicit wake. The gate reads the shared columns only: what
/// the stored status says of the deadline or of a sandbox changes nothing.
/// Once the cancel finished, nothing is refused.
#[tokio::test(flavor = "current_thread")]
async fn a_draining_v2_drain_refuses_only_activations_with_a_503() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id, Some("draining")).await?;
    let V2Residents {
        running,
        paused,
        stopped,
    } = v2_residents(&db.pool, node_id).await?;
    let open = [
        (running, SandboxAction::Spaced),
        (running, SandboxAction::Stats),
        (running, SandboxAction::Wake),
        (running, SandboxAction::Start),
        (running, SandboxAction::Suspend),
        (running, SandboxAction::Pause),
        (running, SandboxAction::Stop),
        (paused, SandboxAction::Suspend),
        (paused, SandboxAction::Kill),
        (paused, SandboxAction::Stats),
        (stopped, SandboxAction::Kill),
        (stopped, SandboxAction::Stats),
    ];
    let refused = [
        (paused, SandboxAction::Wake),
        (paused, SandboxAction::Resume),
        (
            paused,
            SandboxAction::ImplicitWake(ExecutionActivityKind::Access),
        ),
        (paused, SandboxAction::Spaced),
        (stopped, SandboxAction::Start),
        (stopped, SandboxAction::Restore),
    ];
    for (case, phase, deadline) in [
        ("draining, holding", Some("draining"), 600),
        ("draining, evacuating", Some("draining"), -60),
        ("cancel pending", None, -60),
    ] {
        set_v2_drain(&db.pool, node_id, phase, deadline, &[running, paused]).await?;
        for (sandbox_id, action) in open {
            ensure_resolved_node_not_draining(&db.pool, sandbox_id, action)
                .await
                .unwrap_or_else(|err| panic!("{case}: {action:?} on {sandbox_id}: {err:?}"));
        }
        for (sandbox_id, action) in refused {
            let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, action)
                .await
                .expect_err("the sealed node refuses it");
            assert_retryable(err, &format!("{case}: {action:?} on {sandbox_id}"));
        }
    }
    sqlx::query(sql!(
        "UPDATE nodes SET drain_engine = NULL, drain_status = NULL WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    for (sandbox_id, action) in refused {
        ensure_resolved_node_not_draining(&db.pool, sandbox_id, action).await?;
    }
    Ok(())
}

/// A v2 drain `drained` or `terminating` blocks everything, a running
/// sandbox's calls included, as v1 does in those phases, and every refusal
/// is a 503 with `Retry-After`. So does a drain under an engine this build
/// doesn't know, which fails closed.
#[tokio::test(flavor = "current_thread")]
async fn a_drained_or_unknown_v2_drain_blocks_everything_with_a_503() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    insert_node(&db.pool, node_id, Some("draining")).await?;
    let V2Residents {
        running, stopped, ..
    } = v2_residents(&db.pool, node_id).await?;
    for (case, engine, phase) in [
        ("drained", "v2", "drained"),
        ("terminating", "v2", "terminating"),
        ("an unknown engine", "v3", "draining"),
    ] {
        set_v2_drain(&db.pool, node_id, Some(phase), -60, &[]).await?;
        sqlx::query(sql!("UPDATE nodes SET drain_engine = $2 WHERE id = $1"))
            .bind(node_id)
            .bind(engine)
            .execute(&db.pool)
            .await?;
        for (sandbox_id, action) in [
            (running, SandboxAction::Spaced),
            (running, SandboxAction::Wake),
            (stopped, SandboxAction::Start),
        ] {
            let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, action)
                .await
                .expect_err("the drain blocks it");
            assert_retryable(err, &format!("{case}: {action:?}"));
        }
    }
    Ok(())
}

/// A v1 drain's refusal stays its 409, whatever its `drain_status` reads,
/// and its gate is still one read.
#[tokio::test(flavor = "current_thread")]
async fn a_v1_drains_refusal_stays_a_409() -> TestResult {
    let db = pg_test_db().await?;
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_node(&db.pool, node_id, Some("draining")).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'worker-v1')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    insert_sandbox(
        &db.pool,
        sandbox_id,
        "started",
        Some(node_id),
        Some(node_id),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_status = '{\"generation\": 1, \"deadline_at\": 0}'::JSONB
          WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let err = ensure_resolved_node_not_draining(&db.pool, sandbox_id, SandboxAction::Wake)
        .await
        .expect_err("a forced v1 drain blocks it");
    assert_eq!(err.into_response().status(), StatusCode::CONFLICT);
    set_v2_drain(&db.pool, node_id, Some("draining"), 600, &[]).await?;
    let (state, statements) = drain_gate_statements(&db.pool, sandbox_id).await?;
    assert!(state.is_some_and(NodeDrain::is_v2));
    assert_eq!(
        statements.len(),
        1,
        "a v2 drain's gate is one read: {statements:#?}"
    );
    Ok(())
}

/// Under a draining v2 drain, an access path whose runtime client would wake
/// a dormant sandbox is refused on a suspended row whose snapshot isn't
/// durable elsewhere, with the caller's own action in the 503: a `spaced`
/// connect with an activity and a port open. A started sandbox's access
/// passes that gate; with no pod to reach, its `spaced` fallback and the port
/// open then try to recover its runtime, and the recovery gate refuses that
/// with the same retryable 503 instead of booting it elsewhere.
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn a_v2_drain_refuses_access_that_would_wake_a_dormant_sandbox() -> TestResult {
    use crate::controllers::substrate::Substrate;
    use crate::controllers::substrate::kubernetes::tests::substrate_with_store;

    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let suspended = SandboxId::new();
    let running = SandboxId::new();
    insert_node(pool, node_id, Some("draining")).await?;
    insert_sandbox(pool, suspended, "suspended", Some(node_id), Some(node_id)).await?;
    insert_sandbox(pool, running, "started", Some(node_id), Some(node_id)).await?;
    set_v2_drain(pool, node_id, Some("draining"), 600, &[running]).await?;
    let controller = SandboxController::new(Substrate::from_kubernetes(substrate_with_store(
        pool.clone(),
    )));

    for (sandbox_id, refused) in [(suspended, true), (running, false)] {
        let sandbox = crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
            .await?
            .expect("inserted sandbox must be visible");
        // Both rows answer at once: the suspended one at the drain gate, the
        // started one at the recovery gate.
        let wait = std::time::Duration::from_secs(10);
        let spaced = tokio::time::timeout(
            wait,
            crate::controllers::sandbox::spaced::connect_spaced_runtime_client(
                &controller.substrate,
                None,
                true,
                pool,
                &sandbox,
                SandboxApiMetricIdentity::ecw("test"),
                Some(ExecutionActivityKind::Access),
            ),
        )
        .await
        .map(|result| result.err());
        let port = tokio::time::timeout(
            wait,
            controller.ensure_sandbox_port_exposure_applied(pool, &sandbox, 8080, "test"),
        )
        .await
        .map(|result| result.err());
        for (call, error, action) in [("spaced", spaced, "spaced"), ("port", port, "open_port")] {
            let drain_refusal = error
                .as_ref()
                .ok()
                .and_then(Option::as_ref)
                .filter(|error| {
                    error.status() == 503 && error.to_string().contains("under a node drain")
                });
            if refused {
                let error = drain_refusal.unwrap_or_else(|| {
                    panic!("{call} on the suspended row must be refused: {error:?}")
                });
                assert!(
                    error.to_string().contains(&format!("cannot {action} yet")),
                    "{call}: {error}"
                );
            } else {
                assert!(
                    drain_refusal.is_none(),
                    "{call} on the started row must pass the drain gate: {error:?}"
                );
                let recovery = error
                    .as_ref()
                    .ok()
                    .and_then(Option::as_ref)
                    .filter(|error| {
                        error.status() == 503
                            && error.to_string().contains("is not recovered elsewhere")
                    });
                assert!(
                    recovery.is_some(),
                    "{call} on the started row must stop at the recovery gate: {error:?}"
                );
            }
        }
    }
    Ok(())
}
