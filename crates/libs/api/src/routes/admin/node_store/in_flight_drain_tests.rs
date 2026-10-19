//! Postgres-backed contract for the in-place `gentle → forced` escalation
//! write.
//!
//! [`update_in_flight_drain`]'s doc states what the edge must leave alone, and who
//! reads each column. What matters here is that both ways of getting it wrong
//! are **silent** — neither a re-stamped `drain_started_at` nor a re-persisted
//! `terminate_decrement_capacity` fails the request — which is why they are
//! asserted against the columns of a real row rather than against the flags the
//! wrapper passes. The flags are the mechanism; the surviving values are the
//! property.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeDrainMode, NodeId};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;

use super::update_in_flight_drain;

/// The state a `:drain` has left on the row, as the escalation finds it.
#[derive(Debug, PartialEq, Eq)]
struct DrainColumns {
    phase: Option<String>,
    mode: Option<String>,
    started_at: Option<OffsetDateTime>,
    last_error: Option<String>,
    decrement_capacity: bool,
}

/// A node already draining, with `drain_started_at` deliberately backdated and
/// a `drain_last_error` left over from a failed cordon.
///
/// `mode` is `Option` so one fixture covers the NULL column too — the state of
/// every node drained before drain modes existed, and the case most likely to
/// meet an escalation in a live cluster.
///
/// `replace_after_terminate` mirrors the `:drain` request flag: `false` is what
/// `space-admin-cli drain-node --no-replace-after-terminate` persists as
/// `terminate_decrement_capacity = true`, the value a blue-green shrink depends
/// on.
async fn insert_draining_node(
    pool: &PgPool,
    mode: Option<&str>,
    replace_after_terminate: bool,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (
             id, address, role, managed_by, state,
             drain_phase, drain_mode, drain_started_at, drain_last_error,
             terminate_decrement_capacity
         )
         VALUES (
             $1, '10.0.0.21'::INET, 'worker', 'space', 'registered',
             'draining', $2, NOW() - INTERVAL '6 hours', 'cordon patch failed',
             $3
         )"
    ))
    .bind(node_id)
    .bind(mode)
    .bind(!replace_after_terminate)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn drain_columns(pool: &PgPool, node_id: NodeId) -> TestResult<DrainColumns> {
    let row: (
        Option<String>,
        Option<String>,
        Option<OffsetDateTime>,
        Option<String>,
        bool,
    ) = sqlx::query_as(sql!(
        "SELECT drain_phase, drain_mode, drain_started_at, drain_last_error,
                terminate_decrement_capacity
           FROM nodes
          WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?;
    Ok(DrainColumns {
        phase: row.0,
        mode: row.1,
        started_at: row.2,
        last_error: row.3,
        decrement_capacity: row.4,
    })
}

/// The whole point: the mode becomes `forced` and the drain keeps its age.
///
/// The `started_at` assertion is an equality against the value read *before* the
/// write, not a "still in the past" bound — `NOW()` is also in the past by the
/// time the row is re-read, so a bound would pass on the re-stamp this test
/// exists to catch.
#[tokio::test(flavor = "current_thread")]
async fn escalation_rewrites_the_mode_and_keeps_the_drains_age() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = insert_draining_node(pool, Some("gentle"), true).await?;
    let before = drain_columns(pool, node_id).await?;
    // The `last_error` assertion below is an assertion of absence, so the
    // fixture has to make a presence possible first — otherwise it would hold
    // on a write that never touched the column.
    assert_eq!(
        before.last_error.as_deref(),
        Some("cordon patch failed"),
        "fixture must start with an error for the clear to be observable"
    );
    assert_eq!(before.mode.as_deref(), Some("gentle"));

    let mut tx = pool.begin().await?;
    update_in_flight_drain(&mut tx, node_id, Some(NodeDrainMode::Forced)).await?;
    tx.commit().await?;

    let after = drain_columns(pool, node_id).await?;
    assert_eq!(after.mode.as_deref(), Some("forced"));
    assert_eq!(
        after.started_at, before.started_at,
        "an escalation must not restart the drain's clock"
    );
    assert_eq!(
        after.phase.as_deref(),
        Some("draining"),
        "the node stays draining; escalation is a mode change, not a phase move"
    );
    assert_eq!(
        after.last_error, None,
        "a fresh operator intent clears the previous substrate failure"
    );
    Ok(())
}

/// The write is unconditional on the prior `drain_mode`: `COALESCE($6,
/// drain_mode)` overwrites whatever is there. Asserted over every starting
/// value a live row can hold, because the two that are not `gentle` are the ones
/// a reader might expect the CASE to skip — and a skip would answer 200 while
/// leaving the node gentle forever.
///
/// The `forced` prior is not a defensive extra: it pins that the `CASE` does not
/// skip a row whose stored mode is already the destructive one. Note the
/// validator reaches this function with `None`, not `Some(Forced)`, when the mode
/// matches (`nodes::validate_drain_transition`'s same-mode arm) — the
/// `Some(Forced)`-over-`forced` combination here is the store contract, not a
/// shape the handler produces. The handler's own path for that case — forwarding
/// the transition's `None` rather than the request's mode — is covered by
/// `nodes::drain_handler_tests::a_same_mode_repeat_leaves_an_unparseable_mode_alone`,
/// which drives `apply_drain_action`.
/// `a_same_mode_drain_preserves_an_unparseable_mode_column`, further down in
/// this file, is a store test and cannot reach that join — it was once credited
/// with covering it.
#[tokio::test(flavor = "current_thread")]
async fn escalation_overwrites_any_prior_mode_and_keeps_the_age() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for prior in [Some("gentle"), Some("forced"), None] {
        let node_id = insert_draining_node(pool, prior, true).await?;
        let before = drain_columns(pool, node_id).await?;
        assert_eq!(before.mode.as_deref(), prior, "fixture must seed {prior:?}");

        let mut tx = pool.begin().await?;
        update_in_flight_drain(&mut tx, node_id, Some(NodeDrainMode::Forced)).await?;
        tx.commit().await?;

        // One struct compare rather than five field compares: it also pins the
        // columns nobody thought to name, so a future write that touches one is
        // caught here instead of in production.
        assert_eq!(
            drain_columns(pool, node_id).await?,
            DrainColumns {
                mode: Some("forced".to_string()),
                last_error: None,
                ..before
            },
            "{prior:?}: only drain_mode and drain_last_error may move"
        );
    }
    Ok(())
}

/// The write never moves the replacement intent, whatever the request carried.
///
/// The handler refuses a conflicting expressed intent before reaching here
/// (`nodes::in_flight_replace_conflict`), so this function's job is simply to
/// have no path that writes the column — the guard cannot be the only defence,
/// because a future caller that forgot it would then silently overwrite. Both
/// starting values are driven so the assertion proves preservation rather than
/// matching a constant.
#[tokio::test(flavor = "current_thread")]
async fn the_in_flight_write_never_moves_the_replacement_intent() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for replace_after_terminate in [true, false] {
        let node_id = insert_draining_node(pool, Some("gentle"), replace_after_terminate).await?;
        let before = drain_columns(pool, node_id).await?;
        assert_eq!(before.decrement_capacity, !replace_after_terminate);

        let mut tx = pool.begin().await?;
        update_in_flight_drain(&mut tx, node_id, Some(NodeDrainMode::Forced)).await?;
        tx.commit().await?;

        let after = drain_columns(pool, node_id).await?;
        assert_eq!(
            after.decrement_capacity, before.decrement_capacity,
            "replace_after_terminate={replace_after_terminate}: the intent must survive"
        );
        // Pins that the write happened at all — without this the assertion
        // above is satisfied by a function that executes nothing.
        assert_eq!(after.mode.as_deref(), Some("forced"));
        assert_eq!(after.last_error, None);
    }
    Ok(())
}

/// A same-mode `:drain` must leave a raw `drain_mode` this build cannot parse
/// exactly as it found it.
///
/// `from_column_str` absorbs an unrecognized token into `Gentle`, so writing the
/// *resolved* mode back would replace a newer build's value with `'gentle'`.
/// Nothing in this build behaves differently as a result — it already treated
/// the node as gentle — but the pod that wrote the token treats it as
/// destructive, so an older pod answering 200 to a bodiless repeat would disarm
/// that drain with no error and nothing in the audit trail naming the lost value.
///
/// The escalation direction is asserted alongside it: an unrecognized column is
/// still escalatable, so this cannot be satisfied by never writing the mode.
#[tokio::test(flavor = "current_thread")]
async fn a_same_mode_drain_preserves_an_unparseable_mode_column() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let preserved = insert_draining_node(pool, Some("forced_fast"), true).await?;
    let mut tx = pool.begin().await?;
    update_in_flight_drain(&mut tx, preserved, None).await?;
    tx.commit().await?;
    assert_eq!(
        drain_columns(pool, preserved).await?.mode.as_deref(),
        Some("forced_fast"),
        "a same-mode repeat must not normalize a mode this build cannot parse"
    );

    let escalated = insert_draining_node(pool, Some("forced_fast"), true).await?;
    let mut tx = pool.begin().await?;
    update_in_flight_drain(&mut tx, escalated, Some(NodeDrainMode::Forced)).await?;
    tx.commit().await?;
    assert_eq!(
        drain_columns(pool, escalated).await?.mode.as_deref(),
        Some("forced"),
        "an escalation must still overwrite it, or the node could never be escalated"
    );
    Ok(())
}
