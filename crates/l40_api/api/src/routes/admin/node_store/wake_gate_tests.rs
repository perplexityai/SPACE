//! Postgres-backed contract for [`is_node_wake_gate_closed`]'s decoding of the
//! drain columns: a gate-closing state reads closed, and a phase this build
//! does not know fails the check rather than reading as an open gate.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::is_node_wake_gate_closed;

async fn set_drain(
    pool: &PgPool,
    node_id: NodeId,
    phase: Option<&str>,
    mode: Option<&str>,
) -> TestResult {
    sqlx::query("UPDATE nodes SET drain_phase = $2, drain_mode = $3 WHERE id = $1")
        .bind(node_id)
        .bind(phase)
        .bind(mode)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wake_gate_reads_the_drain_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    assert!(!is_node_wake_gate_closed(&db.pool, node_id).await?);
    insert_ready_worker_node(&db.pool, node_id).await?;
    assert!(!is_node_wake_gate_closed(&db.pool, node_id).await?);

    for (phase, mode, closed) in [
        ("draining", Some("gentle"), false),
        ("draining", None, false),
        ("draining", Some("forced"), true),
        ("drained", None, true),
        ("drained", Some("gentle"), true),
        ("drained", Some("forced"), true),
        ("terminating", None, true),
        ("terminating", Some("gentle"), true),
        ("terminating", Some("forced"), true),
    ] {
        set_drain(&db.pool, node_id, Some(phase), mode).await?;
        assert_eq!(
            is_node_wake_gate_closed(&db.pool, node_id).await?,
            closed,
            "phase {phase} mode {mode:?}"
        );
    }
    Ok(())
}

/// An unknown phase is state this build cannot interpret: the check fails with
/// a column-decode error on the phase column, so the fan-out aborts instead of
/// treating the gate as open.
#[tokio::test(flavor = "current_thread")]
async fn unrecognized_drain_phase_fails_the_wake_gate_check() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    // Normal writes cannot create this state because the schema constraint
    // rejects unknown phases. Drop it in this isolated database to model
    // corruption or a newer writer whose vocabulary this server cannot parse.
    sqlx::query("ALTER TABLE nodes DROP CONSTRAINT nodes_drain_phase_check")
        .execute(&db.pool)
        .await?;
    set_drain(
        &db.pool,
        node_id,
        Some("unrecognized-phase"),
        Some("forced"),
    )
    .await?;

    let err = is_node_wake_gate_closed(&db.pool, node_id)
        .await
        .expect_err("an unrecognized drain phase must fail the check");
    assert!(
        matches!(&err, sqlx::Error::ColumnDecode { index, .. } if index == "0"),
        "{err:?}"
    );
    Ok(())
}
