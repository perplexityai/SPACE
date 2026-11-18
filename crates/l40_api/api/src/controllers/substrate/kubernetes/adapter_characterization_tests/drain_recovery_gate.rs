//! The recovery gate through the adapter's entry paths: a `started`
//! sandbox whose runtime is unreachable, on a node under a v2 drain, isn't
//! recovered elsewhere by any of them. The adapter's runtime client (the
//! mid-call wake an access runs into), its access client, the wake and dial
//! once started, and the started recovery itself each answer the drain's
//! retryable 503 and claim no execution: while the node suspends the
//! sandbox its row still reads `started`, and a cold boot elsewhere would
//! run a second copy of it. The `spaced` fallback's own entry is covered in
//! `controllers::sandbox::lifecycle::drain_guard_postgres_tests`.

use super::super::tests::{insert_test_sandbox_row, substrate_with_store};
use super::*;

/// A `started` sandbox with no pod, active on a node under a v2 drain in
/// `phase` (`None`: its cancel pending).
async fn started_on_a_v2_node(
    pool: &PgPool,
    phase: Option<&str>,
) -> Result<(SandboxId, Sandbox), BoxError> {
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(pool, sandbox_id, "started").await?;
    let node_id = pplx_space_accounts::NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at,
                            drain_engine, drain_status)
         VALUES ($1, '10.0.0.82', 'worker', 'k8s', 'ready', $2, NOW(), 'v2',
                 '{\"generation\": 1, \"deadline_at\": 0}'::JSONB)"
    ))
    .bind(node_id)
    .bind(phase)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2, target_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
        .await?
        .ok_or("the inserted sandbox")?;
    Ok((sandbox_id, sandbox))
}

async fn executions(pool: &PgPool, sandbox_id: SandboxId) -> Result<i64, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The recovery gate's refusal: the drain's 503 with `Retry-After`.
fn assert_recovery_refused(error: ControllerError, path: &str) {
    assert_eq!(error.status(), 503, "{path}: {error:?}");
    assert!(
        error.to_string().contains("is not recovered elsewhere"),
        "{path}: {error}"
    );
    let response = axum::response::IntoResponse::into_response(error);
    assert!(
        response
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER),
        "{path}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn no_entry_path_recovers_a_started_sandbox_under_a_v2_drain() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let substrate = substrate_with_store(pool.clone());
    let charge = crate::controllers::substrate::RecoveryCharge::exempt_for_tests;
    for phase in [Some("draining"), None, Some("drained")] {
        let (sandbox_id, sandbox) = started_on_a_v2_node(pool, phase).await?;
        let budget = Duration::from_secs(10);
        let runtime = tokio::time::timeout(
            budget,
            SubstrateAdapter::runtime_client(&substrate, sandbox_id, charge()),
        )
        .await?;
        assert_recovery_refused(runtime.err().ok_or("refused")?, "runtime_client");

        let access = tokio::time::timeout(
            budget,
            SubstrateAdapter::access_runtime_client(
                &substrate,
                SandboxApiMetricIdentity::native("test"),
                &sandbox,
                ExecutionActivityKind::Access,
                budget,
                charge(),
            ),
        )
        .await?;
        assert_recovery_refused(access.err().ok_or("refused")?, "access_runtime_client");

        let dial = tokio::time::timeout(
            budget,
            substrate.wake_and_dial_once_started(
                &sandbox,
                budget,
                SandboxApiMetricIdentity::native("test"),
                charge(),
            ),
        )
        .await?;
        assert_recovery_refused(dial.err().ok_or("refused")?, "wake_and_dial_once_started");

        let recovery = tokio::time::timeout(
            budget,
            SubstrateAdapter::wake_execution(
                &substrate,
                SandboxApiMetricIdentity::native("test"),
                &sandbox,
                budget,
                None,
                charge(),
            ),
        )
        .await?;
        assert_recovery_refused(
            ControllerError::from(recovery.err().ok_or("refused")?),
            "started recovery",
        );
        assert_eq!(executions(pool, sandbox_id).await?, 0, "{phase:?}");
    }
    Ok(())
}
