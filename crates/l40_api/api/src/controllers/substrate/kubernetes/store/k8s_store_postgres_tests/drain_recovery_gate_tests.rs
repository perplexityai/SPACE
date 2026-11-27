//! The recovery gate at the two claims that would boot a `started`
//! sandbox's VM anew: `claim_started_recovery` and the pod-create claim
//! (`ensure_active_execution_claim_for_lifecycle`). Under a v2 drain of the
//! node the row is active on they answer the drain's retryable refusal
//! instead of claiming, and their drain read rides the row lock they
//! already took, so no other row's claim runs another statement.

use super::*;

/// A node row in `engine` and `phase`, the sandbox's active node.
async fn insert_active_node(
    pool: &PgPool,
    sandbox_id: SandboxId,
    engine: Option<&str>,
    phase: Option<&str>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode,
                            drain_started_at, drain_engine, drain_status)
         VALUES ($1, '10.0.0.81', 'worker', 'k8s', 'ready', $3,
                 CASE WHEN $2::TEXT IS NULL AND $3::TEXT IS NOT NULL THEN 'forced' END,
                 CASE WHEN $3::TEXT IS NOT NULL THEN NOW() END, $2,
                 CASE WHEN $2::TEXT IS NOT NULL
                      THEN '{\"generation\": 1, \"deadline_at\": 0}'::JSONB END)"
    ))
    .bind(node_id)
    .bind(engine)
    .bind(phase)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2, target_node_id = $2, placement = 'on_node'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Each drain a started sandbox's node can be in, and whether the two claims
/// refuse it: every v2 phase, its cancel included, and a drain this build
/// doesn't know; never a v1 drain or none, whose claims run as before.
const CASES: [(&str, Option<&str>, Option<&str>, bool); 8] = [
    ("no drain", None, None, false),
    ("v1 forced draining", None, Some("draining"), false),
    ("v1 drained", None, Some("drained"), false),
    ("v2 draining", Some("v2"), Some("draining"), true),
    ("v2 cancel pending", Some("v2"), None, true),
    ("v2 drained", Some("v2"), Some("drained"), true),
    ("v2 terminating", Some("v2"), Some("terminating"), true),
    ("an unknown engine", Some("v3"), Some("draining"), true),
];

async fn executions(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// Both claims of a started sandbox refuse under a v2 drain of its node,
/// with the retryable `NodeSealed` and no execution claimed, and claim as
/// before otherwise.
#[tokio::test(flavor = "current_thread")]
async fn the_recovery_claims_refuse_a_started_sandbox_under_a_v2_drain() -> TestResult {
    let fixture = postgres_store().await?;
    for (case, engine, phase, refused) in CASES {
        for claim in ["started recovery", "pod create"] {
            let sandbox_id = SandboxId::new();
            insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
            insert_active_node(&fixture.pool, sandbox_id, engine, phase).await?;
            let result = match claim {
                "started recovery" => fixture
                    .store
                    .claim_started_recovery(sandbox_id)
                    .await
                    .map(|_| ()),
                _ => fixture
                    .store
                    .ensure_active_execution_claim_for_lifecycle(
                        sandbox_id,
                        SandboxLifecycle::Started,
                        None,
                        None,
                        None,
                        None,
                    )
                    .await
                    .map(|_| ()),
            };
            let case = format!("{case}, {claim}");
            if refused {
                let error = result.expect_err(&case);
                assert!(
                    matches!(error, SubstrateError::NodeSealed { .. }),
                    "{case}: {error:?}"
                );
                assert_eq!(executions(&fixture.pool, sandbox_id).await?, 0, "{case}");
                let response = axum::response::IntoResponse::into_response(
                    crate::controllers::ControllerError::from(error),
                );
                assert_eq!(
                    response.status(),
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "{case}"
                );
                assert!(
                    response
                        .headers()
                        .contains_key(axum::http::header::RETRY_AFTER),
                    "{case}"
                );
            } else {
                result.unwrap_or_else(|error| panic!("{case}: {error:?}"));
                assert_eq!(executions(&fixture.pool, sandbox_id).await?, 1, "{case}");
            }
        }
    }
    Ok(())
}

/// A sandbox that isn't `started` is not this gate's: a suspended row's
/// claim under a v2 drain answers as it did (the portability rules and the
/// user-API gate decide it), never `NodeSealed`.
#[tokio::test(flavor = "current_thread")]
async fn the_recovery_gate_leaves_a_sandbox_that_is_not_started_alone() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
    insert_active_node(&fixture.pool, sandbox_id, Some("v2"), Some("draining")).await?;
    fixture
        .store
        .ensure_active_execution_claim_for_lifecycle(
            sandbox_id,
            SandboxLifecycle::Suspended,
            None,
            None,
            None,
            None,
        )
        .await?;
    let started_recovery = fixture.store.claim_started_recovery(sandbox_id).await;
    assert!(
        matches!(started_recovery, Err(SubstrateError::Conflict { .. })),
        "{started_recovery:?}"
    );
    Ok(())
}

/// Keep `sqlx::query` events enabled for the whole test process; see the
/// drain gate's statement tests (`drain_guard_postgres_tests.rs`).
fn keep_query_events_enabled() {
    static ALWAYS_ENABLED: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    ALWAYS_ENABLED.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
    tracing::callsite::rebuild_interest_cache();
}

/// Every statement `run` sends, as the text sqlx logs.
async fn statements<F, Fut>(run: F) -> TestResult<Vec<String>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt;

    let layer = CapturedEventsLayer::new().with_target("sqlx::query");
    let events = layer.events();
    {
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        keep_query_events_enabled();
        run().await;
    }
    Ok(events
        .events()
        .iter()
        .map(|event| {
            let statement = field_value(event, "db.statement").unwrap_or_default();
            if statement.is_empty() {
                field_value(event, "summary").unwrap_or_default().to_owned()
            } else {
                statement.to_owned()
            }
        })
        .collect())
}

/// The statements each claim sends on main, before the gate: started
/// recovery's fresh-lease read, its transaction's row lock, execution claim
/// and state update; the pod-create claim's row lock and execution claim.
/// Measured on the base commit with this test's own capture.
const MAIN_STARTED_RECOVERY_STATEMENTS: usize = 7;
const MAIN_POD_CREATE_STATEMENTS: usize = 5;

/// The gate runs no extra statement: with no v2 row, a v1 drain or none,
/// each claim sends exactly the statements it sent on main, and the node's
/// drain columns are read by one of them, the sandbox's row lock, which
/// locks the sandbox row only.
#[tokio::test(flavor = "current_thread")]
async fn the_recovery_gate_adds_no_statement() -> TestResult {
    let fixture = postgres_store().await?;
    for (case, engine, phase, _) in &CASES[..3] {
        for (claim, expected) in [
            ("started recovery", MAIN_STARTED_RECOVERY_STATEMENTS),
            ("pod create", MAIN_POD_CREATE_STATEMENTS),
        ] {
            let sandbox_id = SandboxId::new();
            insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
            insert_active_node(&fixture.pool, sandbox_id, *engine, *phase).await?;
            let store = &fixture.store;
            let sent = statements(|| async move {
                let claimed = match claim {
                    "started recovery" => {
                        store.claim_started_recovery(sandbox_id).await.map(|_| ())
                    }
                    _ => store
                        .ensure_active_execution_claim_for_lifecycle(
                            sandbox_id,
                            SandboxLifecycle::Started,
                            None,
                            None,
                            None,
                            None,
                        )
                        .await
                        .map(|_| ()),
                };
                claimed.expect("the claim runs");
            })
            .await?;
            let case = format!("{case}, {claim}");
            assert_eq!(sent.len(), expected, "{case}: {sent:#?}");
            let drain_reads: Vec<&String> = sent
                .iter()
                .filter(|statement| statement.contains("drain_engine"))
                .collect();
            assert_eq!(drain_reads.len(), 1, "{case}: {sent:#?}");
            assert!(
                drain_reads[0].contains("FOR UPDATE OF s"),
                "{case}: {drain_reads:#?}"
            );
        }
    }
    Ok(())
}

/// The orphaned-seal detector: a sealed refusal from a node Postgres
/// records no v2 drain of is counted on
/// `node_drain.v2.orphaned_seal_refusals`, tagged with the node; one from a
/// node a v2 drain holds, its cancel included, is the drain's own and isn't.
#[tokio::test(flavor = "current_thread")]
async fn a_sealed_refusal_with_no_v2_drain_recorded_is_an_orphaned_seal() -> TestResult {
    let fixture = postgres_store().await?;
    for (case, engine, phase, orphaned) in [
        ("no drain", None, None, true),
        ("a v1 drain", None, Some("draining"), true),
        ("a v2 drain", Some("v2"), Some("draining"), false),
        ("a v2 cancel pending", Some("v2"), None, false),
    ] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(&fixture.pool, sandbox_id, "suspended").await?;
        let node_id = insert_active_node(&fixture.pool, sandbox_id, engine, phase).await?;
        let counters = pplx_space_metrics_emit::test_capture::capture_counters_guard();
        fixture
            .store
            .note_sealed_refusal(sandbox_id, "node activations are sealed")
            .await;
        let counted = counters.finish();
        assert_eq!(
            counted.iter().any(
                |counter| counter.name == "node_drain.v2.orphaned_seal_refusals"
                    && counter.tags.contains(&format!("node_id:{node_id}"))
            ),
            orphaned,
            "{case}: {counted:?}"
        );
    }
    Ok(())
}
