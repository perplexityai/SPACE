use super::*;
use axum::http::Method;
use pplx_space_common_core::sql;
use pplx_space_sandbox_store::dead_node::{DeadNodeReportWrite, DeadNodeSample, upsert_report};
use serde_json::{Value, json};

use crate::routes::admin::test_fixtures::{TestResult, admin_router_harness};

async fn insert_node(pool: &sqlx::PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.7'::INET, 'worker', 'k8s')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// `observed_at` with its value checked as RFC 3339 and replaced, so the rest
/// of an entry compares exactly.
fn without_observed_at(mut entry: Value) -> Value {
    let observed_at = entry["observed_at"].as_str().expect("observed_at string");
    OffsetDateTime::parse(observed_at, &time::format_description::well_known::Rfc3339)
        .expect("observed_at is RFC 3339");
    entry["observed_at"] = json!("<observed_at>");
    entry
}

/// Operators only. Empty until the controller judges a node; then one entry
/// per node by node id, with each verdict's reason, counts and samples as
/// stored, and nothing for an outcome without rows.
#[tokio::test(flavor = "current_thread")]
async fn dry_runs_list_each_judged_node_with_its_verdict_and_outcomes() -> TestResult {
    let Some(harness) = admin_router_harness().await? else {
        return Ok(());
    };
    harness
        .assert_refused_to_non_operators(Method::GET, "/dead-node-dry-runs", None)
        .await;

    let (status, body) = harness
        .as_operator(Method::GET, "/dead-node-dry-runs", &[], None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"dry_runs": []}));

    let pool = &harness.db.pool;
    let mut lost_node = insert_node(pool).await?;
    let mut unknown_node = insert_node(pool).await?;
    if unknown_node < lost_node {
        std::mem::swap(&mut lost_node, &mut unknown_node);
    }
    let lost_sandbox = SandboxId::new();
    let stopped_sandbox = SandboxId::new();
    upsert_report(
        pool,
        &DeadNodeReportWrite {
            node_id: lost_node,
            verdict: "lost",
            reason: "node_deleted",
            rows_truncated: true,
            counts: [("lost", 3), ("stays_stopped", 1)].into(),
            samples: [
                (
                    "lost",
                    vec![DeadNodeSample {
                        sandbox_id: lost_sandbox,
                        lifecycle: "started".to_owned(),
                    }],
                ),
                (
                    "stays_stopped",
                    vec![DeadNodeSample {
                        sandbox_id: stopped_sandbox,
                        lifecycle: "stopped".to_owned(),
                    }],
                ),
            ]
            .into(),
        },
    )
    .await?;
    upsert_report(
        pool,
        &DeadNodeReportWrite {
            node_id: unknown_node,
            verdict: "unknown",
            reason: "kube_node_present",
            rows_truncated: false,
            counts: BTreeMap::new(),
            samples: BTreeMap::new(),
        },
    )
    .await?;

    let (status, body) = harness
        .as_operator(Method::GET, "/dead-node-dry-runs", &[], None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let entries: Vec<Value> = body["dry_runs"]
        .as_array()
        .expect("dry_runs array")
        .iter()
        .cloned()
        .map(without_observed_at)
        .collect();
    assert_eq!(
        entries,
        vec![
            json!({
                "node_id": lost_node,
                "observed_at": "<observed_at>",
                "verdict": "lost",
                "reason": "node_deleted",
                "rows_truncated": true,
                "counts": {"lost": 3, "stays_stopped": 1},
                "samples": {
                    "lost": [{"sandbox_id": lost_sandbox, "lifecycle": "started"}],
                    "stays_stopped": [{"sandbox_id": stopped_sandbox, "lifecycle": "stopped"}],
                },
            }),
            json!({
                "node_id": unknown_node,
                "observed_at": "<observed_at>",
                "verdict": "unknown",
                "reason": "kube_node_present",
                "rows_truncated": false,
                "counts": {},
                "samples": {},
            }),
        ]
    );
    assert_eq!(
        body.as_object().map(|object| object.len()),
        Some(1),
        "{body}"
    );
    Ok(())
}
