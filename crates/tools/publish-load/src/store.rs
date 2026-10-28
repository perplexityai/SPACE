//! Synthetic fixture writes and outcome checks; never targets a shared database.

use std::path::Path;

use anyhow::{Context, Result, ensure};
use pplx_space_common_core::types::{Arch, ExecutionMetadata, NodeId, SandboxId, SandboxLifecycle};
use pplx_space_db_test_fixtures::insert_ready_worker_node;
use pplx_space_db_test_fixtures::publish_load::{
    self as fixtures, PublishSandboxFixture, TEMPLATE_NAME,
};
use pplx_space_node_api_client::proto::PublishedSandboxState;
use pplx_space_sbx_mgr_client::proto::{ArtifactKind, ArtifactOrigin, TransitionReceipt};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{Args, Node};

/// Prove the URL points to the launcher-owned cluster before fixture creation
/// or cluster-wide role bootstrap. A user-supplied URL alone is insufficient.
pub async fn verify_owned_cluster() -> Result<()> {
    let url = std::env::var("SPACE_TEST_POSTGRES_ADMIN_URL")?;
    let options: sqlx::postgres::PgConnectOptions = url.parse()?;
    ensure!(
        options.get_host() == "127.0.0.1",
        "only launcher loopback is allowed"
    );
    let expected = std::path::PathBuf::from(std::env::var("SPACE_PUBLISH_POSTGRES_DATA_DIR")?);
    let parent = expected.parent().context("missing cluster parent")?;
    ensure!(
        parent
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("space-publish-pg17-")),
        "not a disposable publish cluster"
    );
    let expected = tokio::fs::canonicalize(expected).await?;
    let pidfile = tokio::fs::read_to_string(expected.join("postmaster.pid")).await?;
    ensure!(
        pidfile.lines().next() == Some(std::env::var("SPACE_PUBLISH_POSTGRES_PID")?.as_str()),
        "postmaster PID does not match the launcher"
    );
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let actual: String = sqlx::query_scalar(sql!("SHOW data_directory"))
        .fetch_one(&pool)
        .await?;
    pool.close().await;
    ensure!(
        tokio::fs::canonicalize(actual).await? == expected,
        "URL does not address the launcher-owned cluster"
    );
    Ok(())
}

fn id(seed: u64, node: u32, sandbox: u32) -> Uuid {
    Uuid::from_u128(
        (u128::from(seed) << 96)
            | (0x7000_u128 << 64)
            | (0x8000_u128 << 48)
            | (u128::from(node) << 24)
            | u128::from(sandbox),
    )
}

fn rare_lifecycles(node: u32) -> (SandboxLifecycle, SandboxLifecycle) {
    use SandboxLifecycle::*;
    match node % 5 {
        0 => (Stopping, Stopped),
        1 => (Restoring, Started),
        2 => (Suspending, Suspended),
        3 => (Stopped, Stopped),
        _ => (Quarantined, Started),
    }
}

/// Seed only a fresh fixture database after `verify_owned_cluster` succeeds.
/// Each node's new sandbox and execution rows share a transaction; conflicts
/// fail rather than overwriting any existing fixture or deployment records.
pub async fn seed(pool: &PgPool, args: &Args) -> Result<Vec<Node>> {
    let execution = ExecutionMetadata::initial_firecracker(Arch::X86_64, Vec::new());
    fixtures::insert_template(pool, id(args.seed, args.nodes, 0))
        .await
        .map_err(anyhow::Error::from_boxed)?;
    let mut nodes = Vec::new();
    for node_index in 0..args.nodes {
        let node_id: NodeId = id(args.seed, node_index, 0).to_string().parse()?;
        insert_ready_worker_node(pool, node_id)
            .await
            .map_err(anyhow::Error::from_boxed)?;
        let mut sandboxes = Vec::new();
        let mut transaction = pool.begin().await?;
        for sandbox_index in 1..=args.sandboxes_per_node {
            let sandbox_id = SandboxId::from_uuid(id(args.seed, node_index, sandbox_index));
            let lifecycle = if sandbox_index == 1 {
                rare_lifecycles(node_index).0
            } else {
                SandboxLifecycle::Started
            };
            fixtures::insert_sandbox(
                &mut transaction,
                &PublishSandboxFixture {
                    id: sandbox_id,
                    node_id,
                    lifecycle,
                    activation_attempt_id: id(args.seed, node_index, args.sandboxes_per_node + 3),
                },
            )
            .await
            .map_err(anyhow::Error::from_boxed)?;
            if lifecycle == SandboxLifecycle::Quarantined {
                fixtures::insert_pause_failed(
                    &mut transaction,
                    sandbox_id,
                    node_id,
                    id(args.seed, node_index, args.sandboxes_per_node + 1),
                )
                .await
                .map_err(anyhow::Error::from_boxed)?;
            }
            let state = PublishedSandboxState {
                sandbox_id: sandbox_id.to_string(),
                present: true,
                status: Some(
                    match lifecycle {
                        SandboxLifecycle::Stopped => "creating",
                        SandboxLifecycle::Quarantined => "pausing",
                        _ => lifecycle.as_str(),
                    }
                    .into(),
                ),
                template: Some(TEMPLATE_NAME.into()),
                runtime: Some("fc".into()),
                snapshots_enabled: true,
                pid: Some(1000 + sandbox_index),
                pod_uid: Some(format!("synthetic-{sandbox_id}")),
                execution_generation: Some(1),
                activation_attempt_id: Some(
                    id(args.seed, node_index, args.sandboxes_per_node + 3).to_string(),
                ),
                execution: Some((&execution).into()),
                ..Default::default()
            };
            sandboxes.push(state);
        }
        transaction.commit().await?;
        let mut rare = sandboxes.first().context("rare sandbox missing")?.clone();
        rare.status = Some(rare_lifecycles(node_index).1.as_str().into());
        if node_index % 5 == 2 {
            rare.current_local_artifact_id =
                Some(id(args.seed, node_index, args.sandboxes_per_node + 2).to_string());
            rare.current_local_artifact_kind = Some(ArtifactKind::Full.into());
            rare.current_local_artifact_origin = Some(ArtifactOrigin::Suspend.into());
            fixtures::insert_ready_suspend(
                pool,
                id(args.seed, node_index, args.sandboxes_per_node + 4),
                SandboxId::from_uuid(id(args.seed, node_index, 1)),
                node_id,
                id(args.seed, node_index, args.sandboxes_per_node + 2),
            )
            .await
            .map_err(anyhow::Error::from_boxed)?;
        }
        if node_index % 5 == 3 {
            rare.transition_receipt = Some(TransitionReceipt {
                id: id(args.seed, node_index, args.sandboxes_per_node + 5).to_string(),
                kind: "activation_rollback".into(),
                execution_generation: 1,
                target_state: "stopped".into(),
                reason_code: "cold_activation_interrupted".into(),
                activation_attempt_id: rare.activation_attempt_id.clone(),
            });
        }
        nodes.push(Node {
            id: node_id.to_string(),
            sandboxes,
            rare,
            retire_after_rare: matches!(node_index % 5, 2 | 3),
        });
    }
    Ok(nodes)
}

/// Read final rare-transition outcomes without acquiring explicit row locks.
/// Missing rows or a transition that matched no eligible row fail the run.
pub async fn verify_transitions(pool: &PgPool, args: &Args) -> Result<()> {
    let mut mismatches = Vec::new();
    for node in 0..args.nodes {
        let fixtures::outcome::RareOutcome {
            lifecycle: actual,
            unplaced,
            active,
            quarantined,
        } = fixtures::outcome::rare_outcome(pool, SandboxId::from_uuid(id(args.seed, node, 1)))
            .await
            .map_err(anyhow::Error::from_boxed)?
            .context("rare sandbox missing")?;
        let expected = rare_lifecycles(node).1;
        if actual != expected.as_str()
            || (node % 5 == 3 && (!unplaced || active))
            || (node % 5 == 4 && quarantined)
        {
            mismatches.push(format!(
                "node {node}: expected {expected}, got {actual}, unplaced={unplaced}, \
                 active_execution={active}, quarantined={quarantined}"
            ));
        }
    }
    ensure!(
        mismatches.is_empty(),
        "rare transitions did not complete: {mismatches:?}"
    );
    Ok(())
}

/// Export publish-owned fixture projections without explicit row locks.
/// Empty tables are meaningful empty JSON arrays, not validation failures.
pub async fn dump(pool: &PgPool, output: &Path) -> Result<()> {
    // Raw rows are retained separately from summary metrics. Comparisons must
    // explicitly account for server timestamps and generated history UUIDs;
    // this export alone does not assert row equivalence.
    for (table, rows) in fixtures::outcome::dump(pool)
        .await
        .map_err(anyhow::Error::from_boxed)?
    {
        tokio::fs::write(
            output.join(format!("{table}.json")),
            serde_json::to_vec_pretty(&rows)?,
        )
        .await?;
    }
    Ok(())
}
