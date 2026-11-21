//! A start of a `stopped` sandbox reuses the execution the stop left active,
//! and so its pod name. When the reflector misses a terminal pod at that
//! name, the start POSTs, gets a 409, deletes the stale pod and retires the
//! execution without creating a pod. The row must keep the node holding its
//! disk through that retirement.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::sandbox::fetch_sandbox;
use crate::test_support::kubernetes::{WORKER_NODE, insert_kubernetes_worker_node};

/// The start that meets the stale pod retires its execution and creates no
/// pod, but the row stays `stopped` on the node holding its disk. The retry
/// then restarts it on that node under a new generation.
#[tokio::test(flavor = "current_thread")]
async fn a_start_that_meets_a_stale_pod_keeps_the_stopped_sandbox_on_its_node() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&pool, sandbox_id, "stopped").await?;
    let node_id = insert_kubernetes_worker_node(&pool).await?;
    sqlx::query("UPDATE sandboxes SET active_node_id = $2 WHERE id = $1")
        .bind(sandbox_id)
        .bind(node_id)
        .execute(&pool)
        .await?;
    // The execution the stop left active: created before the stop, and
    // recording no node, so only the row's placement says where the disk is.
    let pod_name = format!("space-sbx-{}-g1", sandbox_id.as_uuid().simple());
    insert_k8s_execution(&pool, sandbox_id, 1, &pod_name, "uid-stale", None, true).await?;
    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET created_at = NOW() - INTERVAL '1 hour'
          WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
    // The reflector is empty, so the stale pod is a cache miss; the apiserver
    // answers the first POST 409 and GETs back a `Succeeded` pod at the name.
    let substrate = substrate_with_store_and_client(
        pool.clone(),
        conflict_then_created_kubernetes_client(Arc::clone(&captured_pod_creates)),
    );
    let sandbox = fetch_sandbox(&pool, sandbox_id)
        .await?
        .expect("the fixture just inserted this sandbox");

    substrate
        .ensure_pod_exists(&sandbox, None, None, None, Default::default())
        .await?;

    let after_conflict: (String, String, bool, Option<NodeId>) = sqlx::query_as(
        "SELECT s.lifecycle, e.state, e.active, s.active_node_id
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.generation = 1
          WHERE s.id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        after_conflict,
        (
            "stopped".to_string(),
            "replaced".to_string(),
            false,
            Some(node_id)
        ),
        "the conflict retires the stale execution, and the stopped sandbox stays on the node \
         holding its disk, where a drain sees it"
    );

    substrate
        .ensure_pod_exists(&sandbox, None, None, None, Default::default())
        .await?;

    let creates = captured_pod_creates
        .lock()
        .expect("capture mutex poisoned")
        .clone();
    assert_eq!(creates.len(), 2, "one conflicted POST, then the retry's");
    let retry = creates.last().expect("two creates were just counted");
    assert_eq!(
        (
            retry.pointer("/metadata/name").and_then(|v| v.as_str()),
            retry.pointer("/spec/nodeName").and_then(|v| v.as_str()),
        ),
        (
            Some(format!("space-sbx-{}-g2", sandbox_id.as_uuid().simple()).as_str()),
            Some(WORKER_NODE),
        ),
        "the retry must restart the sandbox on its disk's node under a new generation"
    );
    Ok(())
}
