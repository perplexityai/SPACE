//! Stored-lifecycle matrix for the drained-node terminator's mark-lost, run
//! through [`NodeDbStore`] (which claims before it flips) against a row and
//! node that satisfy every other guard, so the stored lifecycle is the only
//! variable. The node drain give-ups of the same shape are swept through
//! their sink in node drain's `lifecycle_writer_tests`.
//!
//! The expected table is written out rather than derived from the statement.
//! The other guards are pinned at the query level in
//! `inflight_upload_terminate_gate_tests`.

use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_placement::test_support::stored_lifecycle_matrix;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;
use crate::lifecycle_writer_test_support::{SandboxSeed, insert_node, insert_sandbox};

/// The kube uid every node here registers and the mark-lost binds.
const NODE_UID: &str = "uid-synthetic";

/// `A`: the mark-lost moves the row to `failed`; `.`: it refuses and the row
/// keeps its lifecycle. The statement compares the lifecycle exactly, so
/// neither non-canonical value is claimed.
const EXPECTED: &[&str] = &[
    "created           .",
    "creating          .",
    "starting          .",
    "started           .",
    "restoring         .",
    "pausing           .",
    "paused            .",
    "resuming          .",
    "suspending        .",
    "suspended         A",
    "stopping          .",
    "stopped           .",
    "deleting          .",
    "checkpointing     .",
    "quarantined       .",
    "failed            .",
    "deleted           .",
    "cleanup_required  .",
    "bogus             .",
    "_Suspended_       .",
];

/// A `drained` `k8s` worker registered under [`NODE_UID`].
async fn insert_drained_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = insert_node(pool, Some(PHASE_DRAINED), None).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'ip-10-0-0-99.synthetic.internal', $2)"
    ))
    .bind(node_id)
    .bind(NODE_UID)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn drained_node_mark_lost_fails_only_suspended_rows() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let store = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let mut mismatches = Vec::new();
    for (stored, [(_, fails)]) in stored_lifecycle_matrix(["mark_lost"], EXPECTED) {
        let fails = fails.applies();
        // An hour-old unmarked `k8s` row in `stored`, active and targeted on
        // its own drained node, with no snapshot.
        let node_id = insert_drained_node(pool).await?;
        let sandbox_id = SandboxId::new();
        insert_sandbox(
            pool,
            sandbox_id,
            &SandboxSeed::new(stored, "k8s").on_node(node_id),
        )
        .await?;

        let flipped = store
            .mark_unrecoverable_sandboxes_lost(node_id, Some(NODE_UID.to_owned()))
            .await?
            .contains(&sandbox_id);

        let lifecycle = lifecycle_of(pool, sandbox_id).await?;
        let expected_lifecycle = if fails { "failed" } else { stored };
        if flipped != fails || lifecycle != expected_lifecycle {
            mismatches.push(format!(
                "from {stored}: reported {flipped}, row now {lifecycle}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
