//! Postgres-backed coverage for the terminator's substrate scope: each
//! statement binds [`SubstrateKind::Kubernetes`] for `nodes.managed_by`, so a
//! `drained` direct (`'space'`) node that otherwise matches is never listed,
//! reaped, or used to give up its sandboxes.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    DEAD_UPLOAD_SETTLE, DrainedNodeTerminatorSink, NodeDbStore, OrphanDeleteOutcome,
    OrphanDeleteRefusal, ResidencyEvidence, mark_unrecoverable_sandboxes_lost_query,
};
use crate::lifecycle_writer_test_support::{
    SandboxSeed, bounded, insert_sandbox, single_backend_pool, wait_until_blocked,
};

const SUBSTRATES: [(SubstrateKind, bool); 2] = [
    (SubstrateKind::Kubernetes, true),
    (SubstrateKind::Space, false),
];

fn node_uid(node_id: NodeId) -> String {
    format!("uid-{node_id}")
}

/// A `drained` node of `managed_by` with a `node_k8s` identity, so only the
/// substrate scope can tell a direct row apart.
async fn insert_drained_node(pool: &PgPool, managed_by: SubstrateKind) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.5'::inet, 'worker', $2, 'registered', 'drained', NOW())"
    ))
    .bind(node_id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("ip-test-{node_id}"))
    .bind(node_uid(node_id))
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A `suspended` sandbox pinned to `node_id` with no snapshot at all: the
/// mark-lost give-up's candidate shape.
async fn insert_unrecoverable_sandbox(pool: &PgPool, node_id: NodeId) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let seed = SandboxSeed {
        target_node_id: Some(node_id),
        ..SandboxSeed::new("suspended", "k8s")
    };
    insert_sandbox(pool, sandbox_id, &seed).await?;
    Ok(sandbox_id)
}

#[tokio::test(flavor = "current_thread")]
async fn list_and_reap_skip_direct_nodes() -> TestResult {
    let db = pg_test_db().await?;
    let store = NodeDbStore::new(db.pool.clone(), ResidencyEvidence::default());
    for (managed_by, kubernetes) in SUBSTRATES {
        let node_id = insert_drained_node(&db.pool, managed_by).await?;
        assert_eq!(
            store.list_drained_nodes().await?.contains(&node_id),
            kubernetes,
            "{managed_by} drained node listing"
        );
        let expected = if kubernetes {
            OrphanDeleteOutcome::Deleted
        } else {
            // The diagnosis re-checks the same scope: a direct row reads as
            // drifted out of the reapable phase, and nothing else blocks it.
            OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
                phase_drifted: true,
                ..OrphanDeleteRefusal::default()
            })
        };
        assert_eq!(
            store
                .delete_drained_node_if_orphan(node_id, Some(node_uid(node_id)))
                .await?,
            expected,
            "{managed_by} drained node reap"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mark_lost_update_skips_direct_nodes() -> TestResult {
    let db = pg_test_db().await?;
    let query = mark_unrecoverable_sandboxes_lost_query();
    for (managed_by, kubernetes) in SUBSTRATES {
        let node_id = insert_drained_node(&db.pool, managed_by).await?;
        let sandbox_id = insert_unrecoverable_sandbox(&db.pool, node_id).await?;
        let marked: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(node_id)
            .bind(Some(node_uid(node_id)))
            .bind(vec![sandbox_id])
            .bind(DEAD_UPLOAD_SETTLE.as_secs().cast_signed())
            .bind(SubstrateKind::Kubernetes)
            .fetch_all(&db.pool)
            .await?;
        let expected = if kubernetes {
            vec![sandbox_id]
        } else {
            Vec::new()
        };
        assert_eq!(marked, expected, "{managed_by} node mark-lost flip");
    }
    Ok(())
}

/// The give-up's `FOR SHARE` recheck refuses a direct node before the
/// transaction claims any sandbox row. A held lock on the candidate tells the
/// two apart: the claim would wait on it, the refusal never reaches it.
#[tokio::test(flavor = "current_thread")]
async fn mark_lost_recheck_refuses_direct_nodes_before_claiming() -> TestResult {
    let db = pg_test_db().await?;
    for (managed_by, kubernetes) in SUBSTRATES {
        let node_id = insert_drained_node(&db.pool, managed_by).await?;
        let sandbox_id = insert_unrecoverable_sandbox(&db.pool, node_id).await?;

        let mut holder = db.pool.begin().await?;
        let holder_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *holder)
            .await?;
        sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id)
            .execute(&mut *holder)
            .await?;

        let (writer_pool, writer_pid) = single_backend_pool(&db).await?;
        let store = NodeDbStore::new(writer_pool, ResidencyEvidence::default());
        let give_up = tokio::spawn(async move {
            store
                .mark_unrecoverable_sandboxes_lost(node_id, Some(node_uid(node_id)))
                .await
        });
        if kubernetes {
            bounded(wait_until_blocked(&db.pool, writer_pid, holder_pid)).await?;
            holder.rollback().await?;
            let marked = bounded(async { Ok(give_up.await??) }).await?;
            assert_eq!(marked, vec![sandbox_id], "{managed_by} node give-up");
        } else {
            let marked = bounded(async { Ok(give_up.await??) }).await?;
            assert_eq!(marked, Vec::<SandboxId>::new(), "{managed_by} node give-up");
            holder.rollback().await?;
        }
    }
    Ok(())
}
