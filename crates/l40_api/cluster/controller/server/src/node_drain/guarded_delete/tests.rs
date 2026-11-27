//! The guarded delete against Postgres: each guard refuses on its own, and
//! names what holds the node; a node nothing names goes with no tombstone.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

const INSTANCE: &str = "i-0000000000000a0a0";
const UID: &str = "uid-synthetic";

/// The first guard that holds the node: `active_node_id`, `target_node_id`
/// or `active execution`. `None` when nothing does.
fn first_guard(references: &NodeReferences) -> Option<&'static str> {
    [
        ("active_node_id", &references.active),
        ("target_node_id", &references.target),
        ("active execution", &references.executions),
    ]
    .into_iter()
    .find_map(|(guard, ids)| (!ids.is_empty()).then_some(guard))
}

fn name_of(node_id: NodeId) -> String {
    format!("worker-{}.synthetic.internal", node_id.as_uuid().simple())
}

/// A Kubernetes node with no heartbeat, recording [`UID`] and [`INSTANCE`].
async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.8'::INET, 'worker', $2)"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(node_id)
    .bind(name_of(node_id))
    .bind(UID)
    .bind(INSTANCE)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn heartbeat(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots
            (node_id, observed_at, stale_after, reachable, services, updated_at)
         VALUES ($1, NOW(), NOW() + INTERVAL '1 minute', TRUE, '[]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_row(
    pool: &PgPool,
    lifecycle: &str,
    active: Option<NodeId>,
    target: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5,
                 CASE WHEN $6::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                 $6, $7, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(lifecycle)
    .bind(active)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_execution(pool: &PgPool, sandbox_id: SandboxId, node_name: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active)
         VALUES ($1, 1, $2, 'sandboxes', $3, 'running', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{}", sandbox_id.as_uuid().simple()))
    .bind(node_name)
    .execute(pool)
    .await?;
    Ok(())
}

async fn node_exists(pool: &PgPool, node_id: NodeId) -> TestResult<bool> {
    Ok(
        sqlx::query_scalar::<_, i32>(sql!("SELECT 1 FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_optional(pool)
            .await?
            .is_some(),
    )
}

async fn delete(
    pool: &PgPool,
    node_id: NodeId,
    uid: &str,
    instance: &str,
) -> TestResult<GuardedDelete> {
    let name = name_of(node_id);
    let mut tx = pool.begin().await?;
    assert!(lock_node(&mut *tx, node_id).await?);
    let outcome = delete_locked_node(
        &mut tx,
        &NodeIdentity {
            node_id,
            managed_by: SubstrateKind::Kubernetes,
            k8s_node_name: Some(&name),
            k8s_node_uid: Some(uid),
            instance_id: Some(instance),
        },
    )
    .await?;
    tx.commit().await?;
    Ok(outcome)
}

/// Each guard refuses the delete on its own and leaves the row: an identity
/// the caller didn't read, a fresh heartbeat, and each reference, which the
/// refusal names. Rows in `failed` or `deleted` hold nothing; a node nothing
/// names goes, with its `node_k8s` row and no tombstone.
#[tokio::test(flavor = "current_thread")]
async fn each_guard_refuses_the_delete_and_names_what_holds_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for case in [
        "uid moved",
        "instance moved",
        "heartbeat fresh",
        "active_node_id",
        "target_node_id",
        "active execution",
        "only terminal rows",
    ] {
        let node_id = insert_node(pool).await?;
        let (mut uid, mut instance) = (UID, INSTANCE);
        let mut held = None;
        match case {
            "uid moved" => uid = "uid-other",
            "instance moved" => instance = "i-0000000000000b0b0",
            "heartbeat fresh" => heartbeat(pool, node_id).await?,
            "active_node_id" => {
                held = Some(insert_row(pool, "quarantined", Some(node_id), None).await?);
            }
            "target_node_id" => {
                held = Some(insert_row(pool, "suspended", None, Some(node_id)).await?);
            }
            "active execution" => {
                let row = insert_row(pool, "started", None, None).await?;
                insert_execution(pool, row, &name_of(node_id)).await?;
                held = Some(row);
            }
            _ => {
                insert_row(pool, "failed", None, Some(node_id)).await?;
                insert_row(pool, "deleted", Some(node_id), Some(node_id)).await?;
            }
        }
        let outcome = delete(pool, node_id, uid, instance).await?;
        let expected = match (case, held) {
            ("uid moved" | "instance moved", _) => GuardedDelete::IdentityMoved,
            ("heartbeat fresh", _) => GuardedDelete::HeartbeatFresh,
            ("only terminal rows", _) => GuardedDelete::Deleted,
            (_, Some(id)) => {
                let mut references = NodeReferences::default();
                match case {
                    "active_node_id" => references.active.push(id),
                    "target_node_id" => references.target.push(id),
                    _ => references.executions.push(id),
                }
                assert_eq!(first_guard(&references), Some(case), "{case}");
                GuardedDelete::Referenced(references)
            }
            (_, None) => unreachable!("{case}"),
        };
        assert_eq!(outcome, expected, "{case}");
        let deleted = expected == GuardedDelete::Deleted;
        assert_eq!(node_exists(pool, node_id).await?, !deleted, "{case}");
        if deleted {
            let left: (bool, bool) = sqlx::query_as(sql!(
                "SELECT EXISTS (SELECT 1 FROM node_k8s WHERE node_id = $1),
                        EXISTS (SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1)"
            ))
            .bind(node_id)
            .fetch_one(pool)
            .await?;
            assert_eq!(left, (false, false), "{case}");
        }
    }
    Ok(())
}
