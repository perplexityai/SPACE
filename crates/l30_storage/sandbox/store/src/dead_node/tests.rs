use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::collections::BTreeMap;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.5'::INET, 'worker', 'k8s')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

fn status<'a>(node_id: NodeId, verdict: &'a str, lost: u64) -> DeadNodeStatusWrite<'a> {
    DeadNodeStatusWrite {
        node_id,
        mode: "dry-run",
        verdict,
        reason: "no_instance_authority",
        rows_truncated: false,
        instances: BTreeMap::from([("i-0123456789abcdef0", "dead")]),
        counts: BTreeMap::from([("lost", lost)]),
        applied: BTreeMap::new(),
        samples: BTreeMap::from([(
            "lost",
            vec![DeadNodeSample {
                sandbox_id: SandboxId::new(),
                lifecycle: "started".to_owned(),
            }],
        )]),
    }
}

/// One status per node: a later write replaces every field, a deleted node
/// takes its status with it, a node that no longer exists gets none, and a
/// node never judged lists none.
#[tokio::test(flavor = "current_thread")]
async fn one_status_per_node_lives_and_dies_with_the_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let kept = insert_node(pool).await?;
    let deleted = insert_node(pool).await?;
    let _never_judged = insert_node(pool).await?;

    assert!(write_status(pool, &status(kept, "unknown", 1)).await?);
    let first_observed = statuses(pool, 10).await?[0].observed_at;
    let latest = DeadNodeStatusWrite {
        mode: "live",
        reason: "instance_dead",
        rows_truncated: true,
        applied: BTreeMap::from([("lost", 1)]),
        ..status(kept, "lost", 2)
    };
    assert!(write_status(pool, &latest).await?);
    assert!(write_status(pool, &status(deleted, "lost", 3)).await?);
    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
        .bind(deleted)
        .execute(pool)
        .await?;
    assert!(!write_status(pool, &status(NodeId::new(), "lost", 4)).await?);

    let stored = statuses(pool, 10).await?;
    let expected_samples: BTreeMap<String, Vec<DeadNodeSample>> = latest
        .samples
        .iter()
        .map(|(outcome, rows)| ((*outcome).to_owned(), rows.clone()))
        .collect();
    assert_eq!(stored.len(), 1, "{stored:?}");
    let only = &stored[0];
    assert_eq!(
        (
            only.node_id,
            only.mode.as_str(),
            only.verdict.as_str(),
            only.reason.as_str(),
            only.rows_truncated,
        ),
        (kept, "live", "lost", "instance_dead", true)
    );
    assert_eq!(
        only.instances,
        BTreeMap::from([("i-0123456789abcdef0".to_owned(), "dead".to_owned())])
    );
    assert_eq!(only.counts, BTreeMap::from([("lost".to_owned(), 2)]));
    assert_eq!(only.applied, BTreeMap::from([("lost".to_owned(), 1)]));
    assert_eq!(only.samples, expected_samples);
    assert!(
        only.observed_at > first_observed,
        "{} then {first_observed}",
        only.observed_at
    );
    Ok(())
}

/// The row read stops at its limit, in id order.
#[tokio::test(flavor = "current_thread")]
async fn rows_on_node_stops_at_its_limit_in_id_order() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = insert_node(pool).await?;
    let mut ids = Vec::new();
    for _ in 0..3 {
        let id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation)
             VALUES ($1, $2::UUID, $3::UUID, $4, 'started', 'on_node', $5, $5, 1)"
        ))
        .bind(id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(SubstrateKind::Kubernetes)
        .bind(node_id)
        .execute(pool)
        .await?;
        ids.push(id);
    }
    ids.sort();
    let read: Vec<SandboxId> = rows_on_node(
        pool,
        NodeRef {
            node_id,
            k8s_node_name: None,
            managed_by: SubstrateKind::Kubernetes,
        },
        crate::lost::DEAD_UPLOAD_SETTLE,
        2,
    )
    .await?
    .into_iter()
    .map(|row| row.sandbox_id)
    .collect();
    assert_eq!(read, ids[..2]);
    Ok(())
}

async fn insert_sandbox(
    pool: &PgPool,
    lifecycle: &str,
    active: Option<NodeId>,
    target: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             placement_changed_at, command_ready_pending)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5,
                 CASE WHEN $6::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                 $6, $7, 1, NOW(), TRUE)"
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

async fn insert_execution(pool: &PgPool, sandbox_id: SandboxId, active: bool) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active,
             pod_ip)
         VALUES ($1, 1, $2, 'sandboxes', 'worker-synthetic', 'running', $3, '10.1.0.1'::INET)
         RETURNING id"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{}", sandbox_id.as_uuid().simple()))
    .bind(active)
    .fetch_one(pool)
    .await?)
}

/// `(lifecycle, placement, active_node_id, target_node_id)`.
type Placement = (String, String, Option<NodeId>, Option<NodeId>);

async fn placement(pool: &PgPool, id: SandboxId) -> TestResult<Placement> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, active_node_id, target_node_id
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn execution_active(pool: &PgPool, execution_id: Uuid) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT active FROM sandbox_k8s_executions WHERE id = $1"
    ))
    .bind(execution_id)
    .fetch_one(pool)
    .await?)
}

/// Each write changes the row only under its guard: the lifecycle the caller
/// re-read and a reference to the node. A release clears both references,
/// or only `active_node_id` when it keeps the target; a lost flip moves only
/// the expected lifecycle; an execution is retired only when it is the one
/// named and still active.
#[tokio::test(flavor = "current_thread")]
async fn each_write_changes_the_row_only_under_its_guard() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other = insert_node(pool).await?;
    let unplaced =
        |lifecycle: &str, target| (lifecycle.to_owned(), "unplaced".to_owned(), None, target);

    // (lifecycle, active, target, guard lifecycle, keep target, released?, after)
    let releases = [
        (
            "suspended",
            Some(node),
            Some(node),
            "suspended",
            false,
            true,
            unplaced("suspended", None),
        ),
        (
            "suspended",
            None,
            Some(node),
            "suspended",
            false,
            true,
            unplaced("suspended", None),
        ),
        (
            "failed",
            Some(node),
            Some(node),
            "failed",
            true,
            true,
            unplaced("failed", Some(node)),
        ),
        (
            "failed",
            None,
            Some(node),
            "failed",
            true,
            false,
            unplaced("failed", Some(node)),
        ),
        (
            "suspended",
            Some(node),
            Some(node),
            "started",
            false,
            false,
            (
                "suspended".to_owned(),
                "on_node".to_owned(),
                Some(node),
                Some(node),
            ),
        ),
        (
            "stopped",
            Some(other),
            Some(other),
            "stopped",
            false,
            false,
            (
                "stopped".to_owned(),
                "on_node".to_owned(),
                Some(other),
                Some(other),
            ),
        ),
    ];
    for (lifecycle, active, target, guard, keep_target, released, after) in releases {
        let id = insert_sandbox(pool, lifecycle, active, target).await?;
        let mut tx = pool.begin().await?;
        assert_eq!(
            release_from_node(&mut tx, id, node, guard, keep_target).await?,
            released,
            "{lifecycle} {active:?} {guard} {keep_target}"
        );
        tx.commit().await?;
        assert_eq!(placement(pool, id).await?, after, "{lifecycle} {guard}");
    }

    for (lifecycle, guard, flipped) in [("started", "started", true), ("started", "paused", false)]
    {
        let id = insert_sandbox(pool, lifecycle, Some(node), Some(node)).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET restore_episode_target = 'started' WHERE id = $1"
        ))
        .bind(id)
        .execute(pool)
        .await?;
        let mut tx = pool.begin().await?;
        assert_eq!(mark_lost(&mut tx, id, guard).await?, flipped, "{guard}");
        tx.commit().await?;
        let (stored, episode): (String, Option<String>) = sqlx::query_as(sql!(
            "SELECT lifecycle, restore_episode_target FROM sandboxes WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await?;
        let expected = if flipped {
            ("failed".to_owned(), None)
        } else {
            (lifecycle.to_owned(), Some("started".to_owned()))
        };
        assert_eq!((stored, episode), expected, "{guard}");
    }

    let id = insert_sandbox(pool, "started", Some(node), Some(node)).await?;
    let active = insert_execution(pool, id, true).await?;
    let mut tx = pool.begin().await?;
    assert!(!retire_source_execution(&mut tx, id, Uuid::now_v7()).await?);
    assert!(retire_source_execution(&mut tx, id, active).await?);
    assert!(!retire_source_execution(&mut tx, id, active).await?);
    tx.commit().await?;
    assert!(!execution_active(pool, active).await?);
    Ok(())
}

/// The dead-node write-off retires only the source execution it names, none
/// when it names none, clears only the node's backup-queue rows, and
/// releases the placement.
#[tokio::test(flavor = "current_thread")]
async fn the_dead_node_write_off_retires_only_its_source_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other = insert_node(pool).await?;
    for named in [true, false] {
        let id = insert_sandbox(pool, "failed", Some(node), Some(node)).await?;
        let execution = insert_execution(pool, id, true).await?;
        for queue_node in [node, other] {
            sqlx::query(sql!(
                "INSERT INTO node_local_backup_pending
                    (node_id, sandbox_id, artifact_id, artifact_kind)
                 VALUES ($1, $2, space_uuid_v7(), 'full')"
            ))
            .bind(queue_node)
            .bind(id)
            .execute(pool)
            .await?;
        }
        let mut tx = pool.begin().await?;
        crate::lost::write_off_lost_sandbox(&mut tx, node, id, named.then_some(execution)).await?;
        tx.commit().await?;

        assert_eq!(execution_active(pool, execution).await?, !named, "{named}");
        let queued: Vec<NodeId> = sqlx::query_scalar(sql!(
            "SELECT node_id FROM node_local_backup_pending WHERE sandbox_id = $1"
        ))
        .bind(id)
        .fetch_all(pool)
        .await?;
        assert_eq!(queued, vec![other], "{named}");
        assert_eq!(
            placement(pool, id).await?,
            ("failed".to_owned(), "unplaced".to_owned(), None, Some(node)),
            "{named}"
        );
    }
    Ok(())
}
