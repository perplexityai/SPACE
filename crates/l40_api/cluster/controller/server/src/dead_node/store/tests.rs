//! The store's node statements against Postgres: which nodes are judged,
//! which serve as a reference, when a lost node's row is deleted, and a row
//! write racing a restore claim for the row lock.

use pplx_space_common_core::types::SandboxId;

use pplx_space_db_test_fixtures::{pg_test_db_with_max_connections, try_pg_test_db};
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::*;
use crate::dead_node::row_seed_tests::{insert_row, insert_stamped_execution};

const OWN: &str = "i-0000000000000a0a0";
const OTHER: &str = "i-0000000000000b0b0";

#[derive(Debug, Clone, Copy)]
enum Status {
    /// Reachable, `stale_after` in the future.
    Fresh,
    /// Reachable, `stale_after` past.
    Expired,
    /// `stale_after` in the future, but the last poll found it unreachable.
    Unreachable,
    /// No `node_status_snapshots` row.
    Missing,
}

fn name_of(node_id: NodeId) -> String {
    format!("worker-{}.synthetic.internal", node_id.as_uuid().simple())
}

async fn insert_node(
    pool: &PgPool,
    managed_by: SubstrateKind,
    drain_phase: Option<&str>,
    status: Status,
    uid: Option<&str>,
    instance_id: Option<&str>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, drain_phase)
         VALUES ($1, '10.0.0.8'::INET, 'worker', $2, $3)"
    ))
    .bind(node_id)
    .bind(managed_by)
    .bind(drain_phase)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(node_id)
    .bind(name_of(node_id))
    .bind(uid)
    .bind(instance_id)
    .execute(pool)
    .await?;
    set_status(pool, node_id, status).await?;
    Ok(node_id)
}

async fn set_status(pool: &PgPool, node_id: NodeId, status: Status) -> TestResult {
    sqlx::query(sql!("DELETE FROM node_status_snapshots WHERE node_id = $1"))
        .bind(node_id)
        .execute(pool)
        .await?;
    let (reachable, stale_after_secs) = match status {
        Status::Fresh => (true, 60),
        Status::Expired => (true, -60),
        Status::Unreachable => (false, 60),
        Status::Missing => return Ok(()),
    };
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots
            (node_id, observed_at, stale_after, reachable, services, updated_at)
         VALUES ($1, NOW(), NOW() + make_interval(secs => $2::BIGINT), $3, '[]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .bind(stale_after_secs)
    .bind(reachable)
    .execute(pool)
    .await?;
    Ok(())
}

/// A row of `lifecycle` pinned to `node` whose executions on its name are
/// stamped `stamps`, oldest generation first.
async fn pinned_with_stamps(
    pool: &PgPool,
    node: NodeId,
    lifecycle: &str,
    stamps: &[Option<&str>],
) -> TestResult<SandboxId> {
    let id = insert_row(pool, lifecycle, SubstrateKind::Kubernetes, None, Some(node)).await?;
    let name = name_of(node);
    for (generation, stamp) in (1..).zip(stamps) {
        let latest = generation == stamps.len() as i64;
        insert_stamped_execution(pool, id, generation, Some(&name), *stamp, latest).await?;
    }
    Ok(id)
}

/// Judged: nodes of the substrate outside any drain whose heartbeat is not
/// fresh by the node-status rule, and fresh ones a live row pins whose latest
/// execution there names another instance. A fresh heartbeat with nothing
/// else, any drain phase, or the other substrate keeps a node out; the
/// cursor pages through the rest.
#[tokio::test(flavor = "current_thread")]
async fn candidates_are_stale_nodes_and_nodes_holding_another_instances_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let k8s = SubstrateKind::Kubernetes;
    let mut judged = Vec::new();
    for status in [Status::Expired, Status::Unreachable, Status::Missing] {
        judged.push(insert_node(pool, k8s, None, status, Some("uid"), Some(OWN)).await?);
    }
    insert_node(pool, k8s, None, Status::Fresh, Some("uid"), Some(OWN)).await?;
    insert_node(
        pool,
        k8s,
        Some("draining"),
        Status::Missing,
        Some("uid"),
        Some(OWN),
    )
    .await?;
    insert_node(
        pool,
        k8s,
        Some("drained"),
        Status::Expired,
        Some("uid"),
        None,
    )
    .await?;
    let direct = insert_node(
        pool,
        SubstrateKind::Space,
        None,
        Status::Missing,
        None,
        None,
    )
    .await?;

    // Fresh nodes holding rows: only a live row whose latest execution on
    // the node names another instance makes it a candidate.
    let replaced = insert_node(pool, k8s, None, Status::Fresh, Some("uid"), Some(OWN)).await?;
    pinned_with_stamps(pool, replaced, "suspended", &[Some(OTHER)]).await?;
    judged.push(replaced);
    for (lifecycle, stamps) in [
        ("started", &[Some(OWN)][..]),
        ("started", &[None][..]),
        ("failed", &[Some(OTHER)][..]),
        ("deleted", &[Some(OTHER)][..]),
        // An older generation on another instance, the latest on this one.
        ("paused", &[Some(OTHER), Some(OWN)][..]),
    ] {
        let node = insert_node(pool, k8s, None, Status::Fresh, Some("uid"), Some(OWN)).await?;
        pinned_with_stamps(pool, node, lifecycle, stamps).await?;
    }
    // The latest execution is on another node's name.
    let moved = insert_node(pool, k8s, None, Status::Fresh, Some("uid"), Some(OWN)).await?;
    let row = insert_row(pool, "started", k8s, Some(moved), Some(moved)).await?;
    insert_stamped_execution(pool, row, 1, Some("worker-elsewhere"), Some(OTHER), true).await?;
    judged.sort();

    let store = DeadNodeDbStore::new(pool.clone());
    let all: Vec<NodeId> = store
        .candidate_nodes(k8s, None, 100)
        .await?
        .into_iter()
        .map(|node| node.node_id)
        .collect();
    assert_eq!(all, judged);
    let first = store.candidate_nodes(k8s, None, 2).await?;
    let rest = store
        .candidate_nodes(k8s, first.last().map(|node| node.node_id), 100)
        .await?;
    let paged: Vec<NodeId> = first.iter().chain(&rest).map(|node| node.node_id).collect();
    assert_eq!(paged, judged);
    let replaced_row = rest
        .iter()
        .chain(&first)
        .find(|node| node.node_id == replaced)
        .expect("replaced node judged");
    assert_eq!(replaced_row.heartbeat, Heartbeat::Fresh);
    assert_eq!(replaced_row.instance_id.as_deref(), Some(OWN));

    let direct_judged = store
        .candidate_nodes(SubstrateKind::Space, None, 100)
        .await?;
    assert_eq!(
        direct_judged
            .iter()
            .map(|node| (node.node_id, node.heartbeat))
            .collect::<Vec<_>>(),
        vec![(direct, Heartbeat::Stale)]
    );
    Ok(())
}

/// References: fresh nodes with a recorded uid and instance.
#[tokio::test(flavor = "current_thread")]
async fn reference_nodes_are_fresh_nodes_with_a_recorded_uid_and_instance() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let k8s = SubstrateKind::Kubernetes;
    insert_node(pool, k8s, None, Status::Fresh, Some("uid-fresh"), Some(OWN)).await?;
    insert_node(
        pool,
        k8s,
        None,
        Status::Fresh,
        Some("uid-no-instance"),
        None,
    )
    .await?;
    insert_node(pool, k8s, None, Status::Fresh, None, Some(OTHER)).await?;
    insert_node(
        pool,
        k8s,
        None,
        Status::Expired,
        Some("uid-stale"),
        Some(OTHER),
    )
    .await?;

    let references = DeadNodeDbStore::new(pool.clone())
        .reference_nodes(k8s, 10)
        .await?;
    assert_eq!(
        references,
        vec![ReferenceNode {
            k8s_node_uid: "uid-fresh".to_owned(),
            instance_id: OWN.to_owned(),
        }]
    );
    Ok(())
}

/// The lost node's row is deleted only while everything the verdict rested
/// on still holds: the uid and recorded instance the tick read, no drain
/// phase, a stale heartbeat, and no live row or active execution left on it.
/// Its `node_k8s` row goes with it; no tombstone is written.
#[tokio::test(flavor = "current_thread")]
async fn the_node_delete_refuses_when_anything_it_rests_on_moved() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = DeadNodeDbStore::new(pool.clone());
    type Change = fn(&mut CandidateNode);
    let read_as: [(&str, Change); 2] = [
        ("uid moved", |node| {
            node.k8s_node_uid = Some("uid-new".to_owned())
        }),
        ("instance moved", |node| {
            node.instance_id = Some(OTHER.to_owned())
        }),
    ];
    for (case, change) in read_as {
        let node_id = insert_node(
            pool,
            SubstrateKind::Kubernetes,
            None,
            Status::Missing,
            Some("uid"),
            Some(OWN),
        )
        .await?;
        let mut node = candidate(pool, node_id).await?;
        change(&mut node);
        assert!(!store.delete_node(&node).await?, "{case}");
        assert!(node_exists(pool, node_id).await?, "{case}");
    }
    for case in [
        "heartbeat back",
        "drain started",
        "live row pinned by target",
        "live row pinned by active",
        "active execution on its name",
        "only terminal rows",
    ] {
        let node_id = insert_node(
            pool,
            SubstrateKind::Kubernetes,
            None,
            Status::Missing,
            Some("uid"),
            Some(OWN),
        )
        .await?;
        let node = candidate(pool, node_id).await?;
        let k8s = SubstrateKind::Kubernetes;
        match case {
            "heartbeat back" => set_status(pool, node_id, Status::Fresh).await?,
            "drain started" => {
                sqlx::query(sql!(
                    "UPDATE nodes SET drain_phase = 'draining', drain_mode = 'gentle',
                            drain_started_at = NOW()
                      WHERE id = $1"
                ))
                .bind(node_id)
                .execute(pool)
                .await?;
            }
            "live row pinned by target" => {
                insert_row(pool, "suspended", k8s, None, Some(node_id)).await?;
            }
            "live row pinned by active" => {
                insert_row(pool, "quarantined", k8s, Some(node_id), None).await?;
            }
            "active execution on its name" => {
                let row = insert_row(pool, "started", k8s, None, None).await?;
                insert_stamped_execution(pool, row, 1, Some(&name_of(node_id)), None, true).await?;
            }
            _ => {
                // A row written off keeps the node as its last place, and
                // a tombstone may still name it: neither pins it.
                insert_row(pool, "failed", k8s, None, Some(node_id)).await?;
                insert_row(pool, "deleted", k8s, Some(node_id), Some(node_id)).await?;
            }
        }
        let deletes = case.starts_with("only terminal rows");
        assert_eq!(store.delete_node(&node).await?, deletes, "{case}");
        assert_eq!(node_exists(pool, node_id).await?, !deletes, "{case}");
        if deletes {
            let identity: Option<i32> =
                sqlx::query_scalar(sql!("SELECT 1 FROM node_k8s WHERE node_id = $1"))
                    .bind(node_id)
                    .fetch_optional(pool)
                    .await?;
            let tombstone: Option<i32> =
                sqlx::query_scalar(sql!("SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1"))
                    .bind(node_id)
                    .fetch_optional(pool)
                    .await?;
            assert_eq!((identity, tombstone), (None, None), "{case}");
        }
    }
    Ok(())
}

async fn candidate(pool: &PgPool, node_id: NodeId) -> TestResult<CandidateNode> {
    let store = DeadNodeDbStore::new(pool.clone());
    Ok(store
        .candidate_nodes(SubstrateKind::Kubernetes, None, 1000)
        .await?
        .into_iter()
        .find(|node| node.node_id == node_id)
        .expect("node judged"))
}

async fn node_exists(pool: &PgPool, node_id: NodeId) -> TestResult<bool> {
    let row: Option<i32> = sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// A drain restore claim that takes the row lock first wins: the write waits
/// for it, re-reads the row with the episode open, and refuses, leaving the
/// claim's row as it committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restore_claim_holding_the_row_lock_wins_over_the_write() -> TestResult {
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = insert_node(
        pool,
        SubstrateKind::Kubernetes,
        None,
        Status::Missing,
        Some("uid"),
        Some(OWN),
    )
    .await?;
    let sandbox_id = pinned_with_stamps(pool, node_id, "suspended", &[Some(OWN)]).await?;
    let node = candidate(pool, node_id).await?;
    // The plan names the row's real source execution, so only the re-read
    // outcome can refuse the write.
    let source: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;

    let mut claim = pool.begin().await?;
    let claim_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *claim)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring', restore_episode_target = 'started'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *claim)
    .await?;

    let store = DeadNodeDbStore::new(pool.clone());
    let writer = tokio::spawn(async move {
        let decide = |row: &DeadNodeRow| super::super::outcome::row_outcome(row);
        let write = RowWrite {
            sandbox_id,
            outcome: RowOutcome::Lost,
            source_execution: Some(source),
            basis: WriteBasis::InstanceDead,
            decide: &decide,
        };
        store
            .write_row(&node, &write, std::time::Duration::from_secs(600))
            .await
    });
    let mut probe = pool.acquire().await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let blocked: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE $1 = ANY(pg_blocking_pids(pid))
             )"
        ))
        .bind(claim_pid)
        .fetch_one(&mut *probe)
        .await?;
        if blocked {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the write never waited on the claim"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    claim.commit().await?;

    assert_eq!(writer.await??, WriteResult::RowChanged);
    let after: (String, Option<String>, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, restore_episode_target, target_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        after,
        (
            "restoring".to_owned(),
            Some("started".to_owned()),
            Some(node_id)
        )
    );
    Ok(())
}

/// A row write re-checks the node under its locks and refuses when anything
/// it rests on moved since the tick read it: the uid, the recorded
/// instance, a drain started, or, for a row the node's verdict selected, the
/// heartbeat back. With nothing moved it writes.
#[tokio::test(flavor = "current_thread")]
async fn a_row_write_refuses_when_the_node_moved() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let store = DeadNodeDbStore::new(pool.clone());
    for (case, basis) in [
        ("uid moved", WriteBasis::InstanceDead),
        ("instance moved", WriteBasis::InstanceDead),
        ("drain started", WriteBasis::InstanceDead),
        ("heartbeat back", WriteBasis::NodeLost),
        ("heartbeat back, stamped", WriteBasis::InstanceDead),
        ("nothing moved", WriteBasis::NodeLost),
    ] {
        let node_id = insert_node(
            pool,
            SubstrateKind::Kubernetes,
            None,
            Status::Missing,
            Some("uid"),
            Some(OWN),
        )
        .await?;
        let sandbox_id = pinned_with_stamps(pool, node_id, "started", &[None]).await?;
        let node = candidate(pool, node_id).await?;
        match case {
            "uid moved" => {
                sqlx::query(sql!(
                    "UPDATE node_k8s SET k8s_node_uid = 'uid-new' WHERE node_id = $1"
                ))
                .bind(node_id)
                .execute(pool)
                .await?;
            }
            "instance moved" => {
                sqlx::query(sql!(
                    "UPDATE node_k8s SET instance_id = $2 WHERE node_id = $1"
                ))
                .bind(node_id)
                .bind(OTHER)
                .execute(pool)
                .await?;
            }
            "drain started" => {
                sqlx::query(sql!(
                    "UPDATE nodes SET drain_phase = 'draining', drain_mode = 'gentle',
                            drain_started_at = NOW()
                      WHERE id = $1"
                ))
                .bind(node_id)
                .execute(pool)
                .await?;
            }
            "heartbeat back" | "heartbeat back, stamped" => {
                set_status(pool, node_id, Status::Fresh).await?;
            }
            _ => {}
        }
        let decide = |row: &DeadNodeRow| super::super::outcome::row_outcome(row);
        let source = sqlx::query_scalar(sql!(
            "SELECT id FROM sandbox_k8s_executions WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        let write = RowWrite {
            sandbox_id,
            outcome: RowOutcome::Lost,
            source_execution: Some(source),
            basis,
            decide: &decide,
        };
        let expected = match case {
            "nothing moved" | "heartbeat back, stamped" => WriteResult::Applied,
            _ => WriteResult::NodeChanged,
        };
        assert_eq!(
            store
                .write_row(&node, &write, std::time::Duration::from_secs(600))
                .await?,
            expected,
            "{case}"
        );
        let lifecycle: String =
            sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(pool)
                .await?;
        let written = expected == WriteResult::Applied;
        assert_eq!(lifecycle == "failed", written, "{case}");
    }
    Ok(())
}
