//! Ticks against Postgres through [`DeadNodeDbStore`], with fakes for Node
//! membership and EC2: a dry run writes only its status, and live mode leaves
//! each row in its rule's state, from the opposite state beforehand.

use pplx_space_common_core::types::NodeId;

use std::collections::BTreeMap;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_sandbox_store::StoredSandboxLifecycle;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::dead_node::row_seed_tests::*;

const LIVE_NAME: &str = "worker-live.synthetic.internal";

/// A fresh node with a fresh complete census: the reference, and a live node
/// rows can be reported by or run on.
async fn insert_live_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = insert_k8s_node(pool, LIVE_NAME, REFERENCE_UID, Some(REFERENCE_INSTANCE)).await?;
    heartbeat(pool, node_id, Some(0)).await?;
    Ok(node_id)
}

/// A fresh heartbeat for `node_id`, with a complete census `census_age_secs`
/// ago, or none.
async fn heartbeat(pool: &PgPool, node_id: NodeId, census_age_secs: Option<i64>) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots
            (node_id, observed_at, stale_after, reachable, services, updated_at,
             sandbox_inventory_pruned_at)
         VALUES ($1, NOW(), NOW() + INTERVAL '1 minute', TRUE, '[]'::JSONB, NOW(),
                 NOW() - make_interval(secs => $2::BIGINT))"
    ))
    .bind(node_id)
    .bind(census_age_secs)
    .execute(pool)
    .await?;
    Ok(())
}

async fn observe(pool: &PgPool, sandbox_id: SandboxId, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
         VALUES ($1, $2, 'running')"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn db_reconciler(pool: &PgPool, mode: Mode) -> Reconciler<DeadNodeDbStore, Arc<FakeMembership>> {
    Reconciler::new(
        ReconcilerConfig {
            absence_window: Duration::ZERO,
            ..config(mode)
        },
        DeadNodeDbStore::new(pool.clone()),
        FakeMembership::dead_node_gone(),
        InstanceAuthority::Ec2(FakeEc2::dead_instances()),
        SubstrateKind::Kubernetes,
    )
}

/// A row's placement and lifecycle, its source execution's activity, and
/// whether the row exists at all.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowState {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    execution_active: Option<bool>,
}

async fn row_state(pool: &PgPool, id: SandboxId) -> TestResult<Option<RowState>> {
    type Row = (String, String, Option<NodeId>, Option<NodeId>, Option<bool>);
    let row: Option<Row> = sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, s.target_node_id,
                    (SELECT e.active FROM sandbox_k8s_executions e
                      WHERE e.sandbox_id = s.id ORDER BY e.generation DESC LIMIT 1)
               FROM sandboxes s
              WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(lifecycle, placement, active_node_id, target_node_id, execution_active)| RowState {
            lifecycle,
            placement,
            active_node_id,
            target_node_id,
            execution_active,
        },
    ))
}

/// Every public table's row count and contents, by name.
async fn table_contents(pool: &PgPool) -> TestResult<BTreeMap<String, (i64, String)>> {
    let tables: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT quote_ident(table_name)
           FROM information_schema.tables
          WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
          ORDER BY 1"
    ))
    .fetch_all(pool)
    .await?;
    let mut contents = BTreeMap::new();
    for table in tables {
        let statement = format!(
            "SELECT COUNT(*),
                    md5(COALESCE(string_agg(t::TEXT, E'\\n' ORDER BY t::TEXT), ''))
               FROM public.{table} t"
        );
        let digest: (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(sql!(statement)))
            .fetch_one(pool)
            .await?;
        contents.insert(table, digest);
    }
    Ok(contents)
}

/// Rows of every stored lifecycle on the dead node, stamped with its dead
/// instance, with snapshots, an upload and a backup-queue row each: what
/// live mode would touch.
async fn seed_every_lifecycle(pool: &PgPool, node_id: NodeId) -> TestResult {
    for lifecycle in StoredSandboxLifecycle::all_as_str() {
        let id = insert_row(
            pool,
            lifecycle,
            SubstrateKind::Kubernetes,
            Some(node_id),
            Some(node_id),
        )
        .await?;
        insert_stamped_execution(pool, id, 1, Some(DEAD_NAME), Some(DEAD_INSTANCE), true).await?;
        seed_snapshot(pool, id, Snapshot::Uncovered).await?;
        insert_snapshot(
            pool,
            id,
            SnapshotSeed {
                source_node: Some(node_id),
                ..SnapshotSeed::full("creating", 3600)
            },
        )
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_local_backup_pending
                (node_id, sandbox_id, artifact_id, artifact_kind)
             VALUES ($1, $2, space_uuid_v7(), 'full')"
        ))
        .bind(node_id)
        .bind(id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// A dry-run tick that judges a node lost changes nothing but the node's
/// `dead_node_status`.
#[tokio::test(flavor = "current_thread")]
async fn dry_run_tick_writes_nothing_but_its_status() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    insert_live_node(pool).await?;
    let dead = insert_k8s_node(pool, DEAD_NAME, DEAD_UID, Some(DEAD_INSTANCE)).await?;
    seed_every_lifecycle(pool, dead).await?;

    let before = table_contents(pool).await?;
    let report = db_reconciler(pool, Mode::DryRun).tick().await;
    let status: Option<serde_json::Value> =
        sqlx::query_scalar(sql!("SELECT dead_node_status FROM nodes WHERE id = $1"))
            .bind(dead)
            .fetch_one(pool)
            .await?;
    sqlx::query(sql!(
        "UPDATE nodes SET dead_node_status = NULL WHERE id = $1"
    ))
    .bind(dead)
    .execute(pool)
    .await?;
    let after = table_contents(pool).await?;

    assert_eq!(report.errors, 0);
    assert_eq!(verdicts(&report), vec![LOST]);
    let total: u64 = report.nodes[0].counts.values().sum();
    assert_eq!(total, StoredSandboxLifecycle::all().len() as u64);
    assert_eq!(report.nodes[0].applied, BTreeMap::new());
    assert_eq!(before, after);
    let status = status.expect("status written");
    assert_eq!(
        (&status["mode"], &status["verdict"], &status["reason"]),
        (
            &serde_json::json!("dry-run"),
            &serde_json::json!("lost"),
            &serde_json::json!("instance_and_node_gone")
        )
    );
    Ok(())
}

/// One live tick over a lost node. Each row starts placed on the node with
/// an active execution there, and ends in its rule's state: lost rows
/// `failed` and unplaced (the node kept as `target_node_id`), restorable and
/// quarantined rows unplaced in their lifecycle, `failed` rows unplaced, a
/// `deleting` row deleted, and its execution retired. Rows a live node's
/// fresh complete census reports, an open restore episode, or an execution
/// elsewhere holds are untouched, as is every row on the live node; and the
/// node row stays while rows pin it. A second lost node holding only rows
/// that write is deleted.
#[tokio::test(flavor = "current_thread")]
async fn live_tick_leaves_each_row_in_its_rules_state() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let live = insert_live_node(pool).await?;
    let dead = insert_k8s_node(pool, DEAD_NAME, DEAD_UID, Some(DEAD_INSTANCE)).await?;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum After {
        /// `failed`, unplaced, the node kept as the last target.
        Lost,
        /// The lifecycle kept, unplaced, no node.
        Released,
        /// `failed` kept, unplaced, the node kept as the last target.
        Cleared,
        Deleted,
        Untouched,
    }
    #[derive(Debug, Clone, Copy)]
    enum Extra {
        None,
        StopArtifact,
        ReportedBy {
            heartbeat_fresh: bool,
            census_age_secs: Option<i64>,
        },
        RestoreLease,
        ExecutionElsewhere,
    }
    let cases: &[(&str, Snapshot, Extra, After)] = &[
        ("started", Snapshot::None, Extra::None, After::Lost),
        ("paused", Snapshot::Uncovered, Extra::None, After::Lost),
        ("suspending", Snapshot::Covered, Extra::None, After::Lost),
        ("suspended", Snapshot::Covered, Extra::None, After::Released),
        ("suspended", Snapshot::Uncovered, Extra::None, After::Lost),
        (
            "stopped",
            Snapshot::None,
            Extra::StopArtifact,
            After::Released,
        ),
        ("stopped", Snapshot::None, Extra::None, After::Lost),
        (
            "quarantined",
            Snapshot::Covered,
            Extra::None,
            After::Released,
        ),
        ("quarantined", Snapshot::None, Extra::None, After::Released),
        ("deleting", Snapshot::None, Extra::None, After::Deleted),
        ("failed", Snapshot::None, Extra::None, After::Cleared),
        ("bogus", Snapshot::None, Extra::None, After::Lost),
        (
            "started",
            Snapshot::None,
            Extra::ReportedBy {
                heartbeat_fresh: true,
                census_age_secs: Some(0),
            },
            After::Untouched,
        ),
        (
            "started",
            Snapshot::None,
            Extra::ReportedBy {
                heartbeat_fresh: true,
                census_age_secs: Some(3600),
            },
            After::Lost,
        ),
        (
            "started",
            Snapshot::None,
            Extra::ReportedBy {
                heartbeat_fresh: true,
                census_age_secs: None,
            },
            After::Lost,
        ),
        (
            "started",
            Snapshot::None,
            Extra::ReportedBy {
                heartbeat_fresh: false,
                census_age_secs: Some(0),
            },
            After::Lost,
        ),
        (
            "restoring",
            Snapshot::Covered,
            Extra::RestoreLease,
            After::Untouched,
        ),
        (
            "restoring",
            Snapshot::Covered,
            Extra::ExecutionElsewhere,
            After::Untouched,
        ),
    ];
    let mut seeded = Vec::new();
    for (lifecycle, snapshot, extra, after) in cases {
        let id = insert_row(
            pool,
            lifecycle,
            SubstrateKind::Kubernetes,
            Some(dead),
            Some(dead),
        )
        .await?;
        seed_snapshot(pool, id, *snapshot).await?;
        // A full upload from the dead node, quiet for an hour: it can never
        // finalize, and is failed with the row it would have covered.
        insert_snapshot(
            pool,
            id,
            SnapshotSeed {
                source_node: Some(dead),
                ..SnapshotSeed::full("creating", 3600)
            },
        )
        .await?;
        let elsewhere = matches!(extra, Extra::ExecutionElsewhere);
        insert_stamped_execution(
            pool,
            id,
            1,
            Some(DEAD_NAME),
            Some(DEAD_INSTANCE),
            !elsewhere,
        )
        .await?;
        match extra {
            Extra::None => {}
            Extra::StopArtifact => {
                let artifact = Uuid::now_v7();
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET current_local_artifact_id = $2,
                            current_local_artifact_kind = 'disk',
                            current_local_artifact_origin = 'stop'
                      WHERE id = $1"
                ))
                .bind(id)
                .bind(artifact)
                .execute(pool)
                .await?;
                insert_snapshot(
                    pool,
                    id,
                    SnapshotSeed {
                        kind: "disk",
                        artifact: Some((artifact, "stop")),
                        ..SnapshotSeed::full("ready", 600)
                    },
                )
                .await?;
            }
            Extra::ReportedBy {
                heartbeat_fresh,
                census_age_secs,
            } => {
                // Its own reporter, whose census is that old.
                let reporter =
                    insert_k8s_node(pool, &format!("reporter-{id}"), &format!("uid-{id}"), None)
                        .await?;
                heartbeat(pool, reporter, *census_age_secs).await?;
                if !heartbeat_fresh {
                    sqlx::query(sql!(
                        "UPDATE node_status_snapshots
                            SET stale_after = NOW() - INTERVAL '1 minute'
                          WHERE node_id = $1"
                    ))
                    .bind(reporter)
                    .execute(pool)
                    .await?;
                }
                observe(pool, id, reporter).await?;
            }
            Extra::RestoreLease => {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET restore_lease_token = gen_random_uuid(),
                            restore_lease_expires_at = NOW() + INTERVAL '1 minute'
                      WHERE id = $1"
                ))
                .bind(id)
                .execute(pool)
                .await?;
            }
            Extra::ExecutionElsewhere => {
                insert_stamped_execution(
                    pool,
                    id,
                    2,
                    Some(LIVE_NAME),
                    Some(REFERENCE_INSTANCE),
                    true,
                )
                .await?;
            }
        }
        seeded.push((id, *lifecycle, *after));
    }
    let mut controls = Vec::new();
    for lifecycle in ["started", "suspended", "deleting"] {
        let id = insert_row(
            pool,
            lifecycle,
            SubstrateKind::Kubernetes,
            Some(live),
            Some(live),
        )
        .await?;
        insert_stamped_execution(pool, id, 1, Some(LIVE_NAME), Some(REFERENCE_INSTANCE), true)
            .await?;
        controls.push(id);
    }
    // A second lost node, holding only rows that write.
    let emptied_name = "worker-emptied.synthetic.internal";
    let emptied = insert_k8s_node(pool, emptied_name, "uid-emptied", Some(OLDER_INSTANCE)).await?;
    for lifecycle in ["started", "deleting"] {
        let id = insert_row(
            pool,
            lifecycle,
            SubstrateKind::Kubernetes,
            Some(emptied),
            Some(emptied),
        )
        .await?;
        insert_stamped_execution(pool, id, 1, Some(emptied_name), Some(OLDER_INSTANCE), true)
            .await?;
    }

    let mut before = BTreeMap::new();
    for id in seeded
        .iter()
        .map(|(id, _, _)| *id)
        .chain(controls.iter().copied())
    {
        before.insert(id, row_state(pool, id).await?.expect("seeded"));
    }
    let membership = FakeMembership::dead_node_gone();
    membership.set_exists(emptied_name, Ok(false));
    let mut reconciler = Reconciler::new(
        ReconcilerConfig {
            absence_window: Duration::ZERO,
            ..config(Mode::Live)
        },
        DeadNodeDbStore::new(pool.clone()),
        membership,
        InstanceAuthority::Ec2(FakeEc2::dead_instances()),
        SubstrateKind::Kubernetes,
    );
    let report = reconciler.tick().await;
    assert_eq!(report.errors, 0, "{report:?}");

    let mut mismatches = Vec::new();
    for (id, lifecycle, after) in &seeded {
        let was = &before[id];
        let expected = match after {
            After::Untouched => Some(was.clone()),
            After::Deleted => None,
            After::Lost | After::Cleared => Some(RowState {
                lifecycle: "failed".to_owned(),
                placement: "unplaced".to_owned(),
                active_node_id: None,
                target_node_id: Some(dead),
                execution_active: Some(false),
            }),
            After::Released => Some(RowState {
                lifecycle: (*lifecycle).to_owned(),
                placement: "unplaced".to_owned(),
                active_node_id: None,
                target_node_id: None,
                execution_active: Some(false),
            }),
        };
        if *after != After::Untouched {
            assert_ne!(
                Some(was),
                expected.as_ref(),
                "{lifecycle} seeded in its end state"
            );
        }
        let got = row_state(pool, *id).await?;
        if got != expected {
            mismatches.push((*lifecycle, *after, expected, got));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    for control in &controls {
        assert_eq!(
            row_state(pool, *control).await?.as_ref(),
            Some(&before[control]),
            "control {control}"
        );
    }

    // The stale reporter is judged too; it records no instance, so it is not
    // lost.
    let nodes: Vec<(NodeId, bool)> = report
        .nodes
        .iter()
        .filter(|node| node.node_id == dead || node.node_id == emptied)
        .map(|node| (node.node_id, node.deleted))
        .collect();
    let mut expected_nodes = vec![(dead, false), (emptied, true)];
    expected_nodes.sort();
    assert_eq!(nodes, expected_nodes);
    let remaining: Vec<NodeId> =
        sqlx::query_scalar(sql!("SELECT id FROM nodes WHERE id = ANY($1) ORDER BY id"))
            .bind([dead, emptied])
            .fetch_all(pool)
            .await?;
    assert_eq!(remaining, vec![dead]);

    // A lost row's quiet dead upload from the node is finalized `failed`,
    // and its `ready` snapshot is kept; a row that was not written off keeps
    // its upload `creating`.
    let lost_paused = seeded
        .iter()
        .find(|(_, lifecycle, _)| *lifecycle == "paused")
        .map(|(id, _, _)| *id)
        .expect("paused case");
    let snapshots: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT state FROM snapshots WHERE sandbox_id = $1 ORDER BY state"
    ))
    .bind(lost_paused)
    .fetch_all(pool)
    .await?;
    assert_eq!(snapshots, ["failed", "ready"]);
    let untouched = seeded
        .iter()
        .find(|(_, _, after)| *after == After::Untouched)
        .map(|(id, _, _)| *id)
        .expect("untouched case");
    let kept: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT state FROM snapshots WHERE sandbox_id = $1 AND type = 'full' AND state = 'creating'"
    ))
    .bind(untouched)
    .fetch_all(pool)
    .await?;
    assert_eq!(kept, ["creating"]);
    Ok(())
}

/// A replaced node: the same id and name, a fresh heartbeat from the new
/// instance. A row stamped with the dead predecessor is written off; the
/// replacement's own row and an unstamped one are untouched, and the node
/// row stays.
#[tokio::test(flavor = "current_thread")]
async fn live_tick_on_a_replaced_node_writes_off_only_the_dead_instances_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    insert_live_node(pool).await?;
    let node = insert_k8s_node(
        pool,
        DEAD_NAME,
        "uid-replacement",
        Some(REPLACEMENT_INSTANCE),
    )
    .await?;
    heartbeat(pool, node, Some(0)).await?;
    let mut ids = BTreeMap::new();
    for (label, stamp) in [
        ("predecessor", Some(DEAD_INSTANCE)),
        ("replacement", Some(REPLACEMENT_INSTANCE)),
        ("unstamped", None),
    ] {
        let id = insert_row(
            pool,
            "started",
            SubstrateKind::Kubernetes,
            Some(node),
            Some(node),
        )
        .await?;
        insert_stamped_execution(pool, id, 1, Some(DEAD_NAME), stamp, true).await?;
        ids.insert(label, (id, row_state(pool, id).await?.expect("seeded")));
    }

    let report = db_reconciler(pool, Mode::Live).tick().await;
    assert_eq!(report.errors, 0);
    assert_eq!(
        verdicts(&report),
        vec![Verdict::Unknown(UnknownReason::HeartbeatFresh)]
    );
    let (predecessor, _) = &ids["predecessor"];
    assert_eq!(
        row_state(pool, *predecessor)
            .await?
            .map(|row| row.lifecycle),
        Some("failed".to_owned())
    );
    for label in ["replacement", "unstamped"] {
        let (id, before) = &ids[label];
        assert_eq!(
            row_state(pool, *id).await?.as_ref(),
            Some(before),
            "{label}"
        );
    }
    let kept: Option<i32> = sqlx::query_scalar(sql!("SELECT 1 FROM nodes WHERE id = $1"))
        .bind(node)
        .fetch_optional(pool)
        .await?;
    assert_eq!(kept, Some(1));
    Ok(())
}
