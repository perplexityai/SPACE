//! The evacuation verdict over every stored lifecycle, home, execution and
//! durability fact: each cell's answer next to the one the controller gave
//! before the verdict existed, and the reads against Postgres.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

/// Where the row is placed, relative to the draining node: its
/// `active_node_id` and its `target_node_id` ([`Home::nodes`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Home {
    /// Both name the node.
    Here,
    /// Both name another node.
    Elsewhere,
    /// Neither is set.
    Unplaced,
    /// Active on the node, targeted at another one.
    ActiveHereTargetElsewhere,
    /// Active on another node, targeted at this one.
    ActiveElsewhereTargetHere,
    /// Active on the node, with no target.
    ActiveOnly,
    /// Targeted at the node, with no active node.
    TargetOnly,
}

impl Home {
    const ALL: [Home; 7] = [
        Home::Here,
        Home::Elsewhere,
        Home::Unplaced,
        Home::ActiveHereTargetElsewhere,
        Home::ActiveElsewhereTargetHere,
        Home::ActiveOnly,
        Home::TargetOnly,
    ];

    /// `(active_node_id, target_node_id)`, with `node` the draining node.
    fn nodes(self, node: NodeId, other: NodeId) -> (Option<NodeId>, Option<NodeId>) {
        match self {
            Home::Here => (Some(node), Some(node)),
            Home::Elsewhere => (Some(other), Some(other)),
            Home::Unplaced => (None, None),
            Home::ActiveHereTargetElsewhere => (Some(node), Some(other)),
            Home::ActiveElsewhereTargetHere => (Some(other), Some(node)),
            Home::ActiveOnly => (Some(node), None),
            Home::TargetOnly => (None, Some(node)),
        }
    }
}

/// The row's active execution, by the node it is bound to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Execution {
    Here,
    Elsewhere,
    /// Active, its pod bound to no node yet.
    Unbound,
    None,
}

/// One cell of the table: the row's facts as the old answer and the new one
/// see them. `fresh` is the restore selector's placement-freshness test,
/// which the old answer asked of a row pinning nothing; the new one asks
/// the terminate's source-release test, `release_durable`, instead.
#[derive(Clone, Copy, Debug)]
struct Cell {
    lifecycle: &'static str,
    home: Home,
    execution: Execution,
    pin: Pin,
    reported_durable: bool,
    pinned_durable: bool,
    fresh: bool,
    release_durable: bool,
}

const UNKNOWN_LIFECYCLE: &str = "hibernating";

fn cells() -> Vec<Cell> {
    let lifecycles = StoredSandboxLifecycle::all_as_str().chain([UNKNOWN_LIFECYCLE]);
    let mut cells = Vec::new();
    for lifecycle in lifecycles {
        for home in Home::ALL {
            for execution in [
                Execution::Here,
                Execution::Elsewhere,
                Execution::Unbound,
                Execution::None,
            ] {
                for pin in [Pin::None, Pin::Reported, Pin::Other] {
                    for reported_durable in [false, true] {
                        // What the pin's durability can be, given the pin.
                        let pinned: &[bool] = match pin {
                            Pin::None => &[false],
                            Pin::Reported => &[reported_durable],
                            Pin::Other => &[false, true],
                        };
                        for &pinned_durable in pinned {
                            for fresh in [false, true] {
                                for release_durable in [false, true] {
                                    cells.push(Cell {
                                        lifecycle,
                                        home,
                                        execution,
                                        pin,
                                        reported_durable,
                                        pinned_durable,
                                        fresh,
                                        release_durable,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    cells
}

fn facts(node: NodeId, other: NodeId, cell: &Cell) -> EvacuationFacts {
    let (active_node_id, target_node_id) = cell.home.nodes(node, other);
    EvacuationFacts {
        lifecycle: cell.lifecycle.parse(),
        active_node_id,
        target_node_id,
        pin: cell.pin,
        runs_on_named_other_node: cell.execution == Execution::Elsewhere,
        reported_artifact_durable: cell.reported_durable,
        pinned_artifact_durable: cell.pinned_durable,
        durable_for_source_release: cell.release_durable,
    }
}

/// Where `home` places the row as the old answer and the verdict both read
/// it: whether it is active on the node (`active_node_id`, the running
/// guard), and its home, `target_node_id` else `active_node_id`, here or
/// on another node (the home and the stopped guard).
fn placement(home: Home) -> (bool, bool, bool) {
    let node = NodeId::from_uuid(Uuid::from_u128(1));
    let other = NodeId::from_uuid(Uuid::from_u128(2));
    let (active, target) = home.nodes(node, other);
    let home = target.or(active);
    (
        active == Some(node),
        home == Some(node),
        home.is_some_and(|home| home != node),
    )
}

/// The answer the controller gave before the verdict: whether it recorded
/// the evacuation. Its own `match` on the lifecycle string, with a
/// wildcard that recorded every lifecycle it didn't name, and its test for
/// an execution elsewhere, `k8s_node_name IS DISTINCT FROM` the node's
/// name, which an unbound execution passed. It read the running guard from
/// `active_node_id`, and the home and the stopped guard from
/// `target_node_id` else `active_node_id`.
fn recorded_before(cell: &Cell) -> bool {
    let (active_here, home_here, homed_elsewhere) = placement(cell.home);
    let executes_elsewhere = matches!(cell.execution, Execution::Elsewhere | Execution::Unbound);
    match cell.lifecycle {
        "suspended" => {}
        "started" | "paused" | "suspending" if active_here => return false,
        "started" | "paused" | "suspending" => return executes_elsewhere,
        "stopped" if home_here => return false,
        _ => return true,
    }
    match cell.pin {
        Pin::Reported => cell.reported_durable,
        _ if homed_elsewhere => true,
        Pin::None => cell.reported_durable && cell.fresh,
        Pin::Other => cell.pinned_durable,
    }
}

/// Why the cell's answer changes, if it does.
fn change(cell: &Cell) -> Option<&'static str> {
    let (active_here, _, homed_elsewhere) = placement(cell.home);
    match cell.lifecycle {
        UNKNOWN_LIFECYCLE => Some("a lifecycle this build doesn't know holds"),
        "created" | "creating" | "starting" | "restoring" | "pausing" | "resuming" | "stopping"
        | "deleting" | "checkpointing" | "quarantined"
            if !homed_elsewhere =>
        {
            Some("a row in a lifecycle the drain doesn't settle, not homed elsewhere, holds")
        }
        "started" | "paused" | "suspending"
            if !active_here && cell.execution == Execution::Unbound =>
        {
            Some("an execution bound to no node runs nowhere known yet, so it holds")
        }
        "suspended"
            if cell.pin == Pin::None
                && !homed_elsewhere
                && cell.reported_durable
                && cell.fresh != cell.release_durable =>
        {
            Some("a row pinning nothing is durable by the terminate's source-release test")
        }
        _ => None,
    }
}

/// Every cell keeps its old answer but the ones [`change`] names. Each of
/// those moves from recorded to held, but a row pinning nothing, which now
/// follows the terminate's source-release test (recorded where it holds,
/// held where it doesn't) instead of placement freshness. A row in a
/// lifecycle the verdict doesn't release by name holds, so the default is
/// to hold. The placements where the active and the target node differ
/// keep the old answer's reads of each.
#[test]
fn every_cell_keeps_its_answer_but_the_listed_changes() {
    let node = NodeId::new();
    let other = NodeId::new();
    let mut changed = 0;
    for cell in cells() {
        let verdict = decide(node, Some(&facts(node, other, &cell)));
        let released = matches!(verdict, EvacuationVerdict::Released(_));
        let before = recorded_before(&cell);
        match change(&cell) {
            None => assert_eq!(released, before, "{cell:?}: {verdict:?}"),
            Some(why) => {
                changed += 1;
                assert_ne!(released, before, "{cell:?}: {why}: {verdict:?}");
                if cell.lifecycle != "suspended" {
                    assert!(
                        !released,
                        "{cell:?}: only a suspended row may newly release"
                    );
                } else {
                    assert_eq!(released, cell.release_durable, "{cell:?}");
                }
            }
        }
        if let EvacuationVerdict::Holds(reason) = &verdict {
            assert!(!reason.is_empty(), "{cell:?}");
        }
    }
    assert!(changed > 0);
}

/// Released, each for its own reason: a gone row, a terminal one, the
/// tombstone among them, and a `stopped` row on no node.
#[test]
fn the_release_reasons() {
    let node = NodeId::new();
    let other = NodeId::new();
    assert_eq!(
        decide(node, None),
        EvacuationVerdict::Released(ReleaseReason::Gone)
    );
    let cell = |lifecycle, home| Cell {
        lifecycle,
        home,
        execution: Execution::None,
        pin: Pin::None,
        reported_durable: false,
        pinned_durable: false,
        fresh: false,
        release_durable: false,
    };
    for (lifecycle, home, reason) in [
        ("failed", Home::Here, ReleaseReason::Terminal),
        ("deleted", Home::Here, ReleaseReason::Terminal),
        ("stopped", Home::Unplaced, ReleaseReason::StoppedUnplaced),
        ("stopped", Home::Elsewhere, ReleaseReason::HomedElsewhere),
        ("restoring", Home::Elsewhere, ReleaseReason::HomedElsewhere),
    ] {
        assert_eq!(
            decide(node, Some(&facts(node, other, &cell(lifecycle, home)))),
            EvacuationVerdict::Released(reason),
            "{lifecycle}"
        );
    }
    assert_eq!(
        decide(
            node,
            Some(&facts(node, other, &cell("stopped", Home::Here)))
        ),
        EvacuationVerdict::Holds(STOPPED_HELD.to_owned())
    );
}

const NODE_NAME: &str = "worker-evacuation-verdict.synthetic.internal";

async fn insert_node(pool: &PgPool, name: &str) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.7'::INET, 'worker', $2)"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, 'uid')"
    ))
    .bind(node_id)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A row in `lifecycle` homed on `home`, placed an hour ago.
async fn insert_sandbox(
    pool: &PgPool,
    lifecycle: &str,
    home: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             placement_changed_at)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5,
                 CASE WHEN $6::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                 $6, $6, 1, NOW() - INTERVAL '1 hour')"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(lifecycle)
    .bind(home)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A full snapshot of the row for `artifact`, in `state`, stored, taken
/// `age_secs` ago.
async fn full_snapshot(
    pool: &PgPool,
    id: SandboxId,
    artifact: Uuid,
    state: &str,
    age_secs: i64,
) -> TestResult<Uuid> {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, local_artifact_id,
                                artifact_kind, artifact_origin, org_id)
         VALUES ($1, $2, 'full', $3, NOW() - make_interval(secs => $4::BIGINT), $5, 'full',
                 'suspend', $6::UUID)"
    ))
    .bind(snapshot)
    .bind(id)
    .bind(state)
    .bind(age_secs)
    .bind(artifact)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (id, snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, 's3://test-bucket/evacuation-verdict?region=us-east-1', $3::UUID)"
    ))
    .bind(Uuid::now_v7())
    .bind(snapshot)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(snapshot)
}

/// An active execution of the row on the node named `node_name`, or on none.
async fn insert_execution(pool: &PgPool, id: SandboxId, node_name: Option<&str>) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active)
         VALUES ($1, 1, $2, 'sandboxes', $3, 'creating', TRUE)"
    ))
    .bind(id)
    .bind(format!("space-sbx-{}", id.as_uuid().simple()))
    .bind(node_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// A running row homed on no node holds the node while its only active
/// execution is bound to no node yet: it runs nowhere the control plane
/// knows of. Once the execution is bound to another node by name it has
/// moved on; bound to the draining node, it holds.
#[tokio::test(flavor = "current_thread")]
async fn an_execution_bound_to_no_node_holds_the_node() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, NODE_NAME).await?;
    insert_node(pool, "worker-elsewhere.synthetic.internal").await?;
    let artifact = Uuid::now_v7();
    let id = insert_sandbox(pool, "started", None).await?;
    insert_execution(pool, id, None).await?;
    let verdict = evacuation_verdict(pool, node, id, artifact).await?;
    assert!(
        matches!(verdict, EvacuationVerdict::Holds(_)),
        "unbound: {verdict:?}"
    );
    for (name, released) in [
        (NODE_NAME, false),
        ("worker-elsewhere.synthetic.internal", true),
    ] {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET k8s_node_name = $2 WHERE sandbox_id = $1"
        ))
        .bind(id)
        .bind(name)
        .execute(pool)
        .await?;
        let verdict = evacuation_verdict(pool, node, id, artifact).await?;
        assert_eq!(
            verdict == EvacuationVerdict::Released(ReleaseReason::RunsElsewhere),
            released,
            "{name}: {verdict:?}"
        );
    }
    Ok(())
}

/// A suspended row pinning no artifact, marked for an evacuation restore,
/// with the reported artifact ready and stored but a newer full snapshot
/// still uploading, holds the node: its restore waits on the newest
/// snapshot, and the terminate wouldn't release it. Ready, it is released.
/// The ready, reported snapshot is newer than the row's placement, so the
/// placement-freshness test alone would have released it.
#[tokio::test(flavor = "current_thread")]
async fn a_marked_row_waits_for_its_newest_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool, NODE_NAME).await?;
    let id = insert_sandbox(pool, "suspended", Some(node)).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    let reported = Uuid::now_v7();
    full_snapshot(pool, id, reported, "ready", 600).await?;
    let newer = full_snapshot(pool, id, Uuid::now_v7(), "creating", 60).await?;
    let verdict = evacuation_verdict(pool, node, id, reported).await?;
    assert!(
        matches!(verdict, EvacuationVerdict::Holds(_)),
        "a newer snapshot uploading: {verdict:?}"
    );
    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(newer)
        .execute(pool)
        .await?;
    assert_eq!(
        evacuation_verdict(pool, node, id, reported).await?,
        EvacuationVerdict::Released(ReleaseReason::Durable)
    );
    Ok(())
}
