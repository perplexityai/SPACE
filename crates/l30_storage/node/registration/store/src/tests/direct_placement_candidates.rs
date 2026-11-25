//! Create-candidate placement reads against Postgres.
//!
//! Every build here is an `fc` build on a node with no runtime inventory and
//! no execution components, so the runtime, format and component checks
//! admit it; the api's Direct substrate tests cover those checks. These pin
//! the node filters, the snapshot each statement asks for, the inventory
//! filters and the gate's refusal column.

use std::collections::HashSet;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use pplx_space_node_model::SubstrateKind;

use super::direct_seed::{
    direct_node, endpoint, inventory, node, snapshot, snapshot_reachable, worker,
};
use crate::direct::placement_candidates::{
    self, CreateCandidateBinds, GatedCandidateRow, PooledCandidateBinds, PullOnCreateBinds,
};

const TEMPLATE: &str = "placement-template";
const HASH: &str = "sha256:aaaa";
/// What the gate refuses a node with no execution evidence for.
const NO_EVIDENCE: &str = "no_evidence";

fn create_binds(require_base_snapshot: bool) -> CreateCandidateBinds<'static> {
    CreateCandidateBinds {
        template: TEMPLATE,
        content_hash: None,
        runtime: "fc",
        require_base_snapshot,
        managed_by: SubstrateKind::Space,
        refuse: false,
        release_environment: None,
    }
}

fn ids(rows: &[GatedCandidateRow]) -> HashSet<NodeId> {
    rows.iter().map(|row| row.id).collect()
}

/// Direct workers holding [`TEMPLATE`], one per filter the create
/// statements apply, and a fresh ready holder of `HASH`.
struct Holders {
    ready: NodeId,
    stale: NodeId,
    unobserved: NodeId,
    full: NodeId,
    no_base_snapshot: NodeId,
    other_hash: NodeId,
}

async fn holders(pool: &sqlx::PgPool) -> TestDbResult<Holders> {
    let ready = worker(pool, "ready").await?;
    snapshot(pool, ready, "ready", false).await?;
    inventory(pool, ready, TEMPLATE, HASH, true, 0).await?;
    let stale = worker(pool, "stale").await?;
    snapshot(pool, stale, "ready", true).await?;
    inventory(pool, stale, TEMPLATE, HASH, true, 0).await?;
    let unobserved = worker(pool, "unobserved").await?;
    inventory(pool, unobserved, TEMPLATE, HASH, true, 0).await?;
    let full = worker(pool, "full").await?;
    snapshot(pool, full, "full", false).await?;
    inventory(pool, full, TEMPLATE, HASH, true, 1).await?;
    let no_base_snapshot = worker(pool, "no-base").await?;
    snapshot(pool, no_base_snapshot, "ready", false).await?;
    inventory(pool, no_base_snapshot, TEMPLATE, HASH, false, 0).await?;
    let other_hash = worker(pool, "other-hash").await?;
    snapshot(pool, other_hash, "ready", false).await?;
    inventory(pool, other_hash, TEMPLATE, "sha256:bbbb", true, 0).await?;

    // Holders no create statement returns. Each has a warm member, so the
    // pooled statement's own filters drop it, not `pool_current`.
    let deleted = direct_node(pool, "worker", "registered", "deleted", true).await?;
    snapshot(pool, deleted, "ready", false).await?;
    inventory(pool, deleted, TEMPLATE, HASH, true, 1).await?;
    let unregistered = direct_node(pool, "worker", "deleted", "unregistered", false).await?;
    snapshot(pool, unregistered, "ready", false).await?;
    inventory(pool, unregistered, TEMPLATE, HASH, true, 1).await?;
    let builder = direct_node(pool, "builder", "registered", "builder", false).await?;
    snapshot(pool, builder, "ready", false).await?;
    inventory(pool, builder, TEMPLATE, HASH, true, 1).await?;
    let k8s = node(pool, "worker", "k8s", Some("ready")).await?;
    snapshot(pool, k8s, "ready", false).await?;
    inventory(pool, k8s, TEMPLATE, HASH, true, 1).await?;
    let other_template = worker(pool, "other-template").await?;
    snapshot(pool, other_template, "ready", false).await?;
    inventory(pool, other_template, "other-template", HASH, true, 1).await?;
    let unreachable = worker(pool, "unreachable").await?;
    snapshot_reachable(pool, unreachable, "ready", false, false).await?;
    inventory(pool, unreachable, TEMPLATE, HASH, true, 1).await?;

    Ok(Holders {
        ready,
        stale,
        unobserved,
        full,
        no_base_snapshot,
        other_hash,
    })
}

#[tokio::test(flavor = "current_thread")]
async fn cached_ready_create_candidates_need_a_fresh_ready_snapshot() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let nodes = holders(pool).await?;

    let rows =
        placement_candidates::cached_ready_worker_create_candidates(pool, create_binds(false))
            .await?;
    assert_eq!(
        ids(&rows),
        HashSet::from([nodes.ready, nodes.no_base_snapshot, nodes.other_hash])
    );
    let ready = rows
        .iter()
        .find(|row| row.id == nodes.ready)
        .expect("the ready holder is a candidate");
    assert_eq!(ready.node_api_endpoint, endpoint("ready"));
    assert_eq!(ready.environment_refusal, None);

    let with_base = placement_candidates::cached_ready_worker_create_candidates(
        pool,
        CreateCandidateBinds {
            content_hash: Some(HASH),
            ..create_binds(true)
        },
    )
    .await?;
    assert_eq!(ids(&with_base), HashSet::from([nodes.ready]));

    assert!(
        placement_candidates::cached_ready_worker_create_candidates(
            pool,
            CreateCandidateBinds {
                managed_by: SubstrateKind::Kubernetes,
                ..create_binds(false)
            },
        )
        .await?
        .is_empty()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn last_known_create_candidates_tolerate_a_missing_or_stale_snapshot() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let nodes = holders(pool).await?;
    // A second build on the ready holder: still one row for the node.
    inventory(pool, nodes.ready, TEMPLATE, "sha256:cccc", true, 0).await?;

    let rows =
        placement_candidates::last_known_worker_create_candidates(pool, create_binds(true)).await?;
    assert_eq!(rows.len(), 4, "one row per node: {rows:?}");
    assert_eq!(
        ids(&rows),
        HashSet::from([nodes.ready, nodes.stale, nodes.unobserved, nodes.other_hash])
    );
    assert!(!ids(&rows).contains(&nodes.full));
    assert!(!ids(&rows).contains(&nodes.no_base_snapshot));
    assert!(
        placement_candidates::last_known_worker_create_candidates(
            pool,
            CreateCandidateBinds {
                managed_by: SubstrateKind::Kubernetes,
                ..create_binds(true)
            },
        )
        .await?
        .is_empty()
    );
    // A content hash narrows the inventory rows: `other_hash` holds another build.
    let with_hash = placement_candidates::last_known_worker_create_candidates(
        pool,
        CreateCandidateBinds {
            content_hash: Some(HASH),
            ..create_binds(true)
        },
    )
    .await?;
    assert_eq!(
        ids(&with_hash),
        HashSet::from([nodes.ready, nodes.stale, nodes.unobserved])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn create_candidates_return_the_gate_refusal_without_filtering_on_it() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let nodes = holders(pool).await?;
    let gated = CreateCandidateBinds {
        content_hash: Some(HASH),
        refuse: true,
        release_environment: Some("managed-env"),
        ..create_binds(false)
    };

    // No node here publishes execution evidence, so the gate refuses each
    // one for the environment; the caller decides what that means.
    let cached = placement_candidates::cached_ready_worker_create_candidates(pool, gated).await?;
    assert_eq!(
        ids(&cached),
        HashSet::from([nodes.ready, nodes.no_base_snapshot])
    );
    assert!(
        cached
            .iter()
            .all(|row| row.environment_refusal.as_deref() == Some(NO_EVIDENCE))
    );
    let last_known = placement_candidates::last_known_worker_create_candidates(pool, gated).await?;
    assert!(ids(&last_known).contains(&nodes.unobserved));
    assert!(
        last_known
            .iter()
            .all(|row| row.environment_refusal.as_deref() == Some(NO_EVIDENCE))
    );
    Ok(())
}

fn pull_binds(environment: Option<&str>, refuse: bool) -> PullOnCreateBinds<'_> {
    PullOnCreateBinds {
        runtime: "fc",
        managed_by: SubstrateKind::Space,
        build_environment: environment,
        refuse,
        template: TEMPLATE,
        content_hash: Some(HASH),
        // A holder's row names no binding, so the release's gates it.
        release_environment: environment,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pull_on_create_candidates_need_no_build_but_a_fresh_ready_snapshot() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let nodes = holders(pool).await?;
    let empty = worker(pool, "empty").await?;
    snapshot(pool, empty, "ready", false).await?;

    let rows = placement_candidates::pull_on_create_worker_candidates(pool, pull_binds(None, true))
        .await?;
    // Every live, registered worker with a fresh, ready snapshot, whatever it
    // holds.
    let other_template = rows
        .iter()
        .find(|row| row.node_api_endpoint == endpoint("other-template"))
        .map(|row| row.id)
        .expect("a worker holding another template can pull this one");
    assert_eq!(
        ids(&rows),
        HashSet::from([
            nodes.ready,
            nodes.no_base_snapshot,
            nodes.other_hash,
            other_template,
            empty,
        ])
    );
    assert!(rows.iter().all(|row| row.environment_refusal.is_none()));

    // With the gate refusing, the statement drops what it refuses; with it
    // off, it returns the refusal for the caller to record.
    assert!(
        placement_candidates::pull_on_create_worker_candidates(
            pool,
            pull_binds(Some("managed-env"), true),
        )
        .await?
        .is_empty()
    );
    let recorded = placement_candidates::pull_on_create_worker_candidates(
        pool,
        pull_binds(Some("managed-env"), false),
    )
    .await?;
    assert_eq!(ids(&recorded), ids(&rows));
    assert!(
        recorded
            .iter()
            .all(|row| row.environment_refusal.as_deref() == Some(NO_EVIDENCE))
    );
    // A holder is gated by its own row's binding, else the release's; any
    // other candidate by the build's.
    let split = placement_candidates::pull_on_create_worker_candidates(
        pool,
        PullOnCreateBinds {
            release_environment: None,
            ..pull_binds(Some("managed-env"), false)
        },
    )
    .await?;
    assert_eq!(ids(&split), ids(&rows));
    for row in &split {
        let holder = row.id == nodes.ready || row.id == nodes.no_base_snapshot;
        assert_eq!(
            row.environment_refusal.as_deref(),
            (!holder).then_some(NO_EVIDENCE),
            "{row:?}"
        );
    }
    assert!(
        placement_candidates::pull_on_create_worker_candidates(
            pool,
            PullOnCreateBinds {
                managed_by: SubstrateKind::Kubernetes,
                ..pull_binds(None, true)
            },
        )
        .await?
        .is_empty()
    );
    Ok(())
}

fn pooled_binds(environment: Option<&str>, refuse: bool) -> PooledCandidateBinds<'_> {
    PooledCandidateBinds {
        template: TEMPLATE,
        content_hash: None,
        runtime: "fc",
        managed_by: SubstrateKind::Space,
        refuse,
        release_environment: environment,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pooled_candidates_need_a_warm_member_fullest_pool_first() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    holders(pool).await?;
    let small = worker(pool, "small-pool").await?;
    snapshot(pool, small, "ready", false).await?;
    inventory(pool, small, TEMPLATE, HASH, true, 1).await?;
    let large = worker(pool, "large-pool").await?;
    snapshot(pool, large, "ready", false).await?;
    inventory(pool, large, TEMPLATE, "sha256:dddd", true, 3).await?;
    let stale = worker(pool, "stale-pool").await?;
    snapshot(pool, stale, "ready", true).await?;
    inventory(pool, stale, TEMPLATE, HASH, true, 5).await?;
    let deleted = direct_node(pool, "worker", "registered", "deleted-pool", true).await?;
    snapshot(pool, deleted, "ready", false).await?;
    inventory(pool, deleted, TEMPLATE, HASH, true, 5).await?;

    let rows =
        placement_candidates::pooled_worker_candidates(pool, pooled_binds(None, true)).await?;
    let order: Vec<NodeId> = rows.iter().map(|row| row.id).collect();
    assert_eq!(order, vec![large, small]);
    let first = &rows[0];
    assert_eq!(first.node_api_endpoint, endpoint("large-pool"));
    assert_eq!(first.content_hash, "sha256:dddd");
    assert_eq!(first.vcpu, Some(2));
    assert_eq!(first.memory_mib, Some(1024));
    assert_eq!(first.environment_refusal, None);

    let hash_only = placement_candidates::pooled_worker_candidates(
        pool,
        PooledCandidateBinds {
            content_hash: Some(HASH),
            ..pooled_binds(None, true)
        },
    )
    .await?;
    assert_eq!(
        hash_only.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![small]
    );

    assert!(
        placement_candidates::pooled_worker_candidates(
            pool,
            pooled_binds(Some("managed-env"), true)
        )
        .await?
        .is_empty()
    );
    let recorded = placement_candidates::pooled_worker_candidates(
        pool,
        pooled_binds(Some("managed-env"), false),
    )
    .await?;
    assert_eq!(recorded.len(), 2);
    assert!(
        recorded
            .iter()
            .all(|row| row.environment_refusal.as_deref() == Some(NO_EVIDENCE))
    );
    assert!(
        placement_candidates::pooled_worker_candidates(
            pool,
            PooledCandidateBinds {
                managed_by: SubstrateKind::Kubernetes,
                ..pooled_binds(None, true)
            },
        )
        .await?
        .is_empty()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn start_eligible_count_tolerates_missing_and_stale_snapshots() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    assert_eq!(
        placement_candidates::count_start_eligible_workers(pool, SubstrateKind::Space).await?,
        0
    );

    worker(pool, "unobserved").await?;
    let stale = worker(pool, "stale").await?;
    snapshot(pool, stale, "ready", true).await?;
    let ready = worker(pool, "ready").await?;
    snapshot(pool, ready, "ready", false).await?;
    for status in ["full", "draining", "unavailable"] {
        let refused = worker(pool, status).await?;
        snapshot(pool, refused, status, false).await?;
    }
    let unreachable = worker(pool, "unreachable").await?;
    snapshot_reachable(pool, unreachable, "ready", false, false).await?;
    direct_node(pool, "worker", "registered", "deleted", true).await?;
    direct_node(pool, "worker", "deleted", "unregistered", false).await?;
    direct_node(pool, "builder", "registered", "builder", false).await?;
    node(pool, "worker", "k8s", Some("ready")).await?;

    assert_eq!(
        placement_candidates::count_start_eligible_workers(pool, SubstrateKind::Space).await?,
        3
    );
    assert_eq!(
        placement_candidates::count_start_eligible_workers(pool, SubstrateKind::Kubernetes).await?,
        0
    );
    Ok(())
}
