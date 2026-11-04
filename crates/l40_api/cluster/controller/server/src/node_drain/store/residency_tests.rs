//! Postgres-backed coverage for the node-census residency discount applied by
//! [`NodeDrainSink::count_resident_sandboxes_on_node`] (the `draining →
//! drained` gate) and [`NodeDrainSink::list_evacuatable_sandboxes_on_node`]
//! (the evacuation work queue).
//!
//! Every case here is about one asymmetry: a row is dropped only when the node
//! answered recently and did not list the sandbox. Getting that direction
//! wrong is not a slow drain, it is a node terminated with a live sandbox on
//! it, so the fail-closed cases below carry as much weight as the discount
//! case itself.

use pplx_space_common_core::types::{NodeId, SandboxId};

use std::time::Duration;

use pplx_space_common_svc::sql::sandbox_residency::{
    CENSUS_INVENTORY_MAX_AGE, CENSUS_PLACEMENT_SETTLE,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The heartbeat state a node's `node_status_snapshots` row is in — the two
/// fields the discount reads, plus their absence.
enum Heartbeat {
    /// Reachable, and the heartbeat has not expired: the node's answer counts.
    Fresh,
    /// Last poll found the node unreachable.
    Unreachable,
    /// Reachable when last polled, but `stale_after` has passed.
    Expired,
    /// The node has never published a status snapshot.
    Absent,
}

impl Heartbeat {
    /// `None` when no row should be written at all; otherwise `reachable` and
    /// the signed offset applied to `NOW()` for `stale_after`.
    fn row(&self) -> Option<(bool, i32)> {
        match self {
            Self::Fresh => Some((true, 30)),
            Self::Unreachable => Some((false, 30)),
            Self::Expired => Some((true, -30)),
            Self::Absent => None,
        }
    }
}

/// How current the node's last **accepted** sandbox census is — what
/// `node_status_snapshots.sandbox_inventory_pruned_at` carries, and the fact a
/// status heartbeat cannot supply. Orthogonal to [`Heartbeat`] on purpose: the
/// failure shape under test combines a fresh heartbeat with a stale census.
#[derive(Debug, Clone, Copy)]
enum Census {
    /// A complete inventory was pruned against just now.
    Fresh,
    /// The node published one, but longer ago than
    /// [`CENSUS_INVENTORY_MAX_AGE`] allows — the wedged-publisher shape.
    Stale,
    /// `sandbox_inventory_pruned_at IS NULL`: no complete inventory has ever
    /// been accepted from this node, which is every row's state at rollout.
    Never,
}

impl Census {
    /// Seconds before `NOW()` to stamp, or `None` to leave the column NULL.
    fn secs_ago(self) -> Option<f64> {
        match self {
            Self::Fresh => Some(0.0),
            Self::Stale => Some((CENSUS_INVENTORY_MAX_AGE * 2).as_secs() as f64),
            Self::Never => None,
        }
    }
}

/// A node that publishes complete inventories normally, so its census is never
/// what a test is varying. Cases about census age call
/// [`insert_node_with_census`].
async fn insert_node(pool: &PgPool, heartbeat: Heartbeat) -> TestResult<NodeId> {
    insert_node_with_census(pool, heartbeat, Census::Fresh).await
}

async fn insert_node_with_census(
    pool: &PgPool,
    heartbeat: Heartbeat,
    census: Census,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.99'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;

    if let Some((reachable, stale_after_secs)) = heartbeat.row() {
        sqlx::query(sql!(
            "INSERT INTO node_status_snapshots (
                 node_id, observed_at, stale_after, reachable, role,
                 scheduling_status, services, sandbox_inventory_pruned_at,
                 updated_at
             )
             VALUES (
                 $1, NOW(), NOW() + make_interval(secs => $3), $2,
                 'worker', 'ready', '[]'::jsonb,
                 CASE WHEN $4::FLOAT8 IS NULL
                      THEN NULL
                      ELSE NOW() - make_interval(secs => $4)
                 END,
                 NOW()
             )"
        ))
        .bind(node_id)
        .bind(reachable)
        .bind(f64::from(stale_after_secs))
        .bind(census.secs_ago())
        .execute(pool)
        .await?;
    }
    Ok(node_id)
}

/// How long ago the row was placed, or that the column is NULL. The two
/// unsettled shapes — recently placed, and placement age unknown — have to
/// behave identically, so the tests take this rather than a bare `Duration`.
#[derive(Debug, Clone, Copy)]
enum Placed {
    Ago(Duration),
    /// `placement_changed_at IS NULL`: legacy rows predating the column, and
    /// the case where three-valued logic would otherwise decide the answer.
    Unknown,
}

impl Placed {
    /// `None` renders SQL NULL; `Some` renders `NOW() - interval`.
    fn secs_ago(self) -> Option<f64> {
        match self {
            Self::Ago(ago) => Some(ago.as_secs() as f64),
            Self::Unknown => None,
        }
    }
}

/// Places the row well outside [`CENSUS_PLACEMENT_SETTLE`], so the census
/// discount is what these tests measure rather than the settle gate. The
/// unsettled cases have their own test.
async fn insert_sandbox(pool: &PgPool, node_id: NodeId, lifecycle: &str) -> TestResult<SandboxId> {
    insert_sandbox_placed(
        pool,
        node_id,
        lifecycle,
        Placed::Ago(CENSUS_PLACEMENT_SETTLE * 2),
    )
    .await
}

async fn insert_sandbox_placed(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
    placed: Placed,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, network_policy_generation,
             placement_changed_at
         ) VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $3, 'on_node', 'k8s', $2, $2, 1,
             CASE WHEN $4::FLOAT8 IS NULL
                  THEN NULL
                  ELSE NOW() - make_interval(secs => $4)
             END
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(lifecycle)
    .bind(placed.secs_ago())
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Publish a census row: the node reporting that it holds `sandbox_id` in
/// `status`.
async fn observe_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    status: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, observed_at, updated_at)
         VALUES ($1, $2, $3, NOW(), NOW())"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

fn residency_sink(pool: PgPool, residency: ResidencyEvidence) -> SubstrateSink {
    let mut sink = SubstrateSink::with_suspend_execution(pool, |_| async move {
        unreachable!("residency scans never suspend");
    });
    sink.residency_evidence = residency;
    sink
}

/// The whole point of the change: on a node that answered recently, a row its
/// census does not list stops blocking the drain and stops entering the
/// evacuation queue, while a row the census does list keeps doing both. The
/// discounted count is reported rather than silently dropped.
#[tokio::test(flavor = "current_thread")]
async fn fresh_node_discounts_only_the_rows_its_census_omits() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_node(&db.pool, Heartbeat::Fresh).await?;
    let live = insert_sandbox(&db.pool, node_id, "started").await?;
    insert_sandbox(&db.pool, node_id, "started").await?;
    observe_sandbox(&db.pool, live, node_id, "started").await?;

    let sink = residency_sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);
    assert_eq!(
        NodeDrainSink::count_resident_sandboxes_on_node(&sink, node_id).await?,
        ResidentSandboxCounts {
            occupying: 1,
            discounted: 1,
        },
        "a row absent from a fresh node's census must not block draining → drained, \
         and must be reported as discounted rather than vanish"
    );
    assert_eq!(
        NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node_id).await?,
        vec![(live, SandboxLifecycle::Started)],
        "a row absent from a fresh node's census must not consume an evacuation slot"
    );

    // The kill switch restores the pre-census behavior for both scans, and
    // reports nothing discounted.
    let lifecycle_only = residency_sink(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
    assert_eq!(
        NodeDrainSink::count_resident_sandboxes_on_node(&lifecycle_only, node_id).await?,
        ResidentSandboxCounts {
            occupying: 2,
            discounted: 0,
        }
    );
    assert_eq!(
        NodeDrainSink::list_evacuatable_sandboxes_on_node(&lifecycle_only, node_id)
            .await?
            .len(),
        2
    );
    Ok(())
}

/// Fail-closed: an unreachable node, an expired heartbeat, and a node that
/// never published one are all "the control plane cannot see this node", not
/// "this node is empty". A brief node outage reading as "every sandbox is
/// gone" would end the drain in a termination.
#[tokio::test(flavor = "current_thread")]
async fn a_node_the_control_plane_cannot_see_keeps_every_row_counting() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = residency_sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);

    for heartbeat in [
        Heartbeat::Unreachable,
        Heartbeat::Expired,
        Heartbeat::Absent,
    ] {
        let node_id = insert_node(&db.pool, heartbeat).await?;
        // No census row at all — the exact shape that IS discounted on a
        // fresh node. Only the heartbeat differs.
        let sandbox_id = insert_sandbox(&db.pool, node_id, "started").await?;

        assert_eq!(
            NodeDrainSink::count_resident_sandboxes_on_node(&sink, node_id).await?,
            ResidentSandboxCounts {
                occupying: 1,
                discounted: 0,
            },
            "a blind node must keep every resident row counting"
        );
        assert_eq!(
            NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node_id).await?,
            vec![(sandbox_id, SandboxLifecycle::Started)],
            "a blind node's rows stay evacuatable — the pipeline, not the \
             census, decides they are gone"
        );
    }
    Ok(())
}

/// Fail-closed, both unsettled shapes, through the composed SQL rather than
/// through the generated string:
///
/// * **Recently placed** — the census facet publishes on its own ~60 s poll,
///   separately from the status heartbeat that gates the discount, so a sandbox
///   placed since the last census is legitimately missing from it. Reading that
///   ordinary lag as absence would discount a sandbox that just started.
/// * **NULL `placement_changed_at`** — a legacy row predating the column. Left
///   to three-valued logic a bare `< NOW() - interval` yields NULL, and the
///   answer would depend on where in the expression that NULL lands; the
///   explicit `IS NOT NULL` is what makes "unknown age" mean "keep counting".
#[tokio::test(flavor = "current_thread")]
async fn an_unsettled_placement_is_not_proven_absent() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = residency_sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);

    for placed in [Placed::Ago(CENSUS_PLACEMENT_SETTLE / 2), Placed::Unknown] {
        let node_id = insert_node(&db.pool, Heartbeat::Fresh).await?;
        // No census row, fresh heartbeat: identical to the discounted shape
        // except for placement age.
        let unsettled = insert_sandbox_placed(&db.pool, node_id, "started", placed).await?;

        assert_eq!(
            NodeDrainSink::count_resident_sandboxes_on_node(&sink, node_id).await?,
            ResidentSandboxCounts {
                occupying: 1,
                discounted: 0,
            },
            "{placed:?} must keep counting against draining → drained"
        );
        assert_eq!(
            NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node_id).await?,
            vec![(unsettled, SandboxLifecycle::Started)],
            "{placed:?} must stay evacuatable, so the drain services it rather \
             than terminating over it"
        );
    }
    Ok(())
}

/// Fail-closed: a census row whose status disagrees with the cluster lifecycle
/// is still the node saying it holds state for that sandbox. The production
/// shape is a cluster-`started` row the node reports `suspended` — a local
/// snapshot on that node's disk, and the only copy. Reading the disagreement
/// as absence would discount exactly the rows whose termination loses data.
#[tokio::test(flavor = "current_thread")]
async fn a_status_mismatch_is_presence_not_absence() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_node(&db.pool, Heartbeat::Fresh).await?;
    let mismatched = insert_sandbox(&db.pool, node_id, "started").await?;
    observe_sandbox(&db.pool, mismatched, node_id, "suspended").await?;

    let sink = residency_sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);
    assert_eq!(
        NodeDrainSink::count_resident_sandboxes_on_node(&sink, node_id).await?,
        ResidentSandboxCounts {
            occupying: 1,
            discounted: 0,
        },
        "an observed status that disagrees with the lifecycle must keep the row counting"
    );
    Ok(())
}

/// A census row published by a *different* node is absence as far as this
/// node is concerned: `sandbox_observed_states` is keyed by `sandbox_id`, so
/// the row belongs to whichever node published last, and this node's own
/// complete-inventory prune would have dropped it.
#[tokio::test(flavor = "current_thread")]
async fn a_census_row_naming_another_node_does_not_hold_this_one() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_node(&db.pool, Heartbeat::Fresh).await?;
    let other_node_id = insert_node(&db.pool, Heartbeat::Fresh).await?;
    let moved = insert_sandbox(&db.pool, node_id, "started").await?;
    observe_sandbox(&db.pool, moved, other_node_id, "started").await?;

    let sink = residency_sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);
    assert_eq!(
        NodeDrainSink::count_resident_sandboxes_on_node(&sink, node_id).await?,
        ResidentSandboxCounts {
            occupying: 0,
            discounted: 1,
        },
        "a sandbox another node reports holding is not resident here"
    );
    Ok(())
}

/// The evacuation scan reaches `suspending` rows through the
/// `evacuation_restore_target` marker rather than through `lifecycle`, so the
/// census filter has to compose with that branch too — an in-flight
/// evacuation suspend the node still reports must stay in the queue for the
/// pod-retirement redrive.
#[tokio::test(flavor = "current_thread")]
async fn the_census_filter_composes_with_the_in_flight_suspend_branch() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = insert_node(&db.pool, Heartbeat::Fresh).await?;
    let redrivable = insert_sandbox(&db.pool, node_id, "suspending").await?;
    let phantom = insert_sandbox(&db.pool, node_id, "suspending").await?;
    for sandbox_id in [redrivable, phantom] {
        sqlx::query(sql!(
            "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&db.pool)
        .await?;
    }
    observe_sandbox(&db.pool, redrivable, node_id, "suspending").await?;

    let sink = residency_sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);
    assert_eq!(
        NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node_id).await?,
        vec![(redrivable, SandboxLifecycle::Started)],
        "the marker branch must carry the same census filter as the lifecycle branch"
    );
    Ok(())
}

/// The exposure this module's census-freshness term exists to close, and the
/// reason the discount could not ship enabled before it.
///
/// A node whose complete-inventory publisher has wedged keeps answering: the
/// status, network-policy, and template facets each refresh `reachable` and
/// `stale_after` on their own tick, all carrying
/// `sandbox_inventory_complete: false`. So the heartbeat stays fresh while the
/// census goes arbitrarily stale. A live sandbox then ages past
/// `CENSUS_PLACEMENT_SETTLE`, is absent from a census nobody is updating, and on
/// the heartbeat alone would be discounted by this gate — ending the drain in a
/// termination with a live VM on the node.
///
/// `Census::Never` is the same fault at rollout: every row's
/// `sandbox_inventory_pruned_at` is NULL when the column lands, so a reader that
/// resolved NULL toward "no rows here" would discount the whole fleet at once.
#[tokio::test(flavor = "current_thread")]
async fn a_fresh_heartbeat_with_no_current_census_discounts_nothing() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = residency_sink(db.pool.clone(), ResidencyEvidence::NodeCensusAware);

    for census in [Census::Stale, Census::Never] {
        let node_id = insert_node_with_census(&db.pool, Heartbeat::Fresh, census).await?;
        // Identical in every respect to the discounted shape — fresh heartbeat,
        // settled placement, no census row — except for how current the node's
        // last accepted inventory is.
        let live = insert_sandbox(&db.pool, node_id, "started").await?;

        assert_eq!(
            NodeDrainSink::count_resident_sandboxes_on_node(&sink, node_id).await?,
            ResidentSandboxCounts {
                occupying: 1,
                discounted: 0,
            },
            "{census:?}: a heartbeat is not evidence about the census, so the row \
             must keep blocking draining → drained"
        );
        assert_eq!(
            NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node_id).await?,
            vec![(live, SandboxLifecycle::Started)],
            "{census:?}: the row must stay evacuatable so the drain services it \
             rather than terminating over it"
        );
    }
    Ok(())
}
