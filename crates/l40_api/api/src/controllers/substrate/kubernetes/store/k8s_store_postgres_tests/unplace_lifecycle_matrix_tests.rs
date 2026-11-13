//! Writer × stored-lifecycle matrices for the two writes that end a row's
//! placement after its pods are torn down: the unplace every pod-tracking
//! cleanup ends with (`clear_pod_tracking_in`), under both placement clocks,
//! and the drain's release of a stopped evacuation's node
//! (`release_stopped_evacuation_placement`). Each runs against each shape a
//! drain can leave on the row. The expected tables are written out rather
//! than derived from the statements.

use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId};
use pplx_space_db_test_fixtures::publish_ready_disk_artifact;
use pplx_space_sandbox_store::test_support::stored_lifecycle_matrix;

use super::*;

/// What a drain can have left on the row before the write runs.
#[derive(Debug, Clone, Copy)]
struct Shape {
    /// The `'stopped'` evacuation marker, which the drain's claim sets.
    marked: bool,
    /// A durable pinned disk artifact, which the drain publishes.
    durable: bool,
    /// An active execution, which a `start` holds.
    executing: bool,
}

impl Shape {
    const NEITHER: Self = Self::new(false, false, false);
    const MARKED: Self = Self::new(true, false, false);
    const DURABLE: Self = Self::new(false, true, false);
    const PUBLISHED: Self = Self::new(true, true, false);
    const STARTING: Self = Self::new(true, true, true);

    const fn new(marked: bool, durable: bool, executing: bool) -> Self {
        Self {
            marked,
            durable,
            executing,
        }
    }
}

/// The unplace on each shape: it refuses every `stopped` row, including one
/// the drain has marked and published, since a start may have written to the
/// rootfs since. `B` and `P` are the bump and preserve clocks. `A`: the row is
/// unplaced; `.`: every placement column is left as it was.
const UNPLACE_EXPECTED: &[&str] = &[
    // shape:        neither marked  durable published
    // clock:        B   P   B   P   B   P   B   P
    "created         A   A   A   A   A   A   A   A",
    "creating        A   A   A   A   A   A   A   A",
    "starting        A   A   A   A   A   A   A   A",
    "started         A   A   A   A   A   A   A   A",
    "restoring       A   A   A   A   A   A   A   A",
    "pausing         A   A   A   A   A   A   A   A",
    "paused          A   A   A   A   A   A   A   A",
    "resuming        A   A   A   A   A   A   A   A",
    "suspending      A   A   A   A   A   A   A   A",
    "suspended       A   A   A   A   A   A   A   A",
    "stopping        A   A   A   A   A   A   A   A",
    "stopped         .   .   .   .   .   .   .   .",
    "deleting        A   A   A   A   A   A   A   A",
    "checkpointing   A   A   A   A   A   A   A   A",
    "quarantined     A   A   A   A   A   A   A   A",
    "failed          A   A   A   A   A   A   A   A",
    "deleted         A   A   A   A   A   A   A   A",
    "bogus           A   A   A   A   A   A   A   A",
    "_Suspended_     A   A   A   A   A   A   A   A",
];

/// The release on each shape: only a `stopped` row the drain has marked and
/// published, with no execution a start holds. Same marks as above; the
/// release always bumps the clock.
const RELEASE_EXPECTED: &[&str] = &[
    // shape:        neither marked durable published starting
    "created         .       .      .       .         .",
    "creating        .       .      .       .         .",
    "starting        .       .      .       .         .",
    "started         .       .      .       .         .",
    "restoring       .       .      .       .         .",
    "pausing         .       .      .       .         .",
    "paused          .       .      .       .         .",
    "resuming        .       .      .       .         .",
    "suspending      .       .      .       .         .",
    "suspended       .       .      .       .         .",
    "stopping        .       .      .       .         .",
    "stopped         .       .      .       A         .",
    "deleting        .       .      .       .         .",
    "checkpointing   .       .      .       .         .",
    "quarantined     .       .      .       .         .",
    "failed          .       .      .       .         .",
    "deleted         .       .      .       .         .",
    "bogus           .       .      .       .         .",
    "_Suspended_     .       .      .       .         .",
];

/// The `sandboxes` columns both writes set.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct PlacementColumns {
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    placement_changed_at: Option<OffsetDateTime>,
}

async fn placement_columns(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<PlacementColumns> {
    Ok(sqlx::query_as(sql!(
        "SELECT placement, active_node_id, target_node_id, placement_changed_at
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// A placed row in `stored`, with an hour-old placement clock, in `shape`.
async fn seed(
    fixture: &PostgresStore,
    stored: &str,
    shape: Shape,
) -> TestResult<(SandboxId, PlacementColumns)> {
    let pool = &fixture.pool;
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, stored).await?;
    // Leaves the lifecycle alone, so the marker-clearing trigger keeps the
    // marker on every stored value.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'on_node',
                active_node_id = $2,
                target_node_id = $3,
                placement_changed_at = NOW() - INTERVAL '1 hour',
                evacuation_restore_target = CASE WHEN $4 THEN 'stopped' END
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(NodeId::new())
    .bind(NodeId::new())
    .bind(shape.marked)
    .execute(pool)
    .await?;
    if shape.durable {
        publish_ready_disk_artifact(
            pool,
            sandbox_id,
            LocalArtifactId::new(),
            "s3://space-test/unplace-matrix-disk?region=us-east-1",
        )
        .await?;
    }
    if shape.executing {
        fixture
            .store
            .ensure_active_execution_row(sandbox_id)
            .await?;
    }
    Ok((sandbox_id, placement_columns(pool, sandbox_id).await?))
}

/// Whether `after` is `before` unplaced, with the clock bumped or kept.
fn unplaced(before: &PlacementColumns, after: &PlacementColumns, clock: PlacementClock) -> bool {
    let clock_matches = match clock {
        PlacementClock::Bump => after.placement_changed_at > before.placement_changed_at,
        PlacementClock::Preserve => after.placement_changed_at == before.placement_changed_at,
    };
    after.placement == "unplaced"
        && after.active_node_id.is_none()
        && after.target_node_id.is_none()
        && clock_matches
}

#[tokio::test(flavor = "current_thread")]
async fn pod_teardown_never_unplaces_a_stopped_row() -> TestResult {
    let fixture = postgres_store().await?;
    let pool = &fixture.pool;
    let columns = [
        (Shape::NEITHER, PlacementClock::Bump),
        (Shape::NEITHER, PlacementClock::Preserve),
        (Shape::MARKED, PlacementClock::Bump),
        (Shape::MARKED, PlacementClock::Preserve),
        (Shape::DURABLE, PlacementClock::Bump),
        (Shape::DURABLE, PlacementClock::Preserve),
        (Shape::PUBLISHED, PlacementClock::Bump),
        (Shape::PUBLISHED, PlacementClock::Preserve),
    ];

    let mut mismatches = Vec::new();
    for (stored, cells) in stored_lifecycle_matrix(columns, UNPLACE_EXPECTED) {
        for ((shape, clock), expect) in cells {
            let (sandbox_id, before) = seed(&fixture, stored, shape).await?;
            let mut cleanup = pool.begin().await?;
            assert!(
                K8sStore::lock_sandbox_for_execution_mutation_in(&mut cleanup, sandbox_id).await?
            );
            fixture
                .store
                .clear_pod_tracking_in(&mut cleanup, sandbox_id, clock)
                .await?;
            cleanup.commit().await?;

            let after = placement_columns(pool, sandbox_id).await?;
            let row_matches = if expect.applies() {
                unplaced(&before, &after, clock)
            } else {
                after == before
            };
            if !row_matches {
                mismatches.push(format!(
                    "{shape:?} {clock:?} from {stored:?}: {before:?} -> {after:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_drain_releases_only_a_published_stopped_row_nothing_runs_in() -> TestResult {
    let fixture = postgres_store().await?;
    let pool = &fixture.pool;
    let columns = [
        Shape::NEITHER,
        Shape::MARKED,
        Shape::DURABLE,
        Shape::PUBLISHED,
        Shape::STARTING,
    ];

    let mut mismatches = Vec::new();
    for (stored, cells) in stored_lifecycle_matrix(columns, RELEASE_EXPECTED) {
        for (shape, expect) in cells {
            let (sandbox_id, before) = seed(&fixture, stored, shape).await?;
            let released = fixture
                .store
                .release_stopped_evacuation_placement(sandbox_id)
                .await?;

            let after = placement_columns(pool, sandbox_id).await?;
            let row_matches = if expect.applies() {
                unplaced(&before, &after, PlacementClock::Bump)
            } else {
                after == before
            };
            if released != expect.applies() || !row_matches {
                mismatches.push(format!(
                    "{shape:?} from {stored:?}: released {released}, {before:?} -> {after:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
