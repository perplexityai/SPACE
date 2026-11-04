//! Postgres coverage for [`super::queued_artifact_recoverable_without_node`]:
//! what it answers.
//!
//! The predicate decides whether a per-node pending-backup queue row may be
//! deleted, so a wrong TRUE releases a node that still holds the only copy of
//! a sandbox. The answer is therefore driven against a live database rather
//! than argued: the answer table below enumerates the artifact states and the
//! sandbox states that can separate the two durability arms.
//!
//! **How Postgres resolves it is pinned elsewhere.** A predicate can select
//! exactly the right rows and still read a sandbox's whole snapshot history
//! per queue row, which no answer test can see, so
//! `the_delete_guard_resolves_through_indexes_not_a_history_scan` reads the
//! plan instead. It lives beside the real statement in
//! `crates/l40_api/internal/api/server/src/node_local_backup_pending.rs`,
//! because the plan belongs to a whole statement rather than to this
//! fragment, and this crate cannot depend on internal-api.
//!
//! The `MarkerlessFresh` row is the one to look at first if this file ever
//! goes red on a predicate change: it is the case a timestamp-based
//! supersession test gets wrong, and the whole reason the predicate is keyed
//! on artifact identity instead.

use pplx_space_common_core::types::{NodeId, SandboxId};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, try_pg_test_db,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::queued_artifact_recoverable_without_node;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The snapshot carrying the queue row's own `(artifact_id, artifact_kind)`.
///
/// Its artifact is never the sandbox's marker, so it can only ever satisfy the
/// own-bytes arm — which is what keeps the two independent across the table.
#[derive(Clone, Copy, Debug)]
enum Own {
    /// The artifact's backup was never begun. The row the queue exists for.
    None,
    /// `ready` with a live storage location: this artifact's bytes are off the
    /// disk.
    ReadyLive,
    /// `ready`, but its only storage location was orphaned.
    ReadyOrphaned,
    /// An upload that started and never finished — the shape most easily
    /// mistaken for durability.
    Creating,
    /// Same artifact id, `disk` rather than `full`. The identity test matches
    /// on both columns, so this is the case a one-column test would get wrong.
    WrongKind,
}

/// Whether the sandbox's **current** artifact is durable, which is the second
/// disjunct's whole question.
#[derive(Clone, Copy, Debug)]
enum Sandbox {
    /// Marker names a `full` artifact that has a `ready`, live snapshot.
    MarkerDurable,
    /// Marker names a **`disk`** artifact that has a `ready`, live snapshot —
    /// what a `stopped` sandbox looks like, since `publish_stopped_artifact`
    /// writes a `disk`/`stop` local artifact.
    ///
    /// The case a `sn.type = 'full'` filter in the departed-marker arm would get
    /// wrong: `snapshots.type` is derived from the artifact kind, so such a
    /// filter makes every stopped sandbox permanently unable to satisfy it.
    MarkerDurableDisk,
    /// Marker names an artifact whose upload is still `creating`.
    MarkerNotDurable,
    /// Rollout-skew row with no marker, and a `ready` full snapshot stamped
    /// **after** the last placement.
    ///
    /// Both markerless cases short-circuit to FALSE on the NULL marker, so to
    /// *this* predicate they are one case. The pair is kept because
    /// `snapshot_durable_for_source_release` separates them — it would call
    /// this one releasable — and re-admitting that predicate here is the
    /// regression the pair exists to catch. `snapshots.timestamp` is insert
    /// time, so a deferred backup of an older artifact also lands after the
    /// placement, and freshness is not evidence about any particular artifact.
    MarkerlessFresh,
    /// No marker, and the only `ready` full snapshot predates the last
    /// placement. Must answer the same as [`Sandbox::MarkerlessFresh`].
    MarkerlessStale,
}

const OWNS: [Own; 5] = [
    Own::None,
    Own::ReadyLive,
    Own::ReadyOrphaned,
    Own::Creating,
    Own::WrongKind,
];
const SANDBOXES: [Sandbox; 5] = [
    Sandbox::MarkerDurable,
    Sandbox::MarkerDurableDisk,
    Sandbox::MarkerNotDurable,
    Sandbox::MarkerlessFresh,
    Sandbox::MarkerlessStale,
];

impl Sandbox {
    /// Whether the sandbox's current artifact is provably durable. Only the
    /// marker can establish that — a markerless row has no current artifact to
    /// name, so no snapshot of it can be identified, however recently stamped.
    const fn current_artifact_durable(self) -> bool {
        matches!(self, Self::MarkerDurable | Self::MarkerDurableDisk)
    }
}

/// Where the sandbox sits relative to the queue row's node.
///
/// The third axis, and not a cosmetic one. A durable marker does not order
/// the marker against the queued artifact, so the departed-marker arm fires only
/// once the sandbox is attached to some *other* node — at which point the
/// marker can only advance to that node's artifact and can never return to
/// the one the queue row tracks. Being merely unattached does not qualify.
/// Without this axis every marker-durable case is seeded identically and the
/// table cannot tell a release from a data-loss delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Residency {
    /// Attached and running here — the `state_degraded` marker-lag shape.
    Attached,
    /// The post-reaper shape: `active_node_id` NULL, but
    /// `target_node_id = COALESCE(target_node_id, active_node_id)` carried the
    /// node forward. Still attached, and the case a residency test on
    /// `active_node_id` alone would misread as a relocation.
    UnplacedButTargeted,
    /// Running here while targeted elsewhere. Attached by `active_node_id`
    /// only, so this is the case that pins the `active_node_id` conjunct:
    /// with `target_node_id` alone the predicate would read this sandbox as
    /// departed and release a row for a disk it is still sitting on.
    ActiveHereTargetedElsewhere,
    /// Detached from every node: `clear_pod_tracking_in` NULLs both columns
    /// and leaves the artifact marker alone. "Not here" is not "somewhere
    /// else" — `IS DISTINCT FROM` is TRUE against NULL, so without a
    /// non-NULL-attachment requirement this reads as departed and the row is
    /// released for a sandbox that may yet be placed back onto this node.
    Detached,
    /// Restored elsewhere: placement wrote both columns to the new node.
    Departed,
}

const RESIDENCIES: [Residency; 5] = [
    Residency::Attached,
    Residency::UnplacedButTargeted,
    Residency::ActiveHereTargetedElsewhere,
    Residency::Detached,
    Residency::Departed,
];

impl Residency {
    /// Whether the sandbox is no longer attached to the queue row's node,
    /// which is the departed-marker arm's precondition.
    const fn left_the_node(self) -> bool {
        matches!(self, Self::Departed)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_queue_row_is_recoverable_exactly_when_its_bytes_or_its_sandbox_are() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;

    let mut cases = Vec::new();
    for own in OWNS {
        for sandbox in SANDBOXES {
            for residency in RESIDENCIES {
                cases.push((
                    insert_case(pool, node, own, sandbox, residency).await?,
                    own,
                    sandbox,
                    residency,
                ));
            }
        }
    }

    let answers = evaluate(pool, node).await?;
    for (queued_sandbox, own, sandbox, residency) in &cases {
        let expected = matches!(own, Own::ReadyLive)
            || (sandbox.current_artifact_durable() && residency.left_the_node());
        assert_eq!(
            lookup(&answers, *queued_sandbox),
            Some(expected),
            "{own:?} artifact on a {sandbox:?} sandbox that is {residency:?} must \
             evaluate to {expected}: the row may be given up exactly when this \
             artifact's own bytes are durable, or when the sandbox has left this \
             node and its current artifact is durable"
        );
    }

    // Non-degeneracy: an answer table asserted against a predicate that is
    // constant across the fixture passes while reading nothing.
    let true_rows = answers.iter().filter(|(_, value)| *value).count();
    assert!(
        true_rows > 0 && true_rows < cases.len(),
        "fixture must contain rows on both sides; got {true_rows} true of {}",
        cases.len()
    );

    // The row the two durability arms are most easily confused on: nothing durable
    // for this artifact, but the sandbox has moved on to one that is. Named
    // explicitly, because if a later edit drops the combination the loop above
    // still passes.
    let (moved_on, _, _, _) = cases
        .iter()
        .find(|(_, own, sandbox, residency)| {
            matches!(own, Own::None)
                && matches!(sandbox, Sandbox::MarkerDurable)
                && matches!(residency, Residency::Departed)
        })
        .expect("the fixture must contain an un-backed artifact on a durable, departed sandbox");
    assert_eq!(
        lookup(&answers, *moved_on),
        Some(true),
        "an artifact the node never backed up, on a sandbox that left this node \
         and whose current artifact is durable, is stale bytes rather than a \
         casualty"
    );

    // The converse, named for the same reason and the one with teeth: identical
    // in every respect except that the sandbox never left. Deleting here
    // discards the only copy of an artifact the marker cannot be shown to
    // postdate. If a later edit drops the residency conjunct, the loop above
    // and this assertion fail together while the departed case still passes.
    let (stayed, _, _, _) = cases
        .iter()
        .find(|(_, own, sandbox, residency)| {
            matches!(own, Own::None)
                && matches!(sandbox, Sandbox::MarkerDurable)
                && matches!(residency, Residency::Attached)
        })
        .expect("the fixture must contain an un-backed artifact on a durable, resident sandbox");
    assert_eq!(
        lookup(&answers, *stayed),
        Some(false),
        "a durable marker on a sandbox still attached to this node does not \
         establish that the marker postdates the queued artifact, so the row \
         must be retained"
    );
    Ok(())
}

/// A tombstoned sandbox does **not** release its row on the tombstone alone.
///
/// `lifecycle = 'deleted'` is not an arm of the predicate, because
/// `prune_absent_node_sandboxes` mints tombstones from census absence: a
/// machine replaced under the same kube node name reuses `nodes.id`, and its
/// first census tombstones every predecessor sandbox in the same transaction
/// that then runs this predicate. Releasing on the tombstone would erase the
/// casualty records for the disk that just went away.
///
/// Seeded in the worst shape for the two real arms — no snapshot for the
/// queued artifact, no marker, detached — so the answer rests on the
/// lifecycle and nothing else.
#[tokio::test(flavor = "current_thread")]
async fn a_tombstoned_sandbox_does_not_release_its_row_on_the_tombstone_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_ready_worker_node(pool, node).await?;

    let mut seeded = Vec::new();
    for lifecycle in ["deleted", "deleting", "suspended"] {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, placement, managed_by,
                 active_node_id, target_node_id, placement_changed_at,
                 lifecycle_changed_at, network_policy_generation
             )
             VALUES ($1, $2, $3, $4, 'unplaced', 'space', NULL, NULL,
                     NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour', 1)"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(lifecycle)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_local_backup_pending
                 (node_id, sandbox_id, artifact_id, artifact_kind)
             VALUES ($1, $2, $3, 'full')"
        ))
        .bind(node)
        .bind(sandbox_id)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
        seeded.push((sandbox_id, lifecycle));
    }

    let answers = evaluate(pool, node).await?;
    for (sandbox_id, lifecycle) in seeded {
        assert_eq!(
            lookup(&answers, sandbox_id),
            Some(false),
            "a {lifecycle} sandbox with no durable artifact anywhere is a \
             casualty whatever its lifecycle says"
        );
    }
    Ok(())
}

fn lookup(rows: &[(SandboxId, bool)], sandbox: SandboxId) -> Option<bool> {
    rows.iter()
        .find(|(id, _)| *id == sandbox)
        .map(|(_, value)| *value)
}

async fn evaluate(pool: &PgPool, node: NodeId) -> TestResult<Vec<(SandboxId, bool)>> {
    let statement = format!(
        "SELECT queued.sandbox_id, {}
           FROM node_local_backup_pending queued
           JOIN sandboxes sandbox ON sandbox.id = queued.sandbox_id
          WHERE queued.node_id = $1",
        queued_artifact_recoverable_without_node(
            "sandbox",
            "queued.artifact_id",
            "queued.artifact_kind",
            "queued.node_id",
        )
    );
    Ok(
        sqlx::query_as::<_, (SandboxId, bool)>(sqlx::AssertSqlSafe(sql!(statement.as_str())))
            .bind(node)
            .fetch_all(pool)
            .await?,
    )
}

/// One fixture `snapshots` row.
///
/// `kind` sets both `type` and `artifact_kind`, because
/// `begin_artifact_backup` derives the former from the latter — letting a
/// fixture set them apart would seed a row production cannot produce, and the
/// departed-marker arm's `disk`-marker case turns on exactly that coupling.
struct SnapshotSpec {
    timestamp: &'static str,
    state: &'static str,
    artifact: Uuid,
    kind: &'static str,
    orphaned: bool,
}

async fn insert_case(
    pool: &PgPool,
    node: NodeId,
    own: Own,
    sandbox: Sandbox,
    residency: Residency,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let queued_artifact = Uuid::now_v7();
    let marker_artifact = Uuid::now_v7();
    let (marker, marker_kind) = match sandbox {
        Sandbox::MarkerDurable | Sandbox::MarkerNotDurable => (Some(marker_artifact), "full"),
        Sandbox::MarkerDurableDisk => (Some(marker_artifact), "disk"),
        Sandbox::MarkerlessFresh | Sandbox::MarkerlessStale => (None, "full"),
    };
    // `Departed` needs a real second node: `target_node_id` is a foreign key,
    // so a synthetic id would fail the insert rather than model a relocation.
    let (active_node, target_node) = match residency {
        Residency::Attached => (Some(node), Some(node)),
        Residency::UnplacedButTargeted => (None, Some(node)),
        Residency::ActiveHereTargetedElsewhere => {
            let other = NodeId::new();
            insert_ready_worker_node(pool, other).await?;
            (Some(node), Some(other))
        }
        Residency::Detached => (None, None),
        Residency::Departed => {
            let other = NodeId::new();
            insert_ready_worker_node(pool, other).await?;
            (Some(other), Some(other))
        }
    };
    let placement = match residency {
        Residency::UnplacedButTargeted | Residency::Detached => "unplaced",
        Residency::Attached | Residency::ActiveHereTargetedElsewhere | Residency::Departed => {
            "on_node"
        }
    };
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, placement_changed_at,
             lifecycle_changed_at, current_local_artifact_id,
             current_local_artifact_kind, current_local_artifact_origin,
             network_policy_generation
         )
         VALUES (
             $1, $2, $3, 'suspended', $7, 'k8s', $8, $4,
             NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
             $5, CASE WHEN $5::uuid IS NULL THEN NULL ELSE $6::TEXT END,
             CASE WHEN $5::uuid IS NULL THEN NULL
                  WHEN $6::TEXT = 'disk' THEN 'stop' ELSE 'suspend' END, 1
         )"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(target_node)
    .bind(marker)
    .bind(marker_kind)
    .bind(placement)
    .bind(active_node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending
             (node_id, sandbox_id, artifact_id, artifact_kind)
         VALUES ($1, $2, $3, 'full')"
    ))
    .bind(node)
    .bind(sandbox_id)
    .bind(queued_artifact)
    .execute(pool)
    .await?;

    // Stamped before `placement_changed_at` throughout, so the queued
    // artifact's own snapshot can only ever satisfy the identity disjunct.
    const OLD: &str = "NOW() - INTERVAL '3 hours'";
    let mut snapshots: Vec<SnapshotSpec> = Vec::new();
    match own {
        Own::None => {}
        Own::ReadyLive => snapshots.push(SnapshotSpec {
            timestamp: OLD,
            state: "ready",
            artifact: queued_artifact,
            kind: "full",
            orphaned: false,
        }),
        Own::ReadyOrphaned => snapshots.push(SnapshotSpec {
            timestamp: OLD,
            state: "ready",
            artifact: queued_artifact,
            kind: "full",
            orphaned: true,
        }),
        Own::Creating => snapshots.push(SnapshotSpec {
            timestamp: OLD,
            state: "creating",
            artifact: queued_artifact,
            kind: "full",
            orphaned: false,
        }),
        Own::WrongKind => snapshots.push(SnapshotSpec {
            timestamp: OLD,
            state: "ready",
            artifact: queued_artifact,
            kind: "disk",
            orphaned: false,
        }),
    }
    match sandbox {
        Sandbox::MarkerDurable => {
            snapshots.push(SnapshotSpec {
                timestamp: OLD,
                state: "ready",
                artifact: marker_artifact,
                kind: "full",
                orphaned: false,
            });
        }
        // `snapshots.type` tracks the artifact kind, so a disk marker's
        // durable copy is a `type='disk'` row.
        Sandbox::MarkerDurableDisk => {
            snapshots.push(SnapshotSpec {
                timestamp: OLD,
                state: "ready",
                artifact: marker_artifact,
                kind: "disk",
                orphaned: false,
            });
        }
        Sandbox::MarkerNotDurable => {
            snapshots.push(SnapshotSpec {
                timestamp: OLD,
                state: "creating",
                artifact: marker_artifact,
                kind: "full",
                orphaned: false,
            });
        }
        // Neither can satisfy the predicate — there is no marker to name an
        // artifact — so the stamps differ only to keep the pair meaningful
        // against a predicate that does read them.
        Sandbox::MarkerlessFresh => {
            snapshots.push(SnapshotSpec {
                timestamp: "NOW()",
                state: "ready",
                artifact: Uuid::now_v7(),
                kind: "full",
                orphaned: false,
            });
        }
        Sandbox::MarkerlessStale => {
            snapshots.push(SnapshotSpec {
                timestamp: OLD,
                state: "ready",
                artifact: Uuid::now_v7(),
                kind: "full",
                orphaned: false,
            });
        }
    }
    for snapshot in snapshots {
        insert_snapshot(pool, node, sandbox_id, snapshot).await?;
    }
    Ok(sandbox_id)
}

async fn insert_snapshot(
    pool: &PgPool,
    node: NodeId,
    sandbox_id: SandboxId,
    spec: SnapshotSpec,
) -> TestResult {
    let SnapshotSpec {
        timestamp,
        state,
        artifact,
        kind,
        orphaned,
    } = spec;
    let snapshot = Uuid::now_v7();
    let statement = format!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, source_node_id, org_id, timestamp,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES ($1, $2, $3, $4, $5, $6, {timestamp}, $7, $8, 'suspend')"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(statement.as_str())))
        .bind(snapshot)
        .bind(sandbox_id)
        .bind(kind)
        .bind(state)
        .bind(node)
        .bind(SEED_ORG_ID)
        .bind(artifact)
        .bind(kind)
        .execute(pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id, orphaned_at)
         VALUES ($1, 's3://space-test/queued-artifact', $2,
                 CASE WHEN $3::BOOLEAN THEN NOW() ELSE NULL END)"
    ))
    .bind(snapshot)
    .bind(SEED_ORG_ID)
    .bind(orphaned)
    .execute(pool)
    .await?;
    Ok(())
}
