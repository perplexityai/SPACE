use super::*;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, NodeId, SandboxId, SandboxLifecycle, SnapshotState, SubstrateKind,
};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, pg_test_db_with_max_connections,
    try_pg_test_db,
};

use pplx_space_test_env::TestResult;

/// A `sandboxes` row placed on `node_id` and labelled with `managed_by`.
/// `SubstrateKind::Space` is what puts it inside the prune's `marked_deleted`
/// arm at all — a Kubernetes-managed row never reaches that arm.
async fn insert_placed_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    lifecycle: SandboxLifecycle,
    managed_by: SubstrateKind,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, active_node_id,
             target_node_id, managed_by, network_policy_generation
         )
         VALUES ($1, $2, $3, $4, 'on_node', $5, $5, $6, 1)",
    )
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle.as_str())
    .bind(node_id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    Ok(())
}

/// What the cluster knows about a sandbox's durable bytes when the node stops
/// reporting it — the axis the prune's exemption turns on.
///
/// One variant rather than a `(kind, origin)` plus `(state, location)` pair of
/// tuples: those are two same-shaped tuples whose slots mean different things,
/// so a case can be misassembled at the call site without failing to compile.
/// Mirrors `sandbox_portability_tests`' `SnapshotSetup`.
#[derive(Debug, Clone, Copy)]
enum ArtifactPin {
    /// No `current_local_artifact_*` marker and no catalog row.
    None,
    /// No marker, but the sandbox owns a durable `ready` artifact anyway.
    ///
    /// Covers the fragment's NULL-safety claim behaviorally: with the identity
    /// match gone, `current_local_artifact_origin = 'stop'` evaluates to NULL
    /// for a markerless row while the `EXISTS` goes TRUE, the conjunction
    /// yields NULL, and `AND NOT NULL` filters the row out — sparing it. The
    /// row must be reaped.
    NoMarkerForeignArtifactDurable,
    /// A stop-origin disk pin whose `ready` snapshot has no storage location
    /// at all, so the `JOIN` finds nothing. Distinct from
    /// [`Self::StopDiskOrphaned`], which has a location and stamps it.
    StopDiskUnlocated,
    /// A stop-origin disk pin whose exact artifact is `ready` on a live
    /// storage location.
    StopDiskDurable,
    /// As [`Self::StopDiskDurable`], but the snapshot predates the row's
    /// `placement_changed_at`. Re-placing a sandbox does not invalidate the
    /// artifact its own pin names, so this must be spared exactly like a fresh
    /// one. `lifecycle_changed_at` defaults to the insert time, which is also
    /// after the backdated snapshot, so this pins that clock too: a producer
    /// that later wants a per-stop-cycle anchor on it has to change this case.
    StopDiskDurableBackdated,
    /// A stop-origin **full**-kind pin, durable. No producer mints this —
    /// sbx-mgr's stopped-artifact publish hard-codes `Disk` + `Stop`, so
    /// `origin = 'stop'` already implies the kind. The case exists because the
    /// predicate deliberately does not *duplicate* that producer invariant: it
    /// follows the pin, and a hard-coded `sn.type = 'disk'` would reap this row
    /// while claiming to. Sparing a shape no producer creates is the harmless
    /// direction for an exemption; reaping one it does create is not.
    StopFullDurable,
    /// A stop-origin disk pin with no catalog row at all — where a stop sits
    /// between minting the artifact id and registering its backup, and where a
    /// half-finished one stops.
    StopDiskUnregistered,
    /// A stop-origin disk pin with no catalog row of its own, alongside a
    /// **different** artifact of the same sandbox that *is* `ready` on a live
    /// location — a prior stop cycle's leftovers.
    ///
    /// The case that catches removing the artifact-id half of the identity
    /// match: another artifact of the same sandbox is durable under the pinned
    /// kind, just not under the pinned id.
    StopDiskForeignArtifactDurable,
    /// A stop-origin disk pin whose artifact id has a durable catalog row of
    /// a **different** kind. The identity match is on `(id, kind)`, and this
    /// is the only case where the kind half alone decides.
    StopDiskKindMismatchDurable,
    /// A stop-origin disk pin with no catalog row of its own, while a
    /// **different** sandbox owns a durable row under the pinned artifact id
    /// and kind. Only the fragment's sandbox correlation rejects it.
    StopDiskPinnedIdOnAnotherSandbox,
    /// A stop-origin disk pin whose upload has not terminalized.
    StopDiskCreating,
    /// A stop-origin disk pin whose `ready` snapshot names a storage location
    /// stamped `orphaned_at`. Production's orphan path nulls `snapshot_id` in
    /// the same transaction, so a real orphan would fail the `JOIN` outright;
    /// keeping the id set is what isolates the `orphaned_at IS NULL` conjunct.
    StopDiskOrphaned,
    /// A prior suspend's full artifact, durable — the wrong origin.
    SuspendFullDurable,
}

impl ArtifactPin {
    /// `(kind, origin)` of the `sandboxes` marker, or `None` for no marker.
    const fn marker(self) -> Option<(ArtifactKind, ArtifactOrigin)> {
        match self {
            Self::None | Self::NoMarkerForeignArtifactDurable => None,
            Self::StopDiskDurable
            | Self::StopDiskDurableBackdated
            | Self::StopDiskUnlocated
            | Self::StopDiskUnregistered
            | Self::StopDiskForeignArtifactDurable
            | Self::StopDiskKindMismatchDurable
            | Self::StopDiskPinnedIdOnAnotherSandbox
            | Self::StopDiskCreating
            | Self::StopDiskOrphaned => Some((ArtifactKind::Disk, ArtifactOrigin::Stop)),
            Self::StopFullDurable => Some((ArtifactKind::Full, ArtifactOrigin::Stop)),
            Self::SuspendFullDurable => Some((ArtifactKind::Full, ArtifactOrigin::Suspend)),
        }
    }

    /// `(snapshot state, storage location)` for the pinned artifact's catalog
    /// row, or `None` for a marker with no catalog row at all.
    const fn backup(self) -> Option<(SnapshotState, Location)> {
        match self {
            Self::None
            | Self::NoMarkerForeignArtifactDurable
            | Self::StopDiskUnregistered
            | Self::StopDiskForeignArtifactDurable
            | Self::StopDiskPinnedIdOnAnotherSandbox => None,
            Self::StopDiskDurable
            | Self::StopDiskDurableBackdated
            | Self::StopFullDurable
            | Self::StopDiskKindMismatchDurable
            | Self::SuspendFullDurable => Some((SnapshotState::Ready, Location::Live)),
            Self::StopDiskUnlocated => Some((SnapshotState::Ready, Location::Absent)),
            Self::StopDiskCreating => Some((SnapshotState::Creating, Location::Live)),
            Self::StopDiskOrphaned => Some((SnapshotState::Ready, Location::Orphaned)),
        }
    }

    /// Whether to stamp the row's `placement_changed_at` *after* the
    /// snapshot's `timestamp`. Both have to move together: with
    /// `placement_changed_at` left NULL, the NULL-safe form every freshness
    /// clause in `pplx_space_sandbox_portability` uses would treat any
    /// snapshot as fresh and the case could not bite.
    const fn backdates_snapshot_before_placement(self) -> bool {
        matches!(self, Self::StopDiskDurableBackdated)
    }

    /// The kind to register the pinned artifact's catalog row under, when it
    /// must differ from the marker's.
    const fn catalog_kind_override(self) -> Option<ArtifactKind> {
        match self {
            Self::StopDiskKindMismatchDurable => Some(ArtifactKind::Full),
            _ => None,
        }
    }

    /// Whether to register the pinned artifact id, durable, under another
    /// sandbox.
    const fn seeds_pinned_id_on_another_sandbox(self) -> bool {
        matches!(self, Self::StopDiskPinnedIdOnAnotherSandbox)
    }

    /// Whether to register a durable artifact that the marker does **not**
    /// name, so the identity match has something to reject.
    const fn seeds_foreign_durable_artifact(self) -> bool {
        matches!(
            self,
            Self::StopDiskForeignArtifactDurable | Self::NoMarkerForeignArtifactDurable
        )
    }
}

/// The `snapshot_storage_locations` row registered for a catalog snapshot.
#[derive(Debug, Clone, Copy)]
enum Location {
    /// A usable location.
    Live,
    /// A location stamped `orphaned_at`.
    Orphaned,
    /// No location row at all, leaving the fragment's `JOIN` nothing to match.
    Absent,
}

/// Register `location` for `snapshot_id`; [`Location::Absent`] writes nothing.
async fn insert_storage_location(
    pool: &sqlx::PgPool,
    snapshot_id: Uuid,
    location: Location,
) -> TestResult {
    let orphaned = match location {
        Location::Absent => return Ok(()),
        Location::Live => false,
        Location::Orphaned => true,
    };
    sqlx::query(
        "INSERT INTO snapshot_storage_locations (
             snapshot_id, storage_connection_string, org_id, orphaned_at
         )
         VALUES ($1, $2, $3, CASE WHEN $4 THEN NOW() ELSE NULL END)",
    )
    .bind(snapshot_id)
    .bind(format!("s3://space-test/{snapshot_id}"))
    .bind(SEED_ORG_ID)
    .bind(orphaned)
    .execute(pool)
    .await?;
    Ok(())
}

/// Point the sandbox's `current_local_artifact_*` marker at a fresh artifact
/// id and, when the variant calls for one, register that artifact's catalog
/// row plus the storage location that makes it durable.
///
/// The catalog row's identity is read back out of the marker rather than
/// passed alongside it, so the pin and the backup cannot describe different
/// artifacts — the shape the `published_state` integration test's `pin_suspend_snapshot_artifact`
/// uses for the same reason.
async fn seed_artifact_pin(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    pin: ArtifactPin,
) -> TestResult {
    if pin.seeds_foreign_durable_artifact() {
        insert_foreign_durable_artifact(pool, sandbox_id).await?;
    }
    let Some((kind, origin)) = pin.marker() else {
        return Ok(());
    };
    sqlx::query(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = $3,
                current_local_artifact_origin = $4
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .bind(kind.as_str())
    .bind(origin.as_str())
    .execute(pool)
    .await?;

    if pin.seeds_pinned_id_on_another_sandbox() {
        insert_pinned_id_on_another_sandbox(pool, sandbox_id).await?;
    }
    let Some((state, location)) = pin.backup() else {
        return Ok(());
    };
    let backdated = pin.backdates_snapshot_before_placement();
    if backdated {
        sqlx::query("UPDATE sandboxes SET placement_changed_at = NOW() WHERE id = $1")
            .bind(sandbox_id)
            .execute(pool)
            .await?;
    }
    let snapshot_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin, timestamp
         )
         SELECT $1,
                s.id,
                COALESCE($5, s.current_local_artifact_kind),
                $3,
                s.org_id,
                s.current_local_artifact_id,
                COALESCE($5, s.current_local_artifact_kind),
                s.current_local_artifact_origin,
                CASE WHEN $4 THEN NOW() - INTERVAL '1 hour' ELSE NOW() END
           FROM sandboxes s
          WHERE s.id = $2",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(state.as_str())
    .bind(backdated)
    .bind(pin.catalog_kind_override().map(ArtifactKind::as_str))
    .execute(pool)
    .await?;
    insert_storage_location(pool, snapshot_id, location).await
}

/// Register a `ready`, live-located artifact for `sandbox_id` under an
/// artifact id the sandbox's marker does not name.
///
/// Deliberately does not read the identity back out of `sandboxes`, unlike
/// [`seed_artifact_pin`]: the point is a row the pin cannot match.
async fn insert_foreign_durable_artifact(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> TestResult {
    let snapshot_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES ($1, $2, $3, $4, $5, $6, $3, $7)",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(ArtifactKind::Disk.as_str())
    .bind(SnapshotState::Ready.as_str())
    .bind(SEED_ORG_ID)
    .bind(Uuid::now_v7())
    .bind(ArtifactOrigin::Stop.as_str())
    .execute(pool)
    .await?;
    insert_storage_location(pool, snapshot_id, Location::Live).await
}

/// Register `sandbox_id`'s pinned artifact identity, `ready` on a live
/// location, as a snapshot of a different, unplaced sandbox.
async fn insert_pinned_id_on_another_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> TestResult {
    let other_id = SandboxId::new();
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_generation
         )
         VALUES ($1, $2, $3, 'stopped', 'unplaced', 'space', 1)",
    )
    .bind(other_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    let snapshot_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         SELECT $1, $2, s.current_local_artifact_kind, $3, s.org_id,
                s.current_local_artifact_id, s.current_local_artifact_kind,
                s.current_local_artifact_origin
           FROM sandboxes s
          WHERE s.id = $4",
    )
    .bind(snapshot_id)
    .bind(other_id)
    .bind(SnapshotState::Ready.as_str())
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    insert_storage_location(pool, snapshot_id, Location::Live).await
}

/// The census observation the node publishes while it still holds the
/// sandbox. Every case seeds one so the prune has something to remove.
async fn observe_sandbox_on_node(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
         VALUES ($1, $2, 'running')",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct PrunedSandboxRow {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
}

impl PrunedSandboxRow {
    /// A row the prune left alone: still `lifecycle`, still placed on `node_id`.
    fn placed(lifecycle: &str, node_id: NodeId) -> Self {
        Self {
            lifecycle: lifecycle.to_owned(),
            placement: "on_node".to_owned(),
            active_node_id: Some(node_id),
            target_node_id: Some(node_id),
        }
    }

    /// A row the `marked_deleted` arm tombstoned.
    fn tombstoned() -> Self {
        Self {
            lifecycle: "deleted".to_owned(),
            placement: "unplaced".to_owned(),
            active_node_id: None,
            target_node_id: None,
        }
    }
}

async fn pruned_sandbox_row(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> TestResult<PrunedSandboxRow> {
    Ok(sqlx::query_as(
        "SELECT lifecycle, placement, active_node_id, target_node_id
           FROM sandboxes
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn sandbox_is_observed(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM sandbox_observed_states WHERE sandbox_id = $1)",
    )
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn complete_inventory_marks_missing_space_sandbox_deleted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    // Otherwise identical, but Kubernetes-managed: the census does not own it.
    let k8s_sandbox_id = SandboxId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert_placed_sandbox(
        &db.pool,
        sandbox_id,
        node_id,
        SandboxLifecycle::Started,
        SubstrateKind::Space,
    )
    .await?;
    insert_placed_sandbox(
        &db.pool,
        k8s_sandbox_id,
        node_id,
        SandboxLifecycle::Started,
        SubstrateKind::Kubernetes,
    )
    .await?;

    let mut conn = db.pool.acquire().await?;
    prune_absent_node_sandboxes(&mut conn, node_id, &[]).await?;
    drop(conn);

    assert_eq!(
        pruned_sandbox_row(&db.pool, sandbox_id).await?,
        PrunedSandboxRow::tombstoned()
    );
    assert_eq!(
        pruned_sandbox_row(&db.pool, k8s_sandbox_id).await?,
        PrunedSandboxRow::placed("started", node_id)
    );
    Ok(())
}

/// A stopped sandbox the cluster deliberately released from its node — its
/// disk is in S3 under the artifact identity the cluster itself pinned — must
/// survive the node dropping it from the census, while a stopped sandbox
/// whose bytes were never made durable there is still reaped.
///
/// Both halves live in one fixture on purpose. Asserting only the survivor
/// would hold just as well against a prune that had stopped reaping anything,
/// and asserting only a reaped row would hold against a guard that never
/// matched.
///
/// Each conjunct has a reaped row that fails it alone, so **widening** any
/// single term breaks a nameable case: a still-`creating` upload and an orphaned storage
/// location for durability, a prior suspend's artifact for the origin, a
/// woken row for the lifecycle.
///
/// The spared rows cover the opposite direction — a term the predicate must
/// never **acquire**. A full-kind pin fails if a hard-coded `sn.type` is ever
/// added, and a snapshot backdated behind `placement_changed_at` fails if a
/// freshness anchor is. Both would otherwise be invisible here, because every
/// other case is disk-kind and freshly stamped.
#[tokio::test(flavor = "current_thread")]
async fn complete_inventory_spares_only_a_stopped_sandbox_with_a_durable_pinned_disk() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;

    struct Case {
        /// What the guard's terms see, and why the outcome follows.
        reason: &'static str,
        lifecycle: SandboxLifecycle,
        pin: ArtifactPin,
        preserved: bool,
    }

    let cases = [
        Case {
            reason: "stopped, disk pinned, exact artifact durable in S3",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskDurable,
            preserved: true,
        },
        Case {
            reason: "durable, but the snapshot predates the row's placement clock",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskDurableBackdated,
            preserved: true,
        },
        Case {
            reason: "durable under a full-kind pin: the exemption follows the pin's kind",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopFullDurable,
            preserved: true,
        },
        Case {
            reason: "no marker at all, but the sandbox owns a durable artifact",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::NoMarkerForeignArtifactDurable,
            preserved: false,
        },
        Case {
            reason: "pinned and ready, but the artifact has no storage location",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskUnlocated,
            preserved: false,
        },
        Case {
            reason: "another artifact of the same sandbox is durable, but not the pinned one",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskForeignArtifactDurable,
            preserved: false,
        },
        Case {
            reason: "the pinned artifact id is durable, but under a different kind",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskKindMismatchDurable,
            preserved: false,
        },
        Case {
            reason: "the pinned identity is durable, but on another sandbox's snapshot",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskPinnedIdOnAnotherSandbox,
            preserved: false,
        },
        Case {
            reason: "pinned, but the artifact was never registered in the catalog",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskUnregistered,
            preserved: false,
        },
        Case {
            reason: "registered and located, but the upload has not terminalized",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskCreating,
            preserved: false,
        },
        Case {
            reason: "pinned and ready, but the storage location it named is orphaned",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::StopDiskOrphaned,
            preserved: false,
        },
        Case {
            reason: "genuinely lost: a stopped sandbox with no artifact anywhere",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::None,
            preserved: false,
        },
        Case {
            reason: "stopped, but the durable artifact is a prior suspend's, not this disk",
            lifecycle: SandboxLifecycle::Stopped,
            pin: ArtifactPin::SuspendFullDurable,
            preserved: false,
        },
        Case {
            reason: "woken past the release: a started row carrying a stale stop pin",
            lifecycle: SandboxLifecycle::Started,
            pin: ArtifactPin::StopDiskDurable,
            preserved: false,
        },
    ];

    // Spared rows keep the whole `SET` list skipped, not just the lifecycle:
    // the row stays placed on the node it was released from. Releasing that
    // node is the census discount's job, not this arm's — and that discount
    // is `ResidencyEvidence::LifecycleOnly` by default, so an environment that
    // has not enabled it holds the node at the terminator's gate.
    let expected = |case: &Case| {
        if case.preserved {
            PrunedSandboxRow::placed(case.lifecycle.as_str(), node_id)
        } else {
            PrunedSandboxRow::tombstoned()
        }
    };

    let mut seeded = Vec::new();
    for case in &cases {
        let sandbox_id = SandboxId::new();
        insert_placed_sandbox(
            &db.pool,
            sandbox_id,
            node_id,
            case.lifecycle,
            SubstrateKind::Space,
        )
        .await?;
        observe_sandbox_on_node(&db.pool, sandbox_id, node_id).await?;
        seed_artifact_pin(&db.pool, sandbox_id, case.pin).await?;
        seeded.push((case, sandbox_id));
    }

    let mut conn = db.pool.acquire().await?;
    prune_absent_node_sandboxes(&mut conn, node_id, &[]).await?;
    drop(conn);

    for &(case, sandbox_id) in &seeded {
        assert_eq!(
            pruned_sandbox_row(&db.pool, sandbox_id).await?,
            expected(case),
            "{}",
            case.reason
        );
        // Every case loses its observation, spared or not. For a spared row
        // this shows the observation prune still runs for it, and that is what
        // a census-aware drain reads to discount the row: a guard that held
        // `sandbox_observed_states` too would keep the node held. For the
        // reaped rows it shows the two halves stay independent.
        assert!(
            !sandbox_is_observed(&db.pool, sandbox_id).await?,
            "{}",
            case.reason
        );
    }

    // The node keeps publishing full censuses that omit every one of these
    // rows, and from here on none of them has an observation left to prune.
    // A released row has to survive that steady state, not just the first
    // census after its release: if the arm ever came to read "absent and
    // unobserved" as lost, every exemption above would be revoked one tick
    // later.
    let mut conn = db.pool.acquire().await?;
    prune_absent_node_sandboxes(&mut conn, node_id, &[]).await?;
    drop(conn);
    for &(case, sandbox_id) in &seeded {
        assert_eq!(
            pruned_sandbox_row(&db.pool, sandbox_id).await?,
            expected(case),
            "second census: {}",
            case.reason
        );
    }
    Ok(())
}

/// Bounds both the wait for the prune to block and its completion.
const RACE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// The concurrent writer that commits while the census prune waits on a row.
#[derive(Clone, Copy, Debug)]
enum CensusRace {
    /// The row was released from its node: the stop pin naming its
    /// already-durable disk is committed.
    Release,
    /// The release commits its catalog rows and its pin together, in the
    /// transaction the prune waits on: the violation of the second producer
    /// obligation the predicate's doc warns about. Its tombstone expectation
    /// records that hazard, not a desired outcome.
    ReleaseWithCatalog,
    /// An unrelated write: the row is still an unpinned stopped sandbox.
    Touch,
}

/// Register a `ready` stop-origin disk snapshot of `sandbox_id` on a live
/// location, under an artifact id nothing pins yet.
async fn insert_unpinned_stop_disk_catalog(
    conn: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
) -> TestResult {
    let snapshot_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, org_id,
             local_artifact_id, artifact_kind, artifact_origin
         )
         VALUES ($1, $2, $3, $4, $5, $6, $3, $7)",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(ArtifactKind::Disk.as_str())
    .bind(SnapshotState::Ready.as_str())
    .bind(SEED_ORG_ID)
    .bind(Uuid::now_v7())
    .bind(ArtifactOrigin::Stop.as_str())
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)",
    )
    .bind(snapshot_id)
    .bind(format!("s3://space-test/{snapshot_id}"))
    .bind(SEED_ORG_ID)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A census omitting a stopped row reaches it while another writer holds it.
/// The `marked_deleted` arm decides on what that writer commits: a row whose
/// stop pin was committed over catalog rows committed before the census is
/// spared, while a row whose catalog rows were committed with the pin, after
/// the census's snapshot, is tombstoned like a row that is still unpinned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_inventory_decides_on_the_row_committed_while_it_waits() -> TestResult {
    let db = pg_test_db_with_max_connections(4).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;

    let cases = [
        (
            CensusRace::Release,
            PrunedSandboxRow::placed(SandboxLifecycle::Stopped.as_str(), node_id),
        ),
        (
            CensusRace::ReleaseWithCatalog,
            PrunedSandboxRow::tombstoned(),
        ),
        (CensusRace::Touch, PrunedSandboxRow::tombstoned()),
    ];
    let mut mismatches = Vec::new();
    for (race, expected) in cases {
        let sandbox_id = SandboxId::new();
        insert_placed_sandbox(
            pool,
            sandbox_id,
            node_id,
            SandboxLifecycle::Stopped,
            SubstrateKind::Space,
        )
        .await?;
        // The catalog a release commits before its pin.
        if !matches!(race, CensusRace::ReleaseWithCatalog) {
            let mut conn = pool.acquire().await?;
            insert_unpinned_stop_disk_catalog(&mut conn, sandbox_id).await?;
        }

        let mut holder = pool.begin().await?;
        let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *holder)
            .await?;
        if matches!(race, CensusRace::ReleaseWithCatalog) {
            insert_unpinned_stop_disk_catalog(&mut holder, sandbox_id).await?;
        }
        match race {
            CensusRace::Release | CensusRace::ReleaseWithCatalog => sqlx::query(
                "UPDATE sandboxes s
                    SET current_local_artifact_id = sn.local_artifact_id,
                        current_local_artifact_kind = sn.artifact_kind,
                        current_local_artifact_origin = sn.artifact_origin
                   FROM snapshots sn
                  WHERE s.id = $1
                    AND sn.sandbox_id = s.id",
            ),
            CensusRace::Touch => {
                sqlx::query("UPDATE sandboxes SET updated_at = NOW() WHERE id = $1")
            }
        }
        .bind(sandbox_id)
        .execute(&mut *holder)
        .await?;

        let mut prune_conn = pool.acquire().await?;
        let waiter_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *prune_conn)
            .await?;
        let prune = tokio::spawn(async move {
            prune_absent_node_sandboxes(&mut prune_conn, node_id, &[]).await?;
            TestResult::Ok(())
        });
        tokio::time::timeout(RACE_BUDGET, async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                    .bind(holder_pid)
                    .bind(waiter_pid)
                    .fetch_one(pool)
                    .await?;
                if blocked {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            TestResult::Ok(())
        })
        .await
        .map_err(|_| format!("{race:?}: the prune never waited on the holder"))??;
        holder.commit().await?;
        tokio::time::timeout(RACE_BUDGET, prune)
            .await
            .map_err(|_| format!("{race:?}: the prune did not finish"))???;

        let actual = pruned_sandbox_row(pool, sandbox_id).await?;
        if actual != expected {
            mismatches.push(format!("{race:?}: {actual:?}, expected {expected:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
