//! Postgres behaviors for the disk-only restore-source selector an evacuated
//! `stopped` sandbox resolves through
//! (`K8sStore::selected_stopped_restore_storage_uri`).
//!
//! A sibling module rather than more lines in the parent: the parent is
//! already very large, and these behaviors share nothing with it but the
//! fixture. They take `&PostgresStore` and are driven from
//! `k8s_store_postgres_behaviors`, so they run against the same database and
//! mint no second container.

use pplx_space_common_core::types::{LocalArtifactId, SandboxId, SnapshotId};

use super::*;
use pplx_space_db_test_fixtures::SEED_ORG_ID;
use pplx_space_sandbox_portability::snapshot_is_live_restore_source;

/// Register a `disk` snapshot with a usable storage location, mirroring what
/// `BeginArtifactBackup(ArtifactKind::Disk)` plus a `READY`
/// `FinalizeArtifactBackup` leave in the catalog. `local_artifact_id` carries
/// the artifact identity the node minted; `None` models a row that predates the
/// identity columns.
///
/// `age_seconds` counts *backwards* from `NOW()`, so a larger value is older.
/// Note that the sibling `insert_ready_full_snapshot_relative_to_lifecycle`
/// takes an offset *forward* from `lifecycle_changed_at`, where larger is
/// newer — the same argument position runs in opposite directions between the
/// two helpers.
///
/// The shared `pplx_space_db_test_fixtures::publish_ready_disk_artifact` covers
/// the "artifact is durable" shape; this one exists because these behaviors
/// have to vary `state`, the timestamp, and the identity *independently of the
/// pin* — which is the whole subject matter.
async fn insert_disk_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    uri: &str,
    state: &str,
    age_seconds: i32,
    local_artifact_id: Option<LocalArtifactId>,
) -> TestResult<SnapshotId> {
    insert_disk_snapshot_with_id(
        pool,
        sandbox_id,
        SnapshotId::new(),
        uri,
        state,
        age_seconds,
        local_artifact_id,
    )
    .await
}

/// As [`insert_disk_snapshot`], with the `snapshots.id` chosen by the caller.
/// Only the tiebreak assertion needs that: it seeds two rows at one timestamp
/// and has to know which id is the larger.
#[allow(clippy::too_many_arguments)]
async fn insert_disk_snapshot_with_id(
    pool: &PgPool,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
    uri: &str,
    state: &str,
    age_seconds: i32,
    local_artifact_id: Option<LocalArtifactId>,
) -> TestResult<SnapshotId> {
    let artifact_kind = local_artifact_id.map(|_| "disk");
    let artifact_origin = local_artifact_id.map(|_| "stop");
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, 'disk', $3,
                 NOW() - ($4 * INTERVAL '1 second'),
                 $5, $6, $7, $8)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(state)
    .bind(age_seconds)
    .bind(SEED_ORG_ID)
    .bind(local_artifact_id)
    .bind(artifact_kind)
    .bind(artifact_origin)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(snapshot_id)
    .bind(uri)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

async fn set_artifact_pin(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: Option<LocalArtifactId>,
    kind: Option<&str>,
) -> TestResult {
    // Origin tracks kind: `publish_stopped_artifact` mints `disk`/`stop`, and
    // the suspend writers mint `full`/`suspend`. Deriving it here keeps the
    // invalid `(Some(id), None)` combination out of every call site.
    let origin = kind.map(|kind| if kind == "disk" { "stop" } else { "suspend" });
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = $3,
                current_local_artifact_origin = $4
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .bind(kind)
    .bind(origin)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_lifecycle(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox_id)
        .bind(lifecycle)
        .execute(pool)
        .await?;
    Ok(())
}

async fn selected_stopped_uri(
    fixture: &PostgresStore,
    sandbox_id: SandboxId,
) -> TestResult<Option<String>> {
    let sandbox = fixture.store.require_sandbox(sandbox_id).await?;
    Ok(fixture
        .store
        .selected_stopped_restore_storage_uri(&sandbox)
        .await?)
}

/// The operator-visible property: a stopped sandbox is restartable off its
/// node exactly when the bytes that *are* its current rootfs are durable and
/// reachable. Not "an upload for it started", not "some disk image of it
/// exists" — the pinned one, finished, with a live location.
///
/// Every negative below is one mutation away from the positive, so none of
/// them can pass because the fixture was incomplete.
pub(super) async fn stopped_disk_selector_requires_the_pinned_ready_disk_artifact(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let artifact_id = LocalArtifactId::new();
    let uri = "s3://space-test/stopped-disk-evacuated?region=us-east-1";
    insert_sandbox(&fixture.pool, sandbox_id, "stopped").await?;
    set_artifact_pin(&fixture.pool, sandbox_id, Some(artifact_id), Some("disk")).await?;
    let snapshot_id = insert_disk_snapshot(
        &fixture.pool,
        sandbox_id,
        uri,
        "creating",
        60,
        Some(artifact_id),
    )
    .await?;

    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?,
        None,
        "an in-flight disk upload is not yet a restore source"
    );

    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(snapshot_id)
        .execute(&fixture.pool)
        .await?;
    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?.as_deref(),
        Some(uri),
        "a ready pinned disk artifact with a live location is the restore source"
    );

    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id = $1"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?,
        None,
        "an orphaned location must not be handed to a restore"
    );
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations SET orphaned_at = NULL WHERE snapshot_id = $1"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;

    // The lifecycle filter is load-bearing, not decoration. `suspended` /
    // `restoring` belong to the suspend selector, and for `started` / `paused`
    // the live copy on the source node is canonical — the reason the
    // portability module gives for treating any published snapshot of them as
    // stale.
    for foreign_lifecycle in ["suspended", "restoring", "started", "paused"] {
        set_lifecycle(&fixture.pool, sandbox_id, foreign_lifecycle).await?;
        assert_eq!(
            selected_stopped_uri(fixture, sandbox_id).await?,
            None,
            "{foreign_lifecycle} must not resolve through the stopped disk selector"
        );
    }
    set_lifecycle(&fixture.pool, sandbox_id, "stopped").await?;

    // No markerless legacy branch. A sandbox that lost its pin, and a
    // snapshot that carries no artifact identity, are each unselectable —
    // there is no timestamp rule that would substitute for the identity.
    set_artifact_pin(&fixture.pool, sandbox_id, None, None).await?;
    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?,
        None,
        "a sandbox with no artifact pin has nothing to resolve the artifact by"
    );
    set_artifact_pin(&fixture.pool, sandbox_id, Some(artifact_id), Some("disk")).await?;
    sqlx::query(sql!(
        "UPDATE snapshots
            SET local_artifact_id = NULL, artifact_kind = NULL, artifact_origin = NULL
          WHERE id = $1"
    ))
    .bind(snapshot_id)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?,
        None,
        "a markerless disk snapshot is not proof it holds this sandbox's current rootfs"
    );
    sqlx::query(sql!(
        "UPDATE snapshots
            SET local_artifact_id = $2, artifact_kind = 'disk', artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(snapshot_id)
    .bind(artifact_id)
    .execute(&fixture.pool)
    .await?;

    // Identity, not freshness. Re-placement bumps `placement_changed_at`,
    // which is what makes a *suspend* snapshot stale; the evacuated rootfs is
    // the same bytes regardless of where the sandbox is placed next.
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?.as_deref(),
        Some(uri),
        "a re-placement must not strand the evacuated rootfs the pin names"
    );

    // A retried backup of one artifact mints a fresh `snapshots` row per
    // attempt under the same identity, so the pin can match several rows.
    // Newest registered location wins.
    let newer_uri = "s3://space-test/stopped-disk-retry?region=us-east-1";
    insert_disk_snapshot(
        &fixture.pool,
        sandbox_id,
        newer_uri,
        "ready",
        1,
        Some(artifact_id),
    )
    .await?;
    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?.as_deref(),
        Some(newer_uri),
        "the newest registered copy of the pinned artifact is the one to load"
    );

    // `ORDER BY sn.timestamp DESC` alone leaves the pick unspecified when two
    // attempts of one artifact register within a single statement timestamp,
    // which is why the selector adds `sn.id DESC`. Seeding both rows at the
    // *same* instant is the only way that clause decides anything at all: with
    // the distinct timestamps used above, it is inert.
    //
    // What this establishes, precisely: inverting the clause to `sn.id ASC`
    // fails here, so the ordering is read and honored. It does **not** prove
    // the clause is load-bearing against deletion — measured, removing it
    // entirely still passes, because Postgres's unspecified order for a tie
    // happens to return the same row for this plan. No test can close that
    // gap; "unspecified" is free to coincide with the intended answer. The
    // clause is what makes the choice guaranteed rather than incidental, and
    // that guarantee is a property of the SQL, not of any fixture.
    //
    // UUIDv7 is time-ordered, so `Uuid::now_v7()` twice gives a known
    // low/high pair.
    let tie_sandbox = SandboxId::new();
    let tie_artifact_id = LocalArtifactId::new();
    let low_id = SnapshotId::new();
    let high_id = SnapshotId::new();
    assert!(low_id < high_id, "UUIDv7 must order by mint time");
    insert_sandbox(&fixture.pool, tie_sandbox, "stopped").await?;
    set_artifact_pin(
        &fixture.pool,
        tie_sandbox,
        Some(tie_artifact_id),
        Some("disk"),
    )
    .await?;
    // Inserted high-id first so insertion order cannot stand in for the
    // ordering under test.
    insert_disk_snapshot_with_id(
        &fixture.pool,
        tie_sandbox,
        high_id,
        "s3://space-test/stopped-disk-tie-high?region=us-east-1",
        "ready",
        30,
        Some(tie_artifact_id),
    )
    .await?;
    insert_disk_snapshot_with_id(
        &fixture.pool,
        tie_sandbox,
        low_id,
        "s3://space-test/stopped-disk-tie-low?region=us-east-1",
        "ready",
        30,
        Some(tie_artifact_id),
    )
    .await?;
    // Each insert commits separately, so their `NOW()` values differ by
    // microseconds — enough for `timestamp DESC` to decide and leave the
    // tiebreak untested. Collapse them onto one instant explicitly; the guard
    // below is what caught this when the fixture relied on `NOW()`.
    sqlx::query(sql!(
        "UPDATE snapshots
            SET timestamp = (SELECT MIN(timestamp) FROM snapshots WHERE sandbox_id = $1)
          WHERE sandbox_id = $1"
    ))
    .bind(tie_sandbox)
    .execute(&fixture.pool)
    .await?;
    let tied: (i64,) = sqlx::query_as(sql!(
        "SELECT COUNT(DISTINCT timestamp) FROM snapshots WHERE sandbox_id = $1"
    ))
    .bind(tie_sandbox)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        tied.0, 1,
        "the two rows must share one timestamp, or this asserts the timestamp \
         ordering again rather than the tiebreak"
    );
    assert_eq!(
        selected_stopped_uri(fixture, tie_sandbox).await?.as_deref(),
        Some("s3://space-test/stopped-disk-tie-high?region=us-east-1"),
        "on a timestamp tie the higher snapshot id must win, deterministically"
    );

    Ok(())
}

/// The two exclusions the suspend selector could not express, from the other
/// side: a `full` artifact carries VM memory a stopped sandbox does not have,
/// and a disk image belonging to some *other* artifact of the same sandbox is
/// not its current rootfs.
pub(super) async fn stopped_disk_selector_ignores_a_full_artifact_and_a_foreign_disk_artifact(
    fixture: &PostgresStore,
) -> TestResult {
    // Only a `full` artifact: a suspended-shaped catalog on a stopped row.
    // Everything else about it selects — the pin matches the snapshot's
    // identity, the row is ready, its location is live — so `sn.type` is the
    // single conjunct standing between this fixture and a hit.
    let full_only = SandboxId::new();
    let full_artifact_id = LocalArtifactId::new();
    insert_sandbox(&fixture.pool, full_only, "stopped").await?;
    set_artifact_pin(
        &fixture.pool,
        full_only,
        Some(full_artifact_id),
        Some("full"),
    )
    .await?;
    let full_snapshot_id = insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        full_only,
        "s3://space-test/stopped-with-full-artifact?region=us-east-1",
        1,
        Some(full_artifact_id),
    )
    .await?;
    assert_eq!(
        selected_stopped_uri(fixture, full_only).await?,
        None,
        "a full artifact cannot restore a sandbox that has no VM memory to restore"
    );

    // Move that one conjunct and nothing else. `artifact_kind` stays `full` on
    // both sides, so the pin still matches; only `snapshots.type` changes.
    // Production never writes this combination — `BeginArtifactBackup` derives
    // `type` from `artifact_kind` — and that is the point: the mutation
    // isolates the filter under test instead of modelling a real row, which is
    // what makes the `None` above attributable to `sn.type` alone.
    sqlx::query(sql!("UPDATE snapshots SET type = 'disk' WHERE id = $1"))
        .bind(full_snapshot_id)
        .execute(&fixture.pool)
        .await?;
    assert_eq!(
        selected_stopped_uri(fixture, full_only).await?.as_deref(),
        Some("s3://space-test/stopped-with-full-artifact?region=us-east-1"),
        "flipping only the snapshot kind must make the same row selectable, or the \
         assertion above proved something other than the kind filter"
    );

    // A disk artifact of this sandbox that the pin has moved past — the shape
    // a superseded stop artifact and a user-taken disk snapshot both leave.
    // The selector rejects it on the pin alone, never on `artifact_origin`, so
    // one rule covers both and loading stale bytes needs no origin allowlist
    // to prevent.
    let foreign = SandboxId::new();
    let pinned_artifact_id = Uuid::now_v7();
    let foreign_artifact_id = LocalArtifactId::new();
    let foreign_uri = "s3://space-test/stopped-foreign-disk?region=us-east-1";
    insert_sandbox(&fixture.pool, foreign, "stopped").await?;
    set_artifact_pin(
        &fixture.pool,
        foreign,
        Some(LocalArtifactId::from_uuid(pinned_artifact_id)),
        Some("disk"),
    )
    .await?;
    insert_disk_snapshot(
        &fixture.pool,
        foreign,
        foreign_uri,
        "ready",
        1,
        Some(foreign_artifact_id),
    )
    .await?;
    assert_eq!(
        selected_stopped_uri(fixture, foreign).await?,
        None,
        "a disk artifact the sandbox does not pin is not its current rootfs"
    );
    set_artifact_pin(
        &fixture.pool,
        foreign,
        Some(foreign_artifact_id),
        Some("disk"),
    )
    .await?;
    assert_eq!(
        selected_stopped_uri(fixture, foreign).await?.as_deref(),
        Some(foreign_uri),
        "moving the pin onto that artifact is what makes it selectable"
    );

    Ok(())
}

/// The selector and the retention sweeper's never-prune-a-live-restore-source
/// guard agree on which row is the live restore source, because both are built
/// from `snapshot_is_current_local_artifact`.
///
/// Read what this does and does not establish. It evaluates the **guard
/// expression** against the row the selector picked; it does not exercise
/// `list_candidates` or `claim_prune`, and it is not what makes a disk row
/// unprunable today — those two filter `type = 'full'`, so a disk row is
/// outside the candidate set before the guard is reached. What this pins is
/// the agreement itself, which is what keeps the sweep safe if that `'full'`
/// narrowing is ever lifted.
///
/// The second assertion is the non-vacuity control: a guard that answered TRUE
/// for every row would satisfy the first one on its own.
pub(super) async fn stopped_disk_selector_agrees_with_the_retention_never_prune_guard(
    fixture: &PostgresStore,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    let pinned_artifact_id = LocalArtifactId::new();
    let stale_artifact_id = LocalArtifactId::new();
    let pinned_uri = "s3://space-test/stopped-guard-pinned?region=us-east-1";
    let stale_uri = "s3://space-test/stopped-guard-stale?region=us-east-1";
    insert_sandbox(&fixture.pool, sandbox_id, "stopped").await?;
    set_artifact_pin(
        &fixture.pool,
        sandbox_id,
        Some(pinned_artifact_id),
        Some("disk"),
    )
    .await?;
    // The stale row is *newer*, so ordering alone would pick it. Only the pin
    // separates them, which is exactly the agreement under test.
    let pinned_snapshot_id = insert_disk_snapshot(
        &fixture.pool,
        sandbox_id,
        pinned_uri,
        "ready",
        60,
        Some(pinned_artifact_id),
    )
    .await?;
    let stale_snapshot_id = insert_disk_snapshot(
        &fixture.pool,
        sandbox_id,
        stale_uri,
        "ready",
        1,
        Some(stale_artifact_id),
    )
    .await?;

    assert_eq!(
        selected_stopped_uri(fixture, sandbox_id).await?.as_deref(),
        Some(pinned_uri),
        "the pin, not the timestamp, decides which disk artifact restore loads"
    );

    let guard = snapshot_is_live_restore_source("s", "sn");
    let guard_query = format!(
        "SELECT {guard}
           FROM sandboxes s
           JOIN snapshots sn ON sn.sandbox_id = s.id
          WHERE s.id = $1
            AND sn.id = $2"
    );
    let guards_pinned: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(guard_query.as_str())))
        .bind(sandbox_id)
        .bind(pinned_snapshot_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert!(
        guards_pinned,
        "the retention guard must shield the disk row the stopped selector loads"
    );
    let guards_stale: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(guard_query.as_str())))
        .bind(sandbox_id)
        .bind(stale_snapshot_id)
        .fetch_one(&fixture.pool)
        .await?;
    assert!(
        !guards_stale,
        "the guard must not shield every disk row, or agreeing with it proves nothing"
    );

    Ok(())
}
