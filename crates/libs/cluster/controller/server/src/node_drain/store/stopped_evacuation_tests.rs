//! Postgres-backed coverage for evacuating a `stopped` resident off a draining
//! node.
//!
//! The property every case here serves is one sentence: **a user's stopped
//! sandbox survives its node being drained, and the node stops being parked by
//! it.** The steps between are claim, publish, prove durable, release the node,
//! re-place — and the middle one is the drain's only check that the bytes exist
//! off the node before the node is terminated with the rootfs on it, because
//! nothing on the node marks a `stopped` rootfs as the only copy.
//!
//! So the durability cases below are written the way a data-loss guard has to
//! be: each near-miss is a *possible* yes turned into a no by exactly one field,
//! seeded alongside a real yes so no assertion holds vacuously.

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// A worker node in some drain state. Tests vary the two columns the claim
/// guards on and nothing else.
async fn insert_node(
    pool: &PgPool,
    drain_phase: Option<&str>,
    drain_mode: &str,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode,
                            drain_started_at)
         VALUES ($1, '10.0.0.42'::inet, 'worker', 'k8s', 'registered', $2, $3, NOW())"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(drain_mode)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn forced_draining_node(pool: &PgPool) -> TestResult<NodeId> {
    insert_node(pool, Some(PHASE_DRAINING), "forced").await
}

/// A resident sandbox in `lifecycle`, placed long enough ago that the census
/// settle window is never what a case is measuring.
async fn insert_sandbox(
    pool: &PgPool,
    node_id: Option<NodeId>,
    lifecycle: &str,
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
             $3, 'on_node', 'k8s', $2, $2, 1, NOW() - make_interval(hours => 4)
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn set_current_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    kind: ArtifactKind,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = $3,
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .bind(kind.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

/// One catalog row for a backup attempt. `orphaned` models a storage location
/// whose bytes have been reclaimed — a `ready` snapshot with nothing left to
/// restore from.
async fn insert_backup(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: LocalArtifactId,
    kind: ArtifactKind,
    snapshot_type: &str,
    state: &str,
    orphaned: bool,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp,
                                local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, $3, $4, NOW(), $5, $6, 'stop')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(snapshot_type)
    .bind(state)
    .bind(artifact_id)
    .bind(kind.as_str())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string,
                                                 orphaned_at)
         VALUES ($1, $2, CASE WHEN $3 THEN NOW() ELSE NULL END)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .bind(orphaned)
    .execute(pool)
    .await?;
    Ok(())
}

async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar::<_, String>(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn marker_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT evacuation_restore_target FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn disk_backup_ready(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    let ready = current_disk_artifact_backup_ready("s");
    let query = format!("SELECT {ready} FROM sandboxes s WHERE s.id = $1");
    Ok(
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn restore_pending(pool: &PgPool) -> TestResult<Vec<(SandboxId, String)>> {
    let query = restore_pending_evacuations_query(true);
    Ok(
        sqlx::query_as::<_, (SandboxId, String)>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .fetch_all(pool)
            .await?,
    )
}

/// Claim with new evacuations enabled — what a cluster that has opted in does.
/// The disabled case has its own test, since it is the one whose behaviour
/// differs between opening and resuming.
async fn claim(
    pool: &PgPool,
    sandbox: SandboxId,
    node: NodeId,
) -> TestResult<StoppedEvacuationClaim> {
    claim_stopped_evacuation(pool, sandbox, node, true)
        .await
        .map_err(Into::into)
}

/// Claim first, then record the artifact — the order production uses, since
/// [`claim_stopped_evacuation`] refuses to open an evacuation on a row that
/// already carries a pin. A fixture that pins first is asserting against a
/// state the claim guard exists to reject.
async fn claim_and_publish(
    pool: &PgPool,
    sandbox: SandboxId,
    node: NodeId,
    pin: LocalArtifactId,
    state: &str,
) -> TestResult {
    claim(pool, sandbox, node).await?;
    set_current_artifact(pool, sandbox, pin, ArtifactKind::Disk).await?;
    insert_backup(pool, sandbox, pin, ArtifactKind::Disk, "disk", state, false).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Enablement: a cluster that has not opted in must not see a stopped resident
// in the evacuation queue at all.
// ---------------------------------------------------------------------------

/// The operator-visible property: turning the feature off leaves a stopped
/// resident exactly as it is today — parked, not half-evacuated — while the
/// evacuable residents on the same node keep being evacuated.
///
/// Both directions on one fixture, because the failure worth catching is the
/// filter being wired to the wrong flag or dropped entirely, and a
/// single-direction assertion cannot tell "correctly withheld" from "the query
/// returns nothing".
#[tokio::test(flavor = "current_thread")]
async fn the_evacuation_queue_offers_a_stopped_resident_only_where_it_is_enabled() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let started = insert_sandbox(&db.pool, Some(node_id), "started").await?;
    let stopped = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;

    let disabled =
        list_evacuatable_sandbox_rows(&db.pool, node_id, ResidencyEvidence::LifecycleOnly, false)
            .await?;
    let disabled_ids: Vec<SandboxId> = disabled.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        disabled_ids,
        vec![started],
        "with the feature off the queue must still carry the started resident and must not \
         carry the stopped one"
    );

    let enabled =
        list_evacuatable_sandbox_rows(&db.pool, node_id, ResidencyEvidence::LifecycleOnly, true)
            .await?;
    let mut enabled_ids: Vec<SandboxId> = enabled.iter().map(|(id, _)| *id).collect();
    enabled_ids.sort();
    let mut want = vec![started, stopped];
    want.sort();
    assert_eq!(
        enabled_ids, want,
        "with the feature on the stopped resident must join the queue"
    );
    assert!(
        enabled
            .iter()
            .any(|(id, lifecycle)| *id == stopped && *lifecycle == SandboxLifecycle::Stopped),
        "the stopped resident must be listed under its own lifecycle, so the reconciler \
         dispatches it to the disk path rather than the suspend one"
    );
    Ok(())
}

#[test]
fn the_enabled_subset_narrows_only_stopped() {
    assert_eq!(
        enabled_evacuable_lifecycles(true),
        super::super::EVACUABLE_LIFECYCLES,
        "with the feature on, the enabled subset is the whole vocabulary"
    );
    assert_eq!(
        enabled_evacuable_lifecycles(false),
        &[SandboxLifecycle::Started, SandboxLifecycle::Paused],
        "with the feature off, only `Stopped` is withheld — withholding a live lifecycle \
         would silently stop evacuating VMs, which is what pins the two constants against \
         each other rather than letting the disabled one drift"
    );
}

// ---------------------------------------------------------------------------
// The claim.
// ---------------------------------------------------------------------------

/// The claim's non-obvious hazard: `sandboxes_clear_evacuation_restore_target`
/// is a `BEFORE UPDATE` trigger that nulls the marker, and the claim is an
/// `UPDATE` that sets it. It survives only because the trigger fires on a
/// lifecycle *change* and this write does not make one.
///
/// Asserted directly rather than inferred from a later step, because if the
/// trigger did eat the marker every downstream step would still "work" — the
/// claim would just be re-taken every tick and the sandbox would never be
/// re-placed, which is a silent stall rather than a failure.
#[tokio::test(flavor = "current_thread")]
async fn claiming_a_stopped_resident_leaves_a_marker_the_trigger_does_not_eat() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;

    assert_eq!(
        claim(&db.pool, sandbox, node_id).await?,
        StoppedEvacuationClaim::Claimed,
        "a stopped resident of a force-draining node must be claimable, and the first claim \
         must report itself as the one that opened the evacuation"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?.as_deref(),
        Some("stopped"),
        "the marker must survive the statement that wrote it"
    );

    assert_eq!(
        claim(&db.pool, sandbox, node_id).await?,
        StoppedEvacuationClaim::Resumed,
        "re-claiming an evacuation this reconciler already owns must be a no-op rather than \
         a conflict — every tick after the first re-enters through it — and must be \
         distinguishable from the first, so the once-per-evacuation event stays once"
    );
    Ok(())
}

/// A user starting their sandbox mid-evacuation takes it back. The reconciler
/// writes nothing to make that happen; the lifecycle change does, through the
/// marker-clearing trigger.
#[tokio::test(flavor = "current_thread")]
async fn a_user_takeover_ends_the_evacuation() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, sandbox, node_id).await?;
    // Assert the marker is *there* before asserting it goes away. Without this
    // the `None` below cannot tell "the trigger dropped it" from "it was never
    // written" — and that is not hypothetical: while the claim was briefly
    // writing NULL instead of `'stopped'`, this test stayed green while the
    // test that does assert the write failed.
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        Some("stopped".to_owned()),
        "fixture precondition: the claim wrote the marker, so the assertion below is about \
         the trigger clearing it rather than about it never existing"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'starting' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;

    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        None,
        "starting a stopped sandbox must drop the evacuation marker, so the restore sweep \
         stops trying to place a sandbox its owner has taken back"
    );
    Ok(())
}

/// A claimed stopped sandbox that gets quarantined loses its marker on the way
/// in, so quarantine recovery can never turn it into a `suspended` row still
/// marked `'stopped'`.
///
/// That recovery moves `quarantined` → `suspended` with no guard on the marker,
/// and the trigger keeps a marker on any move *into* `suspended`. A row arriving
/// there marked `'stopped'` would never be listed by the restore sweep — its
/// suspend arm excludes the `'stopped'` marker and its stopped arm excludes the
/// `suspended` lifecycle — so nothing would re-place it. The recovery
/// only ever acts on a `quarantined` row, so what rules that out is the marker
/// being gone by the time the row is quarantined. This asserts that link, and
/// then runs the recovery's own lifecycle move to show the marker stays gone.
#[tokio::test(flavor = "current_thread")]
async fn a_quarantined_stopped_evacuation_cannot_be_recovered_into_a_marked_suspended_row()
-> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, sandbox, node_id).await?;
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        Some("stopped".to_owned()),
        "fixture precondition: the claim wrote the marker, so the assertions below are \
         about the trigger clearing it rather than about it never existing"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'quarantined' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        None,
        "quarantining a claimed stopped sandbox must drop the evacuation marker"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended', placement = 'unplaced',
             active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        (
            lifecycle_of(&db.pool, sandbox).await?,
            marker_of(&db.pool, sandbox).await?
        ),
        ("suspended".to_owned(), None),
        "the recovered row must be an ordinary unmarked suspended sandbox"
    );
    Ok(())
}

/// Three ways the claim must refuse, next to the one way it must accept. The
/// accept case is what makes the refusals mean something: without it a claim
/// that always returned `false` would pass every refusal assertion.
#[tokio::test(flavor = "current_thread")]
async fn the_claim_refuses_anything_that_is_not_a_stopped_resident_of_a_forced_drain() -> TestResult
{
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let forced = forced_draining_node(&db.pool).await?;
    let gentle = insert_node(&db.pool, Some(PHASE_DRAINING), "gentle").await?;
    let healthy = insert_node(&db.pool, None, "gentle").await?;

    let on_gentle = insert_sandbox(&db.pool, Some(gentle), "stopped").await?;
    let on_healthy = insert_sandbox(&db.pool, Some(healthy), "stopped").await?;
    let wrong_lifecycle = insert_sandbox(&db.pool, Some(forced), "started").await?;
    let claimable = insert_sandbox(&db.pool, Some(forced), "stopped").await?;

    for (sandbox, node, why) in [
        (
            on_gentle,
            gentle,
            "a gentle drain promises not to touch residents",
        ),
        (
            on_healthy,
            healthy,
            "a node that is not draining has nothing to evacuate",
        ),
        (
            wrong_lifecycle,
            forced,
            "a started resident belongs to the suspend pipeline, not this one",
        ),
    ] {
        assert_eq!(
            claim(&db.pool, sandbox, node).await?,
            StoppedEvacuationClaim::Refused,
            "claim must refuse: {why}"
        );
        assert_eq!(
            marker_of(&db.pool, sandbox).await?,
            None,
            "a refused claim must leave no marker behind ({why})"
        );
    }

    assert_eq!(
        claim(&db.pool, claimable, forced).await?,
        StoppedEvacuationClaim::Claimed,
        "the one shape the claim exists for must still be accepted"
    );
    Ok(())
}

/// A claim guarded on the wrong node must not move a sandbox that belongs to a
/// different one. Separate from the case above because the failure is
/// different in kind: not "refused something it should have", but "evacuated
/// someone else's resident off a node it is not on".
#[tokio::test(flavor = "current_thread")]
async fn the_claim_refuses_a_sandbox_resident_on_another_node() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let draining = forced_draining_node(&db.pool).await?;
    let elsewhere = insert_node(&db.pool, None, "gentle").await?;
    let sandbox = insert_sandbox(&db.pool, Some(elsewhere), "stopped").await?;

    assert_eq!(
        claim(&db.pool, sandbox, draining).await?,
        StoppedEvacuationClaim::Refused,
        "the draining node must not claim a sandbox resident on another node"
    );
    assert_eq!(marker_of(&db.pool, sandbox).await?, None);
    Ok(())
}

// ---------------------------------------------------------------------------
// Durability — the gate that authorizes destroying the user's disk.
// ---------------------------------------------------------------------------

/// Every near-miss the durability gate has to reject, each one a *possible*
/// yes: the fixture always carries a pinned artifact and at least one catalog
/// row, so "no" is a decision rather than an absence. The final case flips the
/// one field that should make it a yes, which is what proves the earlier
/// rejections were not vacuous.
#[tokio::test(flavor = "current_thread")]
async fn the_disk_is_only_durable_for_the_exact_pinned_artifact() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();

    // 1. Pinned, upload still running.
    let uploading = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    set_current_artifact(&db.pool, uploading, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        uploading,
        pin,
        ArtifactKind::Disk,
        "disk",
        "creating",
        false,
    )
    .await?;

    // 2. A ready disk backup, but of a *different* artifact — a prior stop's
    //    bytes, which do not describe the rootfs this drain is destroying.
    let stale = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    set_current_artifact(&db.pool, stale, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        stale,
        LocalArtifactId::new(),
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;

    // 3. Ready and correctly pinned, but the catalog row is a full snapshot —
    //    a suspend artifact, which the disk restore selector will not read.
    let wrong_kind = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    set_current_artifact(&db.pool, wrong_kind, pin, ArtifactKind::Full).await?;
    insert_backup(
        &db.pool,
        wrong_kind,
        pin,
        ArtifactKind::Full,
        "full",
        "ready",
        false,
    )
    .await?;

    // 4. Ready, pinned, disk — but its only storage location has been
    //    orphaned, so there is nothing left in S3 to restore from.
    let orphaned = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    set_current_artifact(&db.pool, orphaned, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        orphaned,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        true,
    )
    .await?;

    // 5. No pin at all: the publish has not happened yet.
    let unpinned = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    insert_backup(
        &db.pool,
        unpinned,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;

    for (sandbox, why) in [
        (uploading, "the upload has not finalized"),
        (stale, "the ready backup is of a different artifact"),
        (wrong_kind, "the ready backup is a full snapshot"),
        (orphaned, "the ready backup's storage location is orphaned"),
        (unpinned, "the sandbox carries no artifact pin"),
    ] {
        assert!(
            !disk_backup_ready(&db.pool, sandbox).await?,
            "the drain must not destroy a rootfs when {why}"
        );
    }

    let durable = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    set_current_artifact(&db.pool, durable, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        durable,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    assert!(
        disk_backup_ready(&db.pool, durable).await?,
        "a ready, non-orphaned disk backup of the pinned artifact is exactly what releases \
         the node — if this is false the gate can never open and every drain parks"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Re-placement.
// ---------------------------------------------------------------------------

/// An evacuated stopped sandbox joins the cluster-wide re-placement sweep, and
/// only once it has actually left its node. The `active_node_id IS NULL` term
/// is the whole difference between "durable, still resident" and "durable,
/// nowhere" — re-placing the first would give the sandbox two homes.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_evacuation_is_re_placed_only_after_its_local_copy_is_gone() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();

    // Durable, but the teardown has not run: still pinned to its node.
    let resident = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, resident, node_id, pin, "ready").await?;

    // Torn down but not yet durable: nowhere, and not restorable either.
    let unpublished = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, unpublished, node_id).await?;
    set_current_artifact(&db.pool, unpublished, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        unpublished,
        pin,
        ArtifactKind::Disk,
        "disk",
        "creating",
        false,
    )
    .await?;
    clear_placement(&db.pool, unpublished).await?;

    // Durable and torn down: the one the sweep owns.
    let evacuated = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, evacuated, node_id, pin, "ready").await?;
    clear_placement(&db.pool, evacuated).await?;

    let pending = restore_pending(&db.pool).await?;
    assert_eq!(
        pending,
        vec![(evacuated, "stopped".to_string())],
        "the sweep must place exactly the sandbox whose bytes are durable and whose node \
         no longer holds it"
    );
    Ok(())
}

/// What the drain's teardown does to the cluster row, applied directly: the
/// placement clear that `forget_pod_tracking` performs.
async fn clear_placement(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'unplaced', active_node_id = NULL, target_node_id = NULL
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// The suspend-side population must be unaffected by the arm added beside it.
/// Its readiness test is the newest *full* snapshot, and a `stopped` sandbox
/// never has one — so a single shared predicate would have silently stopped
/// re-placing every suspended evacuation in the fleet.
#[tokio::test(flavor = "current_thread")]
async fn a_suspended_evacuation_still_selects_on_its_full_snapshot() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();
    let sandbox = insert_sandbox(&db.pool, None, "suspended").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    set_current_artifact(&db.pool, sandbox, pin, ArtifactKind::Full).await?;

    assert!(
        restore_pending(&db.pool).await?.is_empty(),
        "a suspended evacuation with no ready full snapshot must not be selected"
    );

    insert_backup(
        &db.pool,
        sandbox,
        pin,
        ArtifactKind::Full,
        "full",
        "ready",
        false,
    )
    .await?;
    assert_eq!(
        restore_pending(&db.pool).await?,
        vec![(sandbox, "started".to_string())],
        "a suspended evacuation whose full snapshot is ready must still be selected"
    );
    let _ = node_id;
    Ok(())
}

// ---------------------------------------------------------------------------
// Turning the feature off must not abandon an evacuation already in flight.
// ---------------------------------------------------------------------------

/// The operator-visible property: **flipping the flag off stops the fleet
/// taking on new stopped evacuations; it does not orphan the ones already
/// running.**
///
/// A row claimed while the feature was on has its disk artifact published and
/// its local copy still on the node. If disabling the feature dropped it, the
/// drain would be held with nothing willing to finish it and — worse — the
/// owner's `start` would be refused indefinitely by the marker guard in
/// `stopped_restore_plan`, because nothing else ever clears that marker. That
/// is strictly worse than never having evacuated it.
///
/// Both directions on one fixture: the disabled cluster must still resume the
/// claimed row and must still refuse the unclaimed one.
#[tokio::test(flavor = "current_thread")]
async fn disabling_the_feature_stops_new_claims_without_abandoning_in_flight_ones() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let in_flight = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let untouched = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, in_flight, node_id).await?;

    // Asserted through the **production entry point**, not the raw SQL
    // builder. The trait method filters the SQL's rows in Rust with
    // `is_evacuable_lifecycle`, and that check reads the whole vocabulary
    // rather than the enabled subset — which is what actually lets the
    // in-flight row through. Testing the query alone would pass just as well
    // with the Rust filter narrowed to the enabled subset, i.e. with the
    // stranding bug present.
    let sink = disabled_sink(db.pool.clone());
    let listed = NodeDrainSink::list_evacuatable_sandboxes_on_node(&sink, node_id).await?;
    let listed_ids: Vec<SandboxId> = listed.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        listed_ids,
        vec![in_flight],
        "a disabled cluster must keep offering an evacuation it already opened, and must not \
         open one on the untouched row"
    );
    assert_eq!(
        listed.first().map(|(_, lifecycle)| *lifecycle),
        Some(SandboxLifecycle::Stopped),
        "and must offer it under `Stopped`, so the reconciler still dispatches it to the \
         disk path rather than dropping it as unrecognized"
    );

    assert_eq!(
        claim_stopped_evacuation(&db.pool, in_flight, node_id, false,).await?,
        StoppedEvacuationClaim::Resumed,
        "the claim must resume an evacuation in flight even with new ones disabled — \
         refusing here is what strands the sandbox"
    );
    assert_eq!(
        claim_stopped_evacuation(&db.pool, untouched, node_id, false,).await?,
        StoppedEvacuationClaim::Refused,
        "and it must refuse to open a new one"
    );
    assert_eq!(
        marker_of(&db.pool, untouched).await?,
        None,
        "a refused claim must leave the untouched row exactly as it was"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The page.
// ---------------------------------------------------------------------------

/// **A sandbox that is on no node counts as pending for as long as that is
/// true — including while its next retry is backed off.**
///
/// This is the one the alert depends on. The re-placement work list honours
/// `next_restore_attempt_at`, so a row whose restore keeps failing leaves it
/// for up to the 30-minute cap; a gauge derived from that list would read zero
/// through the whole window, i.e. fall silent on exactly the sandboxes it
/// exists to page for. The fixture seeds a backed-off row and a durability
/// failure alongside a healthy pending one, so the count cannot be right by
/// accident.
#[tokio::test(flavor = "current_thread")]
async fn the_pending_count_sees_sandboxes_the_retry_backoff_hides() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();

    // 1. Nowhere, durable, eligible now — in both populations.
    let eligible = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, eligible, node_id, pin, "ready").await?;
    clear_placement(&db.pool, eligible).await?;

    // 2. Nowhere, durable, but backed off — the work list drops it.
    let backed_off = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, backed_off, node_id, pin, "ready").await?;
    clear_placement(&db.pool, backed_off).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_attempt_count = 6,
                next_restore_attempt_at = NOW() + make_interval(mins => 25)
          WHERE id = $1"
    ))
    .bind(backed_off)
    .execute(&db.pool)
    .await?;

    // 3. Nowhere, and its only storage location has been orphaned — the work
    //    list drops it on durability, and it has no automatic recovery at all.
    let unresolvable = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, unresolvable, node_id).await?;
    set_current_artifact(&db.pool, unresolvable, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        unresolvable,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        true,
    )
    .await?;
    clear_placement(&db.pool, unresolvable).await?;

    // 4. Claimed but still resident — held by the drain, not nowhere.
    let still_resident = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, still_resident, node_id).await?;

    let work_list: Vec<SandboxId> = restore_pending(&db.pool)
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        work_list,
        vec![eligible],
        "the work list is expected to carry only the retry-eligible, durable row — this is \
         the shape that makes deriving the gauge from it wrong"
    );

    let pending: i64 = sqlx::query_scalar(stopped_replacements_pending_query())
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(
        pending, 3,
        "every sandbox that is on no node must count — the eligible one, the one waiting out \
         its backoff, and the one whose artifact can no longer be resolved — while the row \
         still resident on its node must not"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The one irreversible step.
// ---------------------------------------------------------------------------

/// **Nothing may give up a sandbox's rootfs on evidence that has gone stale.**
///
/// The teardown is the only step of a stopped evacuation that cannot be undone:
/// it releases the node, which the drain then terminates with the rootfs on it,
/// and nothing on the node second-guesses that — a `stopped` row's erase-safety
/// verdict is `Safe`.
/// Every other step is a guarded UPDATE that simply matches nothing when the
/// world moved; this one has to re-ask.
///
/// Each case below is durable — the bytes really are in S3 — so "not
/// authorized" is never standing in for "not ready". They are seeded next to
/// the fully-authorized row, which is what proves the refusals are decisions
/// rather than a query that returns nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_teardown_re_asks_the_whole_claim_before_erasing_anything() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();

    // Set up a claimed, published, durable stopped resident.
    let arm = async |lifecycle: &str| -> TestResult<SandboxId> {
        let sandbox = insert_sandbox(&db.pool, Some(node_id), lifecycle).await?;
        claim(&db.pool, sandbox, node_id).await?;
        set_current_artifact(&db.pool, sandbox, pin, ArtifactKind::Disk).await?;
        insert_backup(
            &db.pool,
            sandbox,
            pin,
            ArtifactKind::Disk,
            "disk",
            "ready",
            false,
        )
        .await?;
        Ok(sandbox)
    };

    let authorized = arm("stopped").await?;

    // A user took the sandbox back: the lifecycle moved and the trigger
    // dropped the marker.
    let taken_over = arm("stopped").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'starting' WHERE id = $1"
    ))
    .bind(taken_over)
    .execute(&db.pool)
    .await?;

    // The sandbox moved to another node between the claim and the teardown.
    let moved = arm("stopped").await?;
    let elsewhere = insert_node(&db.pool, None, "gentle").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(moved)
    .bind(elsewhere)
    .execute(&db.pool)
    .await?;

    // An operator undrained the node, withdrawing the authority to disrupt
    // its residents at all.
    let undrained_node = forced_draining_node(&db.pool).await?;
    let undrained = insert_sandbox(&db.pool, Some(undrained_node), "stopped").await?;
    claim_and_publish(&db.pool, undrained, undrained_node, pin, "ready").await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = NULL, drain_mode = 'gentle' WHERE id = $1"
    ))
    .bind(undrained_node)
    .execute(&db.pool)
    .await?;

    let sink = teardown_sink(db.pool.clone());
    assert_eq!(
        sink.stopped_teardown_authorization(authorized, node_id)
            .await
            .map_err(|err| err.to_string())?,
        TeardownAuthorization::Authorized,
        "a claimed, durable stopped resident of a still-force-draining node is the one \
         shape the teardown exists for — if this refuses, no drain ever converges"
    );
    for (sandbox, node, why) in [
        (taken_over, node_id, "a user took the sandbox back"),
        (
            moved,
            node_id,
            "the sandbox is now resident on another node",
        ),
        (undrained, undrained_node, "an operator undrained the node"),
    ] {
        assert_eq!(
            sink.stopped_teardown_authorization(sandbox, node)
                .await
                .map_err(|err| err.to_string())?,
            TeardownAuthorization::ClaimLost,
            "the teardown must refuse, and must report drift rather than a pending \
             upload, when {why}"
        );
    }

    // A vanished row is drift, not a pending upload.
    assert_eq!(
        sink.stopped_teardown_authorization(SandboxId::new(), node_id)
            .await
            .map_err(|err| err.to_string())?,
        TeardownAuthorization::ClaimLost,
        "a sandbox row that no longer exists must fail closed"
    );

    // And the mid-upload case stays distinguishable from all of the above.
    let uploading = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, uploading, node_id, pin, "creating").await?;
    assert_eq!(
        sink.stopped_teardown_authorization(uploading, node_id)
            .await
            .map_err(|err| err.to_string())?,
        TeardownAuthorization::AwaitingDurableDisk,
        "a claim that still holds but whose upload has not finalized must read as waiting, \
         so the node stays held rather than the evacuation being reported as finished"
    );
    Ok(())
}

/// A sink whose substrate is never reached: these cases stop at the
/// authorization query, which is the point — the teardown must not get as far
/// as an RPC.
fn teardown_sink(pool: PgPool) -> SubstrateSink {
    SubstrateSink::with_suspend_execution(pool, |_| async move {
        unreachable!("the teardown authorization query never suspends");
    })
}

/// **The drain may only destroy a copy whose mint it caused.**
///
/// This is the safety property the whole path rests on, and it is provenance
/// rather than recency because recency is not knowable here. The node mints a
/// `d-stop-{id}` snapshot only from a pinless row; every later request reuses
/// the pin and short-circuits entirely once its backup is `ready`. A successful
/// start clears the pin, but a start that fails after the guest ran is rolled
/// back to `stopped` keeping it, so a pinned row can point at a snapshot taken
/// before writes the failed start made, while its catalog row's `timestamp` —
/// *registration* time — looks recent. No cluster timestamp recovers
/// freshness.
///
/// A fresh claim on a pinless row is therefore the only shape that may proceed:
/// the artifact it publishes is one this evacuation asked for, from the rootfs
/// as it stands. The refusal is not a deferral — only a successful start clears
/// a node-local pin — so it is reported as its own outcome and counted.
#[tokio::test(flavor = "current_thread")]
async fn an_evacuation_opens_only_on_a_row_whose_artifact_it_will_mint() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    // Pinless: this evacuation will cause the mint, so it may open.
    let pinless = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    assert_eq!(
        claim(&db.pool, pinless, node_id).await?,
        StoppedEvacuationClaim::Claimed,
        "a stopped resident with no pin is the one shape an evacuation may open on — if \
         this refuses, no drain ever converges"
    );

    // Already pinned, with a ready backup: indistinguishable from an artifact
    // of the current rootfs, so it must not open. A ready backup is the
    // dangerous case precisely because every other gate would pass it.
    let inherited = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let foreign_pin = LocalArtifactId::new();
    set_current_artifact(&db.pool, inherited, foreign_pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        inherited,
        foreign_pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    assert_eq!(
        claim(&db.pool, inherited, node_id).await?,
        StoppedEvacuationClaim::Refused,
        "a row carrying a pin this drain did not mint must not be evacuated: publishing it \
         would upload bytes that may predate the user's work, and the teardown would then \
         give up the rootfs holding it"
    );
    assert_eq!(
        marker_of(&db.pool, inherited).await?,
        None,
        "and the refusal must leave no marker, so nothing downstream treats it as in flight"
    );

    // Resuming an evacuation whose pin *is* ours stays allowed — the marker can
    // only survive a write that left the lifecycle alone, and changing the
    // rootfs requires a start, which clears it.
    set_current_artifact(
        &db.pool,
        pinless,
        LocalArtifactId::new(),
        ArtifactKind::Disk,
    )
    .await?;
    assert_eq!(
        claim(&db.pool, pinless, node_id).await?,
        StoppedEvacuationClaim::Resumed,
        "once this evacuation has recorded its own pin, every later tick must still be \
         able to resume — otherwise the publish it just made could never be torn down"
    );
    Ok(())
}

/// The single read a tick opens with has to carry enough to skip the work
/// that is already done — otherwise the evacuation re-writes the same marker
/// and the same pin on every tick for the length of the upload, and those
/// writes are not free on `sandboxes`.
///
/// This pins the three fields the skip keys on across the three states a tick
/// can find the row in. The skip itself is a conditional over them; what a
/// test can own is that the conditional is being handed the truth.
#[tokio::test(flavor = "current_thread")]
async fn the_opening_read_reports_what_a_tick_needs_to_skip_finished_work() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sink = teardown_sink(db.pool.clone());
    let pin = LocalArtifactId::new();

    // Before anything: no claim, no pin. The tick must do all of step 1–3.
    let fresh = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let state = sink
        .stopped_evacuation_state(fresh, node_id)
        .await
        .map_err(|err| err.to_string())?;
    assert!(
        !state.claim_holds && !state.bytes_durable && state.pinned_artifact.is_none(),
        "an unclaimed resident must report nothing done, or the tick would skip the claim          it has not made: {state:?}"
    );

    // Claimed and pinned, upload still running: the tick must skip the claim
    // *and* the pin write, and still not tear anything down.
    claim(&db.pool, fresh, node_id).await?;
    set_current_artifact(&db.pool, fresh, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        fresh,
        pin,
        ArtifactKind::Disk,
        "disk",
        "creating",
        false,
    )
    .await?;
    let state = sink
        .stopped_evacuation_state(fresh, node_id)
        .await
        .map_err(|err| err.to_string())?;
    assert!(
        state.claim_holds,
        "the claim is held and must be reported so"
    );
    assert!(
        !state.bytes_durable,
        "an upload still `creating` must not authorize a teardown"
    );
    assert_eq!(
        state.pinned_artifact,
        Some(pin),
        "the pin must come back so the tick can tell the node handed it the same identity          it already recorded — that comparison is the whole reason the write is skipped"
    );

    // Durable: the tick goes straight to the teardown, having written nothing.
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'ready' WHERE sandbox_id = $1"
    ))
    .bind(fresh)
    .execute(&db.pool)
    .await?;
    let state = sink
        .stopped_evacuation_state(fresh, node_id)
        .await
        .map_err(|err| err.to_string())?;
    assert!(
        state.claim_holds && state.bytes_durable && state.pinned_artifact == Some(pin),
        "a durable, still-claimed row must report all three, so the terminal tick reaches          the teardown without a claim write, a publish RPC, or a pin write: {state:?}"
    );
    Ok(())
}

/// **The provenance guard has to survive a writer that pins the row while the
/// claim is waiting for its lock.**
///
/// This is a snapshot-visibility property, not a missing predicate, so only two
/// real interleaved transactions can decide it. Under READ COMMITTED an UPDATE
/// that blocks on a row lock re-checks the quals on *the row it is updating* via
/// EvalPlanQual when it resumes — but Postgres documents that it "does not see
/// effects of those commands on other rows in the database", and a `WITH … AS
/// (SELECT …)` join input is evaluated once against the statement's original
/// snapshot. So reading the pin through the CTE means the claim can wait for a
/// writer, be handed the lock, and still decide on pre-writer data.
///
/// That matters here more than anywhere else in this file: the pinless check is
/// the *entire* provenance argument. If it can be satisfied by a stale read, the
/// drain can open an evacuation on a row that is pinned — and then publish and
/// tear down against an artifact it did not mint, which is the one outcome the
/// whole design exists to prevent. Reading the pin off the target relation
/// instead puts it where EvalPlanQual will re-check it.
///
/// The interleaving is the production one: a writer that installs an artifact
/// pin holds this row's lock and leaves `lifecycle` and `active_node_id` alone
/// — exactly the shape that the EPQ-rechecked quals cannot catch.
#[tokio::test(flavor = "current_thread")]
async fn a_pin_committed_while_the_claim_waits_still_refuses_it() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;

    // The pinning transaction: takes this sandbox's row lock the way the
    // artifact-identity writer does, and holds it uncommitted. It deliberately
    // touches neither `lifecycle` nor `active_node_id`, so every qual the claim
    // evaluates against the target relation still holds afterwards and only the
    // pinless check stands between the drain and an unattributable artifact.
    let foreign_pin = LocalArtifactId::new();
    let mut pinning = db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(foreign_pin)
    .execute(&mut *pinning)
    .await?;

    // Pre-warm the connection the racing task will lease. `try_pg_test_db` caps
    // the pool at 2 and this fixture holds one, so a task that paid TCP + auth
    // inside the race window could arrive *after* the commit below and then
    // refuse for the trivial already-committed reason — green, having proven
    // nothing about snapshot freshness.
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        None,
        "the row must start unmarked, or the claim would take its resume branch \
         and never evaluate the pinless check this case is about"
    );

    let pool = db.pool.clone();
    let racing = tokio::spawn(async move { claim(&pool, sandbox, node_id).await });

    // Wait for the claim to be genuinely *parked on this row's lock*, by asking
    // Postgres rather than sleeping. `!is_finished()` cannot tell "blocked" from
    // "still leasing a connection", and in that second state the commit below
    // lands first — the claim then reads the pin from a fresh snapshot and
    // refuses for a reason this test is not measuring.
    let blocked = super::termination_gate_tests::wait_until_blocked(&mut pinning).await?;
    assert!(
        blocked,
        "the claim never blocked on the sandbox row lock, so this case would pass \
         with the provenance check reading from anywhere at all"
    );
    pinning.commit().await?;

    let outcome = racing.await.expect("the racing claim must not panic")?;
    assert_eq!(
        outcome,
        StoppedEvacuationClaim::Refused,
        "a claim that waited for a writer which pinned this row must refuse it: the \
         artifact is one the drain did not cause, so nothing it publishes can be \
         attributed to the current rootfs and the teardown would erase bytes it \
         cannot vouch for"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        None,
        "and it must leave no marker — a marker here would make the next tick treat \
         this as a resumed claim, which skips the pinless check entirely"
    );
    Ok(())
}

/// The artifact the faked node hands back from a publish.
fn enqueued(pin: LocalArtifactId) -> EnqueuedArtifactBackup {
    EnqueuedArtifactBackup {
        artifact: PublishedLocalArtifact {
            id: pin,
            kind: ArtifactKind::Disk,
        },
        attempt_id: pplx_space_common_core::types::BackupAttemptId::new(),
    }
}

async fn cluster_pin(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<LocalArtifactId>> {
    Ok(sqlx::query_scalar::<_, Option<LocalArtifactId>>(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// **A tick must not re-ask for an upload that is already running.**
///
/// `EnqueueArtifactBackup` is idempotent for two of the three shapes it meets —
/// it joins an in-flight attempt and short-circuits an already-`ready` backup —
/// but it declines to reuse a **failed** job, so a failed upload falls through
/// to a fresh `BackupAttemptId`, and `BeginArtifactBackup` inserts one
/// `snapshots` row per attempt id. Its `ON CONFLICT` is keyed on that id, so it
/// dedupes retries of one attempt, not attempts of one artifact. Re-issuing on
/// every five-second tick therefore leaks a catalog row per tick for a sandbox
/// whose upload keeps failing, into a table the retention sweeper filters to
/// `type = 'full'` and so never prunes for disk rows.
///
/// So the steady-state tick must publish *nothing* while an attempt is live.
/// Asserted on the fake's own record of publish calls, because the outcome is
/// `AwaitingDurableDisk` either way — the observable that distinguishes "asked
/// again" from "waited" is the call, not the verdict.
#[tokio::test(flavor = "current_thread")]
async fn a_live_upload_attempt_is_not_re_enqueued_every_tick() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();
    let sink = stopped_path_sink(db.pool.clone(), pin);

    // Opening tick: nothing is pinned and nothing is in flight, so it must
    // publish — otherwise the gate would make the evacuation unable to start.
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::AwaitingDurableDisk { .. }
        ),
        "the opening tick must open the evacuation; got {outcome:?}"
    );
    assert_eq!(
        sink.publish_calls(),
        1,
        "fixture precondition: the opening tick published once, so the next assertion is \
         about re-asking rather than about never asking"
    );

    // The node registered a `creating` catalog row for exactly those bytes.
    insert_backup(
        &db.pool,
        sandbox,
        pin,
        ArtifactKind::Disk,
        "disk",
        "creating",
        false,
    )
    .await?;

    // Three more ticks. Each must observe the in-flight attempt and return
    // without asking again.
    for tick in 1..=3 {
        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
        assert!(
            matches!(
                outcome,
                StoppedEvacuationOutcome::AwaitingDurableDisk { local_artifact_id }
                    if local_artifact_id == pin
            ),
            "tick {tick} must still report the wait, naming the artifact; got {outcome:?}"
        );
    }
    assert_eq!(
        sink.publish_calls(),
        1,
        "and none of those ticks may have re-published: a failed attempt is not deduped \
         by the node, so one enqueue per tick becomes one catalog row per tick in a table \
         nothing prunes"
    );

    // A *failed* attempt is the shape the `state = 'creating'` restriction
    // exists for, and it must read as "not in flight" so the next tick retries.
    // Widening the predicate to any state would make a failed upload wait
    // forever — the mirror failure of the leak, and worse, since the leak is
    // visible and this would be silent.
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'failed' WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::AwaitingDurableDisk { .. }
        ),
        "a failed attempt must keep the evacuation waiting rather than resolve it; got \
         {outcome:?}"
    );
    assert_eq!(
        sink.publish_calls(),
        2,
        "and it must re-publish: a failed attempt is retryable, so treating it as \
         in-flight would wedge the evacuation permanently. The deliberate residual is one \
         new catalog row per *failure* instead of one per tick"
    );

    // Once the bytes are durable the evacuation completes, so the gate cannot
    // be a way of never finishing.
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'ready' WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Released { .. }),
        "a durable artifact must still release the node; got {outcome:?}"
    );
    assert_eq!(
        sink.recorded_teardowns(),
        vec![sandbox],
        "and release the node exactly once"
    );
    Ok(())
}

/// **The suspend path must refuse a `stopped` row, and nothing asserted that
/// once the widening took the property away for free.**
///
/// `EVACUABLE_LIFECYCLES` gained `Stopped` because it is the source of truth for
/// the `evacuation_restore_target` CHECK. But `evacuate_sandbox`'s entry guard
/// read the same constant, so the widening silently made the suspend path accept
/// `Stopped` — and the narrower CHECK, which would have refused the marker write
/// behind it, was widened in the same change. Two backstops removed at once,
/// leaving one bare early-return in `spawn_evacuations` between a stopped
/// sandbox and a CAS into `'suspending'` for a VM that does not exist.
///
/// Both directions, because a guard that refuses everything would also pass a
/// one-sided assertion.
#[tokio::test(flavor = "current_thread")]
async fn the_suspend_path_refuses_a_stopped_row() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let sink = teardown_sink(db.pool.clone());

    let refused = sink
        .evacuate_sandbox(sandbox, node_id, SandboxLifecycle::Stopped)
        .await
        .expect_err("the suspend path must refuse a stopped lifecycle");
    assert!(
        matches!(refused, EvacuateError::Lookup(_)),
        "and refuse it as a lookup error, before any write: the alternative is a CAS into \
         `suspending` that sbx-mgr then refuses, parking the sandbox in a transient \
         `abandon_stale_resident_sandboxes` deliberately spares; got {refused:?}"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        None,
        "and it must write nothing on the way out — the widened CHECK would now accept \
         `'stopped'` as a restore target, so the write is no longer refused by the schema"
    );
    assert_eq!(
        lifecycle_of(&db.pool, sandbox).await?,
        "stopped",
        "and leave the lifecycle alone rather than fabricating a `suspending` the user \
         never asked for"
    );

    // The other direction, and it needs its own sink: a `started` resident gets
    // *past* the entry guard and reaches the substrate, so a fake that panics
    // there would prove the point by crashing rather than asserting. This one
    // fails the suspend with a recognisable error, so "got past the guard" is
    // observable as a value.
    let reached_substrate =
        SubstrateSink::with_suspend_execution(db.pool.clone(), |_| async move {
            Err(SubstrateError::Failed {
                message: "reached the substrate".to_owned(),
            })
        });
    let started = insert_sandbox(&db.pool, Some(node_id), "started").await?;
    let outcome = reached_substrate
        .evacuate_sandbox(started, node_id, SandboxLifecycle::Started)
        .await;
    assert!(
        matches!(&outcome, Err(err) if err.to_string().contains("reached the substrate")),
        "a started resident must get past the entry guard and reach the suspend RPC — a \
         guard that refused everything would satisfy the assertions above and break every \
         drain in the fleet; got {outcome:?}"
    );
    Ok(())
}

/// **A stopped resident that cannot progress must not crowd a live one out of
/// the node's evacuation budget.**
///
/// `spawn_bounded` walks this list in order and stops at 16 slots, so list
/// position decides who is dispatched. Every other population here eventually
/// leaves the list; a `stopped` row refused on provenance writes **nothing** —
/// it returns before the claim — so it matches again next tick, unchanged. With
/// no ordering, an unmodified heap comes back in a stable order and the same
/// rows win forever. Seed more stopped residents than the budget, put a
/// `started` one behind them by insertion order, and the started row must still
/// come first: otherwise a drain holds live sandboxes on a node in order to keep
/// retrying rows that cannot move, which is the inverse of the whole feature.
///
/// Seventeen, not three: fewer than the slot budget and nothing is contended, so
/// the assertion would hold no matter how the list were ordered.
#[tokio::test(flavor = "current_thread")]
async fn the_suspend_population_is_dispatched_before_stopped_residents() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    // Seventeen stopped residents, all inserted before the started one so that
    // insertion order — which is what an unordered scan returns — puts them
    // ahead of it.
    let mut stopped = Vec::new();
    for _ in 0..17 {
        stopped.push(insert_sandbox(&db.pool, Some(node_id), "stopped").await?);
    }
    let started = insert_sandbox(&db.pool, Some(node_id), "started").await?;

    let listed =
        list_evacuatable_sandbox_rows(&db.pool, node_id, ResidencyEvidence::LifecycleOnly, true)
            .await?;
    let ids: Vec<SandboxId> = listed.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        ids.len(),
        18,
        "fixture precondition: every seeded resident is evacuable, so the ordering is the \
         only thing under test"
    );
    assert_eq!(
        ids.first().copied(),
        Some(started),
        "the started resident must be dispatched first — it is the population that \
         converges and leaves the list, and behind seventeen non-progressing stopped rows \
         it would never reach a slot"
    );

    // And the stopped rows must still all be offered, in a deterministic order —
    // withholding them would trade one starvation for another.
    let mut tail = ids[1..].to_vec();
    tail.sort();
    let mut want = stopped.clone();
    want.sort();
    assert_eq!(
        tail, want,
        "every stopped resident must still be listed after the suspend population, or \
         ordering them last would become a way of never evacuating them at all"
    );
    Ok(())
}

/// **A marker that outlives `stopped` must not read as a held claim.**
///
/// The resumed-claim path skips the provenance check entirely, on the argument
/// that a rootfs change would have dropped the marker. The marker-clear trigger
/// has an allowlist, though — it keeps the marker across `suspending`,
/// `suspended` and `restoring` — and a restore rewrites the rootfs wholesale. So
/// the argument rests on the *conjunction* of marker and `lifecycle = 'stopped'`,
/// not on the marker alone.
///
/// `stopped → restoring` is unreachable today: the restore claim admits only
/// `('suspended','restoring')`, which is a tracked follow-up. Two of this
/// module's queries carry `restoring` for that future and assert nothing about
/// it, so this seeds the state directly and pins the behaviour before the
/// refusal lifts — otherwise the day it lifts, the trigger stops covering this
/// path and nothing says whether anything else does.
#[tokio::test(flavor = "current_thread")]
async fn a_marker_surviving_into_restoring_is_not_a_held_claim() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = teardown_sink(db.pool.clone());
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    // Claimed *and* durable, because the sweep's stopped arm gates on the disk
    // artifact being ready. Without that this case would read as "not selected"
    // for a reason that has nothing to do with the lifecycle it is about.
    claim_and_publish(&db.pool, sandbox, node_id, LocalArtifactId::new(), "ready").await?;
    assert!(
        disk_backup_ready(&db.pool, sandbox).await?,
        "fixture precondition: durable, so the sweep's own durability gate is not what \
         decides the assertions below"
    );

    // The transition the trigger's allowlist exempts.
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        Some("stopped".to_owned()),
        "fixture precondition: the marker must genuinely survive this transition, or the \
         case proves nothing about the allowlist"
    );

    let state = sink
        .stopped_evacuation_state(sandbox, node_id)
        .await
        .expect("the state read must not fail on a restoring row");
    assert!(
        !state.claim_holds,
        "a marker on a row that has left `stopped` must not read as a held claim: the \
         resumed path skips the provenance check, and a restore rewrites the rootfs the \
         check exists to vouch for"
    );

    // The sweep does pick it up — and, unlike the resting `stopped` arm, without
    // waiting for the placement to clear. That asymmetry is deliberate and was
    // never asserted: the `stopped` arm's `active_node_id IS NULL` is what
    // distinguishes "torn down, needs a home" from "still has its copy", but a
    // `restoring` row is a re-placement that a controller died in the middle of,
    // and it may still hold the placement that attempt wrote. Gating it on
    // unplaced would strand exactly the rows the arm exists to recover.
    let pending: Vec<SandboxId> = restore_pending(&db.pool)
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        pending,
        vec![sandbox],
        "a marked `restoring` row must be selected while still placed — it is a restore \
         that died in flight, and the resting arm's unplaced requirement would leave it \
         for nobody"
    );
    clear_placement(&db.pool, sandbox).await?;
    let pending: Vec<SandboxId> = restore_pending(&db.pool)
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        pending,
        vec![sandbox],
        "and it must stay selected once unplaced, so the two arms cannot disagree about \
         the same row as it converges"
    );
    Ok(())
}

/// **The pin write is a write, so it needs the guards every other write here
/// has.**
///
/// It was the only statement in the sequence that didn't re-assert the node was
/// still force-draining, and it overwrote whatever pin was present with whatever
/// the node handed back. Both were argued benign from a *sibling* statement's
/// guard — which is exactly the shape of reasoning that made this path unsafe
/// once already.
///
/// Three refusals, each a possible yes turned into a no by one fact, beside the
/// real yes.
#[tokio::test(flavor = "current_thread")]
async fn the_pin_write_re_asserts_the_claim_it_was_handed() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();

    // The yes: a claimed, still-resident row on a still-force-draining node.
    let ok = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, ok, node_id).await?;
    assert!(
        record_stopped_evacuation_artifact(&db.pool, ok, node_id, pin,).await?,
        "a claimed resident of a force-draining node must accept the pin, or the \
         evacuation cannot proceed at all"
    );
    assert_eq!(
        cluster_pin(&db.pool, ok).await?,
        Some(pin),
        "and the pin must actually land, since the durability gate resolves the artifact \
         through it"
    );
    assert!(
        record_stopped_evacuation_artifact(&db.pool, ok, node_id, pin,).await?,
        "re-writing the *same* pin must stay idempotent — a crash-recovery tick re-issues \
         the identical publish and would otherwise read as drift"
    );

    // No: a different artifact over an existing pin. The claim guarantees the
    // row was pinless when it opened, so a pin appearing here is drift — and
    // silently replacing it is how a teardown ends up authorized against bytes
    // nothing vouched for.
    let other = LocalArtifactId::new();
    assert!(
        !record_stopped_evacuation_artifact(&db.pool, ok, node_id, other,).await?,
        "a second, different artifact must be refused rather than overwrite the first"
    );
    assert_eq!(
        cluster_pin(&db.pool, ok).await?,
        Some(pin),
        "and the original pin must survive the refusal"
    );

    // No: the operator undrained the node between the claim and this write.
    let undrained_node = forced_draining_node(&db.pool).await?;
    let undrained = insert_sandbox(&db.pool, Some(undrained_node), "stopped").await?;
    claim(&db.pool, undrained, undrained_node).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = NULL, drain_mode = 'gentle' WHERE id = $1"
    ))
    .bind(undrained_node)
    .execute(&db.pool)
    .await?;
    assert!(
        !record_stopped_evacuation_artifact(&db.pool, undrained, undrained_node, pin,).await?,
        "an `:undrain` between the claim and this write must stop it — this was the one \
         statement in the sequence that did not ask"
    );
    assert_eq!(
        cluster_pin(&db.pool, undrained).await?,
        None,
        "and it must leave no pin behind on a node that is no longer draining"
    );

    // No: never claimed at all, so the marker term refuses it.
    let unclaimed = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    assert!(
        !record_stopped_evacuation_artifact(&db.pool, unclaimed, node_id, pin,).await?,
        "an unmarked row was never this drain's to pin"
    );
    Ok(())
}

/// **An unreadable pin refuses, and names nothing.**
///
/// Two dispositions that an earlier revision got inconsistent: an empty string
/// was treated as *pinless* — a fresh evacuation may proceed — while a malformed
/// id was treated as pinned. Both are equally unreadable, so both must refuse;
/// only a genuinely absent pin is a real "no".
///
/// And the refusal carries no artifact id. Synthesizing a nil UUID would put it
/// in the outcome and the log looking like a real artifact, which is the exact
/// objection `tear_down_stopped_local_copy` raises for its own impossible case.
#[tokio::test(flavor = "current_thread")]
async fn an_unreadable_node_pin_refuses_without_naming_an_artifact() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;

    // Durable bytes for the artifact the fake's publish returns. Not for a
    // *cluster* pin — this row is pinless by design and
    // `current_disk_artifact_backup_ready` requires
    // `current_local_artifact_id IS NOT NULL`, so a cluster-pinned backup could
    // not be the lever. Seeded this way, removing the `pin_unreadable` guard
    // lets the flow claim, publish, pin that id, find it durable, and destroy —
    // which is what makes the assertions below able to fail.
    let published = LocalArtifactId::new();
    insert_backup(
        &db.pool,
        sandbox,
        published,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    let sink = SubstrateSink::with_stopped_path_node_state(
        db.pool.clone(),
        enqueued(published),
        NodeReportedArtifact {
            present: true,
            pinned_artifact: None,
            pin_unreadable: true,
            reports_stopped: true,
            ..NodeReportedArtifact::default()
        },
    );
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    // Destruction first, for the same reason as the sibling above.
    assert_eq!(
        sink.recorded_teardowns(),
        Vec::<SandboxId>::new(),
        "nothing may be destroyed: the seeded bytes are durable, so with the guard gone \
         this flow would reach the teardown"
    );
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::UnattributableArtifact {
                local_artifact_id: None
            }
        ),
        "and an unreadable pin must refuse exactly as a readable one does — the bytes \
         cannot be attributed either way — naming no artifact, because a synthesized nil \
         id is indistinguishable from a real one wherever it is read; got {outcome:?}"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        None,
        "and it must not claim the row"
    );
    assert_eq!(
        sink.publish_calls(),
        0,
        "and not even publish: an unreadable pin means the next enqueue would reuse bytes \
         nobody can identify"
    );
    Ok(())
}

/// **The guard has to ask the machine that makes the decision.**
///
/// For several revisions it didn't. sbx-mgr chooses between minting a fresh disk
/// artifact and reusing an existing one by matching on its **own SQLite**
/// `(state, current_local_artifact_id)`; the guard checked Postgres'
/// `sandboxes.current_local_artifact_id`. Two columns of the same name, on
/// different machines, and for a `stopped` sandbox nothing keeps them in step:
/// the projection that carries the node's pin up is gated to
/// `status = 'suspended'`, and the retire that causes the stop-time mint
/// discards the enqueue response. So the cluster column is NULL for essentially
/// every stopped sandbox and the check admitted exactly the row it existed to
/// refuse — failing **open**, on the one guard standing between this drain and a
/// user's rootfs.
///
/// The scenario needs no abandoned drain: stop (node mints A, uploads it,
/// catalog `ready`), then a start that boots the guest, which writes, and then
/// fails — rolled back to `stopped` keeping pin A, since only a successful start
/// clears it. The node now reuses A and short-circuits since A's backup is
/// already ready. The cluster sees a durable artifact for A and a NULL pin, and
/// would authorize giving up a rootfs whose current contents were never
/// uploaded.
///
/// Both directions on one fixture: the same sandbox, same cluster state, and the
/// *node's* answer as the only difference — because an assertion on either alone
/// passes under "always refuse" or "never refuse".
#[tokio::test(flavor = "current_thread")]
async fn a_node_held_pin_refuses_the_evacuation_even_when_the_cluster_row_is_pinless() -> TestResult
{
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    assert_eq!(
        cluster_pin(&db.pool, sandbox).await?,
        None,
        "the cluster row must be pinless — that is the normal state for a stopped \
         sandbox and the reason a cluster-side check cannot carry this property"
    );

    // The node holds a pin a failed start left behind. Nothing in the cluster
    // knows about it.
    let stale_pin = LocalArtifactId::new();
    // And its bytes are already durable — the docstring's scenario says so
    // ("node mints A, uploads it, catalog `ready`"), and without it the
    // "nothing was destroyed" assertion below cannot fail: both teardown call
    // sites require `bytes_durable`, so with no ready backup the destroy is
    // unreachable whether or not the guard under test exists. Seeded against
    // the id the fake's publish hands back, because that is the id the pin
    // write would install and therefore the one the durability read resolves.
    insert_backup(
        &db.pool,
        sandbox,
        stale_pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    let refusing = SubstrateSink::with_stopped_path_node_state(
        db.pool.clone(),
        enqueued(stale_pin),
        NodeReportedArtifact {
            present: true,
            pinned_artifact: Some(stale_pin),
            reports_stopped: true,
            ..NodeReportedArtifact::default()
        },
    );
    let outcome = refusing.evacuate_stopped_sandbox(sandbox, node_id).await?;
    // Destruction first, reporting second. Both would be wrong if the guard were
    // removed, and whichever is asserted first is the one that actually fires —
    // so the order decides which property the test is really pinning. Nothing
    // destroyed is the one that matters; how the refusal is named is secondary.
    assert_eq!(
        refusing.recorded_teardowns(),
        Vec::<SandboxId>::new(),
        "nothing may be destroyed: the bytes seeded above are durable, so with the \
         node-pin guard gone this flow would claim, publish, pin and give up a rootfs the \
         artifact does not describe"
    );
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::UnattributableArtifact { local_artifact_id }
                if local_artifact_id == Some(stale_pin)
        ),
        "and the refusal must name the artifact, even though the cluster row is pinless; \
         got {outcome:?}"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        None,
        "and it must not claim the row: a marker here would make the next tick a \
         *resumed* claim, which skips this check by design"
    );

    // Same row, same cluster state; the node now reports itself pinless, so the
    // next enqueue mints from the rootfs as it stands.
    let fresh_pin = LocalArtifactId::new();
    let proceeding = stopped_path_sink(db.pool.clone(), fresh_pin);
    let outcome = proceeding
        .evacuate_stopped_sandbox(sandbox, node_id)
        .await?;
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::AwaitingDurableDisk { local_artifact_id }
                if local_artifact_id == fresh_pin
        ),
        "a pinless node mints for this evacuation, so it must open and wait on the \
         upload — a guard that refused here would make the feature inert; got {outcome:?}"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        Some("stopped".to_owned()),
        "and that one must be claimed"
    );
    Ok(())
}

/// The node knowing nothing about the sandbox, or believing it running, is
/// neither a provenance failure nor permanent — so it must not land on the
/// outcome documented as never self-resolving.
///
/// A missing row means there is no local rootfs to publish or destroy. A running
/// row means the cluster's `stopped` is the stale view, and publishing would
/// capture a live filesystem. A degraded answer is a stub whose pin and liveness
/// are defaults, so it says nothing either way. All of these clear without
/// anyone touching a pin.
#[tokio::test(flavor = "current_thread")]
async fn a_node_that_disagrees_about_the_sandbox_defers_rather_than_refusing_forever() -> TestResult
{
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();

    for (label, node_artifact) in [
        (
            "the node has no row for it",
            NodeReportedArtifact {
                present: false,
                pinned_artifact: None,
                reports_stopped: false,
                ..NodeReportedArtifact::default()
            },
        ),
        (
            "the node believes it is running",
            NodeReportedArtifact {
                present: true,
                pinned_artifact: None,
                reports_stopped: false,
                ..NodeReportedArtifact::default()
            },
        ),
        (
            // The dangerous one, and the reason `reports_stopped` cannot be
            // read alone: the node's status classifier only overrides a
            // persisted state when it can report `paused`, so a `stopped` row
            // whose Firecracker process is alive reports `"stopped"`. Acting on
            // that would snapshot a rootfs a guest is still writing and then
            // erase it — worse than the stale-pin case, because the sandbox is
            // live rather than merely misdescribed.
            "the node's row says stopped but its VM is alive",
            NodeReportedArtifact {
                present: true,
                pinned_artifact: None,
                reports_stopped: true,
                vm_alive: true,
                ..NodeReportedArtifact::default()
            },
        ),
        (
            // The node could not read its row and published a stub whose pin
            // and liveness are defaults: read literally it is the one shape
            // this step admits.
            "the node's answer is degraded",
            NodeReportedArtifact {
                present: true,
                pinned_artifact: None,
                reports_stopped: true,
                state_degraded: true,
                ..NodeReportedArtifact::default()
            },
        ),
    ] {
        let sink = SubstrateSink::with_stopped_path_node_state(
            db.pool.clone(),
            enqueued(pin),
            node_artifact,
        );
        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
        assert!(
            matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
            "{label}: this resolves on its own and must be reported as drift, not as an \
             unattributable artifact that sends a responder to clear a pin; got {outcome:?}"
        );
        assert_eq!(
            marker_of(&db.pool, sandbox).await?,
            None,
            "{label}: and it must not claim the row"
        );
    }
    Ok(())
}

/// A sink whose two node-api legs are faked: the publish hands back `pin`, and
/// the teardown only records that it was asked. Lets a case drive the whole
/// entry point against a real Postgres and then assert on whether the
/// irreversible step ran.
fn stopped_path_sink(pool: PgPool, pin: LocalArtifactId) -> SubstrateSink {
    SubstrateSink::with_stopped_path(pool, enqueued(pin))
}

/// **`unattributable_artifact` has to mean what its docs say it means.**
///
/// Two different worlds produce "the claim does not hold and the row carries a
/// pin", and the metrics catalog sends a responder to opposite places for them.
/// One is permanent: a pin this drain never minted, which nothing in the fleet
/// clears, so the node parks until an operator intervenes. The other is ordinary
/// drift on an evacuation this drain *did* open — an `:undrain` between the
/// listing and the read, a placement change — where the pin is the drain's own
/// and the right action is to wait one tick.
///
/// Reporting the second as the first is worse than a miscounted metric: it sends
/// someone to clear an artifact pin on a sandbox that is fine. The marker is the
/// discriminant, and both shapes are seeded here because an assertion on either
/// alone passes under "always unattributable" or "never unattributable".
#[tokio::test(flavor = "current_thread")]
async fn drift_on_a_drain_owned_pin_is_not_reported_as_unattributable() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();
    let sink = stopped_path_sink(db.pool.clone(), pin);

    // The permanent shape: pinned, never marked, so no evacuation ever opened.
    let inherited = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let foreign_pin = LocalArtifactId::new();
    set_current_artifact(&db.pool, inherited, foreign_pin, ArtifactKind::Disk).await?;
    let outcome = sink.evacuate_stopped_sandbox(inherited, node_id).await?;
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::UnattributableArtifact { local_artifact_id }
                if local_artifact_id == Some(foreign_pin)
        ),
        "a pinned, unmarked row is the shape that never resolves and must be named as \
         such; got {outcome:?}"
    );

    // The transient shape: this drain opened the evacuation and minted the pin,
    // then the operator undrained the node. Same two observable facts — claim
    // does not hold, pin present — opposite meaning.
    let ours = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, ours, node_id, pin, "creating").await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = NULL, drain_mode = 'gentle' WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let outcome = sink.evacuate_stopped_sandbox(ours, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "an evacuation whose node was undrained under it is drift that resolves itself, \
         not an unattributable artifact — reporting it as one sends a responder to clear \
         a pin this drain minted, on a sandbox that is fine; got {outcome:?}"
    );

    // Same drift, but now with the *node* reporting a pin as well — which is the
    // ordinary state for any stopped sandbox. This is the shape that recurred:
    // the cluster-row check grew the marker conjunct and the node-side check did
    // not, so the same drift came back as `unattributable_artifact` one commit
    // later at the sibling site. Both checks have to be behind the same gate.
    let node_pinned = SubstrateSink::with_stopped_path_node_state(
        db.pool.clone(),
        enqueued(pin),
        NodeReportedArtifact {
            present: true,
            pinned_artifact: Some(pin),
            reports_stopped: true,
            ..NodeReportedArtifact::default()
        },
    );
    let outcome = node_pinned.evacuate_stopped_sandbox(ours, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "the node-side provenance check must be gated on the same marker as the \
         cluster-side one: this row's pin is the drain's own, and a node pin is the \
         ordinary state for a stopped sandbox, so an ungated check reports every drifted \
         in-flight evacuation as permanently unattributable; got {outcome:?}"
    );

    // No "and nothing was destroyed" assertion here, deliberately. Every row in
    // this fixture is one whose claim does *not* hold — that is the drift the
    // test is about — and both teardown call sites require `claim_holds`, so the
    // teardown record is structurally empty under every mutation. Asserting it
    // would read as a data-loss guard while being unable to fail. The property
    // is owned by `the_durability_verdict_gates_the_destroy_and_not_only_the_answer`,
    // where the claim does hold and the assertion can go either way.
    Ok(())
}

/// **The authorization has to gate the destroy, not merely be correct.**
///
/// `the_teardown_re_asks_the_whole_claim_before_erasing_anything` proves the
/// predicate answers right. It says nothing about whether the code that erases
/// a rootfs consults the answer — and that wiring is one match arm. Deleting it
/// (so every non-`Authorized` verdict falls through to the teardown) left every
/// other test in this file passing, which is how a data-loss path ships.
///
/// So this drives [`NodeDrainSink::evacuate_stopped_sandbox`] itself, twice on
/// the same fixture, and asserts on the faked teardown's own record:
/// an undurable artifact must reach `AwaitingDurableDisk` with **nothing
/// destroyed**, and the same sandbox one `ready` state later must reach
/// `Released` with exactly one destroy. One assertion without the other proves
/// nothing: "never tears down" and "always tears down" each satisfy half.
#[tokio::test(flavor = "current_thread")]
async fn the_durability_verdict_gates_the_destroy_and_not_only_the_answer() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();
    let sink = stopped_path_sink(db.pool.clone(), pin);

    // The upload is still in flight. The claim opens, the pin is recorded, and
    // the authorization refuses — the one step that cannot be undone must not
    // have run.
    insert_backup(
        &db.pool,
        sandbox,
        pin,
        ArtifactKind::Disk,
        "disk",
        "creating",
        false,
    )
    .await?;
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::AwaitingDurableDisk { local_artifact_id }
                if local_artifact_id == pin
        ),
        "an evacuation whose bytes are not yet durable must report that it is waiting; got \
         {outcome:?}"
    );
    assert_eq!(
        sink.recorded_teardowns(),
        Vec::<SandboxId>::new(),
        "nothing may be destroyed while the disk artifact is still uploading — sbx-mgr's \
         delete calls a stopped row `Safe` to erase, so this refusal is the only thing \
         between the drain and the user's rootfs"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        Some("stopped".to_owned()),
        "the refusal is a deferral, not an abandonment: the claim must survive for the next \
         tick to resume"
    );

    // Same sandbox, same tick order, one column different.
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'ready' WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::Released { local_artifact_id }
                if local_artifact_id == Some(pin)
        ),
        "once the bytes are durable the same evacuation must release the node; got {outcome:?}"
    );
    assert_eq!(
        sink.recorded_teardowns(),
        vec![sandbox],
        "and it must release the node exactly once — a guard that never authorizes is \
         as broken as one that always does"
    );
    Ok(())
}

/// A claimed evacuation whose bytes are durable still releases nothing when
/// its node no longer reports the copy the artifact describes: no pin (a start
/// that failed after the guest ran, on a node that drops the pin on rollback),
/// a different pin, a live VM, or a sandbox the node does not call stopped.
/// Each is one field away from the agreeing answer, and the same claim with the
/// agreeing answer does release, so no refusal here holds vacuously.
#[tokio::test(flavor = "current_thread")]
async fn a_durable_evacuation_is_released_only_while_the_node_still_holds_its_artifact()
-> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();
    claim(&db.pool, sandbox, node_id).await?;
    set_current_artifact(&db.pool, sandbox, pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        sandbox,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    let agreeing = NodeReportedArtifact {
        present: true,
        pinned_artifact: Some(pin),
        reports_stopped: true,
        ..NodeReportedArtifact::default()
    };
    let disagreeing = [
        (
            "no pin",
            NodeReportedArtifact {
                pinned_artifact: None,
                ..agreeing.clone()
            },
        ),
        (
            "a different pin",
            NodeReportedArtifact {
                pinned_artifact: Some(LocalArtifactId::new()),
                ..agreeing.clone()
            },
        ),
        (
            "a live VM",
            NodeReportedArtifact {
                vm_alive: true,
                ..agreeing.clone()
            },
        ),
        (
            "not stopped",
            NodeReportedArtifact {
                reports_stopped: false,
                ..agreeing.clone()
            },
        ),
        (
            "a degraded answer",
            NodeReportedArtifact {
                state_degraded: true,
                ..agreeing.clone()
            },
        ),
    ];
    for (shape, reported) in disagreeing {
        let sink = SubstrateSink::with_stopped_path_teardown_node_state(
            db.pool.clone(),
            enqueued(pin),
            reported,
        );
        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
        assert_eq!(
            sink.recorded_teardowns(),
            Vec::<SandboxId>::new(),
            "a node reporting {shape} must not have its sandbox's node released"
        );
        assert!(
            matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
            "{shape}: the evacuation must defer, got {outcome:?}"
        );
    }

    let sink = SubstrateSink::with_stopped_path_teardown_node_state(
        db.pool.clone(),
        enqueued(pin),
        agreeing,
    );
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Released { .. }),
        "the same claim with a node that agrees must release, got {outcome:?}"
    );
    assert_eq!(sink.recorded_teardowns(), vec![sandbox]);
    Ok(())
}

/// A durable, authorized evacuation still destroys nothing if the sandbox has
/// an active execution by the time the teardown runs — an owner's `start`
/// that got through after an `:undrain` — and reports `Deferred` with the
/// claim and placement intact.
///
/// The execution is seeded while the row still reads `stopped`, so the claim
/// still holds and the durability verdict still authorizes: the fence on the
/// teardown is the only thing that can refuse here. The durability test above
/// shows the same shape of claim, once its backup is ready and with no
/// execution, does tear down.
#[tokio::test(flavor = "current_thread")]
async fn an_execution_that_appears_after_authorization_stops_the_teardown() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();
    let sink = stopped_path_sink(db.pool.clone(), pin);
    insert_backup(
        &db.pool,
        sandbox,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 2, $2, 'space', 'running', TRUE)"
    ))
    .bind(sandbox)
    .bind(format!("space-sbx-{}-g2", sandbox.as_uuid().simple()))
    .execute(&db.pool)
    .await?;

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

    assert_eq!(
        sink.recorded_teardowns(),
        Vec::<SandboxId>::new(),
        "a sandbox with a live execution must not have its copy destroyed"
    );
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "and the evacuation must defer rather than report a release; got {outcome:?}"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        Some("stopped".to_owned()),
        "the claim survives, so a later tick can still finish if the start fails"
    );
    Ok(())
}

/// A sink with stopped-resident evacuation **off**, for the cases about what a
/// cluster that has not opted in does.
fn disabled_sink(pool: PgPool) -> SubstrateSink {
    let mut sink = teardown_sink(pool);
    sink.evacuate_stopped_residents = false;
    sink
}

/// **The sweep must reach the sandbox that is on no node before the ones that
/// still have a copy.**
///
/// Under budget pressure `spawn_restores` fills its slots in list order, so
/// list position decides who waits. A torn-down stopped sandbox is the only
/// population in the list with no live copy anywhere — its owner's `start` is a
/// hard 503 until it is placed — while a suspend evacuation still has its
/// snapshot and its owner gets a retryable error.
///
/// The fixture is the shape that inverts it: a sandbox stopped weeks ago and
/// evacuated moments ago, against a suspend evacuation from a minute ago.
/// Ordering on `lifecycle_changed_at` for both would sort the stopped row last,
/// because its claim and its teardown deliberately change no lifecycle and the
/// column still holds the user's original stop.
#[tokio::test(flavor = "current_thread")]
async fn the_sweep_reaches_a_torn_down_sandbox_before_one_that_still_has_a_copy() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();

    // Stopped weeks ago, evacuated moments ago: nowhere, and unreachable.
    let long_stopped = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, long_stopped, node_id, pin, "ready").await?;
    clear_placement(&db.pool, long_stopped).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - make_interval(weeks => 3),
                placement_changed_at = NOW()
          WHERE id = $1"
    ))
    .bind(long_stopped)
    .execute(&db.pool)
    .await?;

    // Suspended a minute ago: still restorable from its own snapshot.
    let fresh_suspend = insert_sandbox(&db.pool, None, "suspended").await?;
    let suspend_pin = LocalArtifactId::new();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'started',
                lifecycle_changed_at = NOW() - make_interval(mins => 1)
          WHERE id = $1"
    ))
    .bind(fresh_suspend)
    .execute(&db.pool)
    .await?;
    set_current_artifact(&db.pool, fresh_suspend, suspend_pin, ArtifactKind::Full).await?;
    insert_backup(
        &db.pool,
        fresh_suspend,
        suspend_pin,
        ArtifactKind::Full,
        "full",
        "ready",
        false,
    )
    .await?;

    let order: Vec<SandboxId> = restore_pending(&db.pool)
        .await?
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        order,
        vec![long_stopped, fresh_suspend],
        "the torn-down sandbox must be dispatched first — it became the sweep's \
         responsibility a moment ago and has no copy anywhere, while the suspend \
         evacuation ahead of it on a lifecycle-only ordering still has its snapshot"
    );
    Ok(())
}

/// The age the gauge would publish, which is `0` when nothing is waiting — the
/// mapping the emitter does. Cases that care *which* row is oldest assert on
/// `stopped_evacuation_stall` directly.
async fn stall_secs(sink: &SubstrateSink) -> TestResult<u64> {
    Ok(sink
        .stopped_evacuation_stall()
        .await?
        .map_or(0, |stall| stall.age.as_secs()))
}

/// **A wedged evacuation has to be findable.** The whole safety argument for
/// this path is that it fails closed — and failing closed is only a defence if
/// someone finds out, which for a stall *before* the teardown nothing else
/// reports: the pending gauge counts only unplaced rows, and
/// `stopped_evacuation{outcome=awaiting_durable_disk}` looks the same on a
/// healthy multi-gigabyte upload as on a gate that will never open.
///
/// The gauge selects on the marker, so the fixture seeds every shape that
/// carries one or is adjacent to one, and none of the assertions below can hold
/// by accident.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_evacuation_wedged_before_its_teardown_reports_an_age() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = teardown_sink(db.pool.clone());
    let node_id = forced_draining_node(&db.pool).await?;
    // The drain itself started five hours ago. That is the fixture's lever, not
    // scenery: `drain_started_at` is the fallback clock, so aging it gives every
    // *unselected* row below a large age it would report if the selection were
    // wrong. Left at the insert's `NOW()` the zeros would all hold for the wrong
    // reason.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_started_at = NOW() - make_interval(hours => 5) WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    // Never claimed: not a stalled evacuation. Thanks to the drain clock above,
    // a query that selected the node's whole stopped inventory would report five
    // hours here rather than nothing.
    let _unclaimed = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    assert_eq!(
        stall_secs(&sink).await?,
        0,
        "an unclaimed stopped resident is not a stalled evacuation — selecting every \
         stopped resident on a draining node would report the drain's own age on the \
         node's whole inventory the moment a drain starts"
    );

    // Already torn down: the pending gauge owns this one, not the stall age.
    let torn_down = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let torn_pin = LocalArtifactId::new();
    claim_and_publish(&db.pool, torn_down, node_id, torn_pin, "ready").await?;
    clear_placement(&db.pool, torn_down).await?;
    assert_eq!(
        stall_secs(&sink).await?,
        0,
        "an evacuation that finished its teardown is the pending gauge's to report; \
         counting it here would double-page the same sandbox"
    );

    // Claimed, published two hours ago, still not durable: this is the wedge.
    let wedged = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let wedged_pin = LocalArtifactId::new();
    claim_and_publish(&db.pool, wedged, node_id, wedged_pin, "creating").await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = NOW() - make_interval(hours => 2)
          WHERE sandbox_id = $1"
    ))
    .bind(wedged)
    .execute(&db.pool)
    .await?;

    let stalled = stall_secs(&sink).await?;
    assert!(
        (7000..7400).contains(&stalled),
        "the age must be measured from when the publish reached the cluster (~7200s), so a \
         monitor can pick its own window; got {stalled}"
    );

    // A retried publish registers a *second* catalog row for the same artifact
    // — `insert_snapshot` conflicts only on `id` — so the age has to come from
    // the earliest, not the latest. Taking the latest would shorten the
    // reported stall on exactly the sandboxes that have been retried most,
    // which is backwards, and it is invisible until two rows exist.
    insert_backup(
        &db.pool,
        wedged,
        wedged_pin,
        ArtifactKind::Disk,
        "disk",
        "creating",
        false,
    )
    .await?;
    let stalled = stall_secs(&sink).await?;
    assert!(
        (7000..7400).contains(&stalled),
        "a re-registration must not reset the age — the earliest row is the one that says \
         how long this upload has been in flight; got {stalled}"
    );

    // And a claim with no artifact at all still reports, from the drain clock
    // set at the top — otherwise the shape that never publishes is the one
    // nobody sees. It is also older than the wedged row, so it must win.
    let never_published = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, never_published, node_id).await?;
    let stalled = stall_secs(&sink).await?;
    assert!(
        stalled > 17000,
        "a claim that never published must fall back to the drain's own clock (~18000s), \
         and being the oldest it must be the value published; got {stalled}"
    );
    Ok(())
}

/// **The page has to name a row, and it has to be the row it measured.**
///
/// The gauge is a cluster-wide age with no tags, and both the stall and the
/// refusal log at `debug`, which production does not emit. So a responder woken
/// by the documented alert holds a number and nothing to open. The query
/// therefore returns the row it measured — and it has to be *that* row, not
/// merely some stalled one, or the log and the gauge describe different
/// incidents.
///
/// Two claimed evacuations with different ages, the older seeded second, so an
/// implementation that returned the first row it found fails.
#[tokio::test(flavor = "current_thread")]
async fn the_stall_names_the_row_whose_age_it_published() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = teardown_sink(db.pool.clone());
    assert_eq!(
        sink.stopped_evacuation_stall().await?,
        None,
        "with nothing waiting there is no row to name, and the caller publishes the zero"
    );

    let node_id = forced_draining_node(&db.pool).await?;
    let younger = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(
        &db.pool,
        younger,
        node_id,
        LocalArtifactId::new(),
        "creating",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = NOW() - make_interval(hours => 2)
          WHERE sandbox_id = $1"
    ))
    .bind(younger)
    .execute(&db.pool)
    .await?;
    let stall = sink
        .stopped_evacuation_stall()
        .await?
        .expect("a claimed evacuation mid-upload is waiting");
    assert_eq!(
        (stall.sandbox_id, stall.node_id),
        (younger, node_id),
        "the only waiting row must be named, on its node — that is what a responder needs, \
         since the gauge itself carries no tags"
    );

    // An older claim on the same node. It is the one the age now describes, so
    // it must also be the one named.
    let older = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, older, node_id, LocalArtifactId::new(), "creating").await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = NOW() - make_interval(hours => 9)
          WHERE sandbox_id = $1"
    ))
    .bind(older)
    .execute(&db.pool)
    .await?;
    let stall = sink
        .stopped_evacuation_stall()
        .await?
        .expect("both claims are waiting");
    assert_eq!(
        stall.sandbox_id, older,
        "the older row is the one the age describes, so it is the one that must be named — \
         reporting the age of one sandbox beside the identity of another sends a responder \
         to a healthy row"
    );
    assert!(
        stall.age.as_secs() > 32000,
        "and the named row's age must be the published age (~32400s); got {}",
        stall.age.as_secs()
    );

    // Two rows of *equal* age. The age is an integer number of seconds, so this
    // is not a contrived state — two evacuations that publish in the same second
    // compare equal — and without a tiebreak the row named would be whichever
    // the planner happened to return, so the paced WARN could finger a different
    // sandbox each hour for one unchanged incident. Asserting the tiebreak's
    // direction rather than merely that *some* row is named is the difference
    // between pinning determinism and pinning nothing.
    let twin = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(&db.pool, twin, node_id, LocalArtifactId::new(), "creating").await?;
    sqlx::query(sql!(
        "UPDATE snapshots sn
            SET timestamp = (
                SELECT MIN(inner_sn.timestamp) FROM snapshots inner_sn
                 WHERE inner_sn.sandbox_id = $2
            )
          WHERE sn.sandbox_id = $1"
    ))
    .bind(twin)
    .bind(older)
    .execute(&db.pool)
    .await?;
    let expected = older.max(twin);
    for attempt in 1..=5 {
        let stall = sink
            .stopped_evacuation_stall()
            .await?
            .expect("both equal-aged claims are waiting");
        assert_eq!(
            stall.sandbox_id, expected,
            "attempt {attempt}: two claims of equal age must resolve to the same row every \
             time — the higher id, per the `ORDER BY`'s tiebreak — or a page names a \
             different sandbox each time it fires for one incident"
        );
    }
    Ok(())
}

/// **A gauge whose documented use is "alert on it" must be silent where the
/// condition cannot occur.**
///
/// The stall this reports is a *claim* that is not progressing, and a claim can
/// only be opened on a forced drain. A marker that outlives an `:undrain` is
/// drift the claim refuses as `Deferred`, not a stall — and reporting it would
/// mean the first gentle drain of a node holding one marked row pages an
/// on-call against a documented one-hour window.
///
/// The unmarked row is the other half: it is what a query that had widened
/// beyond the marker would select, and a pinned one is seeded so that widening
/// would produce a large age rather than a zero.
#[tokio::test(flavor = "current_thread")]
async fn the_stall_age_is_silent_where_no_claim_could_have_run() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = teardown_sink(db.pool.clone());
    let node_id = forced_draining_node(&db.pool).await?;

    // Unmarked, but carrying a cluster pin with a nine-hour-old catalog row.
    // Nothing on the `managed_by = 'k8s'` nodes this reconciler drains writes
    // such a pin — this fixture is doing by hand what production cannot — so the
    // row's only purpose is to make a widened selection loud instead of silent.
    let unmarked = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let foreign_pin = LocalArtifactId::new();
    set_current_artifact(&db.pool, unmarked, foreign_pin, ArtifactKind::Disk).await?;
    insert_backup(
        &db.pool,
        unmarked,
        foreign_pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = NOW() - make_interval(hours => 9)
          WHERE sandbox_id = $1"
    ))
    .bind(unmarked)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        marker_of(&db.pool, unmarked).await?,
        None,
        "fixture precondition: unmarked, or this case would be the claimed one"
    );
    assert_eq!(
        stall_secs(&sink).await?,
        0,
        "an unmarked row is not a stalled claim, however old its artifact looks. The \
         refusal that this gauge deliberately does not cover — a node-reported pin — \
         leaves no cluster pin at all, so widening to this column would add noise and \
         still miss the case it was meant to catch"
    );

    // The reference non-zero: a real claim on the same forced drain.
    let claimed = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim_and_publish(
        &db.pool,
        claimed,
        node_id,
        LocalArtifactId::new(),
        "creating",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = NOW() - make_interval(hours => 3)
          WHERE sandbox_id = $1"
    ))
    .bind(claimed)
    .execute(&db.pool)
    .await?;
    assert!(
        stall_secs(&sink).await? > 10000,
        "the shape the scoping must not suppress: a claimed evacuation on a forced drain, \
         mid-upload, has to report its age (~10800s)"
    );

    // Same rows, node gently draining. `handle_draining_node` lists no
    // evacuatable residents under gentle, so no claim could be progressing —
    // and a hold there is `node_drain.termination_blocking_residents`' to
    // report, not this series'.
    sqlx::query(sql!("UPDATE nodes SET drain_mode = 'gentle' WHERE id = $1"))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
    assert_eq!(
        stall_secs(&sink).await?,
        0,
        "a gentle drain attempts no evacuation, so it can hold no stalled one — the first \
         gentle drain of a node must not page on a marker left behind by an \
         earlier forced episode"
    );
    Ok(())
}

/// **The value must never be a reassuring zero.** Every clock this gauge can
/// reach is nullable, and SQL arithmetic over NULL yields NULL, which the outer
/// `COALESCE` would turn into `0` — publishing "nothing is stuck" for the one
/// node nobody is watching. The fixture is the shape where the first two clocks
/// are both absent: a claimed evacuation that never published, on a draining
/// node carrying no start time.
#[tokio::test(flavor = "current_thread")]
async fn the_stall_age_never_resolves_to_a_reassuring_zero() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let sink = teardown_sink(db.pool.clone());
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, sandbox, node_id).await?;

    // Neither the artifact clock (never published) nor the drain clock exists.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_started_at = NULL WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - make_interval(hours => 7)
          WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;

    let stalled = stall_secs(&sink).await?;
    assert!(
        stalled > 24000,
        "with no artifact clock and no drain clock the gauge must fall through to when the \
         sandbox became `stopped` (~25200s) rather than report zero; got {stalled}"
    );
    Ok(())
}
