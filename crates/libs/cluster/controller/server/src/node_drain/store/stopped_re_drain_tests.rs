//! A re-placed stopped sandbox drained again before anyone started it.
//!
//! The property: **a cluster pin opens a fresh stopped evacuation only when it
//! names a ready, durable disk backup of this sandbox and the node pins the
//! same artifact and shows it current; then the node is released on that
//! backup without publishing anything.** Every refusal below keeps the disk,
//! claims nothing and publishes nothing, and each sits beside the accepted
//! shape so the refusal is the clause's.

use super::stopped_evacuation_tests::{
    TestResult, enqueued, forced_draining_node, insert_backup, insert_sandbox, marker_of,
};
use super::*;

const STOPPED_AT: i64 = 1_700_000_000;

fn at(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos: 0 })
}

/// What the destination of a stopped re-placement reports: the sandbox stopped
/// and pinning `pin`, recorded as its rootfs was staged, at `captured`.
fn node_pinning(
    pin: Option<LocalArtifactId>,
    captured: Option<prost_types::Timestamp>,
) -> NodeReportedArtifact {
    NodeReportedArtifact {
        present: true,
        pinned_artifact: pin,
        reports_stopped: true,
        stopped_since: at(STOPPED_AT),
        pin_captured_at: captured,
        ..NodeReportedArtifact::default()
    }
}

/// The backup row a stopped re-placement was materialized from, in the shape
/// a case needs.
#[derive(Clone, Copy)]
enum Backup {
    /// Ready, with a live storage location: durable.
    Durable,
    /// Still uploading.
    NotReady,
    /// Ready, but its only storage location is orphaned.
    Orphaned,
    /// Ready and durable, but recorded for a different sandbox.
    OtherSandbox,
}

/// A stopped resident of a force-draining node whose cluster row pins `pin`,
/// as a stopped re-placement's finalize leaves it, with `backup` describing
/// that artifact's backup row.
async fn re_placed(
    pool: &PgPool,
    node_id: NodeId,
    pin: LocalArtifactId,
    backup: Backup,
) -> TestResult<SandboxId> {
    let sandbox = insert_sandbox(pool, Some(node_id), "stopped").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(pin)
    .execute(pool)
    .await?;
    let (owner, state, orphaned) = match backup {
        Backup::Durable => (sandbox, "ready", false),
        Backup::NotReady => (sandbox, "creating", false),
        Backup::Orphaned => (sandbox, "ready", true),
        Backup::OtherSandbox => (insert_sandbox(pool, None, "stopped").await?, "ready", false),
    };
    insert_backup(
        pool,
        owner,
        pin,
        ArtifactKind::Disk,
        "disk",
        state,
        orphaned,
    )
    .await?;
    Ok(sandbox)
}

/// **The re-placement's own pin opens the evacuation and releases the node on
/// its backup, with nothing published.** The cluster row and the node pin the
/// same artifact, the node shows it current, and its backup is durable: the
/// claim takes the row, the teardown runs, and the publish is never called.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placed_sandbox_drained_again_is_released_on_its_own_backup() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let pin = LocalArtifactId::new();
    let sandbox = re_placed(&db.pool, node_id, pin, Backup::Durable).await?;
    let reported = node_pinning(Some(pin), at(STOPPED_AT));
    let sink = SubstrateSink::with_stopped_path_node_states(
        db.pool.clone(),
        enqueued(LocalArtifactId::new()),
        reported.clone(),
        Some(reported),
    );

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::Released { local_artifact_id }
                if local_artifact_id == Some(pin)
        ),
        "the re-placed pin's backup is durable and the node holds those bytes; got {outcome:?}"
    );
    assert_eq!(sink.recorded_teardowns(), vec![sandbox]);
    assert_eq!(
        sink.publish_calls(),
        0,
        "the backup is already durable, so nothing may be published"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?,
        Some("stopped".to_owned())
    );
    Ok(())
}

/// What the node pins, relative to the cluster row's pin.
#[derive(Clone, Copy)]
enum NodePin {
    Same,
    Dropped,
    Other,
}

/// **Every other pinned shape keeps the disk.** Each case differs from the
/// accepted one in one respect:
///
/// * started since, by the node's clock: the node still pins the artifact but
///   its row entered `stopped` after it was captured, so the guest may have
///   written since;
/// * started since, pin dropped: a start cleared the node's pin, so the cluster
///   pin no longer describes the rootfs;
/// * the node pins a different artifact;
/// * the cluster pin's backup isn't ready, isn't durable, or belongs to another
///   sandbox.
///
/// Each refusal names the cluster pin, claims nothing, publishes nothing and
/// destroys nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_cluster_pin_that_cant_be_attributed_keeps_the_disk() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    for (shape, backup, node_pin, captured) in [
        (
            "started since, by the node's clock",
            Backup::Durable,
            NodePin::Same,
            at(STOPPED_AT - 1),
        ),
        (
            "started since, the start dropped the node's pin",
            Backup::Durable,
            NodePin::Dropped,
            None,
        ),
        (
            "the node pins another artifact",
            Backup::Durable,
            NodePin::Other,
            at(STOPPED_AT),
        ),
        (
            "its backup isn't ready",
            Backup::NotReady,
            NodePin::Same,
            at(STOPPED_AT),
        ),
        (
            "its backup isn't durable",
            Backup::Orphaned,
            NodePin::Same,
            at(STOPPED_AT),
        ),
        (
            "its backup is another sandbox's",
            Backup::OtherSandbox,
            NodePin::Same,
            at(STOPPED_AT),
        ),
    ] {
        let pin = LocalArtifactId::new();
        let sandbox = re_placed(&db.pool, node_id, pin, backup).await?;
        let reported_pin = match node_pin {
            NodePin::Same => Some(pin),
            NodePin::Dropped => None,
            NodePin::Other => Some(LocalArtifactId::new()),
        };
        let reported = node_pinning(reported_pin, captured);
        let sink = SubstrateSink::with_stopped_path_node_states(
            db.pool.clone(),
            enqueued(LocalArtifactId::new()),
            reported.clone(),
            Some(reported),
        );

        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

        assert!(
            matches!(
                outcome,
                StoppedEvacuationOutcome::UnattributableArtifact { local_artifact_id }
                    if local_artifact_id == Some(pin)
            ),
            "{shape}: the refusal must name the cluster pin; got {outcome:?}"
        );
        assert_eq!(
            sink.recorded_teardowns(),
            Vec::<SandboxId>::new(),
            "{shape}: nothing may be torn down"
        );
        assert_eq!(sink.publish_calls(), 0, "{shape}: nothing may be published");
        assert_eq!(
            marker_of(&db.pool, sandbox).await?,
            None,
            "{shape}: the row stays unclaimed"
        );
    }
    Ok(())
}

/// **The claim admits the matched pin only, and only while its backup is
/// durable.** The caller's node check happens a moment before the claim, so
/// the claim's own `UPDATE` re-asserts both halves: a pinned row isn't taken
/// for a different expected pin or for none, and the expected pin isn't taken
/// once its backup stops being durable. The same row is taken when both hold.
#[tokio::test(flavor = "current_thread")]
async fn the_claim_takes_a_pinned_row_only_for_its_own_durable_pin() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    let pin = LocalArtifactId::new();
    let durable = re_placed(&db.pool, node_id, pin, Backup::Durable).await?;
    for (shape, expected) in [
        ("no expected pin", None),
        ("another expected pin", Some(LocalArtifactId::new())),
    ] {
        assert_eq!(
            claim_stopped_evacuation(&db.pool, durable, node_id, true, expected).await?,
            StoppedEvacuationClaim::Refused,
            "{shape}"
        );
    }
    let not_durable_pin = LocalArtifactId::new();
    let not_durable = re_placed(&db.pool, node_id, not_durable_pin, Backup::Orphaned).await?;
    assert_eq!(
        claim_stopped_evacuation(&db.pool, not_durable, node_id, true, Some(not_durable_pin))
            .await?,
        StoppedEvacuationClaim::Refused,
        "a pin whose backup isn't durable"
    );
    assert_eq!(marker_of(&db.pool, durable).await?, None);
    assert_eq!(marker_of(&db.pool, not_durable).await?, None);

    assert_eq!(
        claim_stopped_evacuation(&db.pool, durable, node_id, true, Some(pin)).await?,
        StoppedEvacuationClaim::Claimed,
        "the matched, durable pin"
    );
    assert_eq!(
        marker_of(&db.pool, durable).await?,
        Some("stopped".to_owned())
    );
    Ok(())
}

/// **A re-placed sandbox that was started and stopped again drains again.**
/// The start cleared the cluster row's disk pin (the API's post-start write
/// drops a `disk` pin, as the node drops its own) and the stop minted a fresh
/// node pin, captured in the stop's own second. The evacuation then takes the
/// ordinary fresh path: it publishes the node's pin, finds its backup durable
/// and releases the node. Without the clear, the stale cluster pin names an
/// artifact the node no longer pins, and the same sandbox keeps the node
/// parked as unattributable on every tick.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placed_sandbox_started_and_stopped_since_drains_again() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    for (shape, start_cleared_the_pin) in [
        ("the start cleared the disk pin", true),
        ("the disk pin outlived the start", false),
    ] {
        let re_placed_pin = LocalArtifactId::new();
        let sandbox = re_placed(&db.pool, node_id, re_placed_pin, Backup::Durable).await?;
        if start_cleared_the_pin {
            sqlx::query(sql!(
                "UPDATE sandboxes
                    SET current_local_artifact_id = NULL,
                        current_local_artifact_kind = NULL,
                        current_local_artifact_origin = NULL
                  WHERE id = $1"
            ))
            .bind(sandbox)
            .execute(&db.pool)
            .await?;
        }
        // The stop after that start: a new pin, its backup already durable.
        let fresh_pin = LocalArtifactId::new();
        insert_backup(
            &db.pool,
            sandbox,
            fresh_pin,
            ArtifactKind::Disk,
            "disk",
            "ready",
            false,
        )
        .await?;
        let reported = node_pinning(Some(fresh_pin), at(STOPPED_AT));
        let sink = SubstrateSink::with_stopped_path_node_states(
            db.pool.clone(),
            enqueued(fresh_pin),
            reported.clone(),
            Some(reported),
        );

        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

        if start_cleared_the_pin {
            assert!(
                matches!(
                    outcome,
                    StoppedEvacuationOutcome::Released { local_artifact_id }
                        if local_artifact_id == Some(fresh_pin)
                ),
                "{shape}: the fresh pin describes the disk and its backup is durable; \
                 got {outcome:?}"
            );
            assert_eq!(sink.recorded_teardowns(), vec![sandbox], "{shape}");
        } else {
            assert!(
                matches!(
                    outcome,
                    StoppedEvacuationOutcome::UnattributableArtifact { local_artifact_id }
                        if local_artifact_id == Some(re_placed_pin)
                ),
                "{shape}: a stale cluster pin parks the node; got {outcome:?}"
            );
            assert_eq!(
                sink.recorded_teardowns(),
                Vec::<SandboxId>::new(),
                "{shape}"
            );
        }
    }
    Ok(())
}
