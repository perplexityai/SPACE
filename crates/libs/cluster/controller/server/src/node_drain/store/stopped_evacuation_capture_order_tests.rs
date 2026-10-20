//! Postgres-backed coverage for the node's capture-order evidence on a stopped
//! evacuation.
//!
//! The property: **a stopped sandbox whose node already backed up its current
//! disk can be evacuated on that backup, and one whose node can't show its
//! backup is that recent keeps its disk.** The node reports both when its row
//! entered `stopped` and when the pinned artifact was captured, from its own
//! clock. Every refusal below is seeded with a ready backup, so destroying the
//! local copy was possible, and sits beside a case on the same shape that does
//! release.

use super::stopped_evacuation_tests::{
    TestResult, enqueued, forced_draining_node, insert_backup, insert_sandbox, marker_of,
};
use super::*;

const STOPPED_AT: i64 = 1_700_000_000;

fn at(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos: 0 })
}

/// What a node reports for a stopped, pinned sandbox whose row entered
/// `stopped` at `stopped_since` and whose pin was captured at `captured`.
fn node_pinned(
    pin: LocalArtifactId,
    stopped_since: Option<prost_types::Timestamp>,
    captured: Option<prost_types::Timestamp>,
) -> NodeReportedArtifact {
    NodeReportedArtifact {
        present: true,
        pinned_artifact: Some(pin),
        reports_stopped: true,
        stopped_since,
        pin_captured_at: captured,
        ..NodeReportedArtifact::default()
    }
}

/// A stopped resident on a force-draining node whose node pins `pin`, with that
/// artifact's disk backup already `ready` — the state the stop-time backup
/// leaves behind. The cluster row stays pinless, as it is for every ordinarily
/// stopped sandbox.
async fn stopped_with_durable_node_pin(
    pool: &PgPool,
    node_id: NodeId,
    pin: LocalArtifactId,
) -> TestResult<SandboxId> {
    let sandbox = insert_sandbox(pool, Some(node_id), "stopped").await?;
    insert_backup(
        pool,
        sandbox,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    Ok(sandbox)
}

/// **A pin captured since the sandbox stopped is evacuated on; one the node
/// can't show is that recent is not.**
///
/// Accepted: captured in a later second than the row entered `stopped`, or in
/// the stop's own second, which is where the stop-time backup's mint usually
/// lands. The evacuation opens, reuses the node's pin, finds its backup
/// already durable and releases the node in the same tick, with no new upload.
///
/// Refused, each with the same durable backup: captured before the stop (the
/// guest may have written since), no capture time (every pin on a node too old
/// to report one), no stopped-since time, and a negative stopped-since time,
/// which only a corrupt row could hold. A refusal names the pin, doesn't
/// claim the row, publishes nothing and destroys nothing.
#[tokio::test(flavor = "current_thread")]
async fn only_a_pin_captured_since_the_stop_opens_a_stopped_evacuation() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    for (shape, stopped_since, captured, opens) in [
        (
            "captured after the stop",
            at(STOPPED_AT),
            at(STOPPED_AT + 1),
            true,
        ),
        (
            "captured before the stop",
            at(STOPPED_AT),
            at(STOPPED_AT - 1),
            false,
        ),
        (
            "captured in the stop's second",
            at(STOPPED_AT),
            at(STOPPED_AT),
            true,
        ),
        ("no capture time reported", at(STOPPED_AT), None, false),
        (
            "no stopped-since time reported",
            None,
            at(STOPPED_AT + 1),
            false,
        ),
        // A corrupt stop time before the epoch reads as older than every
        // capture; it has to be refused, not taken as proof the pin is current.
        (
            "a negative stopped-since time",
            at(-1),
            at(STOPPED_AT + 1),
            false,
        ),
    ] {
        let pin = LocalArtifactId::new();
        let sandbox = stopped_with_durable_node_pin(&db.pool, node_id, pin).await?;
        let reported = node_pinned(pin, stopped_since, captured);
        let sink = SubstrateSink::with_stopped_path_node_states(
            db.pool.clone(),
            enqueued(pin),
            reported.clone(),
            Some(reported),
        );

        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

        if opens {
            assert!(
                matches!(
                    outcome,
                    StoppedEvacuationOutcome::Released { local_artifact_id }
                        if local_artifact_id == Some(pin)
                ),
                "{shape}: the pin describes the disk being given up and its backup is \
                 already durable; got {outcome:?}"
            );
            assert_eq!(sink.recorded_teardowns(), vec![sandbox], "{shape}");
            assert_eq!(
                sink.publish_calls(),
                1,
                "{shape}: one publish, of the node's pin"
            );
            assert_eq!(
                marker_of(&db.pool, sandbox).await?,
                Some("stopped".to_owned()),
                "{shape}"
            );
        } else {
            assert_eq!(
                sink.recorded_teardowns(),
                Vec::<SandboxId>::new(),
                "{shape}: the backup is durable, so only the guard keeps this disk"
            );
            assert!(
                matches!(
                    outcome,
                    StoppedEvacuationOutcome::UnattributableArtifact { local_artifact_id }
                        if local_artifact_id == Some(pin)
                ),
                "{shape}: the refusal must name the node's pin; got {outcome:?}"
            );
            assert_eq!(sink.publish_calls(), 0, "{shape}: nothing may be published");
            assert_eq!(
                marker_of(&db.pool, sandbox).await?,
                None,
                "{shape}: the row stays unclaimed, or the next tick would resume past \
                 this check"
            );
        }
    }
    Ok(())
}

/// **A pin the node reports without a capture time keeps the disk, however
/// durable its bytes.**
///
/// That's how the node reports an older binary's pin, including one it kept
/// across a rollback: the pin names the pre-rollback bytes and their backup is
/// already durable, so an evacuation that reused it would publish those bytes
/// and give up the rolled-back rootfs. The evacuation refuses it, publishes
/// nothing, claims nothing and destroys nothing. The cluster can't see a
/// rollback; it acts on the report. The contrasting arm shows that the same
/// shape reported with a capture time opens and releases, which on a node
/// that drops pins on rollback means the pin is current.
#[tokio::test(flavor = "current_thread")]
async fn a_pin_reported_without_a_capture_time_keeps_the_disk() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    for (shape, captured, released) in [
        ("reported without a capture time", None, false),
        ("reported with a capture time", at(STOPPED_AT + 10), true),
    ] {
        let pre_rollback = LocalArtifactId::new();
        let sandbox = stopped_with_durable_node_pin(&db.pool, node_id, pre_rollback).await?;
        let reported = node_pinned(pre_rollback, at(STOPPED_AT), captured);
        let sink = SubstrateSink::with_stopped_path_node_states(
            db.pool.clone(),
            enqueued(pre_rollback),
            reported.clone(),
            Some(reported),
        );

        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

        assert_eq!(
            sink.recorded_teardowns() == vec![sandbox],
            released,
            "{shape}: got {outcome:?}"
        );
        if released {
            assert!(
                matches!(outcome, StoppedEvacuationOutcome::Released { .. }),
                "{shape}: got {outcome:?}"
            );
        } else {
            assert!(
                matches!(
                    outcome,
                    StoppedEvacuationOutcome::UnattributableArtifact { local_artifact_id }
                        if local_artifact_id == Some(pre_rollback)
                ),
                "{shape}: got {outcome:?}"
            );
            assert_eq!(sink.publish_calls(), 0, "{shape}");
            assert_eq!(marker_of(&db.pool, sandbox).await?, None, "{shape}");
        }
    }
    Ok(())
}

/// **Drift defers, whatever the pin's times say.** A node that reports the
/// sandbox not stopped, or its VM alive, is the cluster's stale view, which
/// clears on its own; it must not land on `unattributable_artifact`, which the
/// metrics catalog documents as never self-resolving. The pin here was
/// captured before the row's stop, so it would be refused if the sandbox were
/// stopped, which the last arm confirms.
#[tokio::test(flavor = "current_thread")]
async fn drift_defers_even_when_the_pin_would_be_refused() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;

    for (shape, reports_stopped, vm_alive, unattributable) in [
        ("not stopped on the node", false, false, false),
        ("its VM alive", true, true, false),
        ("stopped, quiet VM", true, false, true),
    ] {
        let pin = LocalArtifactId::new();
        let sandbox = stopped_with_durable_node_pin(&db.pool, node_id, pin).await?;
        let reported = NodeReportedArtifact {
            reports_stopped,
            vm_alive,
            ..node_pinned(pin, at(STOPPED_AT), at(STOPPED_AT - 1))
        };
        let sink = SubstrateSink::with_stopped_path_node_states(
            db.pool.clone(),
            enqueued(pin),
            reported.clone(),
            Some(reported),
        );

        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

        assert_eq!(
            matches!(
                outcome,
                StoppedEvacuationOutcome::UnattributableArtifact { .. }
            ),
            unattributable,
            "{shape}: got {outcome:?}"
        );
        assert_eq!(
            matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
            !unattributable,
            "{shape}: got {outcome:?}"
        );
        assert_eq!(
            sink.recorded_teardowns(),
            Vec::<SandboxId>::new(),
            "{shape}"
        );
        assert_eq!(sink.publish_calls(), 0, "{shape}");
    }
    Ok(())
}

/// **The teardown is decided by the pin's identity, not its capture order.**
///
/// An evacuation opens on a pin captured after the stop. Before the teardown,
/// the node's row is restamped while it keeps the pin: a start refused before
/// it launched does that, and leaves the rootfs untouched. The pin now reads as
/// captured before the stop, and the node is still released, because refusing
/// would hold the claim and its node forever. On a node that reports capture
/// times, a restamp that changes the rootfs drops the pin instead, and that's
/// refused: the same claim with the node reporting no pin defers and keeps
/// its marker.
#[tokio::test(flavor = "current_thread")]
async fn the_teardown_is_decided_by_the_pins_identity_not_its_capture_order() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let captured = STOPPED_AT + 1;
    let restamped = at(captured + 30);

    for (shape, kept_pin, released) in [
        ("restamped, pin kept", true, true),
        ("restamped, pin dropped", false, false),
    ] {
        let pin = LocalArtifactId::new();
        let sandbox = stopped_with_durable_node_pin(&db.pool, node_id, pin).await?;
        let teardown = NodeReportedArtifact {
            pinned_artifact: kept_pin.then_some(pin),
            pin_captured_at: if kept_pin { at(captured) } else { None },
            ..node_pinned(pin, restamped, at(captured))
        };
        let sink = SubstrateSink::with_stopped_path_node_states(
            db.pool.clone(),
            enqueued(pin),
            node_pinned(pin, at(STOPPED_AT), at(captured)),
            Some(teardown),
        );

        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;

        assert_eq!(
            matches!(outcome, StoppedEvacuationOutcome::Released { .. }),
            released,
            "{shape}: got {outcome:?}"
        );
        assert_eq!(
            sink.recorded_teardowns() == vec![sandbox],
            released,
            "{shape}"
        );
        if !released {
            assert!(
                matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
                "{shape}: got {outcome:?}"
            );
            assert_eq!(
                marker_of(&db.pool, sandbox).await?,
                Some("stopped".to_owned()),
                "{shape}: the claim stays, so the drain keeps holding the node"
            );
        }
    }
    Ok(())
}
