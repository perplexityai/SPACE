use pplx_space_common_core::types::SandboxLifecycle;
use pplx_space_sandbox_store::drain_restore::{DrainRestoreGateRow, DrainRestoreTarget};
use uuid::Uuid;

use super::{DrainRestoreGate, DrainRestoreRetry, DrainRestoreSuperseded};

mod executor;
mod supersession;

const S: Uuid = Uuid::from_u128(0x5);
const OTHER: Uuid = Uuid::from_u128(0x6);
const TARGET: DrainRestoreTarget = DrainRestoreTarget::Started;

/// A `suspended` row whose selector loads S: the claimable control.
fn claimable() -> DrainRestoreGateRow {
    DrainRestoreGateRow {
        lifecycle: "suspended".into(),
        evacuation_marked: false,
        restore_episode_target: None,
        selected_snapshot: Some(S),
        source_state: Some("ready".into()),
        source_located: true,
        newer_full_snapshot: false,
        source_is_current_artifact: true,
    }
}

fn decide(row: &DrainRestoreGateRow) -> DrainRestoreGate {
    DrainRestoreGate::decide(Some(row), S, TARGET).expect("the lifecycle parses")
}

/// Every lifecycle but `suspended` (and `restoring` in this target's
/// episode) supersedes the restore, `deleting` ends it as deleted, and the
/// control row is claimable.
#[test]
fn every_other_lifecycle_supersedes_the_restore() {
    for lifecycle in SandboxLifecycle::all() {
        let row = DrainRestoreGateRow {
            lifecycle: lifecycle.as_str().into(),
            ..claimable()
        };
        let expected = match lifecycle {
            SandboxLifecycle::Suspended => DrainRestoreGate::Claimable,
            SandboxLifecycle::Deleting => DrainRestoreGate::Deleted,
            // No episode target: another restore holds the row.
            SandboxLifecycle::Restoring => {
                DrainRestoreGate::Superseded(DrainRestoreSuperseded::OtherRestore)
            }
            _ => DrainRestoreGate::Superseded(DrainRestoreSuperseded::Lifecycle),
        };
        assert_eq!(decide(&row), expected, "{lifecycle}");
    }
    assert_eq!(
        DrainRestoreGate::decide(None, S, TARGET),
        Ok(DrainRestoreGate::Deleted)
    );
}

/// The row facts beside the lifecycle: this target's episode is claimable
/// (its re-drive), another's isn't, a marker hands the row to the v1 drain,
/// and the snapshot's state decides the rest.
#[test]
fn the_named_snapshot_and_the_episode_decide_the_rest() {
    let cases: [(&str, DrainRestoreGateRow, DrainRestoreGate); 10] = [
        ("control", claimable(), DrainRestoreGate::Claimable),
        (
            "this target's episode",
            DrainRestoreGateRow {
                lifecycle: "restoring".into(),
                restore_episode_target: Some(TARGET.as_str().into()),
                ..claimable()
            },
            DrainRestoreGate::Claimable,
        ),
        (
            "the other target's episode",
            DrainRestoreGateRow {
                lifecycle: "restoring".into(),
                restore_episode_target: Some(DrainRestoreTarget::Paused.as_str().into()),
                ..claimable()
            },
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::OtherRestore),
        ),
        (
            "a target left on a suspended row",
            DrainRestoreGateRow {
                restore_episode_target: Some(TARGET.as_str().into()),
                ..claimable()
            },
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::OtherRestore),
        ),
        (
            "the v1 marker",
            DrainRestoreGateRow {
                evacuation_marked: true,
                ..claimable()
            },
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::EvacuationMarker),
        ),
        (
            "a newer snapshot",
            DrainRestoreGateRow {
                selected_snapshot: Some(OTHER),
                newer_full_snapshot: true,
                ..claimable()
            },
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::NewerSnapshot),
        ),
        (
            "a later artifact",
            DrainRestoreGateRow {
                selected_snapshot: None,
                source_is_current_artifact: false,
                ..claimable()
            },
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::NewerSnapshot),
        ),
        (
            "S still uploading",
            DrainRestoreGateRow {
                selected_snapshot: None,
                source_state: Some("creating".into()),
                ..claimable()
            },
            DrainRestoreGate::SourcePending,
        ),
        (
            "S failed",
            DrainRestoreGateRow {
                selected_snapshot: None,
                source_state: Some("failed".into()),
                ..claimable()
            },
            DrainRestoreGate::SourceLost,
        ),
        (
            "S stored nowhere",
            DrainRestoreGateRow {
                selected_snapshot: None,
                source_located: false,
                ..claimable()
            },
            DrainRestoreGate::SourceLost,
        ),
    ];
    for (case, row, expected) in cases {
        assert_eq!(decide(&row), expected, "{case}");
    }
    let missing = DrainRestoreGateRow {
        selected_snapshot: None,
        source_state: None,
        source_located: false,
        ..claimable()
    };
    assert_eq!(decide(&missing), DrainRestoreGate::SourceLost, "S missing");
}

/// The backoff doubles from its base to its cap.
#[test]
fn failed_attempts_back_off_to_the_cap() {
    let retry = DrainRestoreRetry::with_max_attempts(16);
    let waits = (1..=7)
        .map(|attempt| retry.backoff(attempt).as_secs())
        .collect::<Vec<_>>();
    assert_eq!(waits, [60, 120, 240, 480, 960, 1800, 1800]);
}

/// A restore toward `stopped` waits on a `stopped` row (or re-drives its own
/// `stopped` episode) and loads the row's pinned disk: S claimable while it
/// is that disk, ready and stored; pending while it uploads; superseded once
/// the row pins another artifact; lost otherwise. A `suspended` row is
/// another restore's, and a `stopped` row toward a VM target has left the
/// lifecycle that target waits on.
#[test]
fn a_stopped_target_waits_on_a_stopped_row_and_its_pinned_disk() {
    let stopped = || DrainRestoreGateRow {
        lifecycle: "stopped".into(),
        // The full-snapshot selector reads nothing for a disk.
        selected_snapshot: None,
        ..claimable()
    };
    let cases: [(
        &str,
        DrainRestoreGateRow,
        DrainRestoreTarget,
        DrainRestoreGate,
    ); 8] = [
        (
            "control",
            stopped(),
            DrainRestoreTarget::Stopped,
            DrainRestoreGate::Claimable,
        ),
        (
            "its own episode",
            DrainRestoreGateRow {
                lifecycle: "restoring".into(),
                restore_episode_target: Some("stopped".into()),
                ..stopped()
            },
            DrainRestoreTarget::Stopped,
            DrainRestoreGate::Claimable,
        ),
        (
            "the disk uploading",
            DrainRestoreGateRow {
                source_state: Some("creating".into()),
                ..stopped()
            },
            DrainRestoreTarget::Stopped,
            DrainRestoreGate::SourcePending,
        ),
        (
            "another artifact pinned",
            DrainRestoreGateRow {
                source_is_current_artifact: false,
                ..stopped()
            },
            DrainRestoreTarget::Stopped,
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::NewerSnapshot),
        ),
        (
            "the disk stored nowhere",
            DrainRestoreGateRow {
                source_located: false,
                ..stopped()
            },
            DrainRestoreTarget::Stopped,
            DrainRestoreGate::SourceLost,
        ),
        (
            "a suspended row",
            claimable(),
            DrainRestoreTarget::Stopped,
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::OtherRestore),
        ),
        (
            "a stopped row toward started",
            stopped(),
            DrainRestoreTarget::Started,
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::Lifecycle),
        ),
        (
            "a started row",
            DrainRestoreGateRow {
                lifecycle: "started".into(),
                ..stopped()
            },
            DrainRestoreTarget::Stopped,
            DrainRestoreGate::Superseded(DrainRestoreSuperseded::Lifecycle),
        ),
    ];
    for (case, row, target, expected) in cases {
        assert_eq!(
            DrainRestoreGate::decide(Some(&row), S, target).expect("the lifecycle parses"),
            expected,
            "{case}"
        );
    }
}
