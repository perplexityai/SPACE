use super::*;
use pplx_space_spaced_update::journal::Restart;
use pplx_space_spaced_update::{ArtifactDigest, Attempt, Selection, Slot, TrialProgress};
use uuid::Uuid;

#[test]
fn recovery_retries_stopped_services_and_bounds_uncommitted_or_unready_generations() {
    let boot = Uuid::now_v7();
    let attempt = Attempt {
        operation: Uuid::now_v7(),
        execution: Uuid::now_v7(),
        boot,
        incarnation: Uuid::now_v7(),
        configuration_revision: 1,
        target: Selection {
            slot: Slot::B,
            digest: ArtifactDigest::of_bytes(b"candidate"),
        },
        confirmation_deadline_ms: 100,
    };
    let restart = Restart {
        operation: attempt.operation,
        execution: attempt.execution,
        boot,
        incarnation: attempt.incarnation,
    };
    let states = [
        (JournalState::Stable, false),
        (
            JournalState::Staged {
                attempt: attempt.clone(),
            },
            false,
        ),
        (
            JournalState::Armed {
                attempt: attempt.clone(),
            },
            true,
        ),
        (
            JournalState::Trial {
                attempt: attempt.clone(),
                progress: TrialProgress::Starting,
            },
            false,
        ),
        (
            JournalState::Trial {
                attempt: attempt.clone(),
                progress: TrialProgress::AwaitingHost,
            },
            false,
        ),
        (
            JournalState::Committed {
                attempt: attempt.clone(),
            },
            false,
        ),
        (
            JournalState::RolledBack {
                attempt: attempt.clone(),
                progress: TrialProgress::AwaitingHost,
            },
            false,
        ),
        (JournalState::RecoveryRequired, false),
        (
            JournalState::Restarting {
                restart: restart.clone(),
            },
            true,
        ),
        (JournalState::Restarted { restart }, false),
    ];
    for (state, interrupt_live) in &states {
        // Failed StartService/driver startup and a missed failure task must all
        // retain the periodic retry owner, including after rollback settled.
        assert!(
            needs_recovery(Some(state), false, false, false, boot, 50),
            "stopped {state:?}"
        );
        assert!(
            needs_recovery(Some(state), true, true, false, boot, 50),
            "hung startup {state:?}"
        );
        assert_eq!(
            needs_recovery(Some(state), true, false, false, boot, 50),
            *interrupt_live,
            "live {state:?}"
        );
    }
    for progress in [TrialProgress::Starting, TrialProgress::AwaitingHost] {
        let trial = JournalState::Trial {
            attempt: attempt.clone(),
            progress,
        };
        assert!(needs_recovery(Some(&trial), true, false, false, boot, 100));
        assert!(needs_recovery(
            Some(&trial),
            true,
            false,
            false,
            Uuid::now_v7(),
            50
        ));
    }
    let rollback = JournalState::RolledBack {
        attempt,
        progress: TrialProgress::Starting,
    };
    assert!(needs_recovery(Some(&rollback), true, false, true, boot, 50));
    assert!(needs_recovery(None, false, false, true, boot, 50));
    assert!(!needs_recovery(None, true, false, true, boot, 50));
}

#[test]
fn startup_deadlines_follow_generations_without_extending_on_progress() {
    let old = StartDeadline {
        process_id: 10,
        creation_time: 100,
        deadline_ms: 1_000,
    };
    for (previous, pid, created, now, expected) in [
        (None, 10, 100, 500, 30_500),
        (Some(old), 10, 100, 500, 1_000),
        (Some(old), 10, 100, 1_001, 1_000),
        (Some(old), 11, 200, 1_001, 31_001),
        // Reused PID is a different generation even when an old deadline exists.
        (Some(old), 10, 200, 1_001, 31_001),
    ] {
        let deadline = deadline_for_start(previous, pid, created, now);
        assert_eq!(deadline.deadline_ms, expected);
        assert_eq!(deadline.process_id, pid);
        assert_eq!(deadline.creation_time, created);
        assert_eq!(
            deadline_for_start(Some(deadline), pid, created, now + 60_000),
            deadline
        );
    }
}
