use super::*;
use pplx_space_sbx_mgr_client::proto::{
    SpacedProcessExit, SpacedProcessExited, SpacedProcessSignaled, SpacedProcessTimedOut,
    SpacedProcessUnknown, spaced_process_exit::Outcome,
};

#[test]
fn process_exit_status_maps_process_outcomes() {
    assert_eq!(
        process_exit_status(SpacedProcessExit {
            outcome: Some(Outcome::Exited(SpacedProcessExited { exit_code: 17 })),
        }),
        17
    );
    assert_eq!(
        process_exit_status(SpacedProcessExit {
            outcome: Some(Outcome::Signaled(SpacedProcessSignaled { signal: Some(9) })),
        }),
        137
    );
    assert_eq!(
        process_exit_status(SpacedProcessExit {
            outcome: Some(Outcome::TimedOut(SpacedProcessTimedOut {
                signal: Some(15)
            })),
        }),
        143
    );
    assert_eq!(
        process_exit_status(SpacedProcessExit {
            outcome: Some(Outcome::Signaled(SpacedProcessSignaled { signal: None })),
        }),
        1
    );
    assert_eq!(
        process_exit_status(SpacedProcessExit {
            outcome: Some(Outcome::Unknown(SpacedProcessUnknown {})),
        }),
        1
    );
    assert_eq!(process_exit_status(SpacedProcessExit { outcome: None }), 1);
}
