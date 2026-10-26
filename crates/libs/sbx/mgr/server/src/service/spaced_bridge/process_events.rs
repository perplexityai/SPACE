//! Process event conversion for sandbox-manager responses.

use super::*;

pub(in crate::service) fn process_info_to_spaced(
    info: host_proto::ProcessInfo,
) -> SpacedProcessInfo {
    SpacedProcessInfo {
        process_id: info.process_id,
        pid: info.pid,
        command: info.command,
        cwd: info.cwd,
        env: info.env,
        user: info.user,
        tag: info.tag,
        pty: info.pty,
        // Unlike a stream's exit event, a listed exit without an outcome is
        // reported as unknown: prost decodes an outcome added by a newer guest
        // as none, and one record must not fail the whole list.
        exit: info.exit.map(|exit| SpacedProcessExit {
            outcome: Some(exit.outcome.map_or(
                crate::proto::spaced_process_exit::Outcome::Unknown(SpacedProcessUnknown {}),
                process_outcome_to_spaced,
            )),
        }),
        ended_at: info.ended_at,
    }
}

// Decode boundary: reject structurally missing wire data (unset oneofs) as a
// protocol error. An explicit `HostOutcome::Unknown` is a valid wire value a
// server may set on purpose and is forwarded as `SpacedOutcome::Unknown`;
// genuinely unknown future oneof variants would decode to `None` under prost
// and hit the same error path, which is the desired behavior here.
pub(in crate::service) fn process_event_to_spaced(
    event: host_proto::ProcessEvent,
) -> Result<SpacedProcessEvent, Status> {
    use crate::proto::spaced_process_event::Event as SpacedEvent;
    use host_proto::process_event::Event as HostEvent;

    let host_event = event
        .event
        .ok_or_else(|| Status::internal("spaced host returned process event without payload"))?;
    let event_payload = match host_event {
        HostEvent::Started(started) => SpacedEvent::Started(started),
        HostEvent::Connected(connected) => SpacedEvent::Connected(SpacedProcessConnected {
            output_cursor: connected.output_cursor,
            resume_after_output_gap: connected.resume_after_output_gap,
        }),
        HostEvent::Stdout(output) => SpacedEvent::Stdout(SpacedProcessOutput { data: output.data }),
        HostEvent::Stderr(output) => SpacedEvent::Stderr(SpacedProcessOutput { data: output.data }),
        HostEvent::OutputGap(gap) => SpacedEvent::OutputGap(SpacedProcessOutputGap {
            skipped_events: gap.skipped_events,
            output_cursor: gap.output_cursor,
            output_bytes_before: gap.output_bytes_before,
        }),
        HostEvent::Exit(exit) => SpacedEvent::Exit(process_exit_to_spaced(exit)?),
    };

    Ok(SpacedProcessEvent {
        process_id: event.process_id,
        pid: event.pid,
        event: Some(event_payload),
    })
}

pub(super) fn process_exit_to_spaced(
    exit: host_proto::ProcessExit,
) -> Result<SpacedProcessExit, Status> {
    let host_outcome = exit
        .outcome
        .ok_or_else(|| Status::internal("spaced host returned exit event without outcome"))?;
    Ok(SpacedProcessExit {
        outcome: Some(process_outcome_to_spaced(host_outcome)),
    })
}

pub(super) fn process_outcome_to_spaced(
    outcome: host_proto::process_exit::Outcome,
) -> crate::proto::spaced_process_exit::Outcome {
    use crate::proto::spaced_process_exit::Outcome as SpacedOutcome;
    use host_proto::process_exit::Outcome as HostOutcome;

    match outcome {
        HostOutcome::Exited(exited) => SpacedOutcome::Exited(SpacedProcessExited {
            exit_code: exited.exit_code,
        }),
        HostOutcome::Signaled(signaled) => SpacedOutcome::Signaled(SpacedProcessSignaled {
            signal: signaled.signal,
        }),
        HostOutcome::TimedOut(timed_out) => SpacedOutcome::TimedOut(SpacedProcessTimedOut {
            signal: timed_out.signal,
        }),
        HostOutcome::Unknown(_) => SpacedOutcome::Unknown(SpacedProcessUnknown {}),
    }
}
