use super::*;
use windows_service::service::{ServiceControlAccept, ServiceExitCode, ServiceType};

#[test]
fn status_output_preserves_state_names_and_optional_pid() {
    for (state, expected) in [
        (ServiceState::Stopped, "Stopped"),
        (ServiceState::StartPending, "StartPending"),
        (ServiceState::StopPending, "StopPending"),
        (ServiceState::Running, "Running"),
        (ServiceState::ContinuePending, "ContinuePending"),
        (ServiceState::PausePending, "PausePending"),
        (ServiceState::Paused, "Paused"),
    ] {
        for pid in [None, Some(42)] {
            let status = ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: state,
                controls_accepted: ServiceControlAccept::empty(),
                exit_code: ServiceExitCode::Win32(0),
                checkpoint: 0,
                wait_hint: std::time::Duration::ZERO,
                process_id: pid,
            };
            let suffix = if pid.is_some() { "pid=42\n" } else { "" };
            assert_eq!(
                status_output(&status),
                format!("state={expected}\n{suffix}")
            );
        }
    }
}
