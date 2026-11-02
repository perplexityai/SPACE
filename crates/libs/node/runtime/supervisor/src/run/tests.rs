use std::path::PathBuf;

use super::{RunRuntimeSupervisorError, RuntimeSupervisorRunConfig, SERVICE_NAME, run};

/// Launcher mode must be taken before the Tokio runtime is built and before
/// telemetry is initialized: it joins a namespace, drops privileges, and execs,
/// which is only correct in a single-threaded process. An invalid descriptor is
/// rejected by the protocol itself, so reaching that rejection with no service
/// name installed proves `run` dispatched on the mode first.
#[test]
fn launcher_mode_runs_before_telemetry_is_initialized() {
    let error = run(RuntimeSupervisorRunConfig {
        socket_path: PathBuf::from("/space/run/runtime-supervisor/must-not-be-bound.sock"),
        sidecar_launcher_fd: Some(-1),
        jailer_start_timeout: pplx_space_node_runtime_core::launch::DEFAULT_JAILER_START_TIMEOUT,
    })
    .expect_err("an invalid launcher descriptor must fail");

    match error {
        RunRuntimeSupervisorError::SidecarLauncher(message) => {
            assert!(
                message.contains("invalid sidecar launcher fd -1"),
                "unexpected launcher failure: {message}"
            );
        }
        other => panic!("launcher mode must report a launcher failure, got {other:?}"),
    }

    assert_ne!(
        pplx_space_common_svc::current_service_name(),
        SERVICE_NAME,
        "launcher mode must not initialize service telemetry"
    );
}
