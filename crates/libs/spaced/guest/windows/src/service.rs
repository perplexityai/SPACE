//! Service Control Manager lifecycle. Running means the authenticated listener
//! is bound; service installation alone never reports guest readiness.
use std::{ffi::OsString, time::Duration};
use tokio_util::sync::CancellationToken;
use windows_service::{
    define_windows_service,
    service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    },
    service_control_handler::{self, ServiceControlHandlerResult, ServiceStatusHandle},
    service_dispatcher,
};

const SERVICE_NAME: &str = "spaced";
define_windows_service!(service_entry, service_main);

pub(super) fn dispatch() -> windows_service::Result<()> {
    service_dispatcher::start(SERVICE_NAME, service_entry)
}

fn status(
    handle: ServiceStatusHandle,
    state: ServiceState,
    failed: bool,
) -> windows_service::Result<()> {
    handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: state,
        controls_accepted: if state == ServiceState::Running {
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN
        } else {
            ServiceControlAccept::empty()
        },
        exit_code: if failed {
            ServiceExitCode::ServiceSpecific(1)
        } else {
            ServiceExitCode::Win32(0)
        },
        checkpoint: if state == ServiceState::StartPending {
            1
        } else {
            0
        },
        wait_hint: Duration::from_secs(10),
        process_id: None,
    })
}

fn service_main(_: Vec<OsString>) {
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let handle = match service_control_handler::register(SERVICE_NAME, move |event| match event {
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        ServiceControl::Stop | ServiceControl::Shutdown => {
            stop.cancel();
            ServiceControlHandlerResult::NoError
        }
        _ => ServiceControlHandlerResult::NotImplemented,
    }) {
        Ok(handle) => handle,
        Err(error) => {
            tracing::error!(%error, "cannot register Windows service control handler");
            return;
        }
    };
    let result = status(handle, ServiceState::StartPending, false)
        .map_err(super::RunError::Service)
        .and_then(|()| {
            super::serve(shutdown, || {
                status(handle, ServiceState::Running, false).map_err(super::RunError::Service)
            })
        });
    if let Err(error) = &result {
        report_failure(error);
        tracing::error!(%error, "Windows guest service failed");
    }
    if let Err(error) = status(handle, ServiceState::Stopped, result.is_err()) {
        tracing::error!(%error, "cannot publish Windows service exit status");
    }
}

/// SCM reports only the numeric exit code; retain the cause in Application events.
fn report_failure(error: &super::RunError) {
    use windows_sys::Win32::System::EventLog::{
        DeregisterEventSource, EVENTLOG_ERROR_TYPE, RegisterEventSourceW, ReportEventW,
    };
    let source: Vec<u16> = SERVICE_NAME.encode_utf16().chain(Some(0)).collect();
    let message: Vec<u16> = error.to_string().encode_utf16().chain(Some(0)).collect();
    // SAFETY: the source is terminated and retained for this synchronous call.
    let handle = unsafe { RegisterEventSourceW(std::ptr::null(), source.as_ptr()) };
    if !handle.is_null() {
        let strings = [message.as_ptr()];
        // SAFETY: the live event handle and terminated string remain valid for
        // the call. One string, no SID and no binary payload match the lengths.
        let _ = unsafe {
            ReportEventW(
                handle,
                EVENTLOG_ERROR_TYPE,
                0,
                1,
                std::ptr::null_mut(),
                1,
                0,
                strings.as_ptr(),
                std::ptr::null(),
            )
        };
        // SAFETY: this is the unique event handle returned above; no subsequent use.
        let _ = unsafe { DeregisterEventSource(handle) };
    }
}
