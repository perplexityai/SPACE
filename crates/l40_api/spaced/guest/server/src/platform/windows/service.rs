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
        tracing::error!(%error, "Windows guest service failed");
    }
    if let Err(error) = status(handle, ServiceState::Stopped, result.is_err()) {
        tracing::error!(%error, "cannot publish Windows service exit status");
    }
}

/// Bounded synchronous writer for the service application's tracing subscriber.
/// SCM has no stderr; warnings and errors belong in the Application event log.
pub struct EventLogWriter;
impl std::io::Write for EventLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        use windows_sys::Win32::System::EventLog::{
            DeregisterEventSource, EVENTLOG_WARNING_TYPE, RegisterEventSourceW, ReportEventW,
        };
        let source: Vec<u16> = SERVICE_NAME.encode_utf16().chain(Some(0)).collect();
        let message: Vec<u16> = String::from_utf8_lossy(bytes)
            .encode_utf16()
            .take(8192)
            .map(|c| if c == 0 { 32 } else { c })
            .chain(Some(0))
            .collect();
        // SAFETY: the terminated source lives for this synchronous call.
        let handle = unsafe { RegisterEventSourceW(std::ptr::null(), source.as_ptr()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let strings = [message.as_ptr()];
        // SAFETY: one terminated string, no SID or binary data, and a live handle.
        let result = unsafe {
            ReportEventW(
                handle,
                EVENTLOG_WARNING_TYPE,
                0,
                1,
                std::ptr::null_mut(),
                1,
                0,
                strings.as_ptr(),
                std::ptr::null(),
            )
        };
        let error = (result == 0).then(std::io::Error::last_os_error);
        // SAFETY: this writer uniquely owns the event-source handle.
        unsafe { DeregisterEventSource(handle) };
        match error {
            Some(error) => Err(error),
            None => Ok(bytes.len()),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
