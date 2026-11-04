use tonic::Status;

pub(crate) fn deadline_exceeded_status(
    service_name: &'static str,
    command: &'static str,
    phase: &'static str,
) -> Status {
    Status::deadline_exceeded(format!(
        "{service_name}/{command} deadline exceeded while {phase}"
    ))
}
