//! Preserve runtime admission failures without authorizing transitional RPC replay.
use tonic::Status;

const FAILURE: &str = "space-runtime-launch-failure";
const ADMISSION: &str = "admission-refusal";

pub fn admission_refusal(mut error: Status) -> Status {
    if matches!(
        error.code(),
        tonic::Code::FailedPrecondition | tonic::Code::InvalidArgument
    ) {
        error.metadata_mut().insert(
            FAILURE,
            tonic::metadata::MetadataValue::from_static(ADMISSION),
        );
    }
    error
}

pub fn supervisor_launch_status(error: Status) -> Status {
    if matches!(
        error.code(),
        tonic::Code::FailedPrecondition | tonic::Code::InvalidArgument
    ) && error
        .metadata()
        .get(FAILURE)
        .is_some_and(|value| value == ADMISSION)
    {
        error
    } else {
        Status::internal(format!("runtime-supervisor launch failed: {error}"))
    }
}

/// Unimplemented is reserved for a missing transport method, so a transitional
/// client may fall back only when no operation reached this implementation.
pub fn runtime_operation_status(status: Status) -> Status {
    if status.code() == tonic::Code::Unimplemented {
        Status::failed_precondition(status.message())
    } else {
        status
    }
}

#[cfg(test)]
mod tests;
