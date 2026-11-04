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
        Status::with_details_and_metadata(
            tonic::Code::FailedPrecondition,
            status.message(),
            status.details().to_vec().into(),
            status.metadata().clone(),
        )
    } else {
        status
    }
}

/// Classify malformed transport contracts before execution ownership or spawn.
pub fn launch_contract_status(error: crate::runtime_launch::LaunchContractError) -> Status {
    let status = match &error {
        crate::runtime_launch::LaunchContractError::InvalidEnvironmentReference(_)
        | crate::runtime_launch::LaunchContractError::Sandbox(_)
        | crate::runtime_launch::LaunchContractError::Execution(_)
        | crate::runtime_launch::LaunchContractError::Path { .. }
        | crate::runtime_launch::LaunchContractError::NulPath(_) => {
            Status::invalid_argument(error.to_string())
        }
        _ => Status::failed_precondition(error.to_string()),
    };
    admission_refusal(status)
}

#[cfg(test)]
mod tests;
