//! Distinguish a protected launch admission refusal from other supervisor failures.
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

#[cfg(test)]
mod tests;
