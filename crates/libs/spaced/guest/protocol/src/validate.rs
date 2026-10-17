use std::time::Duration;

use pplx_space_common_core::types::ReadyTimeout;
use tonic::Status;

use crate::invalid_argument_status;

/// Validates an optional human-readable `reason` string: rejects empty strings,
/// passes through `None` or non-empty values unchanged.
pub fn validate_optional_reason(reason: Option<String>) -> Result<Option<String>, Status> {
    match reason {
        Some(reason) if reason.is_empty() => Err(Status::invalid_argument(
            "reason must not be empty when provided",
        )),
        reason => Ok(reason),
    }
}

/// Validates an optional positive proto duration for a named field.
/// Rejects negative, malformed, and zero values.
pub fn validate_optional_duration(
    field_name: &str,
    duration: Option<prost_types::Duration>,
) -> Result<Option<Duration>, Status> {
    duration
        .map(|duration| validate_duration_value(field_name, duration))
        .transpose()
}

/// Validates a required positive proto duration for a named field.
/// Rejects missing, negative, malformed, and zero values.
pub fn validate_duration(
    field_name: &str,
    duration: Option<prost_types::Duration>,
) -> Result<Duration, Status> {
    let duration =
        duration.ok_or_else(|| Status::invalid_argument(format!("{field_name} is required")))?;
    validate_duration_value(field_name, duration)
}

/// Validates a required proto timeout duration, returning it as a
/// [`ReadyTimeout`]. Rejects missing, negative, and zero values.
pub fn validate_timeout(timeout: Option<prost_types::Duration>) -> Result<ReadyTimeout, Status> {
    let timeout = timeout.ok_or_else(|| Status::invalid_argument("timeout is required"))?;
    validate_timeout_value(timeout)
}

/// Validates an optional proto timeout duration, returning it as a
/// [`ReadyTimeout`] when present. Rejects negative and zero values.
pub fn validate_optional_timeout(
    timeout: Option<prost_types::Duration>,
) -> Result<Option<ReadyTimeout>, Status> {
    timeout.map(validate_timeout_value).transpose()
}

fn validate_timeout_value(timeout: prost_types::Duration) -> Result<ReadyTimeout, Status> {
    let duration = validate_duration_value("timeout", timeout)?;
    if tokio::time::Instant::now().checked_add(duration).is_none() {
        return Err(Status::invalid_argument(
            "timeout is too large for the runtime clock",
        ));
    }
    ReadyTimeout::new(duration).map_err(invalid_argument_status)
}

fn validate_duration_value(
    field_name: &str,
    duration: prost_types::Duration,
) -> Result<Duration, Status> {
    if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument(format!(
            "{field_name} must be a valid positive duration"
        )));
    }
    let duration = Duration::new(
        duration.seconds.cast_unsigned(),
        duration.nanos.cast_unsigned(),
    );
    if duration.is_zero() {
        return Err(Status::invalid_argument(format!(
            "{field_name} must be greater than 0"
        )));
    }
    Ok(duration)
}

#[cfg(test)]
mod tests;
