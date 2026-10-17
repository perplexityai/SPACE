//! Shared conversions between generated protobuf structs and common/core
//! domain types.
//!
//! Keep service methods focused on transport behavior by putting repeated
//! parse/serialize glue here.
//!
//! PII boundary anchor: when a protobuf boundary must expose a value that is
//! normally redacted, keep the `Pii::new(...).expose_pii()` pattern visible so
//! future observability work can grep for intentional PII sinks.

use std::collections::HashMap;

use pplx_space_common_core::types::{
    EnvMap, EnvSanitizationIssue, IdempotencyKey, Pii, ReadyTimeout, RelativeGlobPattern,
};
use tonic::Status;

use crate::invalid_argument_status;

/// Encode a duration without wrapping seconds that exceed the wire range.
pub fn duration_to_proto(duration: std::time::Duration) -> Result<prost_types::Duration, Status> {
    duration.try_into().map_err(invalid_argument_status)
}

/// Encode a wall-clock instant. Spaced rejects pre-epoch clocks rather than
/// silently publishing a fabricated timestamp; optional metadata may omit it.
pub fn system_time_to_proto(time: std::time::SystemTime) -> Result<prost_types::Timestamp, Status> {
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("clock before unix epoch: {error}")))?;
    Ok(prost_types::Timestamp {
        seconds: i64::try_from(duration.as_secs())
            .map_err(|_| Status::internal("clock exceeds timestamp range"))?,
        nanos: duration.subsec_nanos() as i32,
    })
}

pub fn proto_duration(timeout: ReadyTimeout) -> prost_types::Duration {
    let timeout = timeout.as_duration();
    prost_types::Duration {
        seconds: timeout.as_secs() as i64,
        nanos: timeout.subsec_nanos() as i32,
    }
}

pub fn proto_idempotency_key(key: Option<IdempotencyKey>) -> Option<String> {
    key.map(|key| key.as_str().to_string())
}

pub fn proto_env(env: &EnvMap) -> HashMap<String, String> {
    env.as_map()
        .iter()
        .map(|(key, value)| {
            (
                key.as_str().to_string(),
                Pii::new(value).expose_pii().expose().to_string(),
            )
        })
        .collect()
}

pub fn parse_env(env: HashMap<String, String>) -> EnvMap {
    let (env, issues) = EnvMap::sanitize(env);
    warn_env_sanitization_issues("spaced_env", &issues);
    env
}

fn warn_env_sanitization_issues(context: &'static str, issues: &[EnvSanitizationIssue]) {
    if issues.is_empty() {
        return;
    }
    tracing::warn!(
        context = context,
        env_issue_count = issues.len(),
        env_issues = ?issues,
        "dropped invalid env entries"
    );
}

pub fn parse_globs(values: Vec<String>) -> Result<Vec<RelativeGlobPattern>, Status> {
    values
        .into_iter()
        .map(|value| RelativeGlobPattern::parse(value.trim().to_string()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_argument_status)
}

pub fn proto_globs(values: &[RelativeGlobPattern]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.as_str().to_string())
        .collect()
}

#[cfg(test)]
mod tests;
