use pplx_space_common_core::types::{
    ArtifactComponentKind, ArtifactOrigin, ExecutionMetadata, RuntimeKind,
};
use time::format_description::well_known::Rfc3339;
use tonic::Status;

#[derive(Debug)]
pub(crate) struct ExecutionProjection {
    runtime_kind: RuntimeKind,
    pub(crate) runtime: String,
    pub(crate) runtime_version: String,
    pub(crate) guest_os: String,
    pub(crate) guest_arch: String,
    pub(crate) state_format_name: Option<String>,
    pub(crate) state_format_version: Option<String>,
    pub(crate) metadata: serde_json::Value,
    pub(crate) components: Vec<ArtifactComponentProjection>,
}

impl ExecutionProjection {
    pub(crate) fn runtime_kind(&self) -> RuntimeKind {
        self.runtime_kind
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ArtifactComponentProjection {
    pub(crate) kind: ArtifactComponentKind,
    pub(crate) role_key: String,
    pub(crate) version: Option<String>,
    pub(crate) digest: Option<String>,
    pub(crate) format: Option<String>,
}

fn projection_from_execution(execution: ExecutionMetadata) -> Result<ExecutionProjection, Status> {
    let state_format_name = execution
        .state_format()
        .map(|state_format| state_format.format.to_string());
    let state_format_version = execution
        .state_format()
        .map(|state_format| state_format.version.to_string());
    let components = execution
        .components()
        .iter()
        .map(|component| ArtifactComponentProjection {
            kind: component.kind,
            role_key: component
                .role
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            version: component.version.as_ref().map(ToString::to_string),
            digest: component.digest.as_ref().map(ToString::to_string),
            format: component.format.as_ref().map(ToString::to_string),
        })
        .collect();
    Ok(ExecutionProjection {
        runtime_kind: execution.runtime().kind,
        runtime: execution.runtime().kind.alias().to_string(),
        runtime_version: execution.runtime().version.to_string(),
        guest_os: execution.guest().os.to_string(),
        guest_arch: execution.guest().architecture.to_string(),
        state_format_name,
        state_format_version,
        metadata: serde_json::to_value(&execution)
            .map_err(|error| Status::internal(format!("serialize execution metadata: {error}")))?,
        components,
    })
}

pub(crate) fn execution_projection(
    execution: Option<&pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<Option<ExecutionProjection>, Status> {
    let Some(execution) = execution else {
        return Ok(None);
    };
    let execution = ExecutionMetadata::try_from(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    projection_from_execution(execution).map(Some)
}

pub(crate) fn proto_timestamp_to_rfc3339(
    timestamp: &prost_types::Timestamp,
    field: &str,
) -> Result<String, Status> {
    proto_timestamp(timestamp, field)?
        .format(&Rfc3339)
        .map_err(|error| Status::internal(format!("format {field}: {error}")))
}

pub(crate) fn proto_timestamp(
    timestamp: &prost_types::Timestamp,
    field: &str,
) -> Result<time::OffsetDateTime, Status> {
    let nanos = u32::try_from(timestamp.nanos)
        .ok()
        .filter(|nanos| *nanos < 1_000_000_000)
        .ok_or_else(|| Status::invalid_argument(format!("{field} has invalid nanos")))?;
    time::OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .map_err(|error| Status::invalid_argument(format!("{field} is out of range: {error}")))?
        .replace_nanosecond(nanos)
        .map_err(|error| Status::invalid_argument(format!("{field} is invalid: {error}")))
}

/// Execution metadata is required: the node-side senders (spaced and the
/// sbx-mgr durable upload worker) have always populated it, and the former
/// mixed-version backfill forced an expensive template/inventory arch
/// resolution onto every suspend/backup begin. A caller that omits it gets
/// a loud `INVALID_ARGUMENT` instead of a silently backfilled identity.
pub(crate) fn snapshot_execution_metadata(
    execution: Option<&pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<pplx_space_common_core::types::SnapshotExecutionMetadata, Status> {
    let execution =
        execution.ok_or_else(|| Status::invalid_argument("execution metadata is required"))?;
    let execution = ExecutionMetadata::try_from(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    pplx_space_common_core::types::SnapshotExecutionMetadata::new(execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

pub(crate) fn u64_to_i64(value: u64, field: &str) -> Result<i64, Status> {
    i64::try_from(value)
        .map_err(|error| Status::invalid_argument(format!("{field} exceeds i64::MAX: {error}")))
}

pub(crate) fn u32_to_i32(value: u32, field: &str) -> Result<i32, Status> {
    i32::try_from(value)
        .map_err(|error| Status::invalid_argument(format!("{field} exceeds i32::MAX: {error}")))
}

pub(crate) fn non_zero_u64_to_i64(value: u64, field: &str) -> Result<Option<i64>, Status> {
    if value == 0 {
        Ok(None)
    } else {
        u64_to_i64(value, field).map(Some)
    }
}

pub(crate) fn optional_u64_to_i64_when(
    include: bool,
    value: Option<u64>,
    field: &str,
) -> Result<Option<i64>, Status> {
    if include {
        value.map(|value| u64_to_i64(value, field)).transpose()
    } else {
        Ok(None)
    }
}

pub(crate) fn optional_u32_to_i32_when(
    include: bool,
    value: Option<u32>,
    field: &str,
) -> Result<Option<i32>, Status> {
    if include {
        value.map(|value| u32_to_i32(value, field)).transpose()
    } else {
        Ok(None)
    }
}

pub(crate) fn published_artifact_origin(raw: i32) -> Result<ArtifactOrigin, Status> {
    match pplx_space_sbx_mgr_client::proto::ArtifactOrigin::try_from(raw) {
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop) => Ok(ArtifactOrigin::Stop),
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend) => {
            Ok(ArtifactOrigin::Suspend)
        }
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Snapshot) => {
            Ok(ArtifactOrigin::Snapshot)
        }
        Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Fork) => Ok(ArtifactOrigin::Fork),
        _ => Err(Status::invalid_argument(
            "current_local_artifact_origin must be STOP, SUSPEND, SNAPSHOT, or FORK",
        )),
    }
}

#[cfg(test)]
mod tests;
