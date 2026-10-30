//! Sandbox record decoding, keyed by SQL column names rather than projection order.
//!
//! Store queries keep explicit, complete statements. The decoder centralizes
//! persisted-value validation and compatibility fallbacks without requiring each
//! query's projection to share a positional layout.

use std::collections::BTreeMap;

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, AutoLifecyclePolicy, AutoLifecycleRuntimeState,
    ExecutionGeneration, ExecutionMetadata, LocalArtifactId, NodeManagedBy, PolicyAuthority,
    RouteGeneration, SandboxActivationAttemptId, SandboxId, SandboxOwner, SandboxRuntimeLabel,
    SpaceApiRouteIdentity,
};
use pplx_space_net_common::{NetworkPolicy, NetworkPolicyV1};
use rusqlite::types::Type;

use super::{CleanupCompletion, NetworkPolicyAppliedSource, parse_policy_authority};
use crate::endpoint_proof::SpacedEndpointProof;
use crate::restore_operation::RestoreOperation;
use crate::state::SandboxState;

#[derive(Clone)]
pub struct SandboxRow {
    pub guest_image: pplx_space_common_core::types::guest_image::GuestImageContract,
    pub node_execution_id: Option<SandboxActivationAttemptId>,
    pub id: String,
    pub uuid8: SandboxRuntimeLabel,
    pub state: SandboxState,
    pub restore_operation: Option<RestoreOperation>,
    pub failure_reason: Option<String>,
    pub template: String,
    pub template_hash: Option<String>,
    pub user: SandboxOwner,
    pub runtime: String,
    pub uid: Option<u32>,
    pub slot: Option<u32>,
    pub guest_cid: Option<u32>,
    pub pid: Option<u32>,
    pub spaced_host_pid: Option<u32>,
    pub netgtw_pid: Option<u32>,
    pub vsock_uds_path: Option<String>,
    pub api_socket_path: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub snapshots_enabled: bool,
    pub rootfs_size_mb: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub pool_template: Option<String>,
    /// BYOK: this sandbox runs on dm-crypt encrypted volumes. Immutable for
    /// the sandbox's lifetime — stamped at create by the control plane and
    /// never updated. Every start/suspend/restore decision reads this local
    /// value; no service round-trip re-decides it.
    pub volume_encryption: bool,
    pub guest_uptime_secs: Option<u64>,
    pub process_env: BTreeMap<String, String>,
    pub auto_lifecycle: AutoLifecyclePolicy,
    pub auto_lifecycle_state: AutoLifecycleRuntimeState,
    pub network_policy_override: Option<NetworkPolicy>,
    pub network_policy_override_is_corrupt_fallback: bool,
    pub network_policy_generation: u64,
    pub applied_network_policy_generation: u64,
    pub applied_network_policy_source: NetworkPolicyAppliedSource,
    pub network_policy_authority: PolicyAuthority,
    pub auto_lifecycle_authority: PolicyAuthority,
    pub network_tx_bytes: u64,
    pub network_rx_bytes: u64,
    pub next_auto_event_at: Option<u64>,
    pub cleanup_completion: Option<CleanupCompletion>,
    pub cleanup_keep_filesystem: Option<bool>,
    pub current_local_artifact_id: Option<LocalArtifactId>,
    pub current_local_artifact_kind: Option<ArtifactKind>,
    pub current_local_artifact_origin: Option<ArtifactOrigin>,
    pub execution_metadata: Option<ExecutionMetadata>,
    pub space_api_route_identity: SpaceApiRouteIdentity,
    pub spaced_endpoint_proof: Option<SpacedEndpointProof>,
}

impl SandboxRow {
    pub fn node_execution_fence(
        &self,
    ) -> Option<pplx_space_node_mgr_protocol::proto::ExecutionFence> {
        self.node_execution_id.map(|id| {
            pplx_space_node_mgr_protocol::execution_fence::ExecutionFence::new(id, None).into()
        })
    }
}

/// The persisted fields List returns or filters on. List reads only these
/// columns, so its inventory budget measures what it serves rather than every
/// runtime and policy document on the row.
pub struct SandboxListRow {
    pub id: String,
    pub state: SandboxState,
    pub template: String,
    pub template_hash: Option<String>,
    pub user: SandboxOwner,
    pub runtime: String,
    pub created_at: String,
    pub pool_template: Option<String>,
}

pub(super) fn row_to_list_sandbox(row: &rusqlite::Row) -> rusqlite::Result<SandboxListRow> {
    Ok(SandboxListRow {
        id: row.get("id")?,
        state: state_column(row)?,
        template: row.get("template")?,
        template_hash: row
            .get::<_, Option<String>>("template_hash")?
            .filter(|hash| !hash.is_empty()),
        user: user_column(row)?,
        runtime: row.get("runtime")?,
        created_at: row
            .get::<_, super::timestamps::EpochSeconds>("created_at")?
            .to_string(),
        pool_template: row.get("pool_template")?,
    })
}

fn state_column(row: &rusqlite::Row<'_>) -> rusqlite::Result<SandboxState> {
    let state_str: String = row.get("state")?;
    state_str.parse::<SandboxState>().map_err(|_| {
        column_error(
            row,
            "state",
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown sandbox state {state_str:?}"),
            )),
        )
    })
}

fn user_column(row: &rusqlite::Row<'_>) -> rusqlite::Result<SandboxOwner> {
    SandboxOwner::parse(row.get::<_, String>("user")?)
        .map_err(|error| column_error(row, "user", Type::Text, Box::new(error)))
}

pub(super) fn row_to_sandbox(row: &rusqlite::Row) -> rusqlite::Result<SandboxRow> {
    let raw_updated_at: rusqlite::types::Value = row.get("updated_at")?;
    // Keep the exact CAS witness, including invalid text for the caller's
    // explicit refusal/diagnostic path. Time-based decisions decode it strictly.
    let updated_at = match raw_updated_at {
        rusqlite::types::Value::Text(raw) => raw,
        rusqlite::types::Value::Integer(raw) => raw.to_string(),
        raw => {
            return Err(column_error(
                row,
                "updated_at",
                raw.data_type(),
                Box::new(super::timestamps::InvalidTimestamp),
            ));
        }
    };
    let id: String = row.get("id")?;
    let uuid8 = SandboxRuntimeLabel::parse(row.get::<_, String>("uuid8")?)
        .map_err(|error| column_error(row, "uuid8", Type::Text, Box::new(error)))?;
    let state = state_column(row)?;
    let user = user_column(row)?;
    let process_env_json: String = row.get("process_env_json")?;
    let process_env = serde_json::from_str(&process_env_json)
        .map_err(|error| column_error(row, "process_env_json", Type::Text, Box::new(error)))?;
    let auto_lifecycle_json: String = row.get("auto_lifecycle_json")?;
    let auto_lifecycle = serde_json::from_str(&auto_lifecycle_json)
        .map_err(|error| column_error(row, "auto_lifecycle_json", Type::Text, Box::new(error)))?;
    let auto_lifecycle_state_json: String = row.get("auto_lifecycle_state_json")?;
    let auto_lifecycle_state = match serde_json::from_str(&auto_lifecycle_state_json) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %id,
                %error,
                "failed to parse auto lifecycle runtime state; using default state"
            );
            AutoLifecycleRuntimeState::default()
        }
    };
    let execution_metadata = match row.get::<_, Option<String>>("execution_metadata_json")? {
        Some(json) => match serde_json::from_str::<ExecutionMetadata>(&json) {
            Ok(metadata) => Some(metadata),
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %id,
                    %error,
                    "failed to parse sandbox execution metadata; omitting execution metadata from state"
                );
                None
            }
        },
        None => None,
    };
    let route_generation = row
        .get::<_, Option<i64>>("space_api_route_generation")?
        .map(|value| {
            u64::try_from(value)
                .ok()
                .and_then(|value| RouteGeneration::new(value).ok())
                .ok_or_else(|| {
                    column_error(
                        row,
                        "space_api_route_generation",
                        Type::Integer,
                        Box::new(pplx_space_common_core::types::RouteGenerationParseError),
                    )
                })
        })
        .transpose()?;
    let route_node_id = optional_text(row, "space_api_route_node_id")?;
    let route_execution_generation = row
        .get::<_, Option<i64>>("space_api_route_execution_generation")?
        .map(|value| {
            ExecutionGeneration::new(value).map_err(|error| {
                column_error(
                    row,
                    "space_api_route_execution_generation",
                    Type::Integer,
                    Box::new(error),
                )
            })
        })
        .transpose()?;
    let route_pod_uid = optional_text(row, "space_api_route_pod_uid")?;
    let route_substrate_kind =
        optional_text::<NodeManagedBy>(row, "space_api_route_substrate_kind")?;
    let route_substrate_kind = SpaceApiRouteIdentity::effective_substrate_kind_for(
        route_substrate_kind,
        route_pod_uid.as_ref(),
    );
    let route_activation_attempt_id =
        optional_text::<SandboxActivationAttemptId>(row, "space_api_route_activation_attempt_id")?;
    let spaced_endpoint_proof = row
        .get::<_, Option<String>>("spaced_endpoint_proof_json")?
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                column_error(
                    row,
                    "spaced_endpoint_proof_json",
                    Type::Text,
                    Box::new(error),
                )
            })
        })
        .transpose()?;
    let has_route_identity = route_generation.is_some()
        || route_node_id.is_some()
        || route_execution_generation.is_some()
        || route_pod_uid.is_some()
        || route_substrate_kind.is_some()
        || route_activation_attempt_id.is_some();
    let route_sandbox_id = has_route_identity
        .then(|| {
            id.parse::<SandboxId>()
                .map_err(|error| column_error(row, "id", Type::Text, Box::new(error)))
        })
        .transpose()?;
    let mut network_policy_override_is_corrupt_fallback = false;
    let network_policy_override =
        match row.get::<_, Option<String>>("network_policy_override_json")? {
            Some(json) => match serde_json::from_str(&json) {
                Ok(policy) => Some(policy),
                Err(error) => {
                    network_policy_override_is_corrupt_fallback = true;
                    tracing::warn!(
                        sandbox_id = %id,
                        %error,
                        "failed to parse sandbox network policy override; using deny-all override"
                    );
                    Some(NetworkPolicyV1::deny_all())
                }
            },
            None => None,
        };
    let restore_origin = optional_text(row, "restore_origin")?;
    let restore_phase = optional_text(row, "restore_phase")?;
    let restore_source = optional_text(row, "restore_storage_uri")?;
    let restore_operation =
        RestoreOperation::from_persisted(restore_origin, restore_phase, restore_source)
            .map_err(|error| column_error(row, "restore_origin", Type::Text, Box::new(error)))?;
    let disk_io_operations = row.get::<_, i64>("disk_io_max_operations_per_sec")?;
    let disk_io_bandwidth = row.get::<_, i64>("disk_io_max_bandwidth_mib_per_sec")?;
    let disk_io_limits = u64::try_from(disk_io_operations)
        .ok()
        .zip(u64::try_from(disk_io_bandwidth).ok())
        .and_then(|(operations, bandwidth)| {
            pplx_space_common_core::types::DiskIoLimits::try_new(operations, bandwidth).ok()
        })
        .ok_or_else(|| {
            column_error(row, "disk_io_max_operations_per_sec",
                Type::Integer,
                format!(
                    "invalid persisted disk I/O limits: operations={disk_io_operations}, bandwidth_mib={disk_io_bandwidth}"
                )
                .into(),
            )
        })?;
    Ok(SandboxRow {
        guest_image: row
            .get::<_, Option<String>>("guest_image_contract_json")?
            .map(|raw| {
                serde_json::from_str(&raw).map_err(|error| {
                    column_error(
                        row,
                        "guest_image_contract_json",
                        Type::Text,
                        Box::new(error),
                    )
                })
            })
            .transpose()?
            .unwrap_or_default(),
        node_execution_id: row
            .get::<_, Option<String>>("node_execution_id")?
            .map(|raw| {
                raw.parse().map_err(|error| {
                    column_error(row, "node_execution_id", Type::Text, Box::new(error))
                })
            })
            .transpose()?,
        id,
        uuid8,
        state,
        restore_operation,
        failure_reason: row.get("failure_reason")?,
        template: row.get("template")?,
        template_hash: row
            .get::<_, Option<String>>("template_hash")?
            .filter(|hash| !hash.is_empty()),
        user,
        runtime: row.get("runtime")?,
        uid: row.get("uid")?,
        slot: row.get("slot")?,
        guest_cid: row.get("guest_cid")?,
        pid: row.get("pid")?,
        spaced_host_pid: row.get("spaced_host_pid")?,
        netgtw_pid: row.get("netgtw_pid")?,
        vsock_uds_path: row.get("vsock_uds_path")?,
        api_socket_path: row.get("api_socket_path")?,
        created_at: row
            .get::<_, super::timestamps::EpochSeconds>("created_at")?
            .to_string(),
        updated_at,
        snapshots_enabled: row.get("snapshots_enabled")?,
        rootfs_size_mb: row
            .get::<_, Option<i64>>("rootfs_size_mb")?
            .and_then(|value| u32::try_from(value).ok().filter(|value| *value > 0)),
        disk_io_limits,
        pool_template: row.get("pool_template")?,
        volume_encryption: row.get("volume_encryption")?,
        guest_uptime_secs: row
            .get::<_, Option<i64>>("guest_uptime_secs")?
            .and_then(|secs| (secs > 0).then(|| secs.cast_unsigned())),
        process_env,
        auto_lifecycle,
        auto_lifecycle_state,
        network_policy_override,
        network_policy_override_is_corrupt_fallback,
        network_policy_generation: row
            .get::<_, i64>("network_policy_generation")?
            .max(0)
            .cast_unsigned(),
        applied_network_policy_generation: row
            .get::<_, i64>("applied_network_policy_generation")?
            .max(0)
            .cast_unsigned(),
        applied_network_policy_source: NetworkPolicyAppliedSource::parse(
            row.get::<_, Option<String>>("applied_network_policy_source")?
                .as_deref()
                .unwrap_or(""),
        ),
        network_policy_authority: parse_policy_authority(
            row.get::<_, Option<String>>("network_policy_authority")?
                .as_deref()
                .unwrap_or("cluster"),
            row.as_ref().column_index("network_policy_authority")?,
        )?,
        auto_lifecycle_authority: parse_policy_authority(
            row.get::<_, Option<String>>("auto_lifecycle_authority")?
                .as_deref()
                .unwrap_or("cluster"),
            row.as_ref().column_index("auto_lifecycle_authority")?,
        )?,
        network_tx_bytes: row
            .get::<_, i64>("network_tx_bytes")?
            .max(0)
            .cast_unsigned(),
        network_rx_bytes: row
            .get::<_, i64>("network_rx_bytes")?
            .max(0)
            .cast_unsigned(),
        next_auto_event_at: row
            .get::<_, Option<i64>>("next_auto_event_at")?
            .and_then(|value| (value >= 0).then(|| value.cast_unsigned())),
        cleanup_completion: row
            .get::<_, Option<String>>("cleanup_completion")?
            .and_then(|value| value.parse().ok()),
        cleanup_keep_filesystem: row.get("cleanup_keep_filesystem")?,
        current_local_artifact_id: optional_text(row, "current_local_artifact_id")?,
        current_local_artifact_kind: optional_text(row, "current_local_artifact_kind")?,
        current_local_artifact_origin: optional_text(row, "current_local_artifact_origin")?,
        execution_metadata,
        space_api_route_identity: SpaceApiRouteIdentity {
            sandbox_id: route_sandbox_id,
            route_generation,
            current_node_id: route_node_id,
            substrate_kind: route_substrate_kind,
            execution_generation: route_execution_generation,
            pod_uid: route_pod_uid,
            activation_attempt_id: route_activation_attempt_id,
        },
        spaced_endpoint_proof,
    })
}

fn optional_text<T>(row: &rusqlite::Row<'_>, column: &str) -> rusqlite::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    row.get::<_, Option<String>>(column)?
        .map(|value| {
            value
                .parse()
                .map_err(|error| column_error(row, column, Type::Text, Box::new(error)))
        })
        .transpose()
}

fn column_error(
    row: &rusqlite::Row<'_>,
    column: &str,
    kind: Type,
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
) -> rusqlite::Error {
    match row.as_ref().column_index(column) {
        Ok(index) => rusqlite::Error::FromSqlConversionFailure(index, kind, source),
        Err(error) => error,
    }
}

#[cfg(test)]
mod tests;
