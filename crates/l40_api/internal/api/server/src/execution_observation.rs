//! Decode publication evidence without treating omission or collection failure
//! as an empty inventory. Existing placement projections remain unchanged.

use pplx_space_accounts::NodeId;
use pplx_space_execution_common::proto::NodeExecutionInventory;
use pplx_space_execution_spec::{
    ArtifactComponentMetadata, GuestPlatform, RuntimeIdentity, StateFormatIdentity,
};
use pplx_space_node_execution_inventory_store::{
    self as inventory_store, ArtifactComponent, RuntimeGuest, RuntimeInstallation,
    RuntimeStateFormat,
};
use pplx_space_node_registration_store::locks;
use tonic::Status;

use crate::proto::{ExecutionInventoryObservation, execution_inventory_observation::Outcome};

/// The outcome recorded only when the surrounding publication commits.
#[derive(Clone, Copy, Debug)]
pub(super) enum InventoryStatus {
    Complete,
    Unavailable,
    Legacy,
}

impl InventoryStatus {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Unavailable => "unavailable",
            Self::Legacy => "legacy",
        }
    }
}

#[derive(Debug)]
enum Report<'a> {
    Complete(&'a NodeExecutionInventory),
    Legacy(&'a NodeExecutionInventory),
    Unavailable,
}

#[derive(Debug, thiserror::Error)]
enum ObservationError {
    #[error("execution observation is missing a supported outcome")]
    MissingOutcome,
    #[error("complete execution observation disagrees with legacy inventory")]
    ConflictingInventory,
    #[error("unavailable execution observation cannot include legacy inventory")]
    UnavailableWithInventory,
}

fn decode<'a>(
    observation: Option<&'a ExecutionInventoryObservation>,
    legacy: Option<&'a NodeExecutionInventory>,
) -> Result<Option<Report<'a>>, ObservationError> {
    let Some(observation) = observation else {
        return Ok(legacy.map(Report::Legacy));
    };
    match observation.outcome.as_ref() {
        Some(Outcome::Complete(inventory)) => {
            if legacy.is_some_and(|legacy| legacy != inventory) {
                return Err(ObservationError::ConflictingInventory);
            }
            Ok(Some(Report::Complete(inventory)))
        }
        Some(Outcome::Unavailable(_)) => {
            if legacy.is_some() {
                return Err(ObservationError::UnavailableWithInventory);
            }
            Ok(Some(Report::Unavailable))
        }
        None => Err(ObservationError::MissingOutcome),
    }
}

/// Holds decoded evidence after acquiring this publication's node lock.
/// Construct before census writes; consume before the status upsert.
pub(super) struct PreparedObservation<'a> {
    node_id: NodeId,
    report: Report<'a>,
}

pub(super) async fn prepare<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    observation: Option<&'a ExecutionInventoryObservation>,
    legacy: Option<&'a NodeExecutionInventory>,
) -> Result<Option<PreparedObservation<'a>>, Status> {
    let Some(report) =
        decode(observation, legacy).map_err(|error| Status::invalid_argument(error.to_string()))?
    else {
        return Ok(None);
    };
    // Acquire before census writes to preserve node-before-sandbox lock order.
    // Serialize even empty replacements, which have no artifact row to lock.
    // Keep artifact writes before the status upsert, as legacy readers do.
    // Locking the status row first would invert that order during rollout.
    // NO KEY UPDATE permits the foreign-key key-share locks of node reports.
    let exists = locks::lock_node_for_no_key_update(tx, node_id)
        .await
        .map_err(|error| Status::internal(format!("lock node execution publication: {error}")))?;
    if !exists {
        return Err(Status::not_found("publishing node no longer exists"));
    }
    Ok(Some(PreparedObservation { node_id, report }))
}

pub(super) struct PublishedExecutionObservation {
    pub status: InventoryStatus,
    pub policy: Option<pplx_space_execution_spec::ExecutionPolicyObservation>,
    pub environments: crate::environment_observation::EnvironmentRefs,
}

pub(super) async fn publish(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    prepared: Option<PreparedObservation<'_>>,
) -> Result<Option<PublishedExecutionObservation>, Status> {
    let Some(PreparedObservation { node_id, report }) = prepared else {
        return Ok(None);
    };
    let (status, inventory) = match &report {
        Report::Complete(inventory) => (InventoryStatus::Complete, Some(inventory)),
        Report::Legacy(inventory) => (InventoryStatus::Legacy, Some(inventory)),
        Report::Unavailable => (InventoryStatus::Unavailable, None),
    };
    if let Some(inventory) = inventory {
        publish_inventory(tx, node_id, inventory).await?;
    }
    // Environment arrays are stored only from a complete inventory; the
    // other outcomes leave them to age out by their own receipt.
    let environments = if let Report::Complete(inventory) = &report {
        crate::environment_observation::publish(tx, node_id, inventory).await?
    } else {
        crate::environment_observation::EnvironmentRefs::Preserve
    };
    let policy = inventory
        .and_then(|inventory| inventory.policy.as_ref())
        .and_then(|policy| {
            match pplx_space_execution_spec::ExecutionPolicyObservation::try_from(policy) {
                Ok(policy) => Some(policy),
                Err(error) => {
                    tracing::warn!(%error, %node_id, "invalid execution policy observation; withholding policy evidence");
                    None
                }
            }
        });
    Ok(Some(PublishedExecutionObservation {
        status,
        policy,
        environments,
    }))
}

/// Reconcile a complete inventory without rewriting unchanged artifact rows.
/// The publication holds the registered-node lock and commits this projection
/// with its collection receipt. Row timestamps advance on changes; collection
/// freshness is carried by the independent receipt, even for identical samples.
async fn publish_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    inventory: &pplx_space_execution_common::proto::NodeExecutionInventory,
) -> Result<(), Status> {
    let mut runtime_rows = Vec::with_capacity(inventory.runtimes.len());
    for installation in &inventory.runtimes {
        let runtime = installation
            .runtime
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("runtime installation is missing identity"))
            .and_then(|runtime| {
                RuntimeIdentity::try_from(runtime)
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })?;
        let guests = installation
            .supported_guests
            .iter()
            .map(|guest| {
                GuestPlatform::try_from(guest)
                    .map(|guest| RuntimeGuest {
                        guest_os: guest.os.as_str().to_owned(),
                        guest_arch: guest.architecture.as_str().to_owned(),
                    })
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let state_formats = installation
            .state_formats
            .iter()
            .map(|capability| {
                let identity = capability.identity.as_ref().ok_or_else(|| {
                    Status::invalid_argument("state format capability is missing identity")
                })?;
                let identity = StateFormatIdentity::try_from(identity)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                Ok(RuntimeStateFormat {
                    state_format_name: identity.format.as_str().to_owned(),
                    state_format_version: identity.version.as_str().to_owned(),
                    can_create: capability.can_create,
                    can_load: capability.can_load,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        runtime_rows.push(RuntimeInstallation {
            runtime: runtime.kind.alias().to_owned(),
            runtime_version: runtime.version.as_str().to_owned(),
            guests,
            state_formats,
        });
    }

    let component_rows = inventory
        .components
        .iter()
        .map(|installation| {
            let component = installation.component.as_ref().ok_or_else(|| {
                Status::invalid_argument("artifact component installation is missing component")
            })?;
            let component = ArtifactComponentMetadata::try_from(component)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(ArtifactComponent {
                kind: component.kind.as_str().to_owned(),
                role_key: component
                    .role
                    .as_ref()
                    .map_or("", |role| role.as_str())
                    .to_owned(),
                version_key: component
                    .version
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                digest_key: component
                    .digest
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                format_key: component
                    .format
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                version: component.version.as_ref().map(ToString::to_string),
                digest: component.digest.as_ref().map(ToString::to_string),
                format: component.format.as_ref().map(ToString::to_string),
                default_for_new: installation.default_for_new,
                legacy_default: installation.legacy_default,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;

    inventory_store::prune_runtime_installations(&mut *conn, node_id, &runtime_rows)
        .await
        .map_err(|error| Status::internal(format!("replace node runtime inventory: {error}")))?;
    inventory_store::prune_artifact_components(&mut *conn, node_id, &component_rows)
        .await
        .map_err(|error| Status::internal(format!("replace node component inventory: {error}")))?;

    inventory_store::insert_runtime_installations(&mut *conn, node_id, &runtime_rows)
        .await
        .map_err(|error| Status::internal(format!("insert node runtime inventory: {error}")))?;
    // A retained runtime can lose a guest or format; pruning only removed
    // runtime parents would leave those capabilities advertised indefinitely.
    inventory_store::prune_runtime_guests(&mut *conn, node_id, &runtime_rows)
        .await
        .map_err(|error| Status::internal(format!("prune node runtime guests: {error}")))?;
    inventory_store::insert_runtime_guests(&mut *conn, node_id, &runtime_rows)
        .await
        .map_err(|error| Status::internal(format!("insert node runtime guests: {error}")))?;
    inventory_store::prune_runtime_state_formats(&mut *conn, node_id, &runtime_rows)
        .await
        .map_err(|error| Status::internal(format!("prune node state formats: {error}")))?;
    inventory_store::upsert_runtime_state_formats(&mut *conn, node_id, &runtime_rows)
        .await
        .map_err(|error| Status::internal(format!("insert node state formats: {error}")))?;

    inventory_store::upsert_artifact_components(&mut *conn, node_id, &component_rows)
        .await
        .map_err(|error| Status::internal(format!("insert node component inventory: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests;
