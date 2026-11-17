//! Default-off admission policy for the bounded retained-disk experiment.

use std::collections::BTreeSet;

use pplx_space_accounts::OrgId;
use pplx_space_execution_spec::{BlockVolumeMount, RuntimeKind};
use serde::Deserialize;

use crate::controllers::ControllerError;

const POLICY_ENV: &str = "SPACE_EBS_ADMISSION_POLICY";

pub(crate) async fn populate_status(
    pool: &sqlx::PgPool,
    org: OrgId,
    sandboxes: &mut [pplx_space_public_api::PublicSandbox],
) -> Result<(), sqlx::Error> {
    use pplx_space_public_api::{PublicSandboxVolumeMount, SandboxBlockVolumeState as PublicState};
    use pplx_space_sandbox_store::block_volume::{self as store, VolumePhase};
    let ids: Vec<_> = sandboxes.iter().map(|sandbox| sandbox.id.into()).collect();
    let mut statuses: std::collections::HashMap<_, _> = store::statuses(pool, org, &ids)
        .await?
        .into_iter()
        .map(|row| (row.sandbox_id, row))
        .collect();
    for sandbox in sandboxes {
        sandbox.volume_mounts.clear();
        let id: pplx_space_common_core::types::SandboxId = sandbox.id.into();
        if let Some(row) = statuses.remove(&id) {
            let state = match row.phase {
                VolumePhase::Requested => PublicState::Requested,
                VolumePhase::Provisioning => PublicState::Provisioning,
                VolumePhase::Available => PublicState::Available,
                VolumePhase::Attaching => PublicState::Attaching,
                VolumePhase::Attached => PublicState::Attached,
                VolumePhase::Releasing => PublicState::Releasing,
                VolumePhase::Detaching => PublicState::Detaching,
                VolumePhase::Deleting => PublicState::Deleting,
                VolumePhase::Deleted => PublicState::Deleted,
            };
            sandbox.volume_mounts.push(PublicSandboxVolumeMount {
                volume_id: row.id,
                mount_spec: row.mount_spec.0,
                state,
            });
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Admission {
    organization_ids: BTreeSet<OrgId>,
    pub(crate) availability_zone: String,
}

pub(crate) fn admit(
    env: &dyn pplx_space_env_vars::Environment,
    mounts: &[BlockVolumeMount],
    org: OrgId,
    runtime: RuntimeKind,
    byok: bool,
) -> Result<Option<Admission>, ControllerError> {
    if mounts.is_empty() {
        return Ok(None);
    }
    let [mount] = mounts else {
        return Err(ControllerError::bad_request(
            "the block-volume experiment permits one 128-GiB EBS disk",
        ));
    };
    if u32::from(mount.size_gib) != 128 {
        return Err(ControllerError::bad_request(
            "the block-volume experiment permits one 128-GiB EBS disk",
        ));
    }
    if runtime != RuntimeKind::Firecracker || byok {
        return Err(ControllerError::bad_request(
            "block volumes require Firecracker and are not compatible with organization BYOK",
        ));
    }
    let policy = env
        .var_os(POLICY_ENV)
        .ok_or_else(|| ControllerError::bad_request("block-volume admission is disabled"))?;
    let admission: Admission = serde_json::from_str(
        policy
            .to_str()
            .ok_or_else(|| ControllerError::internal("invalid block-volume admission policy"))?,
    )
    .map_err(|_| ControllerError::internal("invalid block-volume admission policy"))?;
    if admission.organization_ids.is_empty()
        || !admission.organization_ids.contains(&org)
        || admission.availability_zone.is_empty()
        || !admission
            .availability_zone
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(ControllerError::bad_request(
            "organization is not admitted to the block-volume experiment",
        ));
    }
    Ok(Some(admission))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg(feature = "kubernetes")]
struct Placement {
    availability_zone: String,
    node_names: BTreeSet<String>,
    #[serde(default)]
    hold_for_placement: bool,
    #[serde(default)]
    tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,
}

#[derive(Debug, thiserror::Error)]
#[cfg(feature = "kubernetes")]
pub(crate) enum PlacementError {
    #[error("retained disk placement is disabled or invalid")]
    Configuration,
    #[error("retained disk has no qualified same-AZ placement")]
    Unqualified,
    #[error("sandbox pod has no spec")]
    MissingSpec,
}

/// Intersect every existing affinity term: adding another OR term would let
/// Kubernetes satisfy the old affinity while ignoring the disk's AZ.
#[cfg(feature = "kubernetes")]
pub(crate) fn constrain_pod(
    pod: &mut k8s_openapi::api::core::v1::Pod,
    zone: &str,
    env: &dyn pplx_space_env_vars::Environment,
) -> Result<(), PlacementError> {
    use k8s_openapi::api::core::v1::{NodeSelector, NodeSelectorRequirement, NodeSelectorTerm};
    let raw = env
        .var("SPACE_EBS_PLACEMENT_POLICY")
        .map_err(|_| PlacementError::Configuration)?;
    let policy: Placement =
        serde_json::from_str(&raw).map_err(|_| PlacementError::Configuration)?;
    if policy.availability_zone != zone
        || policy.node_names.is_empty()
        || policy.node_names.len() > 2
        || policy.node_names.iter().any(|name| {
            name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        })
    {
        return Err(PlacementError::Unqualified);
    }
    let spec = pod.spec.as_mut().ok_or(PlacementError::MissingSpec)?;
    if spec
        .node_name
        .as_ref()
        .is_some_and(|name| !policy.node_names.contains(name))
    {
        return Err(PlacementError::Unqualified);
    }
    // A scheduling gate lets an authorized operator narrow this pod's target
    // without changing fleet availability or replacing CPU/AZ constraints.
    if policy.hold_for_placement {
        spec.scheduling_gates.get_or_insert_default().push(
            k8s_openapi::api::core::v1::PodSchedulingGate {
                name: "space.pplx.net/block-volume-placement".to_string(),
            },
        );
    }
    spec.tolerations
        .get_or_insert_default()
        .extend(policy.tolerations);
    let required = spec
        .affinity
        .get_or_insert_default()
        .node_affinity
        .get_or_insert_default()
        .required_during_scheduling_ignored_during_execution
        .get_or_insert_with(|| NodeSelector {
            node_selector_terms: vec![NodeSelectorTerm::default()],
        });
    // An explicitly empty selector matches no nodes. Preserve that denial.
    for term in &mut required.node_selector_terms {
        term.match_expressions.get_or_insert_default().extend([
            NodeSelectorRequirement {
                key: "topology.kubernetes.io/zone".to_string(),
                operator: "In".to_string(),
                values: Some(vec![zone.to_string()]),
            },
            NodeSelectorRequirement {
                key: "kubernetes.io/hostname".to_string(),
                operator: "In".to_string(),
                values: Some(policy.node_names.iter().cloned().collect()),
            },
        ]);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
