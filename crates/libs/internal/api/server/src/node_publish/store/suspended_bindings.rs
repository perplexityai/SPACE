use pplx_space_common_core::types::{ExecutionGeneration, PodUid, SandboxId};
use uuid::Uuid;

use crate::node_publish::plan::suspended::SuspendedClaim;

/// Nullable column arrays preserve the relationships in each validated claim.
#[derive(Debug)]
pub(super) struct SuspendedBindings<'a> {
    pub(super) ids: Vec<SandboxId>,
    pub(super) receipts: Vec<Option<Uuid>>,
    pub(super) receipt_kinds: Vec<Option<&'static str>>,
    pub(super) targets: Vec<Option<&'static str>>,
    pub(super) receipt_generations: Vec<Option<i64>>,
    pub(super) reasons: Vec<Option<&'static str>>,
    pub(super) policies: Vec<Option<&'static str>>,
    pub(super) artifacts: Vec<Option<Uuid>>,
    pub(super) artifact_kinds: Vec<Option<&'static str>>,
    pub(super) artifact_origins: Vec<Option<&'static str>>,
    pub(super) generations: Vec<Option<i64>>,
    pub(super) pod_uids: Vec<Option<&'a str>>,
}

impl<'a> From<&'a [SuspendedClaim]> for SuspendedBindings<'a> {
    fn from(claims: &'a [SuspendedClaim]) -> Self {
        Self {
            ids: claims.iter().map(|claim| claim.sandbox_id).collect(),
            receipts: claims
                .iter()
                .map(|claim| claim.receipt.map(|receipt| receipt.id().as_uuid()))
                .collect(),
            receipt_kinds: claims
                .iter()
                .map(|claim| claim.receipt.map(|receipt| receipt.kind().as_str()))
                .collect(),
            targets: claims
                .iter()
                .map(|claim| claim.receipt.map(|receipt| receipt.target_state().as_str()))
                .collect(),
            receipt_generations: claims
                .iter()
                .map(|claim| {
                    claim
                        .receipt
                        .map(|receipt| receipt.execution_generation().get())
                })
                .collect(),
            reasons: claims
                .iter()
                .map(|claim| {
                    claim
                        .receipt
                        .and_then(|receipt| receipt.reason_code().map(|reason| reason.as_str()))
                })
                .collect(),
            policies: claims
                .iter()
                .map(|claim| {
                    claim.receipt.and_then(|receipt| {
                        receipt
                            .reason_code()
                            .and_then(|reason| reason.automatic_recovery_policy())
                            .map(|policy| policy.as_str())
                    })
                })
                .collect(),
            artifacts: claims
                .iter()
                .map(|claim| {
                    claim
                        .artifact
                        .as_ref()
                        .map(|artifact| artifact.id().as_uuid())
                })
                .collect(),
            artifact_kinds: claims
                .iter()
                .map(|claim| {
                    claim
                        .artifact
                        .as_ref()
                        .map(|artifact| artifact.kind().as_str())
                })
                .collect(),
            artifact_origins: claims
                .iter()
                .map(|claim| {
                    claim
                        .artifact
                        .as_ref()
                        .map(|artifact| artifact.origin().as_str())
                })
                .collect(),
            generations: claims
                .iter()
                .map(|claim| claim.execution_generation.map(ExecutionGeneration::get))
                .collect(),
            pod_uids: claims
                .iter()
                .map(|claim| claim.pod_uid.as_ref().map(PodUid::as_str))
                .collect(),
        }
    }
}
