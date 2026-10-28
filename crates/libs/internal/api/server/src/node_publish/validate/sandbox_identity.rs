use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, ExecutionGeneration, LocalArtifactId, PodUid,
    SandboxQuarantineReasonCode, SandboxTransitionReceipt, SandboxTransitionReceiptKind,
};
use time::OffsetDateTime;
use tonic::Status;

use super::fields::{proto_timestamp, published_artifact_origin};
use crate::metadata::{artifact_kind_from_proto, optional_trimmed};
use crate::proto::PublishedSandboxState;

#[derive(Clone, Debug)]
pub(crate) struct PublishedLocalArtifact {
    id: LocalArtifactId,
    kind: ArtifactKind,
    origin: ArtifactOrigin,
}

impl PublishedLocalArtifact {
    pub(crate) fn id(&self) -> LocalArtifactId {
        self.id
    }

    pub(crate) fn kind(&self) -> ArtifactKind {
        self.kind
    }

    pub(crate) fn origin(&self) -> ArtifactOrigin {
        self.origin
    }
}

#[derive(Debug)]
pub(crate) struct SandboxIdentityEvidence {
    current_local_artifact: Option<PublishedLocalArtifact>,
    state_changed_at: Option<OffsetDateTime>,
    execution_generation: Option<ExecutionGeneration>,
    pod_uid: Option<PodUid>,
}

impl SandboxIdentityEvidence {
    pub(crate) fn try_from_proto(
        state: &PublishedSandboxState,
        receipt: Option<&SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    ) -> Result<Self, Status> {
        let current_local_artifact = current_local_artifact(state, receipt)?;
        let state_changed_at = state
            .state_changed_at
            .as_ref()
            .map(|timestamp| proto_timestamp(timestamp, "state_changed_at"))
            .transpose()?;
        if optional_trimmed(state.status.as_deref()).is_none() && state_changed_at.is_some() {
            return Err(Status::invalid_argument("state_changed_at requires status"));
        }
        let execution_generation = state
            .execution_generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let pod_uid = state
            .pod_uid
            .as_deref()
            .map(PodUid::parse)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Self {
            current_local_artifact,
            state_changed_at,
            execution_generation,
            pod_uid,
        })
    }

    pub(crate) fn current_local_artifact(&self) -> Option<&PublishedLocalArtifact> {
        self.current_local_artifact.as_ref()
    }

    pub(crate) fn state_changed_at(&self) -> Option<OffsetDateTime> {
        self.state_changed_at
    }

    pub(crate) fn execution_generation(&self) -> Option<ExecutionGeneration> {
        self.execution_generation
    }

    pub(crate) fn pod_uid(&self) -> Option<&PodUid> {
        self.pod_uid.as_ref()
    }
}

fn current_local_artifact(
    state: &PublishedSandboxState,
    receipt: Option<&SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
) -> Result<Option<PublishedLocalArtifact>, Status> {
    match (
        optional_trimmed(state.current_local_artifact_id.as_deref()),
        state.current_local_artifact_kind,
        state.current_local_artifact_origin,
    ) {
        (None, None, None) => Ok(None),
        (Some(raw_id), Some(raw_kind), Some(raw_origin)) => {
            let id = raw_id.parse::<LocalArtifactId>().map_err(|error| {
                Status::invalid_argument(format!("invalid current_local_artifact_id: {error}"))
            })?;
            let kind = artifact_kind_from_proto(raw_kind).ok_or_else(|| {
                Status::invalid_argument("current_local_artifact_kind must be DISK or FULL")
            })?;
            let origin = published_artifact_origin(raw_origin)?;
            if receipt.is_some_and(|receipt| {
                receipt.kind() == SandboxTransitionReceiptKind::QuarantineRecovery
            }) && (kind != ArtifactKind::Full || origin != ArtifactOrigin::Suspend)
            {
                return Err(Status::invalid_argument(
                    "suspended recovery artifact must be a full suspend artifact",
                ));
            }
            Ok(Some(PublishedLocalArtifact { id, kind, origin }))
        }
        _ => Err(Status::invalid_argument(
            "current local artifact identity must be fully present or absent",
        )),
    }
}

#[cfg(test)]
mod tests;
