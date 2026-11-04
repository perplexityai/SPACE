//! Request shapes for the operations declared ahead of their ports —
//! minimal fields so the full [`crate::PlacementStore`] trait can be
//! declared at once; each port PR refines its shape, so none is stable
//! until then.

use pplx_space_common_core::types::{IdempotencyKey, K8sNodeName, K8sNodeUid, SandboxId};

use crate::model::{ArtifactRef, PodRef};

/// Request for [`crate::PlacementStore::create`]: insert the sandbox row
/// `Unplaced` (guards: id free, idempotency key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSandbox {
    pub sandbox: SandboxId,
    pub idempotency_key: IdempotencyKey,
}

/// The pod the substrate reports bound for a claimed attempt — evidence,
/// never authority: it can block a completion but cannot authorize one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodEvidence {
    pub pod: PodRef,
    /// The kube node name the pod was observed scheduled onto; the bind
    /// resolves it to a SPACE node the same way the completion CAS does.
    pub k8s_node_name: K8sNodeName,
}

/// Which drain leg an [`crate::PlacementStore::evacuate`] call advances.
/// The legacy writer family also carries revert and adoption legs; those
/// arrive with the port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvacuationStep {
    /// Mark the resident for evacuation: `Placed` (or `Unplaced` + artifact)
    /// → `Evacuating`.
    Mark,
    /// Commit the suspend leg's durable artifact:
    /// `Evacuating` → `Unplaced { artifact }`.
    CommitSuspend { artifact: ArtifactRef },
}

/// Which incarnation of a node the caller observed before declaring it
/// dead; [`crate::PlacementStore::node_loss`] re-verifies it under lock so
/// a same-name replacement matches nothing.
///
/// The one abstraction this otherwise Kubernetes-concrete crate keeps,
/// because the identity is known to churn: today the kube node UID, later
/// `node_incarnations` rows — the swap stays inside this crate.
///
/// `node_k8s.k8s_node_uid` is nullable. [`NodeIncarnation::Unidentified`]
/// fails closed: with no incarnation, a dead node and its same-name
/// replacement are indistinguishable, so the operation refuses to sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeIncarnation {
    /// The kube node UID observed on the node's registration row.
    KubeUid(K8sNodeUid),
    /// The registration row stored no UID; nothing to verify against.
    Unidentified,
}

impl From<Option<K8sNodeUid>> for NodeIncarnation {
    /// The boundary parse: the nullable column converts exactly once.
    fn from(observed: Option<K8sNodeUid>) -> Self {
        match observed {
            Some(uid) => Self::KubeUid(uid),
            None => Self::Unidentified,
        }
    }
}

/// The hold record a [`crate::PlacementStore::quarantine`] writes in the
/// same transaction as the placement flip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineHold {
    pub reason: String,
}

/// An audited operator bypass for [`crate::PlacementStore::admin_override`]:
/// guards are skipped explicitly, never silently; the principal is recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminOverride {
    pub sandbox: SandboxId,
    pub principal: String,
}
