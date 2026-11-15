//! Whether an EC2 instance a node's rows ran on is dead, and whether the node
//! itself is lost: two functions over typed evidence.
//!
//! The loop gathers the evidence and these decide. Every input that is
//! missing, failed or ambiguous reads unknown; only evidence from independent
//! sources that all agree reads dead or lost.
//!
//! An instance is judged by its exact ID ([`instance_verdict`]): the node's
//! recorded `node_k8s.instance_id` and each execution stamp on its rows. A
//! same-name replacement reuses the node id and the Kubernetes name, never
//! the instance ID, so a dead instance's rows are told apart from its
//! replacement's. The node is lost ([`node_verdict`]) when no live machine
//! holds its id: its heartbeat is stale, its recorded Node is gone, and its
//! recorded instance is dead.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// The node's status heartbeat, by the rule the API's node status uses
/// (`pplx_space_sandbox_residency::node_heartbeat_fresh`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heartbeat {
    Fresh,
    Stale,
}

/// What Kubernetes says about the Node the row recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KubeEvidence {
    /// The row has no Kubernetes name and uid to look for.
    NoRecordedIdentity,
    /// The Node reflector has not finished its first list, or lists nothing,
    /// so absence from it means nothing.
    MembershipUnknown,
    /// The reflector lists a Node with the recorded uid.
    Present,
    /// The reflector has not listed the recorded uid for `absent_for`, and a
    /// direct GET of the Node's name answered `direct_get`.
    Absent {
        absent_for: Duration,
        direct_get: DirectGet,
    },
}

/// A direct GET of the Node by name, which a lagging reflector cannot answer
/// for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectGet {
    /// 404: no Node has the name.
    NotFound,
    /// A Node has the name, with the recorded uid or, after a replacement,
    /// another.
    Found,
    Failed,
}

/// What the cluster's instance authority answered for one tick's question
/// about a node: the instances its rows reference, asked together with a
/// reference instance in one `DescribeInstances` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceEvidence {
    /// The cluster has no instance authority: its terminator is not `aws`,
    /// or the substrate is Direct. An unexpected death there reads unknown.
    NoAuthority,
    /// No fresh node whose Node the reflector lists, with a well-formed
    /// recorded instance, could serve as the reference, so no call was made.
    ReferenceUnavailable,
    /// The call failed or timed out.
    LookupFailed,
    /// EC2 answered. `listed` maps each instance in the answer to its state;
    /// `reference` is the reference instance's ID, which was asked for too.
    Answered {
        listed: BTreeMap<String, String>,
        reference: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceVerdict {
    Dead(DeadReason),
    /// EC2 lists the instance in a state other than `terminated`, or the
    /// node's fresh heartbeat comes from it.
    Live,
    Unknown(UnknownReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadReason {
    /// EC2 lists the instance `terminated`.
    Terminated,
    /// EC2 doesn't list the instance while listing the reference: it
    /// terminated long enough ago to be forgotten.
    Absent,
}

/// Everything [`node_verdict`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeVerdictInputs {
    pub heartbeat: Heartbeat,
    pub kube: KubeEvidence,
    /// How long the recorded Node must stay absent from the reflector.
    pub absence_window: Duration,
    /// The verdict on the node's recorded instance; `None` when it records
    /// none.
    pub recorded_instance: Option<InstanceVerdict>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Lost(LostReason),
    Unknown(UnknownReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostReason {
    /// The recorded instance is dead, the Node is gone and the heartbeat is
    /// stale.
    InstanceAndNodeGone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownReason {
    HeartbeatFresh,
    NoKubeIdentity,
    KubeMembershipUnknown,
    KubeNodePresent,
    KubeAbsenceSettling,
    KubeNodeFound,
    KubeNodeLookupFailed,
    NoRecordedInstance,
    MalformedInstanceId,
    NoInstanceAuthority,
    InstanceLive,
    InstanceLookupFailed,
    ReferenceUnavailable,
    ReferenceMissing,
    ReferenceTerminated,
}

impl Verdict {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lost(_) => "lost",
            Self::Unknown(_) => "unknown",
        }
    }

    pub const fn reason(self) -> &'static str {
        match self {
            Self::Lost(reason) => reason.as_str(),
            Self::Unknown(reason) => reason.as_str(),
        }
    }

    pub const fn is_lost(self) -> bool {
        matches!(self, Self::Lost(_))
    }
}

impl InstanceVerdict {
    pub const fn is_dead(self) -> bool {
        matches!(self, Self::Dead(_))
    }
}

impl LostReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InstanceAndNodeGone => "instance_and_node_gone",
        }
    }
}

impl UnknownReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HeartbeatFresh => "heartbeat_fresh",
            Self::NoKubeIdentity => "no_kube_identity",
            Self::KubeMembershipUnknown => "kube_membership_unknown",
            Self::KubeNodePresent => "kube_node_present",
            Self::KubeAbsenceSettling => "kube_absence_settling",
            Self::KubeNodeFound => "kube_node_found",
            Self::KubeNodeLookupFailed => "kube_node_lookup_failed",
            Self::NoRecordedInstance => "no_recorded_instance",
            Self::MalformedInstanceId => "malformed_instance_id",
            Self::NoInstanceAuthority => "no_instance_authority",
            Self::InstanceLive => "instance_live",
            Self::InstanceLookupFailed => "instance_lookup_failed",
            Self::ReferenceUnavailable => "reference_unavailable",
            Self::ReferenceMissing => "reference_missing",
            Self::ReferenceTerminated => "reference_terminated",
        }
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether `instance_id` is an EC2 instance ID
/// ([`pplx_space_k8s_node_id::Ec2InstanceId`]). Anything else is not asked
/// about: an ID EC2 would not recognize is absent from any answer, which
/// would read dead.
pub fn well_formed_instance_id(instance_id: &str) -> bool {
    pplx_space_k8s_node_id::Ec2InstanceId::parse(instance_id).is_some()
}

/// Whether `instance_id` is dead by `evidence`. Dead only when EC2 answered,
/// listed the reference in a state other than `terminated`, and lists the
/// instance `terminated` or not at all; listed in any other state, including
/// one EC2 adds later, it is live. A malformed ID reads unknown before any
/// evidence is read.
pub fn instance_verdict(instance_id: &str, evidence: &InstanceEvidence) -> InstanceVerdict {
    use UnknownReason as U;
    if !well_formed_instance_id(instance_id) {
        return InstanceVerdict::Unknown(U::MalformedInstanceId);
    }
    let (listed, reference) = match evidence {
        InstanceEvidence::NoAuthority => return InstanceVerdict::Unknown(U::NoInstanceAuthority),
        InstanceEvidence::ReferenceUnavailable => {
            return InstanceVerdict::Unknown(U::ReferenceUnavailable);
        }
        InstanceEvidence::LookupFailed => return InstanceVerdict::Unknown(U::InstanceLookupFailed),
        InstanceEvidence::Answered { listed, reference } => (listed, reference),
    };
    match listed.get(reference).map(String::as_str) {
        None => return InstanceVerdict::Unknown(U::ReferenceMissing),
        Some("terminated") => return InstanceVerdict::Unknown(U::ReferenceTerminated),
        Some(_) => {}
    }
    match listed.get(instance_id).map(String::as_str) {
        None => InstanceVerdict::Dead(DeadReason::Absent),
        Some("terminated") => InstanceVerdict::Dead(DeadReason::Terminated),
        Some(_) => InstanceVerdict::Live,
    }
}

/// Whether the node is lost: no live machine holds its id. Lost only when the
/// heartbeat is stale, the recorded Node has been absent from the reflector
/// for the whole window and a direct GET returns 404, and the node's recorded
/// instance is dead. The first condition that fails names the reason.
pub fn node_verdict(inputs: &NodeVerdictInputs) -> Verdict {
    use UnknownReason as U;
    if inputs.heartbeat == Heartbeat::Fresh {
        return Verdict::Unknown(U::HeartbeatFresh);
    }
    match inputs.kube {
        KubeEvidence::NoRecordedIdentity => return Verdict::Unknown(U::NoKubeIdentity),
        KubeEvidence::MembershipUnknown => return Verdict::Unknown(U::KubeMembershipUnknown),
        KubeEvidence::Present => return Verdict::Unknown(U::KubeNodePresent),
        KubeEvidence::Absent { absent_for, .. } if absent_for < inputs.absence_window => {
            return Verdict::Unknown(U::KubeAbsenceSettling);
        }
        KubeEvidence::Absent { direct_get, .. } => match direct_get {
            DirectGet::NotFound => {}
            DirectGet::Found => return Verdict::Unknown(U::KubeNodeFound),
            DirectGet::Failed => return Verdict::Unknown(U::KubeNodeLookupFailed),
        },
    }
    match inputs.recorded_instance {
        None => Verdict::Unknown(U::NoRecordedInstance),
        Some(InstanceVerdict::Dead(_)) => Verdict::Lost(LostReason::InstanceAndNodeGone),
        Some(InstanceVerdict::Live) => Verdict::Unknown(U::InstanceLive),
        Some(InstanceVerdict::Unknown(reason)) => Verdict::Unknown(reason),
    }
}

#[cfg(test)]
mod tests;
