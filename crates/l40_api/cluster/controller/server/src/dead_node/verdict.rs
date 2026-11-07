//! Whether a node is lost: one function over typed evidence.
//!
//! The loop gathers the evidence and [`verdict`] decides. Every input that is
//! missing, failed or ambiguous reads [`Verdict::Unknown`]; only evidence from
//! independent sources that all agree reads [`Verdict::Lost`]. Swapping the
//! instance lookup from private DNS names to instance IDs changes how
//! [`InstanceEvidence::Ec2`] is gathered, not this function.

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

/// What the cluster's instance authority says about the node's machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceEvidence {
    /// The cluster has no instance authority to ask and its terminator does
    /// not delete Kubernetes Nodes (the `noop` terminator, or a Direct
    /// substrate).
    NoAuthority,
    /// The cluster's terminator is `kube-delete`: a Node's absence from
    /// Kubernetes is what removing its machine means there.
    KubeDelete,
    /// EC2 `DescribeInstances` for the node and for a reference node known to
    /// be live, made the same way.
    Ec2 {
        node: InstanceLookup,
        reference: ReferenceLookup,
    },
}

/// EC2's answer for the node's private DNS name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceLookup {
    /// No match, or every match `terminated`.
    NoLiveMatch,
    /// A match in any other state, including one EC2 adds later.
    Live,
    Failed,
}

/// EC2's answer for the reference node: the control that an empty answer for
/// the node means the instance is gone rather than that the call asked the
/// wrong region, account or filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceLookup {
    /// The reference appears `running`.
    Running,
    /// The reference does not appear, or not `running`.
    NotRunning,
    /// No fresh node whose Node the reflector lists could serve as a
    /// reference.
    Unavailable,
    Failed,
}

/// Everything [`verdict`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerdictInputs {
    pub heartbeat: Heartbeat,
    pub kube: KubeEvidence,
    /// How long the recorded Node must stay absent from the reflector.
    pub absence_window: Duration,
    pub instance: InstanceEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Lost(LostReason),
    Unknown(UnknownReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LostReason {
    /// EC2 has no live instance, the reference is live, and the Node is gone.
    InstanceAndNodeGone,
    /// The Node is gone on a `kube-delete` cluster.
    NodeDeleted,
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
    NoInstanceAuthority,
    InstanceLive,
    InstanceLookupFailed,
    ReferenceUnavailable,
    ReferenceNotRunning,
    ReferenceLookupFailed,
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
}

impl LostReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InstanceAndNodeGone => "instance_and_node_gone",
            Self::NodeDeleted => "node_deleted",
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
            Self::NoInstanceAuthority => "no_instance_authority",
            Self::InstanceLive => "instance_live",
            Self::InstanceLookupFailed => "instance_lookup_failed",
            Self::ReferenceUnavailable => "reference_unavailable",
            Self::ReferenceNotRunning => "reference_not_running",
            Self::ReferenceLookupFailed => "reference_lookup_failed",
        }
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether the node is lost. Lost only when the heartbeat is stale, the
/// recorded Node has been absent from the reflector for the whole window and
/// a direct GET returns 404, and the instance authority agrees: EC2 lists no
/// live instance while listing the reference `running`, or the cluster's
/// terminator is `kube-delete`. The first condition that fails names the
/// reason.
pub fn verdict(inputs: &VerdictInputs) -> Verdict {
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
    match inputs.instance {
        InstanceEvidence::NoAuthority => Verdict::Unknown(U::NoInstanceAuthority),
        InstanceEvidence::KubeDelete => Verdict::Lost(LostReason::NodeDeleted),
        InstanceEvidence::Ec2 { node, reference } => match (node, reference) {
            (InstanceLookup::Failed, _) => Verdict::Unknown(U::InstanceLookupFailed),
            (InstanceLookup::Live, _) => Verdict::Unknown(U::InstanceLive),
            (InstanceLookup::NoLiveMatch, ReferenceLookup::Failed) => {
                Verdict::Unknown(U::ReferenceLookupFailed)
            }
            (InstanceLookup::NoLiveMatch, ReferenceLookup::Unavailable) => {
                Verdict::Unknown(U::ReferenceUnavailable)
            }
            (InstanceLookup::NoLiveMatch, ReferenceLookup::NotRunning) => {
                Verdict::Unknown(U::ReferenceNotRunning)
            }
            (InstanceLookup::NoLiveMatch, ReferenceLookup::Running) => {
                Verdict::Lost(LostReason::InstanceAndNodeGone)
            }
        },
    }
}

#[cfg(test)]
mod tests;
