//! The placement state model: where one sandbox's execution stands, and the
//! fence that proves ownership of an in-flight placement attempt.

use jiff::Timestamp;
use pplx_space_common_core::types::{
    ExecutionGeneration, LocalArtifactId, NodeId, PodName, PodUid, SandboxId, SnapshotId,
};
use uuid::Uuid;

/// Where one sandbox's execution stands. A row is always in exactly one state.
///
/// Backed by today's schema: `Unplaced` is `placement = 'unplaced'` with a
/// NULL `active_node_id`; `Placing` is an active claim over the restore-lease
/// pair plus the active `sandbox_k8s_executions` generation; `Placed` is
/// `placement = 'on_node'` with the active execution row's pod; `Evacuating`
/// is the node-drain window (`nodes.drain_phase`,
/// `evacuation_restore_target`) plus the `Placing` fields during the restore
/// leg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// No execution anywhere; may hold a durable artifact to restore from.
    Unplaced { restorable: Option<ArtifactRef> },
    /// Exactly one claimed attempt to materialize an execution.
    Placing(Attempt),
    /// A live execution serving traffic.
    Placed {
        node: NodeId,
        pod: PodRef,
        generation: ExecutionGeneration,
    },
    /// Drain in progress: suspend to a durable artifact, restore elsewhere.
    Evacuating {
        from_node: NodeId,
        restorable: Option<ArtifactRef>,
    },
}

/// Which shape the claim took the row from: a fresh wake of a `suspended`
/// sandbox, a fresh re-placement of a released stopped evacuation, or the
/// re-drive of a `restoring` row whose prior attempt lapsed. A fresh claim
/// starts substrate work from scratch; a re-drive must first reconcile
/// whatever the lapsed attempt left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOrigin {
    FreshFromSuspended,
    /// Evacuation claims only. Unlike [`Self::FreshFromSuspended`] it can't be
    /// undone by restoring the prior lifecycle: leaving `restoring` for
    /// `stopped` clears the marker through its trigger. An undo keeps the row
    /// `restoring` (the api's rollback does; [`crate::PlacementStore::abandon`]
    /// must too once ported). [`crate::PlacementStore::complete_placement`]
    /// can't complete one yet either.
    FreshFromStopped,
    RedroveRestoring,
}

/// One placement attempt. Holding the fence is the only proof of ownership
/// any completing operation accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attempt {
    pub target_node: Option<NodeId>,
    pub fence: PlacementFence,
    pub lease_expires_at: Timestamp,
    pub attempt: u32,
    pub origin: ClaimOrigin,
}

/// Generation + lease token: the one fencing mechanism. Pod UIDs, lifecycle
/// strings, and freshness timestamps are evidence, never ownership.
///
/// The fields are private on purpose: a fence exists only where the machine
/// handed one out. A `restore_lease_token` read outside the machine cannot
/// be turned back into ownership; lapsed attempts complete under
/// [`PlacementProof::LapsedClaim`], never a caller-assembled fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementFence {
    pub(crate) sandbox: SandboxId,
    pub(crate) generation: ExecutionGeneration,
    pub(crate) lease: LeaseToken,
}

impl PlacementFence {
    pub fn sandbox(&self) -> SandboxId {
        self.sandbox
    }

    pub fn generation(&self) -> ExecutionGeneration {
        self.generation
    }

    pub fn lease(&self) -> LeaseToken {
        self.lease
    }

    /// Re-pair this fence's lease with the generation the holder actually
    /// observed (a cross-node re-drive can replace the generation under the
    /// same lease). Ownership stays the lease: the CAS still refuses a stale
    /// token or unbacked generation.
    pub fn for_generation(self, generation: ExecutionGeneration) -> Self {
        Self { generation, ..self }
    }

    /// Pair a held lease with an observed generation — the legacy bridge
    /// for the markerless `restoring` claim, which still stamps the row
    /// with legacy SQL. Powerless unless the token is the row's current
    /// lease. Doc-hidden rather than feature-gated so the bridge callers
    /// keep compiling while it is not advertised as API; it dies with the
    /// markerless-claim port, together with the public [`LeaseToken::mint`].
    #[doc(hidden)]
    pub fn from_held_lease(
        sandbox: SandboxId,
        generation: ExecutionGeneration,
        lease: LeaseToken,
    ) -> Self {
        Self {
            sandbox,
            generation,
            lease,
        }
    }
}

/// The ownership precondition under which
/// [`crate::PlacementStore::complete_placement`] may flip a claimed row to
/// Placed. One completion verb, two proofs; the CAS re-checks the variant's
/// guard set inside its own UPDATE.
///
/// The proof is also the completion's only identity: the fence carries the
/// sandbox it was minted for, and the lapsed-claim adoption names the row it
/// adopts. There is no separate sandbox parameter to disagree with the
/// proof.
#[derive(Debug, Clone, Copy)]
pub enum PlacementProof<'a> {
    /// The caller holds the row's current fence — it *is* the owner and
    /// completes without waiting. The CAS demands token equality, the
    /// fence's generation as the active execution, no teardown/terminal
    /// lifecycle, and recorded routable evidence (`running` + pod IP on a
    /// registered worker) at flip time — evidence as fresh as the
    /// substrate's last write, not a liveness probe. The completed row is
    /// the fence's own sandbox.
    Fence(&'a PlacementFence),
    /// The owning claim has lapsed — lease present *and expired*, work
    /// finished, completion never landed — and that row state is itself the
    /// proof. The caller holds nothing but the row's name; the CAS
    /// re-verifies inside its own UPDATE that the lease is expired, the row
    /// is still `restoring`, and the active execution records routable
    /// evidence. Checking expiry inside the UPDATE closes the
    /// read-then-assemble window a caller-built fence would reopen.
    LapsedClaim {
        /// The row to adopt — the only identity the caller supplies; every
        /// ownership fact is re-verified against the row itself.
        sandbox: SandboxId,
    },
}

/// The lease half of a [`PlacementFence`], stored in
/// `sandboxes.restore_lease_token`. Minted once per successful claim. There
/// is deliberately no constructor from a raw UUID: an observed column value
/// cannot be laundered back into ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
    /// Mint the lease token for a fresh claim. Public only as the other
    /// half of the legacy fence bridge ([`PlacementFence::from_held_lease`]):
    /// the machine's own claims mint internally, and the markerless
    /// `restoring` claim still mints a token to stamp with legacy SQL.
    /// Doc-hidden rather than feature-gated so those bridge callers keep
    /// compiling; both halves die with the markerless-claim port.
    #[doc(hidden)]
    pub fn mint() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for LeaseToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A durable artifact a placement can restore from: a durable full snapshot
/// row, or a node-published local artifact
/// (`sandboxes.current_local_artifact_id`). The portability predicates in
/// `common-svc` decide which backs a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactRef {
    Snapshot(SnapshotId),
    LocalArtifact(LocalArtifactId),
}

/// The pod an execution rides in — plain identity values; pod mechanics stay
/// outside this crate. The UID is present only once the pod has been observed
/// bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRef {
    pub name: PodName,
    pub uid: Option<PodUid>,
}

#[cfg(test)]
mod tests;
