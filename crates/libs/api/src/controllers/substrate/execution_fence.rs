//! The identity fence that decides whether a recorded execution is the one a retire or teardown may
//! act on.

use super::*;

/// Pod identity a caller observed and wants a retire pinned to.
///
/// `retire_execution` addresses a sandbox by id, resolves whatever pod is
/// tracked at call time, and tears it down. Callers reach that call after a
/// probe and a CAS, and the pod can change in between — recover, be replaced
/// by a same-name successor, or be superseded by a newer-generation execution.
/// A fence carries the identity the caller actually saw so the substrate can
/// refuse instead of acting on stale evidence.
///
/// The name is required; the other two are what the caller happened to know,
/// and each one it has strengthens the fence:
///
/// * **Name alone** may act only on an *absent* pod. It pins the teardown to
///   one execution — the control plane can answer "is this still the active
///   execution?", which catches a successor — but it can prove nothing about
///   a live object at that name, so a live pod refuses. This is the shape the
///   sweeper falls back to for an execution whose pod UID has not been
///   persisted yet, which is a real state: the UID is written after the pod is
///   created. It is what lets the sweep still collect rows whose pod is gone,
///   without ever deleting a pod whose identity nobody observed. The state is
///   self-clearing: once pod metadata persists, the next candidate select
///   carries a UID and the full fence applies.
/// * **`pod_uid`** adds the apiserver-enforced precondition, catching a
///   same-name pod that was replaced.
/// * **`resource_version`** catches the same pod having *moved* — bound,
///   status changed, recovered. Only a caller that read the live pod object
///   has one; `sandbox_k8s_executions` has no column for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodIdentityFence {
    pub pod_name: PodName,
    pub pod_uid: Option<PodUid>,
    /// What the caller's execution row said about itself when it was read.
    /// Distinct from [`Self::pod_uid`], which is what the caller saw of
    /// the live object — a row can record a UID whose pod is long gone, and a
    /// row re-claimed for a replacement records none while its predecessor's
    /// pod is still there.
    pub recorded: RecordedExecution,
    pub resource_version: Option<PodResourceVersion>,
    /// The activation attempt the caller selected.
    ///
    /// Neither the generation nor the pod name identifies an *attempt*: an
    /// activation retry or a cross-node redrive reuses both, and the pod name
    /// is derived from the generation. So a replacement attempt can look
    /// identical to the one the caller observed — same generation, same name,
    /// and before it materializes, the same absent UID. This is the only field
    /// that tells them apart, which matters most where the rest of the fence
    /// is weakest.
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    /// The execution generation the caller selected, when it recorded one.
    pub generation: Option<i64>,
}

/// What the execution row records about itself.
///
/// Grouped by *who observed it* rather than by field. Everything inside
/// [`Self::Read`] came from one read of one row, so it is all known or all
/// unknown together, and "the caller never looked" is one state rather than a
/// `None` on each field. That distinction is the whole point: for a row, each
/// of these absences is a *value* — a re-claimed row records no pod, a
/// completed activation records no attempt — so an `Option` per field would
/// let "I did not look" authorize exactly what "the row records nothing"
/// should be caught by.
///
/// A new fact the row keeps about itself goes inside `Read`, where the
/// exhaustive destructure in [`ExecutionIdentity::reconcile`] forces it to be
/// given a rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedExecution {
    /// The caller did not read the row, so it constrains nothing.
    Unread,
    /// What the row said when the caller read it.
    Read {
        /// Which pod the row claims as its own. Cleared by a replacement
        /// claim, and left behind when a pod is deleted, so it is neither the
        /// live pod's UID nor derivable from it.
        pod_uid: Option<PodUid>,
        /// The durable identity of the activation that owns this execution.
        /// Outlives the ephemeral attempt id, which is cleared on completion —
        /// so two activations that have both completed are otherwise
        /// indistinguishable, and this is what tells them apart.
        route_activation_id: Option<SandboxActivationAttemptId>,
    },
}

impl RecordedExecution {
    /// The row's pod claim. `Unread` is not a row state; only row-derived
    /// values are ever bound into SQL, and those are never `Unread`.
    #[must_use]
    pub fn pod_uid(&self) -> Option<&PodUid> {
        match self {
            Self::Read { pod_uid, .. } => pod_uid.as_ref(),
            Self::Unread => None,
        }
    }

    /// The row's durable activation identity, with the same caveat.
    #[must_use]
    pub fn route_activation_id(&self) -> Option<SandboxActivationAttemptId> {
        match self {
            Self::Read {
                route_activation_id,
                ..
            } => *route_activation_id,
            Self::Unread => None,
        }
    }
}

/// What a caller saw of the pod object itself.
///
/// A sum type rather than two more `Option`s because "I could not see the pod"
/// and "I saw that there is no pod" authorize opposite things, and an `Option`
/// spells them the same way. Every bug this fence has had came from a site
/// reading an absent field as "no expectation here" — so absence is spelled
/// out as a variant that has to be matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedPod {
    /// The caller had no view of the pod object. Nothing about a live pod can
    /// be proven from such an observation, so it may act only on a pod that is
    /// *still* absent.
    Unobserved,
    /// The caller looked and there was no pod at that name.
    Absent,
    /// The caller saw this exact object.
    Seen {
        pod_uid: PodUid,
        /// Only a caller that read the live object has one; the execution
        /// table has no column for it.
        resource_version: Option<PodResourceVersion>,
        /// Whether the pod has reached a phase it cannot leave.
        ///
        /// A pod that is merely running can still *change* — Pending going
        /// Running keeps its UID, which is why a caller with no
        /// resourceVersion may not act on one. A terminal pod cannot, so its
        /// UID is a complete identification. That distinction is what keeps
        /// terminal pods collectable at all: the filtered reflector drops
        /// them, so no caller can ever observe one, and without this every
        /// terminal pod would defer forever behind a name-only fence.
        terminal: bool,
    },
}

/// One execution's identity, as observed at a single instant.
///
/// This type exists because "is this still the execution I observed?" was
/// being answered independently at six places — the pod-name check, the
/// attempt check, the live-pod check, the apiserver delete precondition, the
/// tracking-release CAS, and the UID backfill — and those six disagreed about
/// which fields matter *and* about what an absent field means. Every review
/// finding against this fence was one of those six picking a wrong subset or
/// reading absence as a wildcard. One value, compared in one place, is what
/// makes that class of bug unrepresentable rather than repeatedly patched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionIdentity {
    pub pod_name: PodName,
    pub generation: Option<i64>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    /// What the execution row said about itself when the caller read it.
    ///
    /// Distinct from [`Self::pod`], which is the live object, and not
    /// derivable from it. This is the only part of the identity that can catch
    /// a row changing underneath a caller whose pod is absent — the case where
    /// there is no live object left to compare at all. It is also what the
    /// database guards bind, so the values they enforce and the values
    /// compared here cannot drift apart.
    pub recorded: RecordedExecution,
    pub pod: ObservedPod,
}

impl ExecutionIdentity {
    /// The single answer to "does `current` describe the execution I observed?"
    ///
    /// `self` is the observation; `current` is the world now. Every field is
    /// destructured rather than accessed, so adding one to
    /// [`ExecutionIdentity`] fails to compile here until it is given a rule —
    /// the structural reason a future field cannot be silently left out of the
    /// comparison the way `generation` originally was.
    ///
    /// Absence is a value everywhere except where it is *unobservable*: a
    /// caller that never saw the pod cannot claim anything about a live one,
    /// and a resourceVersion is only available to a caller that read the
    /// object, so its absence constrains nothing.
    pub fn reconcile(&self, current: &Self) -> Result<(), PodFenceMismatch> {
        let Self {
            pod_name,
            generation,
            activation_attempt_id,
            recorded,
            pod,
        } = self;

        if pod_name != &current.pod_name {
            return Err(PodFenceMismatch::ExecutionSuperseded);
        }
        if generation.is_some() && generation != &current.generation {
            return Err(PodFenceMismatch::ExecutionSuperseded);
        }
        if activation_attempt_id != &current.activation_attempt_id {
            return Err(PodFenceMismatch::ActivationAttemptChanged);
        }
        // A row that claimed no pod and now claims one has been taken by a
        // create since the caller looked; one that claimed a pod and now
        // claims none has been re-claimed for a replacement. Both are the row
        // becoming someone else's, and when the pod is absent this is the only
        // field left that can say so.
        if let RecordedExecution::Read {
            pod_uid,
            route_activation_id,
        } = recorded
        {
            if pod_uid.as_ref() != current.recorded.pod_uid() {
                return Err(PodFenceMismatch::PodUidChanged);
            }
            if *route_activation_id != current.recorded.route_activation_id() {
                return Err(PodFenceMismatch::ActivationAttemptChanged);
            }
        }
        match (pod, &current.pod) {
            // Nothing left at that name to protect: the delete would 404 and
            // the rest of the retire is still wanted. Refusing here would
            // strand every row whose pod is genuinely gone. Safe to permit
            // only because the recorded-UID check above already rejected a row
            // that was re-claimed while its replacement's pod had yet to
            // appear — without that, an absent pod would make every remaining
            // field match a row in the middle of becoming someone else's.
            (_, ObservedPod::Absent) => Ok(()),
            // A caller that never saw the pod is normally refused against a
            // live one: a Pending pod going Running keeps its UID, so a UID
            // alone cannot tell "the object I meant" from "the recovery I must
            // not destroy". A *terminal* pod cannot recover, so its UID is a
            // complete identification — and it has to be, because the filtered
            // reflector drops terminal pods and no caller can ever observe
            // one. Without this arm every terminal pod defers forever.
            (
                ObservedPod::Unobserved,
                ObservedPod::Seen {
                    pod_uid: current_uid,
                    terminal: true,
                    ..
                },
            ) if recorded.pod_uid() == Some(current_uid) => Ok(()),
            // No view of the pod, but there is one now.
            (ObservedPod::Unobserved | ObservedPod::Absent, _) => {
                Err(PodFenceMismatch::PodIdentityUnverified)
            }
            (
                ObservedPod::Seen {
                    pod_uid,
                    resource_version,
                    terminal: _,
                },
                ObservedPod::Seen {
                    pod_uid: current_uid,
                    resource_version: current_version,
                    terminal: _,
                },
            ) => {
                if pod_uid != current_uid {
                    return Err(PodFenceMismatch::PodUidChanged);
                }
                if resource_version.is_some() && resource_version != current_version {
                    return Err(PodFenceMismatch::PodResourceVersionChanged);
                }
                Ok(())
            }
            (ObservedPod::Seen { .. }, ObservedPod::Unobserved) => {
                Err(PodFenceMismatch::PodIdentityUnverified)
            }
        }
    }
}

impl PodIdentityFence {
    /// A fence on the pod name alone — supersession only. Prefer
    /// [`Self::with_pod_uid`] whenever a UID is recorded.
    pub fn new(pod_name: PodName) -> Self {
        Self {
            pod_name,
            pod_uid: None,
            resource_version: None,
            activation_attempt_id: None,
            generation: None,
            recorded: RecordedExecution::Unread,
        }
    }

    /// Record what the caller's execution row said about itself, as read at
    /// select time.
    #[must_use]
    pub fn with_recorded(mut self, recorded: RecordedExecution) -> Self {
        self.recorded = recorded;
        self
    }

    #[must_use]
    pub fn with_generation(mut self, generation: i64) -> Self {
        self.generation = Some(generation);
        self
    }

    /// The fence as an [`ExecutionIdentity`] — the one shape every guard on
    /// this path compares and binds from.
    pub fn observed(&self) -> ExecutionIdentity {
        ExecutionIdentity {
            pod_name: self.pod_name.clone(),
            generation: self.generation,
            activation_attempt_id: self.activation_attempt_id,
            recorded: self.recorded.clone(),
            pod: match self.pod_uid.clone() {
                Some(pod_uid) => ObservedPod::Seen {
                    pod_uid,
                    resource_version: self.resource_version.clone(),
                    // A caller's observation comes from the filtered reflector,
                    // which drops terminal pods, so it is never one.
                    terminal: false,
                },
                None => ObservedPod::Unobserved,
            },
        }
    }

    #[must_use]
    pub fn with_activation_attempt(
        mut self,
        activation_attempt_id: SandboxActivationAttemptId,
    ) -> Self {
        self.activation_attempt_id = Some(activation_attempt_id);
        self
    }

    #[must_use]
    pub fn with_pod_uid(mut self, pod_uid: PodUid) -> Self {
        self.pod_uid = Some(pod_uid);
        self
    }

    #[must_use]
    pub fn with_resource_version(mut self, resource_version: PodResourceVersion) -> Self {
        self.resource_version = Some(resource_version);
        self
    }
}

/// What the caller observed about the sandbox's execution, and therefore what
/// the substrate has to re-prove before tearing anything down.
///
/// "I saw no execution" is a real observation and gets its own variant rather
/// than collapsing into "no fence". Collapsing them is a fail-open: a row that
/// had no execution when it was selected is exactly the row a concurrent
/// create or wake is most likely to give one, and the unfenced teardown is the
/// most destructive path there is — it deletes every tracked pod name and
/// releases tracking unconditionally. Having seen nothing must not authorize
/// that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetireFence {
    /// Nothing observed; retire whatever is tracked. Correct only for a caller
    /// that has already made the pod provably irrelevant — it deleted the pod
    /// itself under its own precondition first — not for one that simply has
    /// no identity to hand.
    Unobserved,
    /// The caller observed the sandbox with *no* active execution. The retire
    /// must refuse if one exists by the time it runs.
    NoExecution,
    /// The caller observed this pod.
    Pod(PodIdentityFence),
}

/// A pod's identity as the substrate currently sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedPodIdentity {
    pub pod_uid: PodUid,
    pub resource_version: Option<PodResourceVersion>,
}

/// Arguments for [`SubstrateAdapter::retire_execution`]. Grouped rather than
/// positional for the same reason [`CreateExecutionRequest`] is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireExecutionRequest {
    pub sandbox_id: SandboxId,
    pub fence: RetireFence,
}

impl RetireExecutionRequest {
    pub fn fenced(sandbox_id: SandboxId, pod_fence: PodIdentityFence) -> Self {
        Self {
            sandbox_id,
            fence: RetireFence::Pod(pod_fence),
        }
    }
}

/// Which half of a [`PodIdentityFence`] the substrate found violated. Carried
/// so callers can log and meter *why* they deferred without re-deriving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodFenceMismatch {
    /// The fenced pod's execution row is no longer the sandbox's active
    /// execution: a newer generation took over. Retiring now would delete the
    /// successor's pod, which the caller never observed.
    ExecutionSuperseded,
    /// No execution row records the fenced pod name at all — or, for a caller
    /// that observed no execution, one exists now.
    ExecutionMissing,
    /// A live pod exists at the fenced name with a different UID: the pod was
    /// replaced.
    PodUidChanged,
    /// A different activation attempt owns the execution now. Same generation,
    /// same pod name — a retry or a redrive replaced the attempt the caller
    /// selected.
    ActivationAttemptChanged,
    /// The live pod is the same object but has moved on (bound, status
    /// changed) since the caller observed it.
    PodResourceVersionChanged,
    /// The apiserver rejected the preconditioned delete (409). The pod changed
    /// between the check above and the delete itself.
    DeletePreconditionRejected,
    /// The fence carries no pod UID and a live pod exists at that name, so
    /// there is nothing to prove the object is the one the caller observed.
    /// A name-only fence may only act on an absent pod.
    PodIdentityUnverified,
}

impl PodFenceMismatch {
    /// Whether something the in-VM stop did *not* reach may now own the
    /// sandbox.
    ///
    /// This decides whether [`TeardownProgress::RuntimeStopped`] lets a caller
    /// leave its own state transition standing. The stop is addressed to the
    /// execution the fence observed, so it proves the sandbox is down only
    /// where the mismatch describes *that same object* having moved. Anything
    /// else — a new execution, a new attempt, a different pod at the same
    /// name, or an identity that could not be proven at all — may be running
    /// untouched, and leaving a dormant or terminal lifecycle over it is the
    /// stranded state nothing revisits.
    ///
    /// Exactly one variant is safe to answer `false` to, so the default is
    /// `true` and each exception has to earn it. Answering `true` when the
    /// sandbox happens to be down costs one re-evaluation on the next tick;
    /// answering `false` when it is up strands a live sandbox.
    #[must_use]
    pub fn successor_took_over(self) -> bool {
        match self {
            // The same pod object, at a later resourceVersion: a Pending pod
            // coming up is the canonical case. The stop reached it.
            Self::PodResourceVersionChanged => false,
            // A different pod at that name — the stop reached the old one and
            // this one is untouched.
            Self::PodUidChanged
            // A different execution or attempt owns the sandbox now.
            | Self::ExecutionSuperseded
            | Self::ActivationAttemptChanged
            | Self::ExecutionMissing
            // The apiserver rejected the delete's precondition, which it
            // returns as one 409 whether the pod moved or was replaced. Not
            // worth another read to disambiguate: the conservative answer only
            // costs a re-evaluation.
            | Self::DeletePreconditionRejected
            // Nothing could be proven about what is there.
            | Self::PodIdentityUnverified => true,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionSuperseded => "execution_superseded",
            Self::ExecutionMissing => "execution_missing",
            Self::PodUidChanged => "pod_uid_changed",
            Self::ActivationAttemptChanged => "activation_attempt_changed",
            Self::PodResourceVersionChanged => "pod_resource_version_changed",
            Self::DeletePreconditionRejected => "delete_precondition_rejected",
            Self::PodIdentityUnverified => "pod_identity_unverified",
        }
    }
}

impl std::fmt::Display for PodFenceMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of [`SubstrateAdapter::retire_execution`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireExecutionOutcome {
    /// The execution was retired: node-local state preserved, pod(s) deleted
    /// or already absent, pod tracking released.
    Retired,
    /// A supplied fence no longer matched. This is *defer*, not failure: the
    /// row changed under the caller, and whatever changed it owns the sandbox
    /// now. `progress` says whether anything had already been done by the time
    /// that surfaced, which is what a caller needs to know before undoing a
    /// transition it made to enable this call.
    PodIdentityChanged {
        mismatch: PodFenceMismatch,
        progress: TeardownProgress,
    },
}

impl RetireExecutionOutcome {
    /// Whether a caller may undo a state transition it made to enable this
    /// retire.
    ///
    /// One place, because three sweeper passes ask it and a subset that drifts
    /// apart is how this fence's bugs have always looked. Two facts decide it,
    /// and both have to hold: nothing destructive happened yet, *or* whatever
    /// owns the sandbox now is something this teardown never reached.
    ///
    /// A retire that succeeded is not an undo case at all — the caller's
    /// transition is exactly what it wanted.
    #[must_use]
    pub fn permits_undo(&self) -> bool {
        match self {
            Self::Retired => false,
            Self::PodIdentityChanged { mismatch, progress } => {
                !progress.mutated() || mismatch.successor_took_over()
            }
        }
    }
}

/// How far a refused teardown got before the mismatch surfaced.
///
/// Not every refusal is free. The fence is re-proven at several points and
/// only the earliest run before anything is mutated — the pod delete's
/// precondition, in particular, is evaluated by the apiserver a round trip
/// after the in-VM stop. A caller that compensates its own state transition on
/// a refusal has to distinguish the two, or it will restore a lifecycle
/// claiming a running VM for a sandbox that is already stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownProgress {
    /// No in-VM stop, no pod delete, no tracking release.
    NothingMutated,
    /// The observed pod was deleted under its precondition.
    PodDeleted,
    /// The in-VM stop already ran and its disk backup was enqueued.
    RuntimeStopped,
}

impl TeardownProgress {
    #[must_use]
    pub fn mutated(self) -> bool {
        !matches!(self, Self::NothingMutated)
    }
}
