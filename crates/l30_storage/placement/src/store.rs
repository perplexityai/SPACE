//! The placement persistence trait and its error vocabulary.

use std::future::Future;

use pplx_space_common_core::types::{ExecutionGeneration, NodeId, SandboxId};

use crate::model::{ArtifactRef, PlacementFence, PlacementProof};
use crate::outcome::{
    AbandonOutcome, BindOutcome, ClaimKind, ClaimOutcome, Created, DeleteOutcome,
    EvacuationOutcome, LossClassification, NodeLossOutcome, OverrideOutcome, PlacementOutcome,
    QuarantineOutcome, SuspendOutcome,
};
use crate::request::{
    AdminOverride, CreateSandbox, EvacuationStep, NodeIncarnation, PodEvidence, QuarantineHold,
};

/// Placement persistence: one method per operation, no get/put/query surface.
/// Each operation is one transaction that re-checks its precondition inside
/// the UPDATE's own WHERE — a raced row matches zero rows and nothing is
/// written.
///
/// The full operation inventory is declared
/// up front. An operation ahead of its port returns
/// [`PlacementError::UnimplementedOperation`]; its signature stays refinable
/// until the port PR binds it. The method set is capped by the spec:
/// variations of an operation are parameters on its verb ([`PlacementProof`],
/// [`ClaimKind`], [`EvacuationStep`]), never new methods, and growing this
/// trait requires a spec amendment first. Methods are `impl Future + Send`
/// rather than `async fn` so implementors' futures stay spawnable.
pub trait PlacementStore: Send + Sync {
    /// ∅ → `Unplaced`: insert the sandbox row (guards: id free, idempotency
    /// key). Unimplemented until its port lands.
    fn create(
        &self,
        req: CreateSandbox,
    ) -> impl Future<Output = Result<Created, PlacementError>> + Send;

    /// Claim the exclusive right to materialize an execution for `sandbox`.
    /// A win mints the execution generation and restore lease atomically; the
    /// returned attempt's fence is the caller's proof of ownership.
    fn claim_placement(
        &self,
        sandbox: SandboxId,
        kind: ClaimKind,
    ) -> impl Future<Output = Result<ClaimOutcome, PlacementError>> + Send;

    /// Flip a claimed row to Placed via one guarded CAS. The `proof` both
    /// names the sandbox — the fence carries its own, the lapsed-claim
    /// adoption names the row it adopts — and selects which ownership
    /// precondition the WHERE re-checks (see [`PlacementProof`]); a teardown
    /// or a newer claim makes the CAS match zero rows and the outcome
    /// reports the loss.
    fn complete_placement(
        &self,
        proof: PlacementProof<'_>,
    ) -> impl Future<Output = Result<PlacementOutcome, PlacementError>> + Send;

    /// `Placed` → `Placed` (suspending): open the suspend window (guards:
    /// generation matches, no suspend in flight). Unimplemented until its
    /// port lands.
    fn begin_suspend(
        &self,
        sandbox: SandboxId,
        generation: ExecutionGeneration,
    ) -> impl Future<Output = Result<SuspendOutcome, PlacementError>> + Send;

    /// `Placed` → `Unplaced { artifact }`: project the row back to its
    /// durable artifact (guards: generation matches, artifact durable).
    /// Unimplemented until its port lands.
    fn complete_suspend(
        &self,
        sandbox: SandboxId,
        generation: ExecutionGeneration,
        artifact: ArtifactRef,
    ) -> impl Future<Output = Result<SuspendOutcome, PlacementError>> + Send;

    /// `Placing` → `Unplaced`: give up a claimed attempt (guards: lease
    /// expired, or the owner presents its own fence). Every abandonment
    /// classifies its loss. Unimplemented until its port lands.
    fn abandon(
        &self,
        fence: &PlacementFence,
        loss: LossClassification,
    ) -> impl Future<Output = Result<AbandonOutcome, PlacementError>> + Send;

    /// `Placed` → `Evacuating` → `Unplaced { artifact }`: advance one drain
    /// leg (guards: node drain phase, portability proof per leg).
    /// Unimplemented until its port lands.
    fn evacuate(
        &self,
        sandbox: SandboxId,
        step: EvacuationStep,
    ) -> impl Future<Output = Result<EvacuationOutcome, PlacementError>> + Send;

    /// Every resident of a dead node → `Unplaced` or recorded loss. The
    /// observed [`NodeIncarnation`] is re-verified under lock so a same-name
    /// replacement matches nothing; an unidentified incarnation fails closed.
    /// Unimplemented until its port lands.
    ///
    /// The outer `Result` is the call failing; each inner `Result` isolates
    /// one resident, so one undecodable row does not fail the batch.
    fn node_loss(
        &self,
        node: NodeId,
        observed_incarnation: NodeIncarnation,
    ) -> impl Future<Output = Result<Vec<Result<NodeLossOutcome, PlacementError>>, PlacementError>> + Send;

    /// Any state → `Unplaced` + hold, written in the same transaction.
    /// Unimplemented until its port lands.
    fn quarantine(
        &self,
        sandbox: SandboxId,
        hold: QuarantineHold,
    ) -> impl Future<Output = Result<QuarantineOutcome, PlacementError>> + Send;

    /// Any state → tombstone → gone (guards: delete intent; unplace precedes
    /// the row delete). Unimplemented until its port lands.
    fn finalize_delete(
        &self,
        sandbox: SandboxId,
    ) -> impl Future<Output = Result<DeleteOutcome, PlacementError>> + Send;

    /// Any → any: an audited operator bypass — guards are skipped explicitly,
    /// never silently. Unimplemented until its port lands.
    fn admin_override(
        &self,
        op: AdminOverride,
    ) -> impl Future<Output = Result<OverrideOutcome, PlacementError>> + Send;
}

/// The evidence half of the placement surface: what the substrate observed,
/// recorded against a fenced attempt. Evidence never authorizes a transition —
/// a component holding only `PlacementEvidence` has no operation that can
/// move placement state. The production store implements both traits; the
/// split limits what a caller can reach.
pub trait PlacementEvidence: Send + Sync {
    /// Record the pod the substrate bound to the fenced attempt — evidence,
    /// not a state change. Unimplemented until its port lands.
    fn bind_pod(
        &self,
        fence: &PlacementFence,
        pod: PodEvidence,
    ) -> impl Future<Output = Result<BindOutcome, PlacementError>> + Send;
}

/// Failures where an operation could not run or could not trust what it
/// read. Race losses are not errors — they return typed outcomes.
#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("sandbox {0} does not exist")]
    SandboxNotFound(SandboxId),
    /// The create and pooled claims join the fence protocol when their
    /// legacy writers are absorbed.
    #[error("claim kind {0} does not mint a placement fence yet")]
    UnfencedClaimKind(ClaimKind),
    /// Declared on the trait ahead of its port; no implementation yet.
    #[error("placement operation {operation} is not implemented yet; it lands with its port PR")]
    UnimplementedOperation { operation: &'static str },
    /// A column holds a value outside its vocabulary. Fails this row's
    /// operation only; nothing was written.
    #[error("sandbox {sandbox}: column {column} holds undecodable value {value:?}: {reason}")]
    UndecodableRow {
        sandbox: SandboxId,
        column: &'static str,
        value: String,
        reason: String,
    },
    #[error("placement statement failed: {0}")]
    Database(#[from] sqlx::Error),
}
