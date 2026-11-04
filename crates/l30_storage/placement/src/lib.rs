//! The placement machine: one crate owns sandbox placement.
//!
//! A four-state machine
//! ([`Placement`]) with named operations, each a single guarded Postgres
//! transaction returning a typed outcome. Ownership of an attempt is one
//! fence everywhere — execution generation + lease token, minted atomically
//! by a successful claim. Pod UIDs, lifecycle strings, and freshness
//! timestamps are evidence, never ownership.
//!
//! Deliberately the *Kubernetes* placement machine: pods and kube node names
//! appear by name; direct-substrate sandboxes keep their legacy path, and a
//! substrate abstraction waits for a real second substrate. The one
//! abstraction kept is [`NodeIncarnation`] — node identity is already
//! scheduled to churn, and the swap stays inside this crate.
//!
//! Runs over today's schema columns, no migration. The full trait is
//! declared so callers see the whole interface; operations land one per PR.
//! Today: [`PlacementStore::claim_placement`] (restore-shaped kinds) and
//! [`PlacementStore::complete_placement`] (both proofs) are implemented with
//! no production caller yet — the api's user wake path cuts over in the
//! stack's next PR; everything else returns
//! [`PlacementError::UnimplementedOperation`] until its port lands.

mod lifecycle;
mod model;
mod outcome;
mod postgres;
mod request;
mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use lifecycle::StoredSandboxLifecycle;
pub use model::{
    ArtifactRef, Attempt, ClaimOrigin, LeaseToken, Placement, PlacementFence, PlacementProof,
    PodRef,
};
pub use outcome::{
    AbandonOutcome, BindOutcome, BlockedReason, ClaimKind, ClaimOutcome, Created, DeleteOutcome,
    EvacuationOutcome, LossClassification, NodeLossOutcome, OverrideOutcome, PlacementOutcome,
    QuarantineOutcome, RecoveryDisposition, SuspendOutcome,
};
pub use postgres::PostgresPlacementStore;
pub use request::{
    AdminOverride, CreateSandbox, EvacuationStep, NodeIncarnation, PodEvidence, QuarantineHold,
};
pub use store::{PlacementError, PlacementEvidence, PlacementStore};
