//! The placement executor: one crate drives a sandbox placement to
//! completion.
//!
//! The machine
//! (`pplx-space-placement`) decides and records — every guard is one Postgres
//! transaction returning a typed outcome. This crate does the work: claim →
//! drive substrate work → bind evidence → complete or abandon. The api calls
//! one verb, [`PlacementExecutor::place`], and receives a typed
//! [`PlaceOutcome`]; it never touches a fence.
//!
//! This is the executable half of the spec: the public surface compiles
//! exactly as the spec's contract section. [`PlacementExecutor::place`] is a
//! typed refusal until the wake rung lands — the same declared-ahead pattern
//! as the machine's
//! [`pplx_space_placement::PlacementError::UnimplementedOperation`]. The
//! scenario tests land with the implementation so every green test exercises
//! the behavior its assertions describe.

mod driver;
mod executor;
mod outcome;
mod request;

pub use executor::{PlaceError, PlacementExecutor};
pub use outcome::{PlaceOutcome, PlacedBinding};
// The race and loss vocabularies are the machine's own types, re-exported —
// the executor adds no parallel vocabulary.
pub use pplx_space_placement::{BlockedReason, LossClassification, PodRef};
pub use request::{PlaceIntent, PlaceRequest};

#[doc(hidden)]
pub use driver::{CreatePodRequest, PodCreated, PodDriver, PodDriverError};
