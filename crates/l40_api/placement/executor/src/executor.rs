//! The executor struct and its error vocabulary.

use std::sync::Arc;

use pplx_space_placement::{PlacementError, PostgresPlacementStore};

use crate::driver::PodDriver;
use crate::outcome::PlaceOutcome;
use crate::request::PlaceRequest;

/// The one placement verb the api calls. Concrete struct, not a trait —
/// kubernetes mode is internal dispatch (the executor owns the pod driver);
/// tests script the driver through the internal seam. The Direct substrate
/// stays legacy-exempt via the substrate's option-accessor pattern and never
/// constructs one of these.
pub struct PlacementExecutor {
    store: PostgresPlacementStore,
    driver: Arc<dyn PodDriver>,
}

impl PlacementExecutor {
    /// Construct the executor over the machine store and a pod driver.
    ///
    /// Doc-hidden rather than private so the in-crate scenario suite can
    /// stand a scripted driver behind the seam while `place()` is a
    /// declared-ahead refusal — the same bridge shape as the machine's
    /// doc-hidden `PlacementFence::from_held_lease`. The wake rung lands the
    /// real kubernetes driver and the advertised construction path.
    #[doc(hidden)]
    pub fn with_driver(store: PostgresPlacementStore, driver: Arc<dyn PodDriver>) -> Self {
        Self { store, driver }
    }

    /// Ensure the request's sandbox is placed. Returns within
    /// `request.budget`, always with a typed answer. Exactly one concurrent
    /// caller drives the work; every other caller is told, precisely, what
    /// to wait for.
    ///
    /// Declared ahead of the wake rung: today this is a typed refusal,
    /// mirroring the machine's declared-ahead operations
    /// ([`PlacementError::UnimplementedOperation`]). The wake rung implements
    /// the claim → materialize → complete choreography over the fields below
    /// and adds scenario coverage for the implemented behavior.
    pub async fn place(&self, request: PlaceRequest) -> Result<PlaceOutcome, PlaceError> {
        let _claim_inputs_until_the_wake_rung = (&self.store, &self.driver, &request);
        Err(PlaceError::Unimplemented { operation: "place" })
    }
}

/// Failures where a `place()` call could not run. Race losses are not
/// errors — they return typed [`PlaceOutcome`]s.
#[derive(Debug, thiserror::Error)]
pub enum PlaceError {
    /// Declared on the surface ahead of its rung; no implementation yet.
    #[error(
        "placement executor operation {operation} is not implemented yet; it lands with the wake rung"
    )]
    Unimplemented { operation: &'static str },
    /// The machine could not run an operation the executor asked for.
    #[error(transparent)]
    Machine(#[from] PlacementError),
}
