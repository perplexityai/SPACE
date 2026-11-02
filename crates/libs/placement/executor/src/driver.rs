//! The internal driver seam: the substrate half of the executor's kubernetes
//! mode. The executor owns pod creation and teardown through this trait; the
//! machine never sees it, and it never appears in the executor's public
//! vocabulary.
//!
//! Doc-hidden rather than private so scenario tests can use a scripted driver.
//! The wake rung binds the real Kubernetes driver behind it.

use futures_util::future::BoxFuture;
use pplx_space_common_core::types::{PodName, SandboxId};
use pplx_space_placement::PodRef;

/// Create (or adopt) and tear down the pod an attempt rides in.
#[doc(hidden)]
pub trait PodDriver: Send + Sync {
    /// Materialize the pod for a claimed attempt. An existing pod for the
    /// same attempt is adopted, not duplicated.
    ///
    /// The placement store inserts the attempt's execution row as tracking
    /// pod-create intent, so a real driver must stamp the row's
    /// `pod_create_requested_at` before it creates the pod, as the api
    /// substrate's creates do. Node drain's pod-less give-up reads a tracked
    /// row with no stamp as proof that no pod was ever created for it.
    fn create_pod(
        &self,
        request: CreatePodRequest,
    ) -> BoxFuture<'_, Result<PodCreated, PodDriverError>>;

    /// Tear down a pod a failed or superseded attempt made.
    fn delete_pod(&self, pod: PodName) -> BoxFuture<'_, Result<(), PodDriverError>>;
}

/// What the driver needs to materialize a pod. Refinable until the wake rung
/// binds the real driver.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct CreatePodRequest {
    pub sandbox: SandboxId,
}

/// The pod the driver materialized or adopted.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct PodCreated {
    pub pod: PodRef,
}

#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
pub enum PodDriverError {
    #[error("pod {pod:?} already exists for this sandbox")]
    AlreadyExists { pod: PodRef },
    #[error("pod create failed: {message}")]
    CreateFailed { message: String },
    #[error("pod delete failed for {pod:?}: {message}")]
    DeleteFailed { pod: PodName, message: String },
}
