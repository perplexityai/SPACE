//! Activation admission and the recovery charge a cold or restored activation pays.

use super::*;

/// Capacity classes understood by the substrate admission boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationAdmissionClass {
    /// A fresh execution without snapshot restoration.
    Cold,
    /// An execution restored from a durable sandbox snapshot.
    Restore,
}

/// Adapter-neutral rejection returned by activation admission.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ActivationAdmissionRejection {
    pub(super) message: String,
    pub(super) retry_after: Option<Duration>,
    pub(super) rate_limited: bool,
}

impl ActivationAdmissionRejection {
    pub(crate) fn new(message: String, retry_after: Option<Duration>, rate_limited: bool) -> Self {
        Self {
            message,
            retry_after,
            rate_limited,
        }
    }
}

/// Boxed admission decision future used by [`ActivationAdmission`].
pub type ActivationAdmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ActivationAdmissionRejection>> + Send + 'a>>;

/// Capacity admission capability supplied to substrate adapters.
pub trait ActivationAdmission: Send + Sync {
    /// Admit or reject one capacity-spending activation.
    fn admit(&self, class: ActivationAdmissionClass) -> ActivationAdmissionFuture<'_>;
}

/// Cluster-activation charge policy for a substrate data-plane call that may
/// recreate a missing pod. Explicit at the substrate boundary so a
/// recovery-capable path can't silently bypass the limiter by omitting a handle,
/// and so "limiter unconfigured" is distinguished from "intentionally exempt".
///
/// Recovery-capable paths (data-plane access, control-wake, rollback, resume)
/// pass [`RecoveryCharge::Enforce`]. Teardown and already-paid paths pass
/// [`RecoveryCharge::Exempt`]. Read paths that must
/// never recreate a pod should call `reachable_runtime_client` instead of
/// passing a charge policy at all.
// Owned (`Arc`) rather than borrowed: the wake path hands the charge to a
// detached coalesced run that outlives the borrowing request, so the policy
// must not carry a caller lifetime.
#[derive(Clone)]
pub enum RecoveryCharge {
    /// Charge activation admission when this path wins ownership of capacity-
    /// spending work. Carries `None` only when the process has no limiter
    /// configured (then it's a no-op).
    Enforce(Option<Arc<dyn ActivationAdmission>>),
    /// Never charge here: a teardown/cleanup path, or one whose activation was
    /// already charged upstream.
    Exempt,
}

impl RecoveryCharge {
    pub(crate) fn enforce<T>(admission: Option<&Arc<T>>) -> Self
    where
        T: ActivationAdmission + 'static,
    {
        Self::Enforce(
            admission.map(|admission| Arc::clone(admission) as Arc<dyn ActivationAdmission>),
        )
    }

    pub(super) async fn charge(
        self,
        class: ActivationAdmissionClass,
    ) -> Result<(), SubstrateError> {
        let RecoveryCharge::Enforce(Some(admission)) = self else {
            return Ok(());
        };
        admission
            .admit(class)
            .await
            .map_err(|error| SubstrateError::AdmissionRejected {
                retry_after: error.retry_after,
                rate_limited: error.rate_limited,
                message: error.to_string(),
            })
    }

    /// Charge a Restore activation for a freshly-won restore claim. No-op
    /// for [`RecoveryCharge::Exempt`] or an unconfigured limiter. Exactly
    /// the claim-winner pays: joining followers never call this, and a
    /// lease takeover pays even when its request entered while another
    /// restore was in flight.
    pub(crate) async fn charge_restore_claim(self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Restore).await
    }

    /// Charge a Cold activation if this policy enforces one. No-op for
    /// [`RecoveryCharge::Exempt`] or an unconfigured limiter. Only the
    /// Kubernetes substrate spends deferred cold admission, so this is unused
    /// without that feature.
    #[cfg(feature = "kubernetes")]
    pub(crate) async fn charge_cold_activation(self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Cold).await
    }
}
