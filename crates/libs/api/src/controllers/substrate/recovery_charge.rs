//! Activation admission and the recovery charge a cold or restored activation pays.

use super::*;
use pplx_space_common_core::types::ClientKey;

#[cfg(test)]
#[path = "recovery_charge/tests.rs"]
mod tests;

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
    /// Admit or reject one capacity-spending activation of `class` on behalf
    /// of `client`, the bounded key of the client that is activating.
    fn admit<'a>(
        &'a self,
        class: ActivationAdmissionClass,
        client: &'a ClientKey,
    ) -> ActivationAdmissionFuture<'a>;
}

/// Activation policy for a substrate call that may recreate a missing pod:
/// whether it charges cluster activation admission, and which client the
/// activation is attributed to. Explicit at the substrate boundary so a
/// recovery-capable path can't silently bypass the limiter by omitting a handle,
/// and so "limiter unconfigured" is distinguished from "intentionally exempt".
///
/// Recovery-capable paths (data-plane access, control-wake, rollback, resume)
/// pass [`RecoveryCharge::enforce`]. Already-paid paths pass
/// [`RecoveryCharge::exempt`] with the client that paid. Teardown paths, and
/// exempt paths with no client in hand (an evacuation, a control wake charged
/// as `unknown`), pass [`RecoveryCharge::EXEMPT`]. Read paths that must never recreate a pod
/// should call `reachable_runtime_client` instead of passing a charge policy
/// at all.
///
/// The client is `None` when the path has no activating client in hand (a
/// reconciler, an evacuation, an ingress wake, a teardown). A new execution
/// then inherits its predecessor's client, and admission charges `unknown`.
// Owned (`Arc`) rather than borrowed: the wake path hands the charge to a
// detached coalesced run that outlives the borrowing request, so the policy
// must not carry a caller lifetime.
#[derive(Clone)]
pub struct RecoveryCharge {
    mode: ChargeMode,
    client: Option<ClientKey>,
}

#[derive(Clone)]
enum ChargeMode {
    /// Charge activation admission when this path wins ownership of capacity-
    /// spending work. Carries `None` only when the process has no limiter
    /// configured (then it's a no-op).
    Enforce(Option<Arc<dyn ActivationAdmission>>),
    /// Never charge here: a teardown/cleanup path, or one whose activation was
    /// already charged upstream.
    Exempt,
}

impl RecoveryCharge {
    /// Exempt, with no activating client: a teardown or cleanup path, or an
    /// exempt activation that inherits its predecessor's client.
    pub(crate) const EXEMPT: Self = Self {
        mode: ChargeMode::Exempt,
        client: None,
    };

    pub(crate) fn enforce<T>(admission: Option<&Arc<T>>, client: Option<ClientKey>) -> Self
    where
        T: ActivationAdmission + 'static,
    {
        Self {
            mode: ChargeMode::Enforce(
                admission.map(|admission| Arc::clone(admission) as Arc<dyn ActivationAdmission>),
            ),
            client,
        }
    }

    /// Exempt from admission because `client`'s activation was already
    /// charged upstream.
    pub(crate) fn exempt(client: Option<ClientKey>) -> Self {
        Self {
            mode: ChargeMode::Exempt,
            client,
        }
    }

    /// This activation once it has been charged: exempt from further admission,
    /// still attributed to the same client.
    pub(crate) fn paid(&self) -> Self {
        Self::exempt(self.client.clone())
    }

    /// The client this activation is attributed to, or `None` to inherit the
    /// previous execution's client.
    pub(crate) fn client(&self) -> Option<&ClientKey> {
        self.client.as_ref()
    }

    pub(super) async fn charge(
        &self,
        class: ActivationAdmissionClass,
    ) -> Result<(), SubstrateError> {
        let ChargeMode::Enforce(Some(admission)) = &self.mode else {
            return Ok(());
        };
        let client = self.client.as_ref().unwrap_or(ClientKey::unknown_ref());
        admission
            .admit(class, client)
            .await
            .map_err(|error| SubstrateError::AdmissionRejected {
                retry_after: error.retry_after,
                rate_limited: error.rate_limited,
                message: error.to_string(),
            })
    }

    /// Charge a Restore activation for a freshly-won restore claim. No-op
    /// for an exempt policy or an unconfigured limiter. Exactly
    /// the claim-winner pays: joining followers never call this, and a
    /// lease takeover pays even when its request entered while another
    /// restore was in flight.
    pub(crate) async fn charge_restore_claim(&self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Restore).await
    }

    /// Charge a Cold activation if this policy enforces one. No-op for an
    /// exempt policy or an unconfigured limiter. Only the
    /// Kubernetes substrate spends deferred cold admission, so this is unused
    /// without that feature.
    #[cfg(feature = "kubernetes")]
    pub(crate) async fn charge_cold_activation(&self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Cold).await
    }
}
