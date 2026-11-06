//! Activation admission and the recovery charge a cold or restored activation pays.

use super::*;
use pplx_space_common_core::types::ClientId;

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

/// The kind of client an activation is for (RFC "Per-client admission
/// independent of the substrate", Decision 1).
///
/// `Client` and `Native` are activations a caller explicitly asked for: ECW
/// create, connect, resume and fork are `Client`, and native `/v1` and admin
/// creates and lifecycle calls are `Native`. Anything else that happens to
/// create a runtime acts for the sandbox's recorded client and is `Inherit`:
/// implicit access, a rebuild as a side effect of another operation,
/// reconcilers and evacuation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivationClient {
    /// An ECW activation, for this client id.
    Client(ClientId),
    /// Acting for the sandbox's recorded client.
    Inherit,
    /// A native `/v1` or admin call.
    Native,
}

/// The one client id an activation's pod label and record use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyResolution {
    /// Resolved at entry. `None` means an inheriting activation found nothing
    /// recorded, so its pod is created unlabeled.
    Resolved(Option<ClientId>),
    /// A substrate-started rebuild, with no entry that loaded the sandbox:
    /// the substrate reads the record itself.
    Unresolved,
}

impl KeyResolution {
    /// The client id to label a new pod with, given the sandbox's record.
    pub(crate) fn pod_label(&self, record: Option<&ClientId>) -> Option<ClientId> {
        match self {
            Self::Resolved(id) => id.clone(),
            Self::Unresolved => record.cloned(),
        }
    }
}

/// An [`ActivationClient`] together with the client id it resolved to. The
/// constructors keep the two consistent: a `Client` or `Native` activation
/// resolves to its own id, and only `Inherit` reads the record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationAttribution {
    client: ActivationClient,
    key: KeyResolution,
}

impl ActivationAttribution {
    /// An ECW activation for `id`.
    pub fn client(id: ClientId) -> Self {
        Self {
            key: KeyResolution::Resolved(Some(id.clone())),
            client: ActivationClient::Client(id),
        }
    }

    /// A native `/v1` or admin activation.
    pub fn native() -> Self {
        Self {
            client: ActivationClient::Native,
            key: KeyResolution::Resolved(Some(ClientId::native())),
        }
    }

    /// An activation acting for the sandbox whose record the caller loaded.
    pub fn inherit(record: Option<&ClientId>) -> Self {
        Self {
            client: ActivationClient::Inherit,
            key: KeyResolution::Resolved(record.cloned()),
        }
    }

    /// An activation acting for a sandbox no entry loaded; the substrate
    /// reads the record when it creates the pod.
    pub fn inherit_unresolved() -> Self {
        Self {
            client: ActivationClient::Inherit,
            key: KeyResolution::Unresolved,
        }
    }

    /// Resolve `client` against the record of the sandbox it activates.
    pub fn resolve(client: ActivationClient, record: Option<&ClientId>) -> Self {
        match client {
            ActivationClient::Client(id) => Self::client(id),
            ActivationClient::Native => Self::native(),
            ActivationClient::Inherit => Self::inherit(record),
        }
    }

    pub fn activation_client(&self) -> &ActivationClient {
        &self.client
    }

    pub fn key(&self) -> &KeyResolution {
        &self.key
    }
}

/// Who charges an activation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChargeState {
    /// Charge here: this path owns the activation's admission.
    Enforce,
    /// Charged upstream, and admitted.
    Paid,
    /// Nobody charges it: a teardown, a reconciler or an evacuation.
    Exempt,
}

/// Where a per-client live cap would decide this activation. Nothing reads it
/// yet; each path states it so the cap can be added without revisiting them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapPolicy {
    /// Reserved upstream, before any token was spent.
    Reserved,
    /// Checked where a pod is about to be created, if one is.
    Check,
    /// Decided by the Kubernetes restore-lease winner.
    ReserveAtClaim,
    /// Not capped.
    Skip,
}

/// Activation policy for a substrate call that may recreate a missing pod:
/// whether it charges cluster activation admission, where a live cap would
/// decide it, and which client the activation is attributed to. Explicit at
/// the substrate boundary so a recovery-capable path can't silently bypass
/// the limiter by omitting a handle, and so "limiter unconfigured" is
/// distinguished from "intentionally exempt".
///
/// Every constructor takes an [`ActivationAttribution`], so each call site
/// says whose activation it is. Read paths that must never recreate a pod
/// call `reachable_runtime_client` instead of passing a charge policy at all.
// Owned (`Arc`) rather than borrowed: the wake path hands the charge to a
// detached coalesced run that outlives the borrowing request, so the policy
// must not carry a caller lifetime.
#[derive(Clone)]
pub struct RecoveryCharge {
    state: ChargeState,
    cap: CapPolicy,
    attribution: ActivationAttribution,
    /// `None` when the process has no limiter configured; charging is then a
    /// no-op.
    admission: Option<Arc<dyn ActivationAdmission>>,
}

impl RecoveryCharge {
    /// This path charges the activation's admission.
    pub(crate) fn enforce<T>(
        admission: Option<&Arc<T>>,
        cap: CapPolicy,
        attribution: ActivationAttribution,
    ) -> Self
    where
        T: ActivationAdmission + 'static,
    {
        Self {
            state: ChargeState::Enforce,
            cap,
            attribution,
            admission: admission
                .map(|admission| Arc::clone(admission) as Arc<dyn ActivationAdmission>),
        }
    }

    /// The activation was charged upstream and admitted.
    pub(crate) fn paid(cap: CapPolicy, attribution: ActivationAttribution) -> Self {
        Self {
            state: ChargeState::Paid,
            cap,
            attribution,
            admission: None,
        }
    }

    /// Nobody charges the activation.
    pub(crate) fn exempt(cap: CapPolicy, attribution: ActivationAttribution) -> Self {
        Self {
            state: ChargeState::Exempt,
            cap,
            attribution,
            admission: None,
        }
    }

    /// This activation once this path has charged it: paid, for the same
    /// client. An exempt policy stays exempt.
    pub(crate) fn after_charge(&self) -> Self {
        let state = match self.state {
            ChargeState::Exempt => ChargeState::Exempt,
            ChargeState::Enforce | ChargeState::Paid => ChargeState::Paid,
        };
        Self {
            state,
            cap: self.cap,
            attribution: self.attribution.clone(),
            admission: None,
        }
    }

    pub fn state(&self) -> ChargeState {
        self.state
    }

    pub fn cap(&self) -> CapPolicy {
        self.cap
    }

    pub fn attribution(&self) -> &ActivationAttribution {
        &self.attribution
    }

    pub(super) async fn charge(
        &self,
        class: ActivationAdmissionClass,
    ) -> Result<(), SubstrateError> {
        let (ChargeState::Enforce, Some(admission)) = (self.state, self.admission.as_ref()) else {
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
    /// unless this policy enforces one with a configured limiter. Exactly
    /// the claim-winner pays: joining followers never call this, and a
    /// lease takeover pays even when its request entered while another
    /// restore was in flight.
    pub(crate) async fn charge_restore_claim(&self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Restore).await
    }

    /// Charge a Cold activation if this policy enforces one. Only the
    /// Kubernetes substrate spends deferred cold admission, so this is unused
    /// without that feature.
    #[cfg(feature = "kubernetes")]
    pub(crate) async fn charge_cold_activation(&self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Cold).await
    }
}

#[cfg(test)]
impl RecoveryCharge {
    /// An exempt, uncapped charge acting for the sandbox's record, for tests
    /// that don't exercise attribution or caps.
    pub(crate) fn exempt_for_tests() -> Self {
        Self::exempt(CapPolicy::Skip, ActivationAttribution::inherit_unresolved())
    }

    /// An enforcing charge acting for the sandbox's record, for tests that
    /// exercise admission but not attribution or caps.
    pub(crate) fn enforce_for_tests<T>(admission: Option<&Arc<T>>) -> Self
    where
        T: ActivationAdmission + 'static,
    {
        Self::enforce(
            admission,
            CapPolicy::Check,
            ActivationAttribution::inherit_unresolved(),
        )
    }
}

#[cfg(test)]
#[path = "recovery_charge/tests.rs"]
mod tests;
