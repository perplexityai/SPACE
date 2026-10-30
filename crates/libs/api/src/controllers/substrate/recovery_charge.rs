//! Activation admission and the recovery charge a cold or restored activation pays.

use super::*;
use crate::client_live_cap::LiveReservation;
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

/// Boxed live-cap reservation future used by [`ActivationAdmission`]:
/// `Ok(None)` admits without holding anything (no cap applies, or the check
/// failed open).
pub type LiveCapFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<Option<LiveReservation>, ActivationAdmissionRejection>>
            + Send
            + 'a,
    >,
>;

/// Capacity admission capability supplied to substrate adapters.
pub trait ActivationAdmission: Send + Sync {
    /// Admit or reject one capacity-spending activation.
    fn admit(&self, class: ActivationAdmissionClass) -> ActivationAdmissionFuture<'_>;

    /// Reserve a per-client live slot for `client` activating `sandbox`.
    /// Hold the reservation until the activation ends.
    fn reserve_live(&self, client: ClientId, sandbox: SandboxId) -> LiveCapFuture<'_>;

    /// Refuse when `client` is already at its live cap, reserving nothing:
    /// the pre-check before a deferred charge.
    fn precheck_live(&self, client: ClientId, sandbox: SandboxId) -> ActivationAdmissionFuture<'_>;
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

/// Where the per-client live cap decides this activation (RFC Decision 6).
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

impl std::fmt::Debug for RecoveryCharge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryCharge")
            .field("state", &self.state)
            .field("cap", &self.cap)
            .field("attribution", &self.attribution)
            .field("admission", &self.admission.is_some())
            .finish()
    }
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

    /// The activation was charged upstream and admitted. `admission` is
    /// still needed when `cap` is [`CapPolicy::Check`]: the cap runs where a
    /// pod gets created.
    pub(crate) fn paid<T>(
        admission: Option<&Arc<T>>,
        cap: CapPolicy,
        attribution: ActivationAttribution,
    ) -> Self
    where
        T: ActivationAdmission + 'static,
    {
        Self {
            state: ChargeState::Paid,
            cap,
            attribution,
            admission: admission
                .map(|admission| Arc::clone(admission) as Arc<dyn ActivationAdmission>),
        }
    }

    /// Nobody charges the activation. `admission` is still needed when `cap`
    /// is [`CapPolicy::Check`].
    pub(crate) fn exempt<T>(
        admission: Option<&Arc<T>>,
        cap: CapPolicy,
        attribution: ActivationAttribution,
    ) -> Self
    where
        T: ActivationAdmission + 'static,
    {
        Self {
            state: ChargeState::Exempt,
            cap,
            attribution,
            admission: admission
                .map(|admission| Arc::clone(admission) as Arc<dyn ActivationAdmission>),
        }
    }

    /// A charge with no admission handle: nothing is charged or capped.
    #[cfg(any(test, feature = "kubernetes"))]
    pub(crate) fn uncapped(state: ChargeState, attribution: ActivationAttribution) -> Self {
        Self {
            state,
            cap: CapPolicy::Skip,
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
            admission: self.admission.clone(),
        }
    }

    /// This activation with its cap decided as `cap`: a lease winner turns
    /// [`CapPolicy::ReserveAtClaim`] into `Reserved` once it has reserved, or
    /// into `Check` when it may reuse a pod.
    #[cfg(feature = "kubernetes")]
    pub(crate) fn with_cap(&self, cap: CapPolicy) -> Self {
        Self {
            state: self.state,
            cap,
            attribution: self.attribution.clone(),
            admission: self.admission.clone(),
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

    /// Reserve this activation's live slot for `sandbox`, whose record is
    /// `record`. `Ok(None)` when no cap applies: no admission handle, or an
    /// activation with no client id (an inheriting one whose record is
    /// `NULL`).
    #[cfg(any(test, feature = "kubernetes"))]
    pub(crate) async fn reserve_live_cap(
        &self,
        sandbox: SandboxId,
        record: Option<&ClientId>,
    ) -> Result<Option<LiveReservation>, SubstrateError> {
        let Some(admission) = self.admission.as_ref() else {
            return Ok(None);
        };
        let Some(client) = self.attribution.key().pod_label(record) else {
            return Ok(None);
        };
        admission
            .reserve_live(client, sandbox)
            .await
            .map_err(live_cap_rejection)
    }

    /// The read-only pre-check of this activation's live cap.
    #[cfg(feature = "kubernetes")]
    pub(crate) async fn precheck_live_cap(
        &self,
        sandbox: SandboxId,
        record: Option<&ClientId>,
    ) -> Result<(), SubstrateError> {
        let Some(admission) = self.admission.as_ref() else {
            return Ok(());
        };
        let Some(client) = self.attribution.key().pod_label(record) else {
            return Ok(());
        };
        admission
            .precheck_live(client, sandbox)
            .await
            .map_err(live_cap_rejection)
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

/// A live-cap refusal is a refused charge: a `429` without the cluster
/// limiter's backpressure cause, so the async dispatcher retries it against
/// its attempt budget. A cap lifts when the client releases a sandbox, not
/// with time.
#[cfg(any(test, feature = "kubernetes"))]
fn live_cap_rejection(error: ActivationAdmissionRejection) -> SubstrateError {
    SubstrateError::AdmissionRejected {
        message: error.to_string(),
        retry_after: error.retry_after,
        rate_limited: false,
    }
}

#[cfg(test)]
impl RecoveryCharge {
    /// An exempt, uncapped charge acting for the sandbox's record, for tests
    /// that don't exercise attribution or caps.
    pub(crate) fn exempt_for_tests() -> Self {
        Self::uncapped(
            ChargeState::Exempt,
            ActivationAttribution::inherit_unresolved(),
        )
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
