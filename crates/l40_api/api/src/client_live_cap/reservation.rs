//! A held pending-set member: the guard an admitted activation keeps until
//! its pod exists or the activation ends.

use std::time::Duration;

use pplx_space_common_core::types::{ClientId, SandboxId};
use tokio::task::JoinHandle;

use super::pending_set::{Member, PendingSet, PendingTiming};

/// How often an attempt renews its member while it runs: a third of the
/// TTL (10 s of the 30 s), so one missed renewal doesn't let it lapse.
fn renew_interval(timing: PendingTiming) -> Duration {
    timing.ttl / 3
}

/// A reservation the request that took it shares with the substrate work it
/// starts. That work can outlive the request (a detached wake leader keeps
/// running after its caller disconnects), so it takes the reservation and
/// holds it until it finishes. Untaken, the reservation goes with the last
/// handle: kept on success with [`Self::keep`], or dropped.
#[derive(Clone, Default)]
pub struct LiveHandoff(std::sync::Arc<std::sync::Mutex<Option<LiveReservation>>>);

impl LiveHandoff {
    pub fn new(live: Option<LiveReservation>) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(live)))
    }

    /// Whether a reservation is still here.
    pub fn is_held(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// Take the reservation, if no one has yet.
    pub fn take(&self) -> Option<LiveReservation> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// The activation succeeded: keep the reservation if it's still here.
    pub fn keep(&self) {
        if let Some(live) = self.take() {
            live.keep();
        }
    }
}

/// One admitted activation's member in the pending set.
///
/// While held, a background task renews the member, so a slow activation
/// stays counted however long it takes to create its pod. Finish it one of
/// two ways:
///
/// - [`Self::keep`] when the activation succeeded: renewal stops and the
///   member lapses within one TTL of its last renewal (20 to 30 s), which
///   covers the watch delay before the reflectors show the pod.
/// - Dropping it, on a refusal or a failure before any pod can exist,
///   removes this attempt's owner token, so the attempt stops counting at
///   once and never uncounts a sibling attempt still creating the pod.
///   Once its substrate call is next, the holder calls
///   [`Self::pod_may_exist`], and dropping it lets the member lapse instead.
#[must_use = "dropping a reservation releases it (or, after pod_may_exist(), lets it lapse); call keep() once the activation succeeds"]
pub struct LiveReservation {
    store: PendingSet,
    client: ClientId,
    sandbox: SandboxId,
    owner: String,
    timing: PendingTiming,
    renewer: Option<JoinHandle<()>>,
    /// Set by [`Self::keep`] or [`Self::pod_may_exist`], so `Drop` leaves the
    /// member to lapse; cleared by [`Self::no_pod_yet`].
    kept: bool,
}

impl LiveReservation {
    pub(super) fn hold(
        store: PendingSet,
        client: ClientId,
        sandbox: SandboxId,
        owner: String,
        timing: PendingTiming,
    ) -> Self {
        let mut reservation = Self {
            store,
            client,
            sandbox,
            owner,
            timing,
            renewer: None,
            kept: false,
        };
        reservation.start_renewing();
        reservation
    }

    pub fn sandbox(&self) -> SandboxId {
        self.sandbox
    }

    fn start_renewing(&mut self) {
        let store = self.store.clone();
        let client = self.client.clone();
        let sandbox = self.sandbox;
        let owner = self.owner.clone();
        let timing = self.timing;
        let previous = self.renewer.replace(tokio::spawn(async move {
            let mut ticks = tokio::time::interval(renew_interval(timing));
            ticks.tick().await;
            loop {
                ticks.tick().await;
                let member = Member {
                    client: &client,
                    sandbox,
                    owner: &owner,
                };
                match store.renew(&member, timing).await {
                    Ok(true) => {}
                    // Released, forgotten or lapsed: nothing left to keep alive.
                    Ok(false) => return,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            %sandbox,
                            "live-cap pending member renewal failed; retrying next tick"
                        );
                    }
                }
            }
        }));
        if let Some(previous) = previous {
            previous.abort();
        }
    }

    /// Move this member to `to`: a create that adopted a warm-pool sandbox
    /// counts under the adopted sandbox's id from here on. The create already
    /// reserved, so the move isn't checked against the cap.
    pub async fn swap(&mut self, to: SandboxId) {
        if to == self.sandbox {
            return;
        }
        let member = Member {
            client: &self.client,
            sandbox: self.sandbox,
            owner: &self.owner,
        };
        match self.store.swap(&member, to, self.timing).await {
            Ok(true) => {
                self.sandbox = to;
                self.start_renewing();
            }
            // Released, forgotten or lapsed meanwhile: there's nothing to
            // move.
            Ok(false) => {}
            Err(error) => {
                // The planned id normally stays counted and renewed while the
                // adopted pod becomes visible to the reflectors. If the swap
                // ran despite the error, the adopted id counts unrenewed
                // until its TTL and the renewer stops at its next tick.
                tracing::warn!(
                    %error,
                    from = %self.sandbox,
                    %to,
                    "live-cap pending member swap failed"
                );
            }
        }
    }

    /// From here on a failure may leave a pod behind, so dropping the
    /// reservation lets the member lapse after one TTL rather than releasing
    /// it. It's still renewed while held.
    pub fn pod_may_exist(&mut self) {
        self.kept = true;
    }

    /// Undo [`Self::pod_may_exist`]: the reservation no longer covers that
    /// pod (a rolled-back warm-pool adoption, which the reflector counts if
    /// it's still there), and stands for a create that has no pod yet.
    pub fn no_pod_yet(&mut self) {
        self.kept = false;
    }

    /// A pod exists, or may exist: stop renewing and let the member lapse
    /// within one TTL of its last renewal, covering the watch delay until the
    /// reflectors show the pod.
    pub fn keep(mut self) {
        if let Some(renewer) = self.renewer.take() {
            renewer.abort();
        }
        self.kept = true;
    }
}

impl Drop for LiveReservation {
    fn drop(&mut self) {
        if let Some(renewer) = self.renewer.take() {
            renewer.abort();
        }
        if self.kept {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // No runtime to release on: the member lapses after its TTL.
            return;
        };
        let store = self.store.clone();
        let client = self.client.clone();
        let sandbox = self.sandbox;
        let owner = std::mem::take(&mut self.owner);
        runtime.spawn(async move {
            let member = Member {
                client: &client,
                sandbox,
                owner: &owner,
            };
            if let Err(error) = store.release(&member).await {
                tracing::warn!(
                    %error,
                    %sandbox,
                    "live-cap pending member release failed; it lapses after its TTL"
                );
            }
        });
    }
}

impl std::fmt::Debug for LiveReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveReservation")
            .field("client", &self.client)
            .field("sandbox", &self.sandbox)
            .finish_non_exhaustive()
    }
}
