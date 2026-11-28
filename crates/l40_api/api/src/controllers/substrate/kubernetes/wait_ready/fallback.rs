use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::time::Instant;

use super::super::pod_ops::ReadinessPodResolution;
use super::super::{SandboxId, SubstrateError};
use crate::controllers::substrate::kubernetes::{KubernetesSubstrate, SubstrateBudgets};

/// Jitter fraction on [`SubstrateBudgets::readiness_quiet_reflector`] so
/// degraded-reflector fallback reads spread across waiters instead of firing in
/// lockstep.
const READINESS_FALLBACK_JITTER: f64 = 0.2;
/// Minimum grace (as a fraction of `readiness_quiet_reflector`) after a watch
/// fault before any waiter falls back to uncached `GET`s, letting brief watch
/// reconnects / `410 Gone` re-lists finish with zero per-pod `GET`s.
const READINESS_FAULT_GRACE_MIN: f64 = 0.1;
/// Additional per-waiter jitter (as a fraction of `readiness_quiet_reflector`)
/// added on top of [`READINESS_FAULT_GRACE_MIN`] when the watch faults.
const READINESS_FAULT_JITTER: f64 = 0.3;

/// Paces degraded-reflector apiserver pod reads during a readiness wait.
///
/// While the shared sandbox-pod reflector is healthy — initial sync complete,
/// no watch error on the live stream, and a watch delivery observed within
/// [`SubstrateBudgets::readiness_quiet_reflector`] — readiness waits emit zero
/// fallback `GET`s. Only when the reflector is cold, its watch has faulted, or
/// its stream has been silent for at least `readiness_quiet_reflector` does a
/// wait fall back to direct pod reads, staggered per waiter on both watch
/// faults and watch silence and spaced by at least one jittered quiet interval
/// after each read completes so slow apiserver responses never stack or herd.
pub(in crate::controllers::substrate::kubernetes) struct ReadinessFallback {
    started_at: Instant,
    quiet: Duration,
    fault_delay: Duration,
    silence_delay: Duration,
    initial_silence_floor: Instant,
    cooldown_until: Option<Instant>,
}

impl ReadinessFallback {
    pub(super) fn new(budgets: &SubstrateBudgets) -> Self {
        Self::new_at(Instant::now(), budgets.readiness_quiet_reflector)
    }

    pub(super) fn new_at(now: Instant, quiet: Duration) -> Self {
        Self::with_jitter(now, quiet, rand::random::<f64>())
    }

    pub(in crate::controllers::substrate::kubernetes) fn with_jitter(
        now: Instant,
        quiet: Duration,
        jitter: f64,
    ) -> Self {
        let j = jitter.clamp(0.0, 1.0);
        Self {
            started_at: now,
            quiet,
            fault_delay: quiet.mul_f64(READINESS_FAULT_GRACE_MIN + j * READINESS_FAULT_JITTER),
            silence_delay: quiet.mul_f64(1.0 + j * READINESS_FALLBACK_JITTER),
            initial_silence_floor: now + quiet.mul_f64(1.0 - j * READINESS_FALLBACK_JITTER),
            cooldown_until: None,
        }
    }

    /// When a degraded-mode fallback read may next become eligible.
    pub(super) fn next_read(&self, substrate: &KubernetesSubstrate) -> Instant {
        let candidate = if !substrate.pod_reflector_ready.is_ready() {
            self.started_at + self.fault_delay
        } else if let Some(faulted_at) = substrate.pod_client_index.watch_faulted_at() {
            faulted_at.max(self.started_at) + self.fault_delay
        } else {
            match substrate.pod_events.last_delivery() {
                Some(last) => (last + self.silence_delay).max(self.initial_silence_floor),
                None => self.initial_silence_floor,
            }
        };
        match self.cooldown_until {
            Some(cooldown) => candidate.max(cooldown),
            None => candidate,
        }
    }

    pub(super) fn due(&self, now: Instant, substrate: &KubernetesSubstrate) -> bool {
        !substrate.pod_reflector_watch_healthy_at(now) && now >= self.next_read(substrate)
    }

    /// Record completion of a fallback read so the next read waits out another
    /// quiet interval from when this one finished.
    pub(super) fn record_completed_at(&mut self, now: Instant) {
        self.record_completed_with_jitter(now, rand::random::<f64>());
    }

    pub(super) fn record_completed_with_jitter(&mut self, now: Instant, jitter: f64) {
        let j = jitter.clamp(0.0, 1.0);
        self.cooldown_until = Some(now + self.quiet.mul_f64(1.0 - j * READINESS_FALLBACK_JITTER));
    }
}

/// An in-flight uncached readiness pod read, carrying whether the read is
/// confirming a reflector-observed pod deletion or terminal transition rather
/// than an opportunistic degraded-mode backstop poll.
pub(super) struct InFlightFallback<'a> {
    fut: Pin<Box<dyn Future<Output = Result<ReadinessPodResolution, SubstrateError>> + Send + 'a>>,
    confirms_loss: bool,
}

impl<'a> InFlightFallback<'a> {
    pub(super) fn start(
        fut: impl Future<Output = Result<ReadinessPodResolution, SubstrateError>> + Send + 'a,
        confirms_loss: bool,
    ) -> Self {
        Self {
            fut: Box::pin(fut),
            confirms_loss,
        }
    }

    pub(super) fn confirms_loss(&self) -> bool {
        self.confirms_loss
    }

    pub(super) async fn resolve(
        self,
        fallback: &mut ReadinessFallback,
        substrate: &KubernetesSubstrate,
        sandbox_id: SandboxId,
        pod_name: &str,
    ) -> Result<Option<ReadinessPodResolution>, SubstrateError> {
        let outcome = self.fut.await;
        Self::finish(
            outcome,
            self.confirms_loss,
            fallback,
            substrate,
            sandbox_id,
            pod_name,
        )
    }

    pub(super) fn finish(
        outcome: Result<ReadinessPodResolution, SubstrateError>,
        confirms_loss: bool,
        fallback: &mut ReadinessFallback,
        substrate: &KubernetesSubstrate,
        sandbox_id: SandboxId,
        pod_name: &str,
    ) -> Result<Option<ReadinessPodResolution>, SubstrateError> {
        let now = Instant::now();
        fallback.record_completed_at(now);
        match outcome {
            Ok(outcome) => Ok(Some(outcome)),
            Err(err)
                if !confirms_loss
                    && matches!(err, SubstrateError::Unavailable { .. })
                    && substrate.pod_reflector_watch_healthy_at(now) =>
            {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    pod_name = %pod_name,
                    error = %err,
                    "ignoring degraded-mode readiness fallback GET failure after shared pod reflector recovered"
                );
                #[cfg(test)]
                substrate
                    .readiness_test_hooks
                    .suppressed_healthy_fallback_error
                    .notify_waiters();
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }
}

pub(super) async fn poll_in_flight_fallback(
    slot: &mut Option<InFlightFallback<'_>>,
) -> (Result<ReadinessPodResolution, SubstrateError>, bool) {
    let outcome = match slot.as_mut() {
        Some(in_flight) => in_flight.fut.as_mut().await,
        None => std::future::pending().await,
    };
    let confirms_loss = slot.take().is_some_and(|in_flight| in_flight.confirms_loss);
    (outcome, confirms_loss)
}
