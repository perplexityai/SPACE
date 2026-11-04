//! First-readiness timing only; admission continues to use the live policy and
//! TLS-interception dependency gate.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_metrics::gateway::readiness::{self as metrics, Outcome};
use pplx_space_net_common::TlsInterception;

use super::NetworkStateInner;
use crate::proto::NetworkState;

pub(super) struct StartupTiming {
    startup: Mutex<Startup>,
}

#[derive(Default)]
struct Startup {
    complete: bool,
    wait: Option<Wait>,
}

struct Wait {
    started: Instant,
    interception_dependency_wait: Duration,
    interception_dependency_blocked_since: Option<Instant>,
}

impl Wait {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            interception_dependency_wait: Duration::ZERO,
            interception_dependency_blocked_since: None,
        }
    }

    fn emit(mut self, mode: TlsInterception, outcome: Outcome) {
        if let Some(since) = self.interception_dependency_blocked_since {
            self.interception_dependency_wait += since.elapsed();
        }
        metrics::startup(
            mode.as_str(),
            outcome,
            self.started.elapsed(),
            self.interception_dependency_wait,
        );
    }
}

impl StartupTiming {
    pub(super) fn new(inner: Arc<NetworkStateInner>) -> Self {
        let timing = Self {
            startup: Mutex::new(Startup::default()),
        };
        timing.observe(|| (inner, false));
        timing
    }

    pub(super) fn observe(&self, snapshot: impl FnOnce() -> (Arc<NetworkStateInner>, bool)) {
        let completed = {
            let mut startup = self.startup.lock().unwrap_or_else(|e| e.into_inner());
            if startup.complete {
                return;
            }
            // Sample under the timing lock so callbacks cannot publish old
            // policy/dependency observations after a newer startup transition.
            let (inner, interception_dependencies_ready) = snapshot();
            let mode = inner
                .policy
                .as_ref()
                .map_or(TlsInterception::Off, |policy| policy.tls_interception);
            let outcome = match inner.mode {
                NetworkState::NoIdentity => Outcome::Invalidated,
                NetworkState::Draining | NetworkState::Stopped => Outcome::Cancelled,
                _ => {
                    let wait = startup.wait.get_or_insert_with(Wait::new);
                    let policy_ready = inner.mode == NetworkState::Ready && inner.policy.is_some();
                    let dependencies_ready =
                        mode == TlsInterception::Off || interception_dependencies_ready;
                    if policy_ready && !dependencies_ready {
                        wait.interception_dependency_blocked_since
                            .get_or_insert_with(Instant::now);
                    } else if let Some(since) = wait.interception_dependency_blocked_since.take() {
                        wait.interception_dependency_wait += since.elapsed();
                    }
                    if !policy_ready || !dependencies_ready {
                        return;
                    }
                    Outcome::Ready
                }
            };
            startup.complete = outcome != Outcome::Invalidated;
            startup.wait.take().map(|wait| (wait, mode, outcome))
        };
        if let Some((wait, mode, outcome)) = completed {
            wait.emit(mode, outcome);
        }
    }
}

impl Drop for StartupTiming {
    fn drop(&mut self) {
        let startup = self.startup.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(wait) = startup.wait.take() {
            wait.emit(TlsInterception::Off, Outcome::Cancelled);
        }
    }
}
