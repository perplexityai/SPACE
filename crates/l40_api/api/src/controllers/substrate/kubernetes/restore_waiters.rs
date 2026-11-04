use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use pplx_space_common_core::types::SandboxId;
use tokio::sync::{Notify, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::store::{K8sStore, UserRestoreObservation};
use crate::controllers::substrate::SubstrateError;

pub(super) const RESTORE_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MIN_RESTORE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_PENDING_RESTORE_SANDBOXES: usize = 1_024;
const RESTORE_WAITER_CAP_RETRY_AFTER: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub(super) enum RestorePollStatus {
    Pending,
    Observed(UserRestoreObservation),
    Failed(String),
    NotFound,
}

struct RestoreWaitersInner {
    store: K8sStore,
    senders: Mutex<HashMap<SandboxId, watch::Sender<RestorePollStatus>>>,
    poller_started: AtomicBool,
    poller_wakeup: Arc<Notify>,
    max_pending_sandboxes: usize,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Per-process fan-out for user-restore followers.
///
/// All request futures for one sandbox hold only a `watch::Receiver`. One
/// background task snapshots every subscribed sandbox and observes them with
/// one batched Postgres query per interval. This deliberately uses polling,
/// not Postgres LISTEN/NOTIFY: reconnecting an API pod cannot lose a restore
/// transition, and the database sees a bounded query rate per API process.
#[derive(Clone)]
pub(super) struct RestoreWaiters {
    inner: Arc<RestoreWaitersInner>,
}

impl RestoreWaiters {
    pub(super) fn new(
        store: K8sStore,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Self {
        Self::with_capacity(
            store,
            shutdown_token,
            task_tracker,
            MAX_PENDING_RESTORE_SANDBOXES,
        )
    }

    fn with_capacity(
        store: K8sStore,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
        max_pending_sandboxes: usize,
    ) -> Self {
        Self {
            inner: Arc::new(RestoreWaitersInner {
                store,
                senders: Mutex::new(HashMap::new()),
                poller_started: AtomicBool::new(false),
                poller_wakeup: Arc::new(Notify::new()),
                max_pending_sandboxes,
                shutdown_token,
                task_tracker,
            }),
        }
    }

    pub(super) fn subscribe(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<watch::Receiver<RestorePollStatus>, SubstrateError> {
        let receiver = self.register(sandbox_id)?;
        self.ensure_poller();
        self.inner.poller_wakeup.notify_one();
        Ok(receiver)
    }

    fn register(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<watch::Receiver<RestorePollStatus>, SubstrateError> {
        let mut senders = self
            .inner
            .senders
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if let Some(sender) = senders.get(&sandbox_id) {
            return Ok(sender.subscribe());
        }
        if senders.len() >= self.inner.max_pending_sandboxes {
            return Err(SubstrateError::restore_in_flight_with_lease(
                format!(
                    "this API pod is already following {} sandbox restores; retry another pod",
                    self.inner.max_pending_sandboxes
                ),
                RESTORE_WAITER_CAP_RETRY_AFTER,
            ));
        }

        let (sender, receiver) = watch::channel(RestorePollStatus::Pending);
        senders.insert(sandbox_id, sender);
        Ok(receiver)
    }

    fn ensure_poller(&self) {
        if self
            .inner
            .poller_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let inner = Arc::downgrade(&self.inner);
        let poller_wakeup = Arc::clone(&self.inner.poller_wakeup);
        let shutdown_token = self.inner.shutdown_token.clone();
        self.inner
            .task_tracker
            .spawn(poll_restores(inner, poller_wakeup, shutdown_token));
    }
}

async fn poll_restores(
    inner: Weak<RestoreWaitersInner>,
    poller_wakeup: Arc<Notify>,
    shutdown_token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                clear_waiters_on_shutdown(&inner);
                return;
            },
            _ = poller_wakeup.notified() => {}
        }
        let Some(current) = inner.upgrade() else {
            return;
        };
        loop {
            let sandbox_ids = {
                let mut senders = current
                    .senders
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                senders.retain(|_, sender| sender.receiver_count() > 0);
                senders.keys().copied().collect::<Vec<_>>()
            };
            if sandbox_ids.is_empty() {
                break;
            }

            let observations = tokio::select! {
                _ = shutdown_token.cancelled() => {
                    clear_waiters_on_shutdown(&inner);
                    return;
                },
                result = current.store.observe_user_restores(&sandbox_ids) => result,
            };
            match observations {
                Ok(mut observations) => {
                    let next_poll = observations
                        .values()
                        .filter_map(|observation| match observation {
                            UserRestoreObservation::InFlight { lease_remaining } => {
                                Some(*lease_remaining)
                            }
                            _ => None,
                        })
                        .min()
                        .map(|interval| {
                            interval.clamp(MIN_RESTORE_POLL_INTERVAL, RESTORE_POLL_INTERVAL)
                        });
                    publish_poll_results(&current, &sandbox_ids, |sandbox_id| {
                        observations
                            .remove(&sandbox_id)
                            .map(RestorePollStatus::Observed)
                            .unwrap_or(RestorePollStatus::NotFound)
                    });
                    if let Some(next_poll) = next_poll {
                        tokio::select! {
                            _ = shutdown_token.cancelled() => {
                                clear_waiters_on_shutdown(&inner);
                                return;
                            },
                            _ = tokio::time::sleep(next_poll) => {}
                        }
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    publish_poll_results(&current, &sandbox_ids, |_| {
                        RestorePollStatus::Failed(message.clone())
                    });
                }
            }
        }
    }
}

fn clear_waiters_on_shutdown(inner: &Weak<RestoreWaitersInner>) {
    if let Some(inner) = inner.upgrade() {
        inner
            .senders
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clear();
    }
}

fn publish_poll_results(
    inner: &RestoreWaitersInner,
    sandbox_ids: &[SandboxId],
    mut status_for: impl FnMut(SandboxId) -> RestorePollStatus,
) {
    let mut senders = inner.senders.lock().unwrap_or_else(|err| err.into_inner());
    for sandbox_id in sandbox_ids {
        let status = status_for(*sandbox_id);
        let terminal = !matches!(
            status,
            RestorePollStatus::Pending
                | RestorePollStatus::Observed(UserRestoreObservation::InFlight { .. })
        );
        if let Some(sender) = senders.get(sandbox_id) {
            sender.send_replace(status);
        }
        if terminal {
            senders.remove(sandbox_id);
        }
    }
}

#[tracing::instrument(name = "substrate.restore_waiter.poll_wait", skip_all, err)]
pub(super) async fn wait_for_restore_update(
    receiver: &mut watch::Receiver<RestorePollStatus>,
) -> Result<UserRestoreObservation, SubstrateError> {
    loop {
        receiver
            .changed()
            .await
            .map_err(|_| SubstrateError::Failed {
                message: "restore polling loop stopped while a request was waiting".to_string(),
            })?;
        let status = receiver.borrow_and_update().clone();
        match status {
            RestorePollStatus::Pending => {}
            RestorePollStatus::Observed(observation) => return Ok(observation),
            RestorePollStatus::Failed(message) => {
                return Err(SubstrateError::Failed { message });
            }
            RestorePollStatus::NotFound => {
                return Err(SubstrateError::NotFound {
                    message: "sandbox disappeared while waiting for restore".to_string(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests;
