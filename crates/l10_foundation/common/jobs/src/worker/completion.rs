use pplx_space_common_core::types::JobId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::watch;
use tonic::Status;

const WAITERS_MUTEX_POISONED: &str = "common jobs completion waiter map mutex poisoned";

#[derive(Clone, Default)]
pub(super) struct CompletionWaiters {
    entries: Arc<Mutex<HashMap<JobId, WaiterEntry>>>,
}

struct WaiterEntry {
    signal: Arc<CompletionSignal>,
    registrations: usize,
}

struct CompletionSignal {
    sender: watch::Sender<Option<Instant>>,
}

pub(super) struct CompletionWait {
    job_id: JobId,
    signal: Arc<CompletionSignal>,
    receiver: watch::Receiver<Option<Instant>>,
    waiters: CompletionWaiters,
}

impl CompletionWaiters {
    pub(super) fn register(&self, job_id: &JobId) -> Result<CompletionWait, Status> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Status::internal(WAITERS_MUTEX_POISONED))?;
        let entry = entries.entry(*job_id).or_insert_with(|| {
            let (sender, _) = watch::channel(None);
            WaiterEntry {
                signal: Arc::new(CompletionSignal { sender }),
                registrations: 0,
            }
        });
        entry.registrations += 1;
        let signal = Arc::clone(&entry.signal);
        let receiver = signal.sender.subscribe();
        Ok(CompletionWait {
            job_id: *job_id,
            signal,
            receiver,
            waiters: self.clone(),
        })
    }

    pub(super) fn notify(&self, job_id: &JobId) {
        let signal = self
            .entries
            .lock()
            .expect(WAITERS_MUTEX_POISONED)
            .remove(job_id)
            .map(|entry| entry.signal);
        if let Some(signal) = signal {
            signal.sender.send_replace(Some(Instant::now()));
        }
    }

    #[cfg(test)]
    pub(super) fn contains(&self, job_id: &JobId) -> bool {
        self.entries
            .lock()
            .expect(WAITERS_MUTEX_POISONED)
            .contains_key(job_id)
    }
}

impl CompletionWait {
    pub(super) async fn notified(&mut self) -> Option<Instant> {
        if let Some(notified_at) = *self.receiver.borrow_and_update() {
            return Some(notified_at);
        }
        self.receiver.changed().await.ok()?;
        *self.receiver.borrow_and_update()
    }
}

impl Drop for CompletionWait {
    fn drop(&mut self) {
        let Ok(mut entries) = self.waiters.entries.lock() else {
            return;
        };
        let remove = entries.get_mut(&self.job_id).is_some_and(|entry| {
            if !Arc::ptr_eq(&entry.signal, &self.signal) {
                return false;
            }
            entry.registrations -= 1;
            entry.registrations == 0
        });
        if remove {
            entries.remove(&self.job_id);
        }
    }
}

#[cfg(test)]
mod tests;
