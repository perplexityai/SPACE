//! Opportunistic handoff of a bounded stream prefix to node-manager receive.
//!
//! No pages are pinned: memory pressure may reclaim them at any time. Admission
//! reserves a full per-pull prefix without waiting, and remains charged while
//! its writer runs and during a short post-download lease. Receive evicts its pinned
//! input when the child finishes; absent an acknowledgement we conservatively
//! keep the reservation until expiry. Failure, cancellation and orderly shutdown
//! expire it immediately. A crash loses only this optimization: ordinary clean
//! page cache remains reclaimable, and restarted pulls acquire fresh admission.
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use pplx_space_common_svc::page_cache;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const UNIT_BYTES: u64 = 4096;
const RETENTION_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct ReceiveCache(Arc<Controller>);

struct Controller {
    admission: Mutex<()>,
    budget: Arc<Semaphore>,
    per_pull_units: u32,
    shutdown: CancellationToken,
    tasks: TaskTracker,
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Debug)]
pub(crate) struct ReceiveCacheLease {
    _permit: OwnedSemaphorePermit,
    bytes: u64,
    active: AtomicBool,
    successful: AtomicBool,
    complete: CancellationToken,
    file: OnceLock<Arc<File>>,
}

pub(crate) struct ReceiveCacheAttempt(pub(crate) Arc<ReceiveCacheLease>);

impl Drop for ReceiveCacheAttempt {
    fn drop(&mut self) {
        self.0.complete.cancel();
    }
}

impl ReceiveCache {
    pub(crate) fn new(node_bytes: u64, per_pull_bytes: u64) -> Self {
        let units = (node_bytes / UNIT_BYTES).min(u32::MAX as u64) as usize;
        let per_pull_units = (per_pull_bytes / UNIT_BYTES).min(u32::MAX as u64) as u32;
        Self(Arc::new(Controller {
            admission: Mutex::new(()),
            budget: Arc::new(Semaphore::new(units)),
            per_pull_units,
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
        }))
    }

    pub(crate) fn try_reserve(&self) -> Option<ReceiveCacheAttempt> {
        let _admission = self
            .0
            .admission
            .lock()
            .expect("receive cache admission poisoned");
        if self.0.per_pull_units == 0 {
            return None;
        }
        let permit = self
            .0
            .budget
            .clone()
            .try_acquire_many_owned(self.0.per_pull_units)
            .ok()?;
        let lease = Arc::new(ReceiveCacheLease {
            _permit: permit,
            bytes: u64::from(self.0.per_pull_units) * UNIT_BYTES,
            active: AtomicBool::new(true),
            successful: AtomicBool::new(false),
            complete: CancellationToken::new(),
            file: OnceLock::new(),
        });
        let owner = lease.clone();
        let shutdown = self.0.shutdown.clone();
        self.0.tasks.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = owner.complete.cancelled() => {},
            }
            if owner.successful.load(Ordering::Acquire) && owner.file.get().is_some() {
                tokio::select! {
                    _ = shutdown.cancelled() => {},
                    _ = tokio::time::sleep(RETENTION_TTL) => {},
                }
            }
            // Future ranges observe this before advice. Only active writers
            // keep admission charged after expiry; lazy queued ranges hold a
            // weak reference so idle writeback cannot prevent budget reuse.
            owner.active.store(false, Ordering::Release);
            if let Err(error) = tokio::task::spawn_blocking(move || {
                if let Some(file) = owner.file.get()
                    && let Err(error) = page_cache::advise_range_dontneed(file, 0, owner.bytes)
                {
                    tracing::warn!(%error, "failed to expire receive stream cache");
                }
            })
            .await
            {
                tracing::warn!(%error, "receive stream cache expiry task failed");
            }
        });
        Some(ReceiveCacheAttempt(lease))
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        {
            let _admission = self
                .0
                .admission
                .lock()
                .expect("receive cache admission poisoned");
            self.0.budget.close();
            self.0.shutdown.cancel();
            self.0.tasks.close();
        }
        tokio::time::timeout(timeout, self.0.tasks.wait())
            .await
            .is_ok()
    }
}

impl ReceiveCacheLease {
    pub(crate) fn attach(&self, file: &File) {
        match file.try_clone() {
            Ok(file) => {
                let _ = self.file.set(Arc::new(file));
            }
            Err(error) => {
                self.active.store(false, Ordering::Release);
                tracing::warn!(%error, "receive cache descriptor unavailable; using ordinary eviction");
            }
        }
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        if self.active.load(Ordering::Acquire) && self.file.get().is_some() {
            self.bytes
        } else {
            0
        }
    }

    pub(crate) fn mark_success(&self) {
        self.successful.store(true, Ordering::Release);
    }
}

/// Run potentially expensive whole-file advice off Tokio, retaining only an
/// admitted prefix of the same pinned inode used by the streamed writer.
pub(crate) async fn finish_output(path: &Path, lease: Option<Arc<ReceiveCacheLease>>) {
    let path = path.to_owned();
    if let Err(error) = tokio::task::spawn_blocking(move || {
        if let Some(lease) = lease
            && let Some(file) = lease.file.get()
        {
            if let Err(error) = page_cache::advise_range_dontneed(file, lease.retained_bytes(), 0) {
                tracing::warn!(%error, "failed to release pull output page cache");
            }
        } else {
            page_cache::advise_path_dontneed(&path, "download volume output");
        }
    })
    .await
    {
        tracing::warn!(%error, "pull output cache advice task failed");
    }
}

#[cfg(test)]
mod tests;
