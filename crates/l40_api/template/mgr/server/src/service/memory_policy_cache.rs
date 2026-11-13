//! The sandbox memory policy `ListTemplates` reports snapshot memory under.
//!
//! Resolving the policy is a chain of local RPCs: template-manager dials
//! sandbox-manager, which reads the node's cgroup budget from node-manager.
//! Every hop can wait seconds on a busy node, while the answer is node
//! configuration that only changes when those services restart. A listing
//! therefore reads the last resolution and refreshes it in the background,
//! so a slow or stalled sandbox-manager doesn't hold the listing.

use std::collections::BTreeSet;

use futures_util::future::{BoxFuture, FutureExt, Shared};
use tokio::time::{Duration, Instant};

use super::*;

/// How long a resolution is used before a listing refreshes it.
const MEMORY_POLICY_REFRESH_AFTER: Duration = Duration::from_secs(30);

/// How long a listing waits for a size it has never resolved. Past this it
/// reports the requested size, as it does when resolution fails.
const MEMORY_POLICY_FIRST_RESOLVE_WAIT: Duration = Duration::from_millis(500);

type ResolveFn = dyn Fn(
        u32,
        pplx_space_sbx_mgr_client::GrpcRetryPolicy,
    ) -> BoxFuture<'static, Result<ResolvedSandboxMemory, Status>>
    + Send
    + Sync;

type Refresh = Shared<BoxFuture<'static, ()>>;

#[derive(Default)]
struct Entry {
    effective_memory_mib: Option<u32>,
    resolved_at: Option<Instant>,
    refresh: Option<Refresh>,
}

/// Effective snapshot memory by requested size, refreshed in the background.
#[derive(Clone)]
pub(super) struct SandboxMemoryPolicyCache {
    resolve: Arc<ResolveFn>,
    entries: Arc<Mutex<HashMap<u32, Entry>>>,
}

impl SandboxMemoryPolicyCache {
    pub(super) fn new(
        resolve: impl Fn(
            u32,
            pplx_space_sbx_mgr_client::GrpcRetryPolicy,
        ) -> BoxFuture<'static, Result<ResolvedSandboxMemory, Status>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            resolve: Arc::new(resolve),
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn sandbox_manager() -> Self {
        Self::new(|requested_memory_mib, retry_policy| {
            Box::pin(resolve_sandbox_memory(requested_memory_mib, retry_policy))
        })
    }

    /// The effective memory for each requested size. A size resolved within
    /// [`MEMORY_POLICY_REFRESH_AFTER`] is answered at once; an older one is
    /// answered with its last resolution while a refresh runs. A size never
    /// resolved waits up to [`MEMORY_POLICY_FIRST_RESOLVE_WAIT`] and then
    /// reads as the requested size until its resolution lands.
    pub(super) async fn effective_memory_mib(
        &self,
        requested: &BTreeSet<u32>,
        retry_policy: pplx_space_sbx_mgr_client::GrpcRetryPolicy,
    ) -> HashMap<u32, u32> {
        let mut first_resolves = Vec::new();
        {
            let mut entries = self.lock();
            for &requested_memory_mib in requested {
                let entry = entries.entry(requested_memory_mib).or_default();
                if entry
                    .resolved_at
                    .is_some_and(|at| at.elapsed() < MEMORY_POLICY_REFRESH_AFTER)
                {
                    continue;
                }
                let refresh = entry
                    .refresh
                    .get_or_insert_with(|| self.spawn_refresh(requested_memory_mib, retry_policy))
                    .clone();
                if entry.effective_memory_mib.is_none() {
                    first_resolves.push(refresh);
                }
            }
        }
        if !first_resolves.is_empty() {
            let _ = tokio::time::timeout(
                MEMORY_POLICY_FIRST_RESOLVE_WAIT,
                futures_util::future::join_all(first_resolves),
            )
            .await;
        }
        let entries = self.lock();
        requested
            .iter()
            .map(|&requested_memory_mib| {
                let effective = entries
                    .get(&requested_memory_mib)
                    .and_then(|entry| entry.effective_memory_mib)
                    .unwrap_or(requested_memory_mib);
                (requested_memory_mib, effective)
            })
            .collect()
    }

    fn spawn_refresh(
        &self,
        requested_memory_mib: u32,
        retry_policy: pplx_space_sbx_mgr_client::GrpcRetryPolicy,
    ) -> Refresh {
        let resolve = (self.resolve)(requested_memory_mib, retry_policy);
        let entries = Arc::clone(&self.entries);
        let task = tokio::spawn(async move {
            // Cleared on every exit, a panic included, so the next listing
            // can start another refresh.
            let _clear = ClearRefresh {
                entries: Arc::clone(&entries),
                requested_memory_mib,
            };
            let result = resolve.await;
            let mut entries = entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = entries.entry(requested_memory_mib).or_default();
            match result {
                Ok(resolved) => {
                    if resolved.effective_memory_mib < resolved.requested_memory_mib
                        && entry.effective_memory_mib != Some(resolved.effective_memory_mib)
                    {
                        tracing::info!(
                            requested_snapshot_memory_mib = resolved.requested_memory_mib,
                            effective_snapshot_memory_mib = resolved.effective_memory_mib,
                            sandbox_guest_memory_max_mib = resolved.guest_memory_max_mib,
                            sandbox_cgroup_memory_mib = resolved.cgroup_memory_mib,
                            sandbox_cgroup_memory_max_mib = resolved.cgroup_memory_max_mib,
                            sandbox_memory_headroom_mib = resolved.memory_headroom_mib,
                            "clamped template list snapshot memory to sandbox policy"
                        );
                    }
                    entry.effective_memory_mib = Some(resolved.effective_memory_mib);
                    entry.resolved_at = Some(Instant::now());
                }
                Err(status) => {
                    tracing::warn!(
                        code = ?status.code(),
                        error = %status.message(),
                        requested_snapshot_memory_mib = requested_memory_mib,
                        "could not resolve sandbox memory policy for template listing"
                    );
                }
            }
        });
        task.map(|_| ()).boxed().shared()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u32, Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct ClearRefresh {
    entries: Arc<Mutex<HashMap<u32, Entry>>>,
    requested_memory_mib: u32,
}

impl Drop for ClearRefresh {
    fn drop(&mut self) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.get_mut(&self.requested_memory_mib) {
            entry.refresh = None;
        }
    }
}
