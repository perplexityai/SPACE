//! Single-flight coalescing for reserved-DNS upstream lookups.
//!
//! Concurrent guest queries for the same `(reserved resolver, canonical
//! question)` share one upstream lookup instead of each opening its own Hickory
//! request. Bursty sandboxes commonly issue many identical A queries at once
//! (for example, a preload fan-out resolving the same storage host); without
//! coalescing every one of them is pipelined to the upstream resolver and
//! consumes a lookup slot.
//!
//! Only the raw upstream answer is shared. Each waiter still applies its own
//! admitted policy through `sanitize_resolver_answer`, and still observes its
//! own policy cancellation: a cancelled waiter detaches from the shared lookup
//! without cancelling it for the others. When every waiter has gone away the
//! shared future is dropped, which cancels the upstream lookup and releases its
//! lookup slot exactly as an uncoalesced cancellation would.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared, WeakShared};
use hickory_resolver::proto::op::Query;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use super::{ReservedResolver, ResolverAnswer, ResolverBackend, ResolverFailure};

pub(super) type SharedLookup = Shared<BoxFuture<'static, Result<ResolverAnswer, ResolverFailure>>>;

type LookupKey = (ReservedResolver, Query);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LookupRole {
    /// This caller started the upstream lookup.
    Leader,
    /// This caller joined a lookup that was already in flight.
    Follower,
}

impl LookupRole {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Follower => "follower",
        }
    }
}

#[derive(Default)]
pub(super) struct InflightLookups {
    entries: Mutex<HashMap<LookupKey, InflightEntry>>,
    next_id: AtomicU64,
}

struct InflightEntry {
    id: u64,
    // Weak so the map never keeps an abandoned lookup (and its lookup slot)
    // alive after every waiter has been cancelled.
    lookup: WeakShared<BoxFuture<'static, Result<ResolverAnswer, ResolverFailure>>>,
}

impl InflightLookups {
    /// Join the in-flight lookup for `question` on `resolver`, or start one.
    ///
    /// The started lookup acquires one slot from `lookup_slots` for its whole
    /// duration, so N coalesced callers consume one slot rather than N.
    pub(super) fn join_or_start(
        self: &Arc<Self>,
        backend: &Arc<dyn ResolverBackend>,
        lookup_slots: &Arc<Semaphore>,
        resolver: ReservedResolver,
        question: &Query,
    ) -> (SharedLookup, LookupRole) {
        let key = (resolver, question.clone());
        let mut entries = self.entries.lock();
        if let Some(lookup) = entries.get(&key).and_then(|entry| entry.lookup.upgrade()) {
            return (lookup, LookupRole::Follower);
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let guard = EntryGuard {
            inflight: Arc::clone(self),
            key: key.clone(),
            id,
        };
        let backend = Arc::clone(backend);
        let lookup_slots = Arc::clone(lookup_slots);
        let question = question.clone();
        let lookup = async move {
            // Dropped on completion *and* when every waiter abandons the
            // lookup, so the entry never outlives the upstream request.
            let _guard = guard;
            let _permit = lookup_slots
                .try_acquire_owned()
                .map_err(|_| ResolverFailure::Capacity)?;
            // Emitted only once the slot is held, so `role=leader` counts
            // upstream lookups actually started (a leader rejected for
            // capacity is not one).
            pplx_space_metrics::gateway::reserved_dns_lookup_coalesce(
                resolver.as_str(),
                LookupRole::Leader.as_str(),
            );
            // Per-waiter policy cancellation is applied by each waiter; the
            // shared lookup itself is bounded by the backend's total deadline
            // and is cancelled by dropping it once no waiter remains.
            backend.lookup(resolver, question).await
        }
        .boxed()
        .shared();
        let weak = lookup
            .downgrade()
            .expect("a freshly created shared lookup has not completed");
        entries.insert(key, InflightEntry { id, lookup: weak });
        (lookup, LookupRole::Leader)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

struct EntryGuard {
    inflight: Arc<InflightLookups>,
    key: LookupKey,
    id: u64,
}

impl Drop for EntryGuard {
    fn drop(&mut self) {
        let mut entries = self.inflight.entries.lock();
        // A newer lookup may already occupy the key if this one finished and a
        // fresh query raced in; only remove the entry this guard owns.
        if entries
            .get(&self.key)
            .is_some_and(|entry| entry.id == self.id)
        {
            entries.remove(&self.key);
        }
    }
}
