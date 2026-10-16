//! Per-tenant shares of the upload budget, keyed by org or sandbox the way
//! `AttachedStreamLimiter` keys its counters, with an entry only while some
//! upload holds or waits on it.

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use pplx_space_metrics::data_plane::UploadRejectReason;
use tokio::time::Instant;

use super::{Budget, BudgetPermit, UploadShareConfig};

pub(super) struct ShareMap<K: Eq + Hash> {
    config: UploadShareConfig,
    shares: Arc<DashMap<K, Arc<Budget>>>,
}

/// Holds a share's slot and reservation, and the lease that keeps the share
/// in the map. Fields drop in order, and the permits must go first: a lease
/// dropped while they're still held could remove the share, and a new upload
/// for the key would then get a fresh budget beside the permits still out.
pub(super) struct SharePermit<K: Eq + Hash> {
    _budget: BudgetPermit,
    lease: ShareLease<K>,
}

impl<K: Eq + Hash + fmt::Debug> fmt::Debug for SharePermit<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharePermit")
            .field("key", &self.lease.key)
            .finish_non_exhaustive()
    }
}

/// A reference to one key's share, held by every upload that holds or waits
/// on it. Dropping the last one removes the share, so the map tracks only
/// tenants with uploads in flight.
struct ShareLease<K: Eq + Hash> {
    key: K,
    /// `None` only inside `drop`.
    budget: Option<Arc<Budget>>,
    shares: Arc<DashMap<K, Arc<Budget>>>,
}

impl<K: Eq + Hash> ShareLease<K> {
    fn budget(&self) -> &Budget {
        self.budget
            .as_deref()
            .expect("a lease keeps its budget until dropped")
    }
}

impl<K: Eq + Hash> Drop for ShareLease<K> {
    fn drop(&mut self) {
        // `drop` runs before the fields do, so release this lease's reference
        // first. Otherwise leases dropped at once on different threads would
        // each count the others and all leave the share behind. Then only the
        // map's reference is left once every lease is gone. A lease is only
        // ever cloned out of the map under the shard lock `remove_if` takes,
        // so no upload can pick the share up between the count and the
        // removal.
        drop(self.budget.take());
        self.shares
            .remove_if(&self.key, |_, budget| Arc::strong_count(budget) == 1);
    }
}

impl<K: Eq + Hash + Clone> ShareMap<K> {
    pub(super) fn new(config: UploadShareConfig) -> Self {
        Self {
            config,
            shares: Arc::new(DashMap::new()),
        }
    }

    pub(super) async fn admit(
        &self,
        key: K,
        deadline: Instant,
        reserve_bytes: u64,
    ) -> Result<SharePermit<K>, UploadRejectReason> {
        let budget = self
            .shares
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Budget::new(self.config)))
            .clone();
        // Taken before waiting, so a timed-out waiter still releases the share.
        let lease = ShareLease {
            key,
            budget: Some(budget),
            shares: Arc::clone(&self.shares),
        };
        let permit = lease.budget().admit(deadline, reserve_bytes).await?;
        Ok(SharePermit {
            _budget: permit,
            lease,
        })
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.shares.len()
    }
}
