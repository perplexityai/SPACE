//! Per-org byte-rate token buckets for file transfers.
//!
//! A transfer debits each chunk when it reads it, and a chunk that leaves the
//! bucket in debt is held until the debt is slept off, so the balance may go
//! negative. Concurrent transfers in one org queue behind each other's debt
//! and share the rate. The sleeping transfer stops polling its body, which is
//! what turns the bucket into TCP backpressure rather than an error.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use pplx_space_common_core::types::OrgId;
use tokio::time::Instant;

use super::ByteRateConfig;

/// A full, unreferenced bucket is indistinguishable from a new one, so the
/// sweep that drops those only has to run often enough to bound the map.
pub(super) const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Far longer than any real debt, and small enough to add to an `Instant`.
const MAX_WAIT: Duration = Duration::from_secs(u32::MAX as u64);

pub(super) struct OrgByteRates {
    config: ByteRateConfig,
    buckets: DashMap<OrgId, Arc<ByteRateBucket>>,
    last_sweep: Mutex<Instant>,
}

impl OrgByteRates {
    pub(super) fn new(config: ByteRateConfig) -> Self {
        Self {
            config,
            buckets: DashMap::new(),
            last_sweep: Mutex::new(Instant::now()),
        }
    }

    pub(super) fn for_org(&self, org_id: OrgId) -> Arc<ByteRateBucket> {
        self.sweep_if_due();
        self.buckets
            .entry(org_id)
            .or_insert_with(|| Arc::new(ByteRateBucket::new(self.config)))
            .clone()
    }

    fn sweep_if_due(&self) {
        let now = Instant::now();
        {
            // A request that loses the race skips the sweep rather than wait.
            let Ok(mut last_sweep) = self.last_sweep.try_lock() else {
                return;
            };
            if now.duration_since(*last_sweep) < SWEEP_INTERVAL {
                return;
            }
            *last_sweep = now;
        }
        // `retain` holds each shard's write lock, and `for_org` clones under
        // the same lock, so a bucket seen unreferenced here can't be picked
        // up before it's removed.
        self.buckets
            .retain(|_, bucket| Arc::strong_count(bucket) > 1 || !bucket.is_full(now));
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.buckets.len()
    }
}

pub(crate) struct ByteRateBucket {
    bytes_per_second: f64,
    burst_bytes: f64,
    state: Mutex<BucketState>,
}

struct BucketState {
    /// Negative while transfers owe the bucket.
    balance: f64,
    updated: Instant,
}

impl ByteRateBucket {
    fn new(config: ByteRateConfig) -> Self {
        let burst_bytes = config.burst_bytes.get() as f64;
        Self {
            bytes_per_second: config.bytes_per_second.get() as f64,
            burst_bytes,
            state: Mutex::new(BucketState {
                balance: burst_bytes,
                updated: Instant::now(),
            }),
        }
    }

    /// Takes `bytes` from the bucket and returns how long the caller must wait
    /// before moving more, zero when the bucket covered them.
    pub(crate) fn debit(&self, bytes: usize, now: Instant) -> Duration {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let balance = self.refilled(&state, now) - bytes as f64;
        state.balance = balance;
        // `now` was read before the lock, so a caller that took the lock
        // later may already have moved `updated` past it.
        state.updated = state.updated.max(now);
        if balance >= 0.0 {
            Duration::ZERO
        } else {
            // Finite, so the caller's `now + wait` can't overflow.
            Duration::try_from_secs_f64(-balance / self.bytes_per_second)
                .unwrap_or(MAX_WAIT)
                .min(MAX_WAIT)
        }
    }

    /// Returns `bytes` a transfer was debited for but never moved on, up to
    /// the burst.
    pub(crate) fn credit(&self, bytes: usize, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.balance = (self.refilled(&state, now) + bytes as f64).min(self.burst_bytes);
        state.updated = state.updated.max(now);
    }

    fn is_full(&self, now: Instant) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.refilled(&state, now) >= self.burst_bytes
    }

    fn refilled(&self, state: &BucketState, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(state.updated).as_secs_f64();
        (state.balance + elapsed * self.bytes_per_second).min(self.burst_bytes)
    }
}
