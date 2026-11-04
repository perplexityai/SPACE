//! Cancel-storm backpressure tracker for `create_execution`.
//!
//! Background: when node-manager slows
//! under load, every cancelled `create_execution` in space-api spawns a
//! background pod-cleanup task. Those cleanups feed the saturated
//! node-manager via the sandbox-manager cleanup path, growing its queue
//! further — a self-reinforcing cascade. The node-manager per-method
//! lifecycle limiter closed node-manager's immediate failure mode, but the
//! amplifier remains open from the api side.
//!
//! This tracker bounds the *rate* at which space-api accepts new creates
//! while cancellations are spiking. It is a lock-free bucketed sliding
//! window: 10 buckets of 100 ms each cover a 1 s window. Each bucket is a
//! single `AtomicU64` packing the bucket epoch (high 32 bits) with the
//! count (low 32 bits) — so update is one CAS, and `is_over_budget` is a
//! cheap read of `NUM_BUCKETS` atomics with no allocation.
//!
//! The budget is a fixed constant. It is conservative-but-not-tight: well above steady-state cancellations on
//! a healthy cluster, low enough to bite during a real cascade.
//!
//! This is the dual of `lifecycle_limiter` in node-manager: both close one
//! direction of the cascade.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const NUM_BUCKETS: usize = 10;
const NUM_BUCKETS_U64: u64 = 10;
const BUCKET_MS: u64 = 100;
const DEFAULT_BUDGET_PER_SEC: u64 = 64;

/// Bucketed sliding-window counter of `create_execution` cancellations.
#[derive(Debug)]
pub struct CancelRateTracker {
    /// One slot per bucket. Packed as (epoch << 32) | count, where epoch is
    /// `floor(elapsed_ms / BUCKET_MS)`. A slot whose epoch is older than
    /// the current epoch by `>= NUM_BUCKETS` is treated as empty.
    buckets: [AtomicU64; NUM_BUCKETS],
    started_at: Instant,
    budget_per_sec: u64,
}

impl CancelRateTracker {
    /// Construct with an explicit budget. Production uses [`Self::with_default_budget`].
    pub fn with_budget(budget_per_sec: u64) -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            started_at: Instant::now(),
            budget_per_sec,
        }
    }

    /// Construct with [`DEFAULT_BUDGET_PER_SEC`]. Returns an `Arc` so the
    /// tracker can be cloned cheaply into per-request guards.
    pub fn with_default_budget() -> Arc<Self> {
        Arc::new(Self::with_budget(DEFAULT_BUDGET_PER_SEC))
    }

    /// Configured budget. Exposed for tests and metrics.
    pub fn budget_per_sec(&self) -> u64 {
        self.budget_per_sec
    }

    /// Record one cancel event in the current bucket. Advisory counter —
    /// `Relaxed` is sufficient because the value isn't used to publish
    /// other memory.
    pub fn record_cancel(&self) {
        loop {
            // Re-sample the epoch each attempt so a long CAS retry (or a
            // descheduled thread) doesn't act on a stale epoch.
            let now_epoch = self.current_epoch();
            let slot = usize::try_from(now_epoch % NUM_BUCKETS_U64)
                .expect("cancel-rate bucket slot fits usize");
            let bucket = self
                .buckets
                .get(slot)
                .expect("cancel-rate bucket slot is bounded by NUM_BUCKETS");
            let cur = bucket.load(Ordering::Relaxed);
            let cur_epoch = cur >> 32;
            let cur_count = cur & 0xFFFF_FFFF;
            // Never rewind a slot to an older epoch: if a concurrent writer
            // already advanced it past our sampled `now_epoch`, adopt the
            // newer epoch instead of clobbering their count. `slot` is keyed
            // on `epoch % NUM_BUCKETS`, so any epoch observed in this slot is
            // a legal target for it.
            let target_epoch = now_epoch.max(cur_epoch);
            let new_packed = if cur_epoch == target_epoch {
                // Increment the existing bucket, clamping at u32::MAX (the
                // low-32-bit slot capacity).
                (target_epoch << 32) | (cur_count + 1).min(u64::from(u32::MAX))
            } else {
                // The slot held an older epoch that wrapped around to us;
                // start the new epoch's window at count = 1.
                (target_epoch << 32) | 1
            };
            if bucket
                .compare_exchange_weak(cur, new_packed, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// True if cancellations over the last `NUM_BUCKETS * BUCKET_MS`
    /// have exceeded the configured per-second budget.
    pub fn is_over_budget(&self) -> bool {
        self.current_count() > self.budget_per_sec
    }

    /// Sum cancellations whose bucket falls within the last `NUM_BUCKETS`
    /// epochs (i.e. the trailing 1 s window). Stale-epoch buckets are
    /// skipped. Cheap: `NUM_BUCKETS` `Relaxed` atomic loads.
    pub fn current_count(&self) -> u64 {
        let now_epoch = self.current_epoch();
        let mut total: u64 = 0;
        for slot in &self.buckets {
            let packed = slot.load(Ordering::Relaxed);
            let epoch = packed >> 32;
            let count = packed & 0xFFFF_FFFF;
            if now_epoch.saturating_sub(epoch) < NUM_BUCKETS_U64 {
                total = total.saturating_add(count);
            }
        }
        total
    }

    fn current_epoch(&self) -> u64 {
        let elapsed_ms = self
            .started_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        elapsed_ms / BUCKET_MS
    }
}

#[cfg(test)]
mod tests;
