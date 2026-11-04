//! Per-org byte-rate token buckets for file transfers.
//!
//! A transfer debits each chunk when it reads it, so the balance may go
//! negative. A transfer that may stop reading, an HTTP/1.x upload, holds a
//! chunk that leaves the bucket in debt until the debt is slept off:
//! concurrent ones in one org queue behind each other's debt and share the
//! rate, and the sleeping transfer stops polling its body, which turns the
//! bucket into TCP backpressure rather than an error.
//!
//! A transfer that mustn't stop reading, because its stream shares an HTTP/2
//! connection's flow control with others, is [charged] instead: its bytes go
//! on the balance with no sleep, and it's started only while the org owes
//! nothing. HTTP/2 uploads and every download are charged; a download's
//! response body is read from a pooled backend connection.
//!
//! [charged]: ByteRateBucket::charge

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use pplx_space_common_core::types::OrgId;
use tokio::time::Instant;

use super::ByteRateConfig;

/// A full, unreferenced bucket is indistinguishable from a new one, so the
/// sweep that drops those only has to run often enough to bound the map.
pub(super) const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// The least time between sweeps the size bound starts, so a map held past
/// its bound by buckets that transfers hold can't make every new org rescan it.
pub(super) const BOUND_SWEEP_SPACING: Duration = Duration::from_secs(1);
/// An unreferenced bucket untouched this long goes even if it isn't full yet.
/// At the default rate a bucket refills from a burst's worth of debt well
/// within it. A bucket that hasn't, with a slower rate or deeper debt, goes
/// all the same: the org's next transfer starts from a full burst, whatever
/// the bucket still had or owed.
pub(super) const BUCKET_IDLE_EXPIRY: Duration = Duration::from_secs(600);
/// Past this many tracked orgs, unreferenced buckets go fullest first, so the
/// orgs that lose rate state lose the least of it. Buckets that transfers
/// hold are never dropped; open connections bound those.
const MAX_TRACKED_ORGS: usize = 65_536;
/// Far longer than any real debt, and small enough to add to an `Instant`.
const MAX_WAIT: Duration = Duration::from_secs(u32::MAX as u64);

/// Clears the one-sweep-at-a-time flag when dropped.
struct SweepingGuard<'a>(&'a AtomicBool);

impl Drop for SweepingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(super) struct OrgByteRates {
    config: ByteRateConfig,
    buckets: DashMap<OrgId, Arc<ByteRateBucket>>,
    last_sweep: Mutex<Instant>,
    /// Set while a sweep runs, so at most one does at a time.
    sweeping: AtomicBool,
    max_tracked: usize,
    bound_sweep_spacing: Duration,
}

impl OrgByteRates {
    pub(super) fn new(config: ByteRateConfig) -> Self {
        Self::with_max_tracked(config, MAX_TRACKED_ORGS)
    }

    pub(super) fn with_max_tracked(config: ByteRateConfig, max_tracked: usize) -> Self {
        Self {
            config,
            buckets: DashMap::new(),
            last_sweep: Mutex::new(Instant::now()),
            sweeping: AtomicBool::new(false),
            max_tracked,
            bound_sweep_spacing: BOUND_SWEEP_SPACING,
        }
    }

    /// For tests that watch a background sweep in real time.
    #[cfg(test)]
    pub(super) fn with_bound_sweep_spacing(mut self, spacing: Duration) -> Self {
        self.bound_sweep_spacing = spacing;
        self
    }

    pub(super) fn for_org(self: &Arc<Self>, org_id: OrgId) -> Arc<ByteRateBucket> {
        // Detached: the transfer doesn't wait for the sweep.
        drop(self.start_sweep_if_due(org_id));
        self.buckets
            .entry(org_id)
            .or_insert_with(|| Arc::new(ByteRateBucket::new(self.config)))
            .clone()
    }

    /// Starts a sweep at most once a `SWEEP_INTERVAL`, or, when `org_id` would
    /// take the map past its size bound, at most once a `BOUND_SWEEP_SPACING`.
    /// Transfers start sweeps; there's no timer. The sweep runs on the
    /// blocking pool: it scans the whole map, which at the bound is too long
    /// to spend on the request that triggered it, though a request can still
    /// wait briefly on a shard lock the sweep holds. Until it finishes the map
    /// may sit a little past the bound. `None` when no sweep starts.
    pub(super) fn start_sweep_if_due(
        self: &Arc<Self>,
        org_id: OrgId,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let now = Instant::now();
        {
            // A request that loses the race skips the sweep rather than wait.
            let Ok(mut last_sweep) = self.last_sweep.try_lock() else {
                return None;
            };
            // One sweep at a time, and none of the checks below while one runs.
            if self.sweeping.load(Ordering::Acquire) {
                return None;
            }
            let since_last = now.duration_since(*last_sweep);
            // `len` takes every shard's lock, so it's asked for only for an
            // org the lookup would add, and not within a bound sweep's
            // spacing of the last sweep.
            let over_bound = since_last >= self.bound_sweep_spacing
                && !self.buckets.contains_key(&org_id)
                && self.buckets.len() >= self.max_tracked;
            if since_last < SWEEP_INTERVAL && !over_bound {
                return None;
            }
            if self.sweeping.swap(true, Ordering::AcqRel) {
                return None;
            }
            *last_sweep = now;
        }
        let rates = Arc::clone(self);
        Some(tokio::task::spawn_blocking(move || {
            // Cleared however the sweep ends, so a sweep that panicked can't
            // stop every later one.
            let _sweeping = SweepingGuard(&rates.sweeping);
            // Detached, so a panic would otherwise go unseen.
            let swept = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rates.sweep(Instant::now(), org_id);
            }));
            if swept.is_err() {
                tracing::error!("org byte-rate bucket sweep panicked");
            }
        }))
    }

    /// Expires idle buckets, then, if the map with `org_id` in it would be
    /// past its bound, evicts down under it. `org_id` may or may not be in
    /// the map by now; it counts either way.
    fn sweep(&self, now: Instant, org_id: OrgId) {
        // `retain` and `remove_if` hold each shard's write lock, and `for_org`
        // clones under the same lock, so a bucket seen unreferenced here can't
        // be picked up before it's removed.
        self.buckets.retain(|_, bucket| {
            Arc::strong_count(bucket) > 1
                || (!bucket.is_full(now) && bucket.idle_for(now) < BUCKET_IDLE_EXPIRY)
        });
        let tracked = self.buckets.len() + usize::from(!self.buckets.contains_key(&org_id));
        if tracked <= self.max_tracked {
            return;
        }
        // Down to fifteen sixteenths of the bound, so a map held at the bound
        // pays for one sort per many new orgs rather than one per org.
        let excess = tracked - (self.max_tracked - self.max_tracked / 16);
        let mut unreferenced: Vec<(OrgId, f64)> = self
            .buckets
            .iter()
            .filter(|entry| Arc::strong_count(entry.value()) == 1)
            .map(|entry| (*entry.key(), entry.value().level(now)))
            .collect();
        unreferenced.sort_by(|a, b| b.1.total_cmp(&a.1));
        for (org_id, level) in unreferenced.into_iter().take(excess) {
            // Still unreferenced, and no emptier than it was ranked: a transfer
            // that went into debt since the ranking keeps its bucket.
            self.buckets.remove_if(&org_id, |_, bucket| {
                Arc::strong_count(bucket) == 1 && bucket.level(now) >= level
            });
        }
    }

    #[cfg(test)]
    pub(super) fn bucket(&self, org_id: OrgId) -> Option<Arc<ByteRateBucket>> {
        self.buckets.get(&org_id).map(|bucket| Arc::clone(&bucket))
    }

    #[cfg(test)]
    pub(super) fn tracks(&self, org_id: OrgId) -> bool {
        self.buckets.contains_key(&org_id)
    }

    #[cfg(test)]
    pub(super) fn tracked(&self) -> usize {
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

    /// Takes `bytes` from the bucket. `None` when the bucket covered them;
    /// otherwise when the debt they leave is slept off.
    pub(crate) fn debit(&self, bytes: usize, now: Instant) -> Option<Instant> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.refill(&mut state, now);
        state.balance -= bytes as f64;
        if state.balance >= 0.0 {
            return None;
        }
        // Finite, so the end can't overflow. Measured from `updated`, the
        // instant the balance stands at, not the caller's `now`, which a
        // caller that took the lock later may already have passed.
        let wait = Duration::try_from_secs_f64(-state.balance / self.bytes_per_second)
            .unwrap_or(MAX_WAIT)
            .min(MAX_WAIT);
        Some(state.updated + wait)
    }

    /// Takes `bytes` from the bucket without waiting for them: whatever debt
    /// they leave is for the org's other transfers to sleep off.
    pub(crate) fn charge(&self, bytes: usize, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.refill(&mut state, now);
        state.balance -= bytes as f64;
    }

    /// How long, as of `now`, the debt the org's transfers owe the bucket
    /// takes to pay off; zero when they owe nothing.
    pub(crate) fn debt_clears_in(&self, now: Instant) -> Duration {
        let level = self.level(now);
        if level >= 0.0 {
            return Duration::ZERO;
        }
        Duration::try_from_secs_f64(-level / self.bytes_per_second)
            .unwrap_or(MAX_WAIT)
            .min(MAX_WAIT)
    }

    #[cfg(test)]
    pub(crate) fn balance(&self) -> f64 {
        self.level(Instant::now())
    }

    fn is_full(&self, now: Instant) -> bool {
        self.level(now) >= self.burst_bytes
    }

    /// The balance as of `now`.
    fn level(&self, now: Instant) -> f64 {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.refilled(&state, now)
    }

    /// How long since a transfer last debited or charged the bucket.
    fn idle_for(&self, now: Instant) -> Duration {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        now.saturating_duration_since(state.updated)
    }

    /// Brings the balance up to `now`.
    fn refill(&self, state: &mut BucketState, now: Instant) {
        state.balance = self.refilled(state, now);
        // `now` was read before the lock, so a caller that took the lock
        // later may already have moved `updated` past it.
        state.updated = state.updated.max(now);
    }

    fn refilled(&self, state: &BucketState, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(state.updated).as_secs_f64();
        (state.balance + elapsed * self.bytes_per_second).min(self.burst_bytes)
    }
}
