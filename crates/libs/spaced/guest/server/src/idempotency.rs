use super::*;
use pplx_space_common_core::types::{IdempotencyKey, ProcessId};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

// Retry evidence belongs to this daemon lifetime. A restart loses these records;
// callers cannot infer exactly-once execution across that boundary.
// Every record is kept for `IDEMPOTENCY_RECORD_TTL`, so this caps the
// sustained rate of keyed operations (process starts, file mutations) at
// MAX / TTL per second. Records are small; `MAX_IDEMPOTENCY_KEY_BYTES` bounds
// their memory.
const MAX_IDEMPOTENCY_RECORDS: usize = 16_384;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONCURRENT_IDEMPOTENCY_LOOKUPS: usize = 1_024;
const IDEMPOTENCY_PRUNE_INTERVAL: Duration = Duration::from_secs(1);
const CAPACITY_WARNING_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub(super) struct IdempotencyStore {
    scope: &'static str,
    inner: Arc<Mutex<IdempotencyState>>,
    lookup_slots: Arc<tokio::sync::Semaphore>,
    diagnostics: Arc<IdempotencyDiagnostics>,
}

#[derive(Default)]
struct IdempotencyDiagnostics {
    capacity: RejectionDiagnostics,
    lookup: RejectionDiagnostics,
}

#[derive(Default)]
struct RejectionDiagnostics {
    count: AtomicU64,
    last_warning: std::sync::Mutex<Option<Instant>>,
}

impl IdempotencyStore {
    pub(super) fn new(scope: &'static str) -> Self {
        Self {
            scope,
            inner: Arc::new(Mutex::new(IdempotencyState::default())),
            lookup_slots: Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_IDEMPOTENCY_LOOKUPS,
            )),
            diagnostics: Arc::new(IdempotencyDiagnostics::default()),
        }
    }

    // State is bounded and no critical section performs I/O or awaits. Keeping
    // publication synchronous makes a known outcome inseparable from its effect
    // by RPC cancellation, and lets Drop release or fail a claim immediately.
    // No caller code runs while locked. Recovery retains the recorded generation;
    // it must never clear the table or turn an interrupted effect into new work.
    fn state(&self) -> MutexGuard<'_, IdempotencyState> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            let mut state = poisoned.into_inner();
            state.key_bytes = state.records.keys().map(|key| key.as_str().len()).sum();
            self.inner.clear_poison();
            state
        })
    }
}

#[derive(Default)]
struct IdempotencyState {
    records: HashMap<IdempotencyKey, StoredIdempotencyRecord>,
    key_bytes: usize,
    last_pruned_at: Option<Instant>,
}

#[derive(Clone)]
pub(super) enum IdempotencyRecord {
    ProcessStarted { process_id: ProcessId },
}

enum StoredIdempotencyRecord {
    InProgress {
        created_at: Instant,
        notify: Arc<Notify>,
    },
    Completed {
        result: Result<IdempotencyRecord, Status>,
        created_at: Instant,
    },
    Expired {
        expired_at: Instant,
        in_progress_generation: Option<Arc<Notify>>,
    },
}

pub(super) enum IdempotencyLookup {
    Reserved(IdempotencyReservation),
    Completed(Result<IdempotencyRecord, Status>),
}

struct IdempotencyClaim {
    store: IdempotencyStore,
    key: IdempotencyKey,
    generation: Arc<Notify>,
}

/// No effect may be started until `begin` transfers ownership to an operation.
/// Dropping a reservation proves the key can be safely retried.
#[must_use]
pub(super) struct IdempotencyReservation {
    claim: Option<IdempotencyClaim>,
}

/// An effect may have occurred. Losing its owner must never authorize a retry.
#[must_use]
pub(super) struct IdempotencyOperation {
    claim: Option<IdempotencyClaim>,
}

impl IdempotencyStore {
    pub(super) async fn reserve_or_get(
        &self,
        key: IdempotencyKey,
    ) -> Result<IdempotencyLookup, Status> {
        // Hold a slot through duplicate waiting, including every wake/recheck
        // loop. `OwnedSemaphorePermit` releases on completion, timeout, and
        // request cancellation, bounding waiters that do not consume records.
        let _lookup_permit = self.lookup_slots.clone().try_acquire_owned().map_err(|_| {
            self.record_lookup_rejection();
            Status::resource_exhausted(
                "too many concurrent idempotency lookups; retry with backoff",
            )
        })?;
        loop {
            let notify = {
                let mut state = self.state();
                let now = Instant::now();
                state.prune_if_due(now);
                match state.records.get(&key) {
                    Some(StoredIdempotencyRecord::Completed { result, created_at }) => {
                        if created_at.elapsed() > IDEMPOTENCY_RECORD_TTL {
                            state.records.insert(
                                key,
                                StoredIdempotencyRecord::Expired {
                                    expired_at: Instant::now(),
                                    in_progress_generation: None,
                                },
                            );
                            return Err(Status::failed_precondition("idempotency_key expired"));
                        }
                        return Ok(IdempotencyLookup::Completed(result.clone()));
                    }
                    Some(StoredIdempotencyRecord::InProgress { created_at, notify }) => {
                        if created_at.elapsed() > IDEMPOTENCY_RECORD_TTL {
                            let notify = notify.clone();
                            notify.notify_waiters();
                            state.records.insert(
                                key,
                                StoredIdempotencyRecord::Expired {
                                    expired_at: Instant::now(),
                                    in_progress_generation: Some(notify),
                                },
                            );
                            return Err(Status::failed_precondition("idempotency_key expired"));
                        }
                        // Register under the state lock. `notify_waiters` does
                        // not retain a permit for a future waiter, so creating
                        // this only after unlocking can lose completion and
                        // sleep until the full TTL timeout.
                        enabled_notification(notify.clone())
                    }
                    Some(StoredIdempotencyRecord::Expired { .. }) => {
                        return Err(Status::failed_precondition("idempotency_key expired"));
                    }
                    None => {
                        if !state.can_insert(&key) {
                            state.evict_tombstones_for(&key);
                        }
                        if !state.can_insert(&key) {
                            let entries = state.records.len();
                            let key_bytes = state.key_bytes;
                            drop(state);
                            self.record_capacity_rejection(entries, key_bytes);
                            return Err(Status::resource_exhausted(
                                "idempotency store capacity exhausted; retry after existing keys expire",
                            ));
                        }
                        let generation = Arc::new(Notify::new());
                        state.insert(
                            key.clone(),
                            StoredIdempotencyRecord::InProgress {
                                created_at: Instant::now(),
                                notify: generation.clone(),
                            },
                        );
                        return Ok(IdempotencyLookup::Reserved(IdempotencyReservation {
                            claim: Some(IdempotencyClaim {
                                store: self.clone(),
                                key,
                                generation,
                            }),
                        }));
                    }
                }
            };
            match tokio::time::timeout(IDEMPOTENCY_RECORD_TTL, notify).await {
                Ok(()) | Err(_) => {}
            }
        }
    }

    fn complete(
        &self,
        key: &IdempotencyKey,
        generation: &Arc<Notify>,
        result: Result<IdempotencyRecord, Status>,
    ) {
        let mut state = self.state();
        let mut notify = None;
        match state.records.get(key) {
            Some(StoredIdempotencyRecord::InProgress {
                notify: current_generation,
                ..
            }) if Arc::ptr_eq(current_generation, generation) => {
                notify = Some(current_generation.clone());
                state.records.insert(
                    key.clone(),
                    StoredIdempotencyRecord::Completed {
                        result,
                        created_at: Instant::now(),
                    },
                );
            }
            Some(StoredIdempotencyRecord::Expired {
                expired_at,
                in_progress_generation: Some(current_generation),
            }) if Arc::ptr_eq(current_generation, generation) => {
                let expired_at = *expired_at;
                state.records.insert(
                    key.clone(),
                    StoredIdempotencyRecord::Expired {
                        expired_at,
                        in_progress_generation: None,
                    },
                );
            }
            _ => {}
        }
        drop(state);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    fn abandon(&self, key: &IdempotencyKey, generation: &Arc<Notify>) {
        let mut state = self.state();
        let notify = match state.records.get(key) {
            Some(StoredIdempotencyRecord::InProgress {
                notify: current_generation,
                ..
            }) if Arc::ptr_eq(current_generation, generation) => Some(current_generation.clone()),
            Some(StoredIdempotencyRecord::Expired {
                in_progress_generation: Some(current_generation),
                ..
            }) if Arc::ptr_eq(current_generation, generation) => Some(current_generation.clone()),
            _ => None,
        };
        if notify.is_some() {
            state.records.remove(key);
            state.key_bytes = state.key_bytes.saturating_sub(key.as_str().len());
        }
        drop(state);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    #[cfg(test)]
    fn insert_expired(&self, key: IdempotencyKey, record: IdempotencyRecord) {
        self.state().insert(
            key,
            StoredIdempotencyRecord::Completed {
                result: Ok(record),
                created_at: Instant::now() - IDEMPOTENCY_RECORD_TTL - Duration::from_secs(1),
            },
        );
    }

    #[cfg(test)]
    fn with_lookup_limit(limit: usize) -> Self {
        Self {
            scope: "test",
            inner: Arc::new(Mutex::new(IdempotencyState::default())),
            lookup_slots: Arc::new(tokio::sync::Semaphore::new(limit)),
            diagnostics: Arc::new(IdempotencyDiagnostics::default()),
        }
    }

    fn record_capacity_rejection(&self, entries: usize, key_bytes: usize) {
        if self.diagnostics.capacity.record() {
            let rejected_since_last_warning = self.diagnostics.capacity.take_count();
            tracing::warn!(
                store = self.scope,
                entries,
                key_bytes,
                rejected_since_last_warning,
                "idempotency store capacity exhausted (further warnings throttled)"
            );
        }
    }

    fn record_lookup_rejection(&self) {
        if self.diagnostics.lookup.record() {
            let rejected_since_last_warning = self.diagnostics.lookup.take_count();
            tracing::warn!(
                store = self.scope,
                limit = MAX_CONCURRENT_IDEMPOTENCY_LOOKUPS,
                rejected_since_last_warning,
                "concurrent idempotency lookup limit reached (further warnings throttled)"
            );
        }
    }
}

impl RejectionDiagnostics {
    fn record(&self) -> bool {
        self.count.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut last_warning = self
            .last_warning
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let should_warn =
            last_warning.is_none_or(|last| now.duration_since(last) >= CAPACITY_WARNING_INTERVAL);
        if should_warn {
            *last_warning = Some(now);
        }
        should_warn
    }

    fn take_count(&self) -> u64 {
        self.count.swap(0, Ordering::Relaxed)
    }
}

impl IdempotencyState {
    fn can_insert(&self, key: &IdempotencyKey) -> bool {
        self.records.len() < MAX_IDEMPOTENCY_RECORDS
            && self.key_bytes.saturating_add(key.as_str().len()) <= MAX_IDEMPOTENCY_KEY_BYTES
    }

    fn insert(&mut self, key: IdempotencyKey, record: StoredIdempotencyRecord) {
        if !self.records.contains_key(&key) {
            self.key_bytes = self.key_bytes.saturating_add(key.as_str().len());
        }
        self.records.insert(key, record);
    }

    fn prune(&mut self, now: Instant) {
        self.last_pruned_at = Some(now);
        let mut notify = Vec::new();
        for record in self.records.values_mut() {
            match record {
                StoredIdempotencyRecord::InProgress {
                    created_at,
                    notify: waiter,
                } if now.duration_since(*created_at) > IDEMPOTENCY_RECORD_TTL => {
                    notify.push(waiter.clone());
                    *record = StoredIdempotencyRecord::Expired {
                        expired_at: now,
                        in_progress_generation: Some(waiter.clone()),
                    };
                }
                StoredIdempotencyRecord::Completed { created_at, .. }
                    if now.duration_since(*created_at) > IDEMPOTENCY_RECORD_TTL =>
                {
                    *record = StoredIdempotencyRecord::Expired {
                        expired_at: now,
                        in_progress_generation: None,
                    };
                }
                _ => {}
            }
        }
        for waiter in notify {
            waiter.notify_waiters();
        }
    }

    fn prune_if_due(&mut self, now: Instant) {
        if self.last_pruned_at.is_none_or(|last_pruned_at| {
            now.duration_since(last_pruned_at) >= IDEMPOTENCY_PRUNE_INTERVAL
        }) {
            self.prune(now);
        }
    }

    fn evict_tombstones_for(&mut self, key: &IdempotencyKey) {
        // Retain tombstones while there is room so a reused key receives an
        // explicit expiry error. Under pressure, reclaim completed or abandoned
        // tombstones in one allocation-free pass. A live claim protects its
        // expired generation until completion or Drop explicitly releases it.
        let records_to_free = self
            .records
            .len()
            .saturating_add(1)
            .saturating_sub(MAX_IDEMPOTENCY_RECORDS);
        let bytes_to_free = self
            .key_bytes
            .saturating_add(key.as_str().len())
            .saturating_sub(MAX_IDEMPOTENCY_KEY_BYTES);
        let mut evicted_records = 0usize;
        let mut evicted_bytes = 0usize;
        self.records.retain(|existing_key, record| {
            let needs_capacity = evicted_records < records_to_free || evicted_bytes < bytes_to_free;
            let evictable = matches!(
                record,
                StoredIdempotencyRecord::Expired {
                    in_progress_generation: None,
                    ..
                }
            );
            if needs_capacity && evictable {
                evicted_records = evicted_records.saturating_add(1);
                evicted_bytes = evicted_bytes.saturating_add(existing_key.as_str().len());
                false
            } else {
                true
            }
        });
        self.key_bytes = self.key_bytes.saturating_sub(evicted_bytes);
    }
}

fn enabled_notification(notify: Arc<Notify>) -> impl Future<Output = ()> + Send + 'static {
    let mut notified = Box::pin(notify.notified_owned());
    notified.as_mut().enable();
    notified
}

impl IdempotencyReservation {
    pub(super) fn begin(mut self) -> Result<IdempotencyOperation, Status> {
        let claim = self.claim.as_ref().expect("reservation owns its claim");
        let current = matches!(
            claim.store.state().records.get(&claim.key),
            Some(StoredIdempotencyRecord::InProgress { created_at, notify, .. })
                if Arc::ptr_eq(notify, &claim.generation)
                    && created_at.elapsed() <= IDEMPOTENCY_RECORD_TTL
        );
        if !current {
            return Err(Status::failed_precondition("idempotency_key expired"));
        }
        Ok(IdempotencyOperation {
            claim: self.claim.take(),
        })
    }
}

impl Drop for IdempotencyReservation {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            claim.store.abandon(&claim.key, &claim.generation);
        }
    }
}

impl IdempotencyOperation {
    pub(super) fn complete(mut self, result: Result<IdempotencyRecord, Status>) {
        let claim = self.claim.take().expect("operation owns its claim");
        claim.store.complete(&claim.key, &claim.generation, result);
    }
}

impl Drop for IdempotencyOperation {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            // Async filesystem calls can leave partial effects or blocking I/O
            // still running. A missing owner is not evidence of rollback.
            claim.store.complete(
                &claim.key,
                &claim.generation,
                Err(Status::failed_precondition(
                    "operation interrupted; outcome unknown",
                )),
            );
            tracing::warn!(
                store = claim.store.scope,
                "idempotent operation interrupted with an unknown outcome"
            );
        }
    }
}

pub(super) fn require_idempotency_key(
    key: Option<String>,
    operation: &str,
) -> Result<IdempotencyKey, Status> {
    match key {
        Some(key) => IdempotencyKey::parse(key)
            .map_err(|error| Status::invalid_argument(format!("invalid idempotency_key: {error}"))),
        None => Err(Status::failed_precondition(format!(
            "{operation} requires idempotency_key for retry safety"
        ))),
    }
}

pub(super) fn optional_idempotency_key(
    key: Option<String>,
) -> Result<Option<IdempotencyKey>, Status> {
    key.map(IdempotencyKey::parse)
        .transpose()
        .map_err(|error| Status::invalid_argument(format!("invalid idempotency_key: {error}")))
}

#[cfg(test)]
mod tests;
