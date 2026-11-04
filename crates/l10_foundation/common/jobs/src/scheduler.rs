use pplx_space_common_core::types::JobId;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::oneshot;

use crate::JobPriority;

const WAITING: usize = 0;
const GRANTED: usize = 1;
const CONSUMED: usize = 2;
const CANCELLED: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SchedulerBand {
    pub min_priority: JobPriority,
    pub concurrency: usize,
}

pub(crate) struct PriorityScheduler {
    default_concurrency: usize,
    bands: Vec<SchedulerBand>,
    max_concurrency: usize,
    next_waiter_id: AtomicU64,
    state: Mutex<SchedulerState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchedulerSnapshot {
    pub bands: Vec<SchedulerBandSnapshot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SchedulerBandSnapshot {
    pub min_priority: Option<JobPriority>,
    pub running: usize,
    pub waiting: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SchedulerBandBudget {
    pub min_priority: Option<JobPriority>,
    pub permits: usize,
}

struct SchedulerState {
    running: Vec<usize>,
    waiters: Vec<Waiter>,
}

struct Waiter {
    id: u64,
    job_id: JobId,
    priority: JobPriority,
    band: usize,
    band_state: Arc<AtomicUsize>,
    state: Arc<AtomicUsize>,
    granted: oneshot::Sender<usize>,
}

pub(crate) struct PriorityPermit {
    scheduler: Arc<PriorityScheduler>,
    band: usize,
}

impl PriorityScheduler {
    pub(crate) fn new(default_concurrency: usize, bands: Vec<SchedulerBand>) -> Arc<Self> {
        for pair in bands.windows(2) {
            assert!(
                matches!(pair, [lower, upper] if lower.min_priority < upper.min_priority),
                "priority bands must have unique ascending minimum priorities"
            );
        }
        assert!(
            bands.iter().all(|band| band.concurrency > 0),
            "priority band concurrency must be greater than zero"
        );
        let max_concurrency = bands.iter().fold(default_concurrency, |total, band| {
            total
                .checked_add(band.concurrency)
                .expect("priority band concurrency overflow")
        });
        let band_count = bands.len() + 1;
        Arc::new(Self {
            default_concurrency,
            bands,
            max_concurrency,
            next_waiter_id: AtomicU64::new(0),
            state: Mutex::new(SchedulerState {
                running: vec![0; band_count],
                waiters: Vec::new(),
            }),
        })
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        job_id: JobId,
        priority: JobPriority,
    ) -> impl Future<Output = PriorityPermit> + Send + 'static {
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let band = self.band(priority);
        let band_state = Arc::new(AtomicUsize::new(band));
        let waiter_state = Arc::new(AtomicUsize::new(WAITING));
        let (granted, receiver) = oneshot::channel();
        {
            let mut state = self
                .state
                .lock()
                .expect("priority scheduler mutex poisoned");
            state.waiters.push(Waiter {
                id: waiter_id,
                job_id,
                priority,
                band,
                band_state: Arc::clone(&band_state),
                state: Arc::clone(&waiter_state),
                granted,
            });
            self.dispatch(&mut state);
        }

        let cancellation = WaitCancellation {
            scheduler: Arc::downgrade(self),
            waiter_id,
            waiter_state,
            band_state,
            armed: true,
        };
        let scheduler = Arc::clone(self);
        async move {
            let mut cancellation = cancellation;
            let band = receiver
                .await
                .expect("priority scheduler dropped a pending waiter");
            cancellation.waiter_state.store(CONSUMED, Ordering::Release);
            cancellation.armed = false;

            PriorityPermit { scheduler, band }
        }
    }

    pub(crate) fn raise_waiting_priority(&self, job_id: &JobId, priority: JobPriority) {
        let mut state = self
            .state
            .lock()
            .expect("priority scheduler mutex poisoned");
        let Some(waiter) = state
            .waiters
            .iter_mut()
            .find(|waiter| waiter.job_id == *job_id && waiter.priority < priority)
        else {
            return;
        };
        waiter.priority = priority;
        waiter.band = self.band(priority);
        waiter.band_state.store(waiter.band, Ordering::Release);
        self.dispatch(&mut state);
    }

    pub(crate) fn spawn_budgets(&self, priorities: &[JobPriority]) -> Vec<SchedulerBandBudget> {
        let state = self
            .state
            .lock()
            .expect("priority scheduler mutex poisoned");
        let mut due = vec![0usize; state.running.len()];
        for priority in priorities {
            *due.get_mut(self.band(*priority))
                .expect("due priority band must exist") += 1;
        }
        let mut occupied = state.running.clone();
        for waiter in &state.waiters {
            *occupied
                .get_mut(waiter.band)
                .expect("waiter priority band must exist") += 1;
        }
        let mut occupied_prefix = 0usize;
        let mut allocated_prefix = 0usize;
        occupied
            .iter()
            .enumerate()
            .map(|(band, occupied)| {
                occupied_prefix += *occupied;
                let permits = due
                    .get(band)
                    .copied()
                    .expect("due priority band must exist")
                    .min(
                        self.prefix_capacity(band)
                            .saturating_sub(occupied_prefix + allocated_prefix),
                    );
                allocated_prefix += permits;
                SchedulerBandBudget {
                    min_priority: band
                        .checked_sub(1)
                        .and_then(|index| self.bands.get(index))
                        .map(|band| band.min_priority),
                    permits,
                }
            })
            .collect()
    }

    pub(crate) fn priority_band(&self, priority: JobPriority) -> Option<JobPriority> {
        let band = self.band(priority);
        band.checked_sub(1)
            .and_then(|index| self.bands.get(index))
            .map(|band| band.min_priority)
    }

    pub(crate) fn snapshot(&self) -> SchedulerSnapshot {
        let state = self
            .state
            .lock()
            .expect("priority scheduler mutex poisoned");
        let mut waiting = vec![0; state.running.len()];
        for waiter in &state.waiters {
            *waiting
                .get_mut(waiter.band)
                .expect("waiter priority band must exist") += 1;
        }
        SchedulerSnapshot {
            bands: state
                .running
                .iter()
                .enumerate()
                .map(|(band, running)| SchedulerBandSnapshot {
                    min_priority: band
                        .checked_sub(1)
                        .and_then(|index| self.bands.get(index))
                        .map(|band| band.min_priority),
                    running: *running,
                    waiting: *waiting
                        .get(band)
                        .expect("scheduler snapshot priority band must exist"),
                })
                .collect(),
        }
    }

    fn band(&self, priority: JobPriority) -> usize {
        self.bands
            .partition_point(|band| priority >= band.min_priority)
    }

    fn band_capacity(&self, band: usize) -> usize {
        band.checked_sub(1)
            .map_or(self.default_concurrency, |index| {
                self.bands
                    .get(index)
                    .expect("priority band must exist")
                    .concurrency
            })
    }

    fn prefix_capacity(&self, upper_band: usize) -> usize {
        self.default_concurrency
            + self
                .bands
                .get(..upper_band)
                .expect("priority band prefix must exist")
                .iter()
                .map(|band| band.concurrency)
                .sum::<usize>()
    }

    fn has_capacity(&self, state: &SchedulerState, band: usize) -> bool {
        (band..state.running.len()).all(|upper_band| {
            state
                .running
                .get(..=upper_band)
                .expect("running priority band prefix must exist")
                .iter()
                .sum::<usize>()
                < self.prefix_capacity(upper_band)
        })
    }

    fn dispatch(&self, state: &mut SchedulerState) {
        while state.running.iter().sum::<usize>() < self.max_concurrency {
            let Some(index) = self.next_waiter(state) else {
                return;
            };
            let waiter = state.waiters.swap_remove(index);
            if waiter
                .state
                .compare_exchange(WAITING, GRANTED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            *state
                .running
                .get_mut(waiter.band)
                .expect("waiter priority band must exist") += 1;
            if waiter.granted.send(waiter.band).is_err() {
                *state
                    .running
                    .get_mut(waiter.band)
                    .expect("waiter priority band must exist") -= 1;
                waiter.state.store(CANCELLED, Ordering::Release);
            }
        }
    }

    fn next_waiter(&self, state: &SchedulerState) -> Option<usize> {
        let has_runnable_deficit = state.waiters.iter().any(|waiter| {
            self.has_capacity(state, waiter.band)
                && state
                    .running
                    .get(waiter.band)
                    .is_some_and(|running| *running < self.band_capacity(waiter.band))
        });

        state
            .waiters
            .iter()
            .enumerate()
            .filter(|(_, waiter)| self.has_capacity(state, waiter.band))
            .filter(|(_, waiter)| {
                !has_runnable_deficit
                    || state
                        .running
                        .get(waiter.band)
                        .is_some_and(|running| *running < self.band_capacity(waiter.band))
            })
            .max_by(|(_, left), (_, right)| {
                left.priority
                    .cmp(&right.priority)
                    .then_with(|| right.id.cmp(&left.id))
            })
            .map(|(index, _)| index)
    }

    fn cancel(&self, waiter_id: u64, waiter_state: &AtomicUsize, band: usize) {
        let mut state = self
            .state
            .lock()
            .expect("priority scheduler mutex poisoned");
        match waiter_state.swap(CANCELLED, Ordering::AcqRel) {
            WAITING => state.waiters.retain(|waiter| waiter.id != waiter_id),
            GRANTED => {
                *state
                    .running
                    .get_mut(band)
                    .expect("granted priority band must exist") -= 1;
            }
            CONSUMED | CANCELLED => return,
            _ => unreachable!("invalid priority waiter state"),
        }
        self.dispatch(&mut state);
    }

    fn release(&self, band: usize) {
        let mut state = self
            .state
            .lock()
            .expect("priority scheduler mutex poisoned");
        *state
            .running
            .get_mut(band)
            .expect("released priority band must exist") -= 1;
        self.dispatch(&mut state);
    }
}

impl Drop for PriorityPermit {
    fn drop(&mut self) {
        self.scheduler.release(self.band);
    }
}

struct WaitCancellation {
    scheduler: Weak<PriorityScheduler>,
    waiter_id: u64,
    waiter_state: Arc<AtomicUsize>,
    band_state: Arc<AtomicUsize>,
    armed: bool,
}

impl Drop for WaitCancellation {
    fn drop(&mut self) {
        if self.armed
            && let Some(scheduler) = self.scheduler.upgrade()
        {
            let band = self.band_state.load(Ordering::Acquire);
            scheduler.cancel(self.waiter_id, &self.waiter_state, band);
        }
    }
}

#[cfg(test)]
mod tests;
