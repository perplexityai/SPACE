//! Concurrency slots with a bounded wait queue.
//!
//! A request takes a free slot at once. When none is free it may wait for one,
//! alongside at most a fixed number of other waiters and for at most a fixed
//! time, instead of being rejected straight away: a short burst is absorbed,
//! while sustained overload still fails fast.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct QueuedSlots {
    slots: Arc<Semaphore>,
    queue: Option<(Arc<Semaphore>, Duration)>,
}

/// The immediate answer to a slot request.
#[derive(Debug)]
pub enum SlotRequest {
    Admitted(OwnedSemaphorePermit),
    /// No slot was free; this request holds a place in the queue.
    Queued(QueuedSlotWait),
    Rejected,
}

/// A queued request; its place is released when it is admitted, times out or
/// is dropped.
#[derive(Debug)]
pub struct QueuedSlotWait {
    slots: Arc<Semaphore>,
    weight: u32,
    deadline: tokio::time::Instant,
    _place: OwnedSemaphorePermit,
}

impl QueuedSlots {
    /// Slots without a queue: a request is admitted or rejected at once.
    pub fn new(slots: Arc<Semaphore>) -> Self {
        Self { slots, queue: None }
    }

    /// Let up to `waiters` requests wait up to `max_wait` for a slot. Every
    /// clone shares the queue, so clones front one budget.
    pub fn with_queue(mut self, waiters: usize, max_wait: Duration) -> Self {
        self.queue = Some((Arc::new(Semaphore::new(waiters)), max_wait));
        self
    }

    pub fn request(&self, weight: u32) -> SlotRequest {
        if let Ok(permit) = self.slots.clone().try_acquire_many_owned(weight) {
            return SlotRequest::Admitted(permit);
        }
        let Some((waiters, max_wait)) = &self.queue else {
            return SlotRequest::Rejected;
        };
        match waiters.clone().try_acquire_owned() {
            Ok(place) => SlotRequest::Queued(QueuedSlotWait {
                slots: self.slots.clone(),
                weight,
                deadline: tokio::time::Instant::now() + *max_wait,
                _place: place,
            }),
            Err(_) => SlotRequest::Rejected,
        }
    }

    /// Take `weight` slots, waiting in the queue if there is room; `None` when
    /// rejected or when the wait ran out.
    pub async fn acquire(&self, weight: u32) -> Option<OwnedSemaphorePermit> {
        match self.request(weight) {
            SlotRequest::Admitted(permit) => Some(permit),
            SlotRequest::Queued(wait) => wait.admitted().await,
            SlotRequest::Rejected => None,
        }
    }
}

impl QueuedSlotWait {
    /// Wait for the slots until the deadline; `None` if it passes first.
    pub async fn admitted(self) -> Option<OwnedSemaphorePermit> {
        tokio::time::timeout_at(self.deadline, self.slots.acquire_many_owned(self.weight))
            .await
            .ok()
            .and_then(Result::ok)
            .filter(|_| tokio::time::Instant::now() < self.deadline)
    }
}

#[cfg(test)]
mod tests;
