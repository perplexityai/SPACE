//! Admission for CPU-bound codec passes (zstd compress or decompress).
//!
//! One pool of permits, one core each, bounds how many passes run at once.
//! A small part of it can be reserved for high-priority passes: they race the
//! reserve and the shared pool and take whichever frees first, while normal
//! passes only ever draw on the shared pool. Tokio semaphores are FIFO and
//! hand released permits to queued waiters first, so admission starts passes
//! in arrival order within each pool; it does not finish them in order.
//!
//! Every permit is an RAII value. Callers normally move it into the blocking
//! closure that runs the CPU work, so it releases only when that work exits,
//! also when the async caller is cancelled. The exceptions: a sparse-shadow
//! comparison holds its permit in the async task around its `spawn_blocking`,
//! a streaming producer's slot is lent while the producer is blocked on its
//! output, and a streamed pull's decode guard is held by the object-store
//! download path.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Which pools a codec pass may draw on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecPriority {
    /// Shared pool only.
    Normal,
    /// Reserve or shared pool, whichever frees first.
    High,
}

/// The one way admission can fail: the permit pool closed. Admission never
/// closes its semaphores, so this is unreachable while the owner lives.
#[derive(Debug, thiserror::Error)]
#[error("codec permit pool closed")]
pub struct CodecAdmissionClosed;

/// Shared and reserved codec permit pools. `Clone` shares the pools.
#[derive(Clone)]
pub struct CodecAdmission {
    shared: Arc<Semaphore>,
    reserve: Arc<Semaphore>,
    reserved: usize,
}

impl CodecAdmission {
    /// `total` permits, of which `reserve` are kept for high-priority passes.
    /// The reserve is capped at `total - 1` so the shared pool keeps at least
    /// one permit for normal work. Codec capacity is CPU-bound, so the
    /// reserve is carved from the total rather than added to it.
    pub fn new(total: usize, reserve: usize) -> Self {
        let reserve = reserve.min(total.saturating_sub(1));
        Self {
            shared: Arc::new(Semaphore::new(total - reserve)),
            reserve: Arc::new(Semaphore::new(reserve)),
            reserved: reserve,
        }
    }

    /// Whether any permits are reserved. Without a reserve,
    /// [`CodecPriority::High`] behaves exactly like
    /// [`CodecPriority::Normal`].
    pub fn has_reserve(&self) -> bool {
        self.reserved > 0
    }

    /// Wait for one permit. See [`CodecPriority`] for which pools it may
    /// come from. Cancel-safe: dropping the future returns any permit
    /// assigned to it.
    pub async fn acquire(
        &self,
        priority: CodecPriority,
    ) -> Result<CodecPermit, CodecAdmissionClosed> {
        let permit = match priority {
            CodecPriority::High => tokio::select! {
                biased;
                permit = Arc::clone(&self.reserve).acquire_owned() => permit,
                permit = Arc::clone(&self.shared).acquire_owned() => permit,
            },
            CodecPriority::Normal => Arc::clone(&self.shared).acquire_owned().await,
        };
        permit
            .map(|permit| CodecPermit { _permit: permit })
            .map_err(|_| CodecAdmissionClosed)
    }

    /// [`Self::acquire`] for a pass that may later lend its permit while it
    /// cannot run, such as a producer blocked on a full output pipe.
    pub async fn acquire_slot(
        &self,
        priority: CodecPriority,
    ) -> Result<CodecSlot, CodecAdmissionClosed> {
        let permit = self.acquire(priority).await?;
        Ok(CodecSlot {
            admission: self.clone(),
            priority,
            permit: Some(permit),
        })
    }

    /// One shared permit if it is free right now, without queueing. Opportunistic
    /// work uses this so it never competes with queued passes or the reserve.
    pub fn try_acquire_shared(&self) -> Option<CodecPermit> {
        Arc::clone(&self.shared)
            .try_acquire_owned()
            .ok()
            .map(|permit| CodecPermit { _permit: permit })
    }

    #[cfg(test)]
    pub fn shared_available(&self) -> usize {
        self.shared.available_permits()
    }

    #[cfg(test)]
    pub fn reserve_available(&self) -> usize {
        self.reserve.available_permits()
    }
}

/// One admitted codec pass. Dropping it releases the permit.
#[must_use = "dropping a codec permit releases its admission"]
pub struct CodecPermit {
    _permit: OwnedSemaphorePermit,
}

/// A codec permit its holder can lend while it cannot run and must
/// reacquire before running again.
#[must_use = "dropping a codec slot releases its admission"]
pub struct CodecSlot {
    admission: CodecAdmission,
    priority: CodecPriority,
    /// `None` while lent.
    permit: Option<CodecPermit>,
}

impl CodecSlot {
    /// Release the permit to queued work. Idempotent.
    pub fn lend(&mut self) {
        self.permit = None;
    }

    pub fn is_lent(&self) -> bool {
        self.permit.is_none()
    }

    /// Take a permit again if lent, queueing FIFO behind passes already
    /// waiting. Cancel-safe: a cancelled reacquire leaves the slot lent.
    pub async fn reacquire(&mut self) -> Result<(), CodecAdmissionClosed> {
        if self.permit.is_none() {
            self.permit = Some(self.admission.acquire(self.priority).await?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
