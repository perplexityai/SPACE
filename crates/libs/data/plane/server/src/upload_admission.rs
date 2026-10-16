//! Per-pod admission for ECW `POST /files` uploads.
//!
//! Each upload takes one concurrent-upload slot and reserves its declared body
//! size against an in-flight byte budget before the handler reads the body.
//! Both permits live until the handler returns, so the budget tracks bytes the
//! pod may still be holding, not bytes already read. Spool files are unlinked
//! in the background as the handler returns, so disk frees shortly after the
//! permit does. An upload that can't get both permits within the admission
//! wait is rejected with 429 and a jittered `Retry-After`.
//!
//! The rejection is deliberately 429, never 503: 503 invites the immediate
//! retries that turn an overloaded pod into a retry storm, while 429 with a
//! jittered `Retry-After` asks clients to back off.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use pplx_space_metrics::data_plane::{TooManyRequestsReason, UploadRejectReason};
use rand::RngExt as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::state::EcwProxyError;

pub const DEFAULT_MAX_CONCURRENT_UPLOADS: NonZeroUsize = NonZeroUsize::new(64).unwrap();
/// 1 GiB of declared request bodies. Octet-stream and multipart uploads spool
/// to local disk, so for them the budget bounds the spool. JSON writes are
/// capped at a 1 MiB body each and reserve at most that, though parsing a
/// byte-array `content` holds well over ten times its body in memory. Re-size the
/// budget against the spool volume's size limit once deployments set one.
pub const DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES: NonZeroU64 = NonZeroU64::new(1 << 30).unwrap();
pub const DEFAULT_UPLOAD_ADMISSION_WAIT: Duration = Duration::from_secs(2);

/// Byte-budget granularity. `acquire_many` takes a `u32`, so KiB permits let a
/// reservation, and the budget, which is clamped to the same `u32` range,
/// reach 4 TiB rather than 4 GiB. Uploads are capped far below either today.
const BYTES_PER_PERMIT: u64 = 1024;
const RETRY_AFTER_MIN: Duration = Duration::from_secs(1);
const RETRY_AFTER_MAX: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct UploadAdmissionConfig {
    pub max_concurrent_uploads: NonZeroUsize,
    pub max_in_flight_bytes: NonZeroU64,
    pub wait: Duration,
}

impl Default for UploadAdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_UPLOADS,
            max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES,
            wait: DEFAULT_UPLOAD_ADMISSION_WAIT,
        }
    }
}

pub(crate) struct UploadAdmission {
    uploads: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    budget_permits: u32,
    wait: Duration,
}

/// Held for the lifetime of an admitted upload. Dropping it returns the slot
/// and the reserved bytes.
#[derive(Debug)]
pub(crate) struct UploadPermit {
    _upload: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    reserved_bytes: u64,
}

impl UploadPermit {
    /// Budget bytes this permit holds: the reservation rounded up to whole
    /// permits and capped at the budget, which can differ from what was asked.
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }
}

impl UploadAdmission {
    pub(crate) fn new(config: &UploadAdmissionConfig) -> Self {
        let budget_permits = config
            .max_in_flight_bytes
            .get()
            .div_ceil(BYTES_PER_PERMIT)
            .min(u64::from(u32::MAX)) as u32;
        Self {
            uploads: Arc::new(Semaphore::new(config.max_concurrent_uploads.get())),
            bytes: Arc::new(Semaphore::new(budget_permits as usize)),
            budget_permits,
            wait: config.wait,
        }
    }

    /// Wait up to the admission wait for a concurrent-upload slot and then
    /// for `reserve_bytes` of the budget. Both waits share one deadline. A
    /// reservation never exceeds the whole budget, so a budget smaller than
    /// one upload still admits uploads one at a time instead of none.
    ///
    /// Tokio semaphores are FIFO, so a large reservation at the head of the
    /// byte queue holds every smaller upload behind it, even ones the free
    /// bytes would cover. That's the price of never starving large uploads,
    /// and it holds whichever permit is taken first. Taking the slot first
    /// means a timeout behind byte waiters that fill every slot reports
    /// `concurrency`, not `bytes`.
    pub(crate) async fn admit(&self, reserve_bytes: u64) -> Result<UploadPermit, EcwProxyError> {
        let deadline = Instant::now() + self.wait;
        let permits = reserve_bytes
            .div_ceil(BYTES_PER_PERMIT)
            .min(u64::from(self.budget_permits)) as u32;
        let upload = acquire_by(deadline, self.uploads.clone().acquire_owned())
            .await
            .ok_or_else(|| upload_rejected(UploadRejectReason::Concurrency))?;
        let bytes = acquire_by(deadline, self.bytes.clone().acquire_many_owned(permits))
            .await
            .ok_or_else(|| upload_rejected(UploadRejectReason::Bytes))?;
        Ok(UploadPermit {
            _upload: upload,
            _bytes: bytes,
            reserved_bytes: u64::from(permits) * BYTES_PER_PERMIT,
        })
    }

    #[cfg(test)]
    pub(crate) fn available_uploads(&self) -> usize {
        self.uploads.available_permits()
    }

    #[cfg(test)]
    pub(crate) fn available_bytes(&self) -> u64 {
        self.bytes.available_permits() as u64 * BYTES_PER_PERMIT
    }
}

/// One deadline spans both acquires in `admit`, which is why this isn't
/// `pplx_space_common_grpc::queued_slots`: a `QueuedSlots` request fixes its
/// own deadline, so two of them can't share one.
///
/// `None` when the deadline passes first. The semaphores are never closed,
/// so an acquire error can't happen; it's folded into `None` all the same.
async fn acquire_by(
    deadline: Instant,
    acquire: impl Future<Output = Result<OwnedSemaphorePermit, tokio::sync::AcquireError>>,
) -> Option<OwnedSemaphorePermit> {
    tokio::time::timeout_at(deadline, acquire).await.ok()?.ok()
}

fn upload_rejected(reason: UploadRejectReason) -> EcwProxyError {
    pplx_space_metrics::data_plane::upload_rejected(reason);
    let retry_after = Duration::from_secs(
        rand::rng().random_range(RETRY_AFTER_MIN.as_secs()..=RETRY_AFTER_MAX.as_secs()),
    );
    EcwProxyError::too_many_uploads(retry_after).with_too_many_requests_reason(match reason {
        UploadRejectReason::Concurrency => TooManyRequestsReason::UploadConcurrency,
        UploadRejectReason::Bytes => TooManyRequestsReason::UploadBytes,
    })
}

#[cfg(test)]
mod tests;
