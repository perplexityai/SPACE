//! Admission for ECW `POST /files` uploads, per pod, per org and per sandbox.
//!
//! Each upload takes one concurrent-upload slot and reserves its declared body
//! size against an in-flight byte budget before the handler reads the body. It
//! does so three times: against its sandbox's share, its org's share, and the
//! pod's whole budget. All permits live until the handler returns, so each
//! budget tracks bytes the pod may still be holding, not bytes already read.
//! Spool files are unlinked in the background as the handler returns, so disk
//! frees shortly after the permit does. An upload that can't get every permit
//! within the admission wait is rejected with 429 and a jittered `Retry-After`.
//!
//! HTTP/2 uploads never wait, for admission or for the byte-rate bucket. Flow
//! control on an HTTP/2 connection is shared by its streams, so a stream the
//! pod stops reading holds back the others. An HTTP/2 upload takes its
//! permits at once or is rejected, is rejected while its org owes the bucket,
//! and once admitted is charged to the bucket without being slowed. Downloads
//! are admitted and charged the same way, since their bodies come from
//! pooled HTTP/2 backend connections.
//!
//! The org and sandbox shares exist because the pod budget alone lets one
//! tenant hold every slot and byte on every replica at once. The share is keyed
//! by org, not by client id: one client id can front many orgs' traffic, so a
//! client-id key would isolate nothing.
//!
//! Admitted bodies are governed by [`body::GovernedBody`]: an optional
//! per-org byte-rate bucket slows long HTTP/1.x uploads through TCP
//! backpressure instead of failing them, and an optional minimum body rate ends an upload
//! that stalls while it holds permits.
//!
//! The rejection is deliberately 429, never 503: 503 invites the immediate
//! retries that turn an overloaded pod into a retry storm, while 429 with a
//! jittered `Retry-After` asks clients to back off.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Version;
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_metrics::data_plane::{
    TooManyRequestsReason, UploadAdmissionScope, UploadRejectReason,
};
use rand::RngExt as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::state::EcwProxyError;

mod body;
mod byte_rate;
mod flags;
mod shares;

pub(crate) use body::BodyEnd;
use body::{BodyEnded, Limits};
use body::{GovernedBody, Metering};
use byte_rate::{ByteRateBucket, OrgByteRates};
pub use flags::{LimitOrOff, UploadAdmissionArgs};
use shares::{ShareMap, SharePermit};

pub const DEFAULT_MAX_CONCURRENT_UPLOADS: NonZeroUsize = NonZeroUsize::new(64).unwrap();
/// 1 GiB of declared request bodies. Octet-stream and multipart uploads spool
/// to local disk, so for them the budget bounds the spool. JSON writes are
/// capped at a 1 MiB body each and reserve at most that, though parsing a
/// byte-array `content` holds well over ten times its body in memory. Re-size the
/// budget against the spool volume's size limit once deployments set one.
pub const DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES: NonZeroU64 = NonZeroU64::new(1 << 30).unwrap();
pub const DEFAULT_UPLOAD_ADMISSION_WAIT: Duration = Duration::from_secs(2);
/// A quarter of the default pod slots, so one org can't take the pod.
pub const DEFAULT_MAX_CONCURRENT_UPLOADS_PER_ORG: NonZeroUsize = NonZeroUsize::new(16).unwrap();
/// A quarter of the default pod byte budget. That's one full-size upload: an
/// undeclared-length octet-stream or multipart upload reserves the whole
/// share, so while one is in flight every other upload from the org on the
/// replica that reserves any bytes waits, and gets 429 if the wait runs out
/// first; an HTTP/2 one gets the 429 at once. Uploads that declare their length reserve only that, so several of
/// those fit at once.
pub const DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG: NonZeroU64 =
    NonZeroU64::new(256 << 20).unwrap();
pub const DEFAULT_MAX_CONCURRENT_UPLOADS_PER_SANDBOX: NonZeroUsize = NonZeroUsize::new(4).unwrap();
/// One full-size upload, so an upload alone in its sandbox always fits its
/// sandbox's share. As with the org share, one undeclared-length upload
/// fills it.
pub const DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX: NonZeroU64 =
    NonZeroU64::new(256 << 20).unwrap();
/// A starting value for testing, not a measured one: 100 MB/s sustained with a
/// 1 GB burst per org on each replica.
pub const DEFAULT_ORG_TRANSFER_RATE: ByteRateConfig = ByteRateConfig {
    bytes_per_second: NonZeroU64::new(100_000_000).unwrap(),
    burst_bytes: NonZeroU64::new(1_000_000_000).unwrap(),
};
/// A body that delivers less than 64 KiB/s × 15 s, 960 KiB, in a window of
/// 15 s spent waiting on the client is treated as stalled. Windows are
/// consecutive, not sliding, and bytes that arrive without a wait count toward
/// the window they arrive in. For an upload smaller than 960 KiB that makes it
/// a deadline more than a rate: it must finish before the pod has waited 15 s
/// on its client in total.
pub const DEFAULT_UPLOAD_MIN_BODY_RATE: MinBodyRateConfig = MinBodyRateConfig {
    bytes_per_second: NonZeroU64::new(64 << 10).unwrap(),
    window: Duration::from_secs(15),
};
/// Generous, since it's a time rather than a rate: a full-size 256 MiB upload
/// has it at about 450 KB/s.
pub const DEFAULT_HTTP2_UPLOAD_MAX_DURATION: Duration = Duration::from_secs(600);
/// The longest an admitted HTTP/1.x upload's frame may sleep off its org's
/// debt. Debt the org's charged transfers add after admission can be far
/// deeper; an upload facing more ends with 429 and frees its permits rather
/// than hold them through the sleep.
const MAX_UPLOAD_THROTTLE_SLEEP: Duration = Duration::from_secs(15);

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
    pub per_org: UploadShareConfig,
    pub per_sandbox: UploadShareConfig,
    /// Meters each org's `POST /files` request bodies and `GET /files`
    /// response bodies together: HTTP/1.x uploads are slowed, HTTP/2 uploads
    /// and downloads charged and refused while the org owes. `None` leaves
    /// transfers unmetered.
    pub org_transfer_rate: Option<ByteRateConfig>,
    /// Ends an admitted upload whose body arrives slower than this. `None`
    /// lets a stalled upload hold its permits until the client gives up.
    pub min_body_rate: Option<MinBodyRateConfig>,
    /// Ends an admitted HTTP/2 upload, which isn't held to the minimum body
    /// rate, that hasn't finished this long after admission. `None` lets it
    /// hold its permits until the client gives up.
    pub http2_upload_max_duration: Option<Duration>,
}

/// One tenant's share of the pod's upload slots and byte budget.
#[derive(Debug, Clone, Copy)]
pub struct UploadShareConfig {
    pub max_concurrent_uploads: NonZeroUsize,
    pub max_in_flight_bytes: NonZeroU64,
}

#[derive(Debug, Clone, Copy)]
pub struct ByteRateConfig {
    pub bytes_per_second: NonZeroU64,
    pub burst_bytes: NonZeroU64,
}

/// An HTTP/1.x upload must deliver `bytes_per_second × window` bytes in every
/// `window` of time spent waiting on the client. Time the pod spends
/// elsewhere, in the byte-rate bucket or writing to the spool or the guest,
/// doesn't count. HTTP/2 uploads aren't held to it; see
/// [`UploadAdmission::admit_upload`].
#[derive(Debug, Clone, Copy)]
pub struct MinBodyRateConfig {
    pub bytes_per_second: NonZeroU64,
    pub window: Duration,
}

impl Default for UploadAdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_UPLOADS,
            max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES,
            wait: DEFAULT_UPLOAD_ADMISSION_WAIT,
            per_org: UploadShareConfig {
                max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_UPLOADS_PER_ORG,
                max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG,
            },
            per_sandbox: UploadShareConfig {
                max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_UPLOADS_PER_SANDBOX,
                max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX,
            },
            org_transfer_rate: Some(DEFAULT_ORG_TRANSFER_RATE),
            min_body_rate: Some(DEFAULT_UPLOAD_MIN_BODY_RATE),
            http2_upload_max_duration: Some(DEFAULT_HTTP2_UPLOAD_MAX_DURATION),
        }
    }
}

/// Who an upload is charged to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UploadTenant {
    pub(crate) org_id: OrgId,
    pub(crate) sandbox_id: SandboxId,
}

pub(crate) struct UploadAdmission {
    pod: Budget,
    orgs: ShareMap<OrgId>,
    sandboxes: ShareMap<SandboxId>,
    org_transfer_rates: Option<Arc<OrgByteRates>>,
    min_body_rate: Option<MinBodyRateConfig>,
    http2_upload_max_duration: Option<Duration>,
    wait: Duration,
}

/// A download [`UploadAdmission::admit_download`] let through.
pub(crate) struct AdmittedDownload(Option<Arc<ByteRateBucket>>);

impl AdmittedDownload {
    /// `body`, charged to the org's bucket as it's read.
    pub(crate) fn govern(self, body: Body) -> Body {
        match self.0 {
            Some(rate) => Body::new(GovernedBody::new(
                body,
                Some(Metering::Charge(rate)),
                Limits::default(),
                BodyEnd::default(),
            )),
            None => body,
        }
    }
}

/// An admitted upload: hold `permit` until the handler returns and read
/// `body` in place of the request's. Map a body-phase failure through
/// [`body_ended_error`] when `ended` reports that the data plane ended it.
pub(crate) struct AdmittedUpload {
    pub(crate) permit: UploadPermit,
    pub(crate) body: Body,
    pub(crate) ended: BodyEnd,
}

/// The response for an upload body the data plane ended, if it did.
pub(crate) fn body_ended_error(ended: &BodyEnd) -> Option<EcwProxyError> {
    Some(match ended.ended()? {
        BodyEnded::Stalled => EcwProxyError::upload_body_stalled(),
        BodyEnded::TimedOut => EcwProxyError::upload_body_timed_out(),
        // Ended after admission, so not counted as an admission refusal.
        BodyEnded::OverRate { excess } => upload_rate_error(excess),
    })
}

/// Held for the lifetime of an admitted upload. Dropping it returns every
/// slot and reservation. Fields drop in order, pod first.
#[derive(Debug)]
pub(crate) struct UploadPermit {
    pod: BudgetPermit,
    _org: SharePermit<OrgId>,
    _sandbox: SharePermit<SandboxId>,
}

impl UploadPermit {
    /// Bytes of the pod's budget this permit holds: the reservation rounded up
    /// to whole KiB permits and capped at the pod budget, itself rounded up to
    /// whole KiB. It can differ from what was asked, and from what the org and
    /// sandbox shares hold, which each cap the reservation at their own size.
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.pod.reserved_bytes
    }
}

impl UploadAdmission {
    pub(crate) fn new(config: &UploadAdmissionConfig) -> Self {
        Self {
            pod: Budget::new(UploadShareConfig {
                max_concurrent_uploads: config.max_concurrent_uploads,
                max_in_flight_bytes: config.max_in_flight_bytes,
            }),
            orgs: ShareMap::new(config.per_org),
            sandboxes: ShareMap::new(config.per_sandbox),
            org_transfer_rates: config
                .org_transfer_rate
                .map(|rate| Arc::new(OrgByteRates::new(rate))),
            min_body_rate: config.min_body_rate,
            http2_upload_max_duration: config.http2_upload_max_duration,
            wait: config.wait,
        }
    }

    /// Admits an upload and governs its request body: the permit, the body to
    /// read in place of `body`, and the signal that reports whether, and why,
    /// the data plane ended that body early. Admission and governance come as
    /// one, so no upload can hold permits without the governance that applies
    /// to it.
    ///
    /// An HTTP/1.x upload has its connection to itself, so it may wait for
    /// admission and in the byte-rate bucket, and is held to the minimum body
    /// rate. An HTTP/2 upload does neither wait (see the module docs), and
    /// isn't held to the minimum rate either: a wait for its body can be on
    /// the connection's other streams rather than its client. Its time limit
    /// bounds how long its body may take instead.
    pub(crate) async fn admit_upload(
        &self,
        tenant: UploadTenant,
        reserve_bytes: u64,
        body: Body,
        version: Version,
    ) -> Result<AdmittedUpload, EcwProxyError> {
        let shares_connection = version >= Version::HTTP_2;
        let wait = if shares_connection {
            Duration::ZERO
        } else {
            self.wait
        };
        // An admitted upload ends rather than sleep past the longest throttle
        // sleep, so it's never admitted into more debt than that either.
        let admitted_debt = wait.min(MAX_UPLOAD_THROTTLE_SLEEP);
        let rate = self.org_rate(tenant.org_id);
        // Checked before any permit is taken. An upload is admitted only
        // into as much of its org's debt as the admission wait covers, so
        // none starts by holding its permits through a long sleep; an HTTP/2
        // upload, which can't sleep at all, only while the org owes nothing.
        // Debt the org's other transfers add later can still lengthen an
        // admitted upload's sleeps, within the org's share.
        let debt = rate
            .as_ref()
            .map_or(Duration::ZERO, |rate| rate.debt_clears_in(Instant::now()));
        if debt > admitted_debt {
            return Err(upload_over_rate(debt - admitted_debt));
        }
        let permit = self.admit_permits(tenant, reserve_bytes, wait).await?;
        let ended = BodyEnd::default();
        let floor = self.min_body_rate.filter(|_| !shares_connection);
        let max_duration = self.http2_upload_max_duration.filter(|_| shares_connection);
        let metering = rate.map(|rate| {
            if shares_connection {
                Metering::Charge(rate)
            } else {
                Metering::Throttle(rate)
            }
        });
        let body = if metering.is_none() && floor.is_none() && max_duration.is_none() {
            body
        } else {
            Body::new(GovernedBody::new(
                body,
                metering,
                Limits {
                    floor,
                    max_duration,
                    max_throttle: MAX_UPLOAD_THROTTLE_SLEEP,
                    admitted_debt,
                },
                ended.clone(),
            ))
        };
        Ok(AdmittedUpload {
            permit,
            body,
            ended,
        })
    }

    /// Wait up to `wait` for the sandbox's, then the org's, then the pod's
    /// slot and `reserve_bytes` of each budget. Every wait shares one
    /// deadline; a zero `wait` takes only what's free at once. The narrowest
    /// share goes first, so an upload queued behind its own sandbox or org
    /// holds none of the pod's slots or bytes meanwhile. A reservation never exceeds a whole budget, so a budget
    /// smaller than one upload still admits uploads one at a time instead of
    /// none; each budget caps the reservation it holds independently.
    ///
    /// Tokio semaphores are FIFO, so a large reservation at the head of a
    /// byte queue holds every smaller upload behind it, even ones the free
    /// bytes would cover. That's the price of never starving large uploads,
    /// and it holds whichever permit is taken first. A queued waiter is
    /// handed permits as they free, so while one waits a zero `wait` finds
    /// none free and gets 429 at once. On the pod's budget, the waiter and
    /// the refused upload can belong to different orgs. Taking the slot first
    /// means a timeout behind byte waiters that fill every slot reports
    /// `concurrency`, not `bytes`.
    async fn admit_permits(
        &self,
        tenant: UploadTenant,
        reserve_bytes: u64,
        wait: Duration,
    ) -> Result<UploadPermit, EcwProxyError> {
        // `None` for no wait at all.
        let deadline = (!wait.is_zero()).then(|| Instant::now() + wait);
        let sandbox = self
            .sandboxes
            .admit(tenant.sandbox_id, deadline, reserve_bytes)
            .await
            .map_err(|reason| upload_rejected(UploadAdmissionScope::Sandbox, reason))?;
        let org = self
            .orgs
            .admit(tenant.org_id, deadline, reserve_bytes)
            .await
            .map_err(|reason| upload_rejected(UploadAdmissionScope::Org, reason))?;
        let pod = self
            .pod
            .admit(deadline, reserve_bytes)
            .await
            .map_err(|reason| upload_rejected(UploadAdmissionScope::Pod, reason))?;
        Ok(UploadPermit {
            pod,
            _org: org,
            _sandbox: sandbox,
        })
    }

    /// Admits a `GET /files` download against its org's byte-rate bucket,
    /// before the read is sent to the guest: 429 while the org owes the
    /// bucket. An admitted download is charged as it streams, never slowed.
    /// Its body is read from a pooled backend connection whose streams share
    /// flow control with other sandboxes' calls, so it mustn't stop being
    /// read. The rate binds a download only as it starts: one already
    /// running moves at whatever pace its client reads.
    pub(crate) fn admit_download(&self, org_id: OrgId) -> Result<AdmittedDownload, EcwProxyError> {
        let rate = self.org_rate(org_id);
        let debt = rate
            .as_ref()
            .map_or(Duration::ZERO, |rate| rate.debt_clears_in(Instant::now()));
        if !debt.is_zero() {
            return Err(transfer_rate_exceeded(debt)
                .with_too_many_requests_reason(TooManyRequestsReason::DownloadTransferRatePerOrg));
        }
        Ok(AdmittedDownload(rate))
    }

    fn org_rate(&self, org_id: OrgId) -> Option<Arc<ByteRateBucket>> {
        self.org_transfer_rates
            .as_ref()
            .map(|rates| rates.for_org(org_id))
    }

    /// Permits alone, without a governed body, for tests that hold a budget
    /// or share. Production uploads go through [`Self::admit_upload`].
    #[cfg(test)]
    pub(crate) async fn admit(
        &self,
        tenant: UploadTenant,
        reserve_bytes: u64,
    ) -> Result<UploadPermit, EcwProxyError> {
        self.admit_permits(tenant, reserve_bytes, self.wait).await
    }

    #[cfg(test)]
    pub(crate) fn available_uploads(&self) -> usize {
        self.pod.uploads.available_permits()
    }

    #[cfg(test)]
    pub(crate) fn available_bytes(&self) -> u64 {
        self.pod.bytes.available_permits() as u64 * BYTES_PER_PERMIT
    }

    #[cfg(test)]
    pub(crate) fn tracked_orgs(&self) -> usize {
        self.orgs.len()
    }

    #[cfg(test)]
    pub(crate) fn org_bucket(&self, org_id: OrgId) -> Option<Arc<ByteRateBucket>> {
        self.org_transfer_rates.as_ref()?.bucket(org_id)
    }

    #[cfg(test)]
    pub(crate) fn tracked_sandboxes(&self) -> usize {
        self.sandboxes.len()
    }
}

/// A slot semaphore and a KiB-granular byte semaphore: the pod's budget, or
/// one tenant's share of it.
#[derive(Debug)]
struct Budget {
    uploads: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    budget_permits: u32,
}

#[derive(Debug)]
struct BudgetPermit {
    _upload: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    reserved_bytes: u64,
}

impl Budget {
    fn new(config: UploadShareConfig) -> Self {
        let budget_permits = config
            .max_in_flight_bytes
            .get()
            .div_ceil(BYTES_PER_PERMIT)
            .min(u64::from(u32::MAX)) as u32;
        Self {
            // Clamped, since `Semaphore::new` panics above its maximum and
            // shares are built on the request path.
            uploads: Arc::new(Semaphore::new(
                config
                    .max_concurrent_uploads
                    .get()
                    .min(Semaphore::MAX_PERMITS),
            )),
            bytes: Arc::new(Semaphore::new(budget_permits as usize)),
            budget_permits,
        }
    }

    /// `deadline` is `None` to take only what's free at once.
    async fn admit(
        &self,
        deadline: Option<Instant>,
        reserve_bytes: u64,
    ) -> Result<BudgetPermit, UploadRejectReason> {
        let permits = reserve_bytes
            .div_ceil(BYTES_PER_PERMIT)
            .min(u64::from(self.budget_permits)) as u32;
        let upload = acquire_by(deadline, &self.uploads, 1)
            .await
            .ok_or(UploadRejectReason::Concurrency)?;
        let bytes = acquire_by(deadline, &self.bytes, permits)
            .await
            .ok_or(UploadRejectReason::Bytes)?;
        Ok(BudgetPermit {
            _upload: upload,
            _bytes: bytes,
            reserved_bytes: u64::from(permits) * BYTES_PER_PERMIT,
        })
    }
}

/// One deadline spans every acquire in `admit`, which is why this isn't
/// `pplx_space_common_grpc::queued_slots`: a `QueuedSlots` request fixes its
/// own deadline, so several of them can't share one.
///
/// `None` when the deadline passes first, or, without a deadline, when the
/// permits aren't free now. That case doesn't poll an acquire against an
/// elapsed deadline: tokio's cooperative budget could make free permits look
/// taken. The semaphores are never closed, so an acquire error can't happen;
/// it's folded into `None` all the same.
async fn acquire_by(
    deadline: Option<Instant>,
    semaphore: &Arc<Semaphore>,
    permits: u32,
) -> Option<OwnedSemaphorePermit> {
    let Some(deadline) = deadline else {
        return Arc::clone(semaphore).try_acquire_many_owned(permits).ok();
    };
    tokio::time::timeout_at(deadline, Arc::clone(semaphore).acquire_many_owned(permits))
        .await
        .ok()?
        .ok()
}

fn jittered_retry_after() -> Duration {
    Duration::from_secs(
        rand::rng().random_range(RETRY_AFTER_MIN.as_secs()..=RETRY_AFTER_MAX.as_secs()),
    )
}

/// A 429 for a transfer refused over `debt`, the part of its org's debt it
/// couldn't wait off. Its `Retry-After` is the time that debt takes to clear,
/// in whole seconds, plus the usual jitter on top, so clients refused at
/// different moments don't all come back the moment it clears.
fn transfer_rate_exceeded(debt: Duration) -> EcwProxyError {
    let clears_in = Duration::from_secs(debt.as_secs() + u64::from(debt.subsec_nanos() > 0));
    EcwProxyError::org_transfer_rate_exceeded(clears_in + jittered_retry_after())
}

/// An upload refused at admission over its org's transfer rate.
fn upload_over_rate(debt: Duration) -> EcwProxyError {
    pplx_space_metrics::data_plane::upload_rejected(
        UploadAdmissionScope::Org,
        UploadRejectReason::TransferRate,
    );
    upload_rate_error(debt)
}

/// The 429 for an upload over its org's transfer rate, refused at admission
/// or ended mid-body; see [`transfer_rate_exceeded`].
fn upload_rate_error(debt: Duration) -> EcwProxyError {
    transfer_rate_exceeded(debt)
        .with_too_many_requests_reason(TooManyRequestsReason::UploadTransferRatePerOrg)
}

/// An upload refused for a budget's slots or bytes. Rate refusals go
/// through [`upload_over_rate`], which knows the debt.
fn upload_rejected(scope: UploadAdmissionScope, reason: UploadRejectReason) -> EcwProxyError {
    pplx_space_metrics::data_plane::upload_rejected(scope, reason);
    let error = EcwProxyError::too_many_uploads(jittered_retry_after());
    error.with_too_many_requests_reason(match (scope, reason) {
        (UploadAdmissionScope::Pod, UploadRejectReason::Concurrency) => {
            TooManyRequestsReason::UploadConcurrency
        }
        (UploadAdmissionScope::Pod, UploadRejectReason::Bytes) => {
            TooManyRequestsReason::UploadBytes
        }
        (UploadAdmissionScope::Org, UploadRejectReason::Concurrency) => {
            TooManyRequestsReason::UploadConcurrencyPerOrg
        }
        (UploadAdmissionScope::Org, UploadRejectReason::Bytes) => {
            TooManyRequestsReason::UploadBytesPerOrg
        }
        (UploadAdmissionScope::Sandbox, UploadRejectReason::Concurrency) => {
            TooManyRequestsReason::UploadConcurrencyPerSandbox
        }
        (UploadAdmissionScope::Sandbox, UploadRejectReason::Bytes) => {
            TooManyRequestsReason::UploadBytesPerSandbox
        }
        // Only the org's bucket rejects for its rate.
        // Not reached: rate refusals go through `upload_over_rate`.
        (_, UploadRejectReason::TransferRate) => TooManyRequestsReason::UploadTransferRatePerOrg,
    })
}

#[cfg(test)]
mod tests;
