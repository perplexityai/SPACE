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
//! The org and sandbox shares exist because the pod budget alone lets one
//! tenant hold every slot and byte on every replica at once. The share is keyed
//! by org, not by client id: one client id can front many orgs' traffic, so a
//! client-id key would isolate nothing.
//!
//! Admitted bodies are governed by [`body::GovernedBody`]: an optional
//! per-org byte-rate bucket slows long transfers through TCP backpressure
//! instead of failing them, and an optional minimum body rate ends an upload
//! that stalls while it holds permits.
//!
//! The rejection is deliberately 429, never 503: 503 invites the immediate
//! retries that turn an overloaded pod into a retry storm, while 429 with a
//! jittered `Retry-After` asks clients to back off.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_metrics::data_plane::{TransferDirection, UploadAdmissionScope, UploadRejectReason};
use rand::RngExt as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::state::EcwProxyError;

mod body;
mod byte_rate;
mod shares;

use body::GovernedBody;
pub(crate) use body::StallSignal;
use byte_rate::{ByteRateBucket, OrgByteRates};
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
/// first. Uploads that declare their length reserve only that, so several of
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
/// A body that delivers less than 64 KiB/s × 15 s in a window of 15 s spent
/// waiting on the client is treated as stalled. Windows are consecutive, not
/// sliding, and bytes that arrive without a wait count toward the window
/// they arrive in.
pub const DEFAULT_UPLOAD_MIN_BODY_RATE: MinBodyRateConfig = MinBodyRateConfig {
    bytes_per_second: NonZeroU64::new(64 << 10).unwrap(),
    window: Duration::from_secs(15),
};

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
    /// Throttles each org's `POST /files` request bodies and `GET /files`
    /// response bodies together. `None` leaves transfers unthrottled.
    pub org_transfer_rate: Option<ByteRateConfig>,
    /// Ends an admitted upload whose body arrives slower than this. `None`
    /// lets a stalled upload hold its permits until the client gives up.
    pub min_body_rate: Option<MinBodyRateConfig>,
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

/// An upload must deliver `bytes_per_second × window` bytes in every `window`
/// of time spent waiting on the client. Time the pod spends elsewhere, in the
/// byte-rate bucket or writing to the spool or the guest, doesn't count.
#[derive(Debug, Clone, Copy)]
pub struct MinBodyRateConfig {
    pub bytes_per_second: NonZeroU64,
    pub window: Duration,
}

impl UploadShareConfig {
    pub fn new(max_concurrent_uploads: NonZeroUsize, max_in_flight_bytes: NonZeroU64) -> Self {
        Self {
            max_concurrent_uploads,
            max_in_flight_bytes,
        }
    }
}

impl ByteRateConfig {
    /// `None` when the rate is `off`.
    pub fn unless_off(bytes_per_second: LimitOrOff, burst_bytes: NonZeroU64) -> Option<Self> {
        bytes_per_second.0.map(|bytes_per_second| Self {
            bytes_per_second,
            burst_bytes,
        })
    }
}

impl MinBodyRateConfig {
    /// `None` when the rate is `off`.
    pub fn unless_off(bytes_per_second: LimitOrOff, window: Duration) -> Option<Self> {
        bytes_per_second.0.map(|bytes_per_second| Self {
            bytes_per_second,
            window,
        })
    }
}

/// A non-zero limit, or `off` for none, as a flag spells it. Zero is
/// rejected rather than read as `off`, so a typo can't disable a limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitOrOff(pub Option<NonZeroU64>);

impl std::str::FromStr for LimitOrOff {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.eq_ignore_ascii_case("off") {
            return Ok(Self(None));
        }
        value
            .parse::<NonZeroU64>()
            .map(|limit| Self(Some(limit)))
            .map_err(|error| format!("expected a non-zero integer or `off`: {error}"))
    }
}

impl std::fmt::Display for LimitOrOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(limit) => limit.fmt(f),
            None => f.write_str("off"),
        }
    }
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
    org_transfer_rates: Option<OrgByteRates>,
    min_body_rate: Option<MinBodyRateConfig>,
    wait: Duration,
}

/// An admitted upload: hold `permit` until the handler returns and read
/// `body` in place of the request's. Map a body-phase failure to
/// [`EcwProxyError::upload_body_stalled`] when `stalled` reports one.
pub(crate) struct AdmittedUpload {
    pub(crate) permit: UploadPermit,
    pub(crate) body: Body,
    pub(crate) stalled: StallSignal,
}

/// Held for the lifetime of an admitted upload. Dropping it returns every
/// slot and reservation. Fields drop in order, pod first.
#[derive(Debug)]
pub(crate) struct UploadPermit {
    _pod: BudgetPermit,
    _org: SharePermit<OrgId>,
    _sandbox: SharePermit<SandboxId>,
}

impl UploadAdmission {
    pub(crate) fn new(config: &UploadAdmissionConfig) -> Self {
        Self {
            pod: Budget::new(UploadShareConfig::new(
                config.max_concurrent_uploads,
                config.max_in_flight_bytes,
            )),
            orgs: ShareMap::new(config.per_org),
            sandboxes: ShareMap::new(config.per_sandbox),
            org_transfer_rates: config.org_transfer_rate.map(OrgByteRates::new),
            min_body_rate: config.min_body_rate,
            wait: config.wait,
        }
    }

    /// Admits an upload and governs its request body: the permit, the body to
    /// read in place of `body`, and the signal that reports whether that body
    /// ended because the client stalled. Admission and governance come as
    /// one, so no upload can hold permits without the minimum body rate that
    /// stops a stalled client from keeping them.
    pub(crate) async fn admit_upload(
        &self,
        tenant: UploadTenant,
        reserve_bytes: u64,
        body: Body,
    ) -> Result<AdmittedUpload, EcwProxyError> {
        let permit = self.admit(tenant, reserve_bytes).await?;
        let stalled = StallSignal::default();
        let rate = self.org_rate(tenant.org_id);
        let body = if rate.is_none() && self.min_body_rate.is_none() {
            body
        } else {
            Body::new(GovernedBody::new(
                body,
                TransferDirection::Upload,
                rate,
                self.min_body_rate.map(|floor| (floor, stalled.clone())),
            ))
        };
        Ok(AdmittedUpload {
            permit,
            body,
            stalled,
        })
    }

    /// Wait up to the admission wait for the sandbox's, then the org's, then
    /// the pod's slot and `reserve_bytes` of each budget. Every wait shares
    /// one deadline. The narrowest share goes first, so an upload queued
    /// behind its own sandbox or org holds none of the pod's slots or bytes
    /// meanwhile. A reservation never exceeds a whole budget, so a budget
    /// smaller than one upload still admits uploads one at a time instead of
    /// none; each budget caps the reservation it holds independently.
    ///
    /// Tokio semaphores are FIFO, so a large reservation at the head of a
    /// byte queue holds every smaller upload behind it, even ones the free
    /// bytes would cover. That's the price of never starving large uploads,
    /// and it holds whichever permit is taken first. Taking the slot first
    /// means a timeout behind byte waiters that fill every slot reports
    /// `concurrency`, not `bytes`.
    pub(crate) async fn admit(
        &self,
        tenant: UploadTenant,
        reserve_bytes: u64,
    ) -> Result<UploadPermit, EcwProxyError> {
        let deadline = Instant::now() + self.wait;
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
            _pod: pod,
            _org: org,
            _sandbox: sandbox,
        })
    }

    /// Throttles a `GET /files` response body by the org's byte-rate bucket.
    /// A download holds no permit, so a slow reader costs the pod only its
    /// own connection and is left to TCP backpressure.
    pub(crate) fn govern_download(&self, org_id: OrgId, body: Body) -> Body {
        match self.org_rate(org_id) {
            Some(rate) => Body::new(GovernedBody::new(
                body,
                TransferDirection::Download,
                Some(rate),
                None,
            )),
            None => body,
        }
    }

    fn org_rate(&self, org_id: OrgId) -> Option<Arc<ByteRateBucket>> {
        self.org_transfer_rates
            .as_ref()
            .map(|rates| rates.for_org(org_id))
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

    async fn admit(
        &self,
        deadline: Instant,
        reserve_bytes: u64,
    ) -> Result<BudgetPermit, UploadRejectReason> {
        let permits = reserve_bytes
            .div_ceil(BYTES_PER_PERMIT)
            .min(u64::from(self.budget_permits)) as u32;
        let upload = acquire_by(deadline, self.uploads.clone().acquire_owned())
            .await
            .ok_or(UploadRejectReason::Concurrency)?;
        let bytes = acquire_by(deadline, self.bytes.clone().acquire_many_owned(permits))
            .await
            .ok_or(UploadRejectReason::Bytes)?;
        Ok(BudgetPermit {
            _upload: upload,
            _bytes: bytes,
        })
    }
}

/// One deadline spans every acquire in `admit`, which is why this isn't
/// `pplx_space_common_grpc::queued_slots`: a `QueuedSlots` request fixes its
/// own deadline, so several of them can't share one.
///
/// `None` when the deadline passes first. The semaphores are never closed,
/// so an acquire error can't happen; it's folded into `None` all the same.
async fn acquire_by(
    deadline: Instant,
    acquire: impl Future<Output = Result<OwnedSemaphorePermit, tokio::sync::AcquireError>>,
) -> Option<OwnedSemaphorePermit> {
    tokio::time::timeout_at(deadline, acquire).await.ok()?.ok()
}

fn upload_rejected(scope: UploadAdmissionScope, reason: UploadRejectReason) -> EcwProxyError {
    pplx_space_metrics::data_plane::upload_rejected(scope, reason);
    let retry_after = Duration::from_secs(
        rand::rng().random_range(RETRY_AFTER_MIN.as_secs()..=RETRY_AFTER_MAX.as_secs()),
    );
    EcwProxyError::too_many_uploads(retry_after)
}

#[cfg(test)]
mod tests;
