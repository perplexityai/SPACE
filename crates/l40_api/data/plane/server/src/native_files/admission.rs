//! Admission for Native `files:write` and `files:write_batch`, apart from
//! ECW's upload admission: its own switch, slots and byte budgets.
//!
//! A write is admitted after it is authorized and before its body is read. It
//! takes one slot and reserves its declared length, or its route's whole body
//! limit when it declares none, from its sandbox's share, its org's share and
//! the replica's budget, as an ECW upload does. A refused write gets the
//! Native `rate_limited` 429 with a jittered `Retry-After`.
//!
//! The defaults are sized for Native writes rather than ECW's 256 MiB spooled
//! uploads. A write's body is at most [`WRITE_FILE_BODY_LIMIT_BYTES`], and the
//! guest runs 8 file operations per sandbox at once and queues a burst past
//! that for up to 2 s, so a sandbox's share admits 8 writes of any size at
//! once and a write waits up to 2 s for a slot before it is refused.
//!
//! Over HTTP/2 only a small write waits; ECW uploads never do. An HTTP/2
//! stream left unread holds back its connection's other streams by as much of
//! the connection's 1 MiB receive window as its client has sent. A write that
//! declares at most [`HTTP2_WAITING_WRITE_MAX_BYTES`], a sixteenth of that
//! window, can hold only that much, so a burst of small writes past the
//! sandbox's slots waits for them as it would over HTTP/1.x. A larger write,
//! or one that declares no length, takes what is free at once: a few of them
//! waiting could fill the window, and the writes already admitted on the same
//! connection could then not finish sending their bodies to free the slots
//! the waiters wait for. On Kind, 12 writes of 4 MiB to one sandbox on one
//! connection, all let wait, took 2.3 s and 3 were still refused; with the
//! large ones refused at once, the same 3 are refused and the rest land in
//! about 0.3 s.
//!
//! The org transfer rate, minimum body rate and HTTP/2 duration limit of ECW's
//! admission don't apply, so no 408 or rate-limit 429 is new to Native
//! clients.
//!
//! A write's peak memory is about twice its reservation: the read body and
//! its parsed base64 are alive together briefly, and so are the base64 and
//! the decoded chunks. The replica's byte budget therefore bounds about twice
//! its size in memory, on top of ECW's.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use pplx_space_native_files::limits::{
    WRITE_FILE_BATCH_BODY_LIMIT_BYTES, WRITE_FILE_BODY_LIMIT_BYTES,
};

use crate::upload_admission::{UploadAdmissionConfig, UploadShareConfig};

pub const DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// About 2 GiB of peak memory at about twice the reservation.
pub const DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES: NonZeroU64 = NonZeroU64::new(1 << 30).unwrap();
/// As long as the guest queues a file operation that finds its slots busy.
pub const DEFAULT_NATIVE_UPLOAD_ADMISSION_WAIT: Duration = Duration::from_secs(2);
/// The largest declared length an HTTP/2 write waits for admission with: a
/// sixteenth of the 1 MiB receive window hyper gives an HTTP/2 connection.
pub(super) const HTTP2_WAITING_WRITE_MAX_BYTES: u64 = 64 << 10;
/// A quarter of the replica's, so one org can't take the replica.
pub const DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_ORG: NonZeroUsize =
    NonZeroUsize::new(64).unwrap();
/// A quarter of the replica's.
pub const DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_ORG: NonZeroU64 =
    NonZeroU64::new(256 << 20).unwrap();
/// The file operations the guest runs at once for a sandbox.
pub const DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_SANDBOX: NonZeroUsize =
    NonZeroUsize::new(8).unwrap();
/// Fits the sandbox's 8 slots of full-size writes, or batches, at once.
pub const DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_SANDBOX: NonZeroU64 =
    NonZeroU64::new(256 << 20).unwrap();

const _: () = {
    let largest = if WRITE_FILE_BODY_LIMIT_BYTES > WRITE_FILE_BATCH_BODY_LIMIT_BYTES {
        WRITE_FILE_BODY_LIMIT_BYTES
    } else {
        WRITE_FILE_BATCH_BODY_LIMIT_BYTES
    } as u64;
    let sandbox_slots = DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_SANDBOX.get() as u64;
    let sandbox_bytes = DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_SANDBOX.get();
    assert!(sandbox_slots * largest <= sandbox_bytes);
    assert!(sandbox_bytes <= DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_ORG.get());
    assert!(
        DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_SANDBOX.get()
            <= DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_ORG.get()
    );
};

#[derive(Debug, Clone)]
pub struct NativeUploadAdmissionConfig {
    pub max_concurrent_uploads: NonZeroUsize,
    pub max_in_flight_bytes: NonZeroU64,
    /// How long a write waits for its slots and bytes: over HTTP/1.x, and
    /// over HTTP/2 a write that declares at most
    /// [`HTTP2_WAITING_WRITE_MAX_BYTES`].
    pub wait: Duration,
    pub per_org: UploadShareConfig,
    pub per_sandbox: UploadShareConfig,
}

impl Default for NativeUploadAdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS,
            max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES,
            wait: DEFAULT_NATIVE_UPLOAD_ADMISSION_WAIT,
            per_org: UploadShareConfig {
                max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_ORG,
                max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_ORG,
            },
            per_sandbox: UploadShareConfig {
                max_concurrent_uploads: DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_SANDBOX,
                max_in_flight_bytes: DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_SANDBOX,
            },
        }
    }
}

impl NativeUploadAdmissionConfig {
    /// The same budgets as an upload admission whose bodies aren't governed.
    pub(super) fn budgets(&self) -> UploadAdmissionConfig {
        UploadAdmissionConfig {
            max_concurrent_uploads: self.max_concurrent_uploads,
            max_in_flight_bytes: self.max_in_flight_bytes,
            wait: self.wait,
            per_org: self.per_org,
            per_sandbox: self.per_sandbox,
            org_transfer_rate: None,
            min_body_rate: None,
            http2_upload_max_duration: None,
        }
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct NativeUploadAdmissionArgs {
    /// Admit Native `files:write` and `files:write_batch` under the
    /// per-replica, per-org and per-sandbox limits below; refused writes get
    /// 429. Apart from `--upload-admission`, which is ECW's. Accepts the
    /// usual boolean spellings (`true`/`1`/`yes`/`on` and their negations).
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_NATIVE_UPLOAD_ADMISSION",
        action = clap::ArgAction::Set,
        value_parser = clap::builder::BoolishValueParser::new(),
        num_args = 0..=1,
        default_value_t = false,
        default_missing_value = "true"
    )]
    pub native_upload_admission: bool,

    /// Maximum concurrent Native writes per replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_CONCURRENT_NATIVE_UPLOADS",
        default_value_t = DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS
    )]
    pub max_concurrent_native_uploads: NonZeroUsize,

    /// In-flight Native write bytes per replica, reserved from
    /// `Content-Length` (the route's body limit when absent).
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES",
        default_value_t = DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES
    )]
    pub max_in_flight_native_upload_bytes: NonZeroU64,

    /// How long a Native write waits for admission before it's refused: over
    /// HTTP/1.x, and over HTTP/2 one that declares at most 64 KiB. Other
    /// HTTP/2 writes take what's free at once.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_NATIVE_UPLOAD_ADMISSION_WAIT_MS",
        default_value_t = DEFAULT_NATIVE_UPLOAD_ADMISSION_WAIT.as_millis() as u64
    )]
    pub native_upload_admission_wait_ms: u64,

    /// Maximum concurrent Native writes per org on this replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_CONCURRENT_NATIVE_UPLOADS_PER_ORG",
        default_value_t = DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_ORG
    )]
    pub max_concurrent_native_uploads_per_org: NonZeroUsize,

    /// In-flight Native write bytes per org on this replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_ORG",
        default_value_t = DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_ORG
    )]
    pub max_in_flight_native_upload_bytes_per_org: NonZeroU64,

    /// Maximum concurrent Native writes per sandbox on this replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_CONCURRENT_NATIVE_UPLOADS_PER_SANDBOX",
        default_value_t = DEFAULT_MAX_CONCURRENT_NATIVE_UPLOADS_PER_SANDBOX
    )]
    pub max_concurrent_native_uploads_per_sandbox: NonZeroUsize,

    /// In-flight Native write bytes per sandbox on this replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_SANDBOX",
        default_value_t = DEFAULT_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_SANDBOX
    )]
    pub max_in_flight_native_upload_bytes_per_sandbox: NonZeroU64,
}

impl NativeUploadAdmissionArgs {
    /// `None` unless `--native-upload-admission` is on.
    pub fn config(&self) -> Option<NativeUploadAdmissionConfig> {
        self.native_upload_admission
            .then(|| NativeUploadAdmissionConfig {
                max_concurrent_uploads: self.max_concurrent_native_uploads,
                max_in_flight_bytes: self.max_in_flight_native_upload_bytes,
                wait: Duration::from_millis(self.native_upload_admission_wait_ms),
                per_org: UploadShareConfig {
                    max_concurrent_uploads: self.max_concurrent_native_uploads_per_org,
                    max_in_flight_bytes: self.max_in_flight_native_upload_bytes_per_org,
                },
                per_sandbox: UploadShareConfig {
                    max_concurrent_uploads: self.max_concurrent_native_uploads_per_sandbox,
                    max_in_flight_bytes: self.max_in_flight_native_upload_bytes_per_sandbox,
                },
            })
    }
}
