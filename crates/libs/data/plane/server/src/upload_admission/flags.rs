//! The upload-admission flags, flattened into the service's command line.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use super::{
    ByteRateConfig, DEFAULT_HTTP2_UPLOAD_MAX_DURATION, DEFAULT_MAX_CONCURRENT_UPLOADS,
    DEFAULT_MAX_CONCURRENT_UPLOADS_PER_ORG, DEFAULT_MAX_CONCURRENT_UPLOADS_PER_SANDBOX,
    DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES, DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG,
    DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX, DEFAULT_ORG_TRANSFER_RATE,
    DEFAULT_UPLOAD_ADMISSION_WAIT, DEFAULT_UPLOAD_MIN_BODY_RATE, MinBodyRateConfig,
    UploadAdmissionConfig, UploadShareConfig,
};

#[derive(Debug, Clone, clap::Args)]
pub struct UploadAdmissionArgs {
    /// Admit `POST /files` uploads under the per-replica, per-org and
    /// per-sandbox limits below; rejected uploads get 429. Accepts the usual
    /// boolean spellings (`true`/`1`/`yes`/`on` and their negations).
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_UPLOAD_ADMISSION",
        action = clap::ArgAction::Set,
        value_parser = clap::builder::BoolishValueParser::new(),
        num_args = 0..=1,
        default_value_t = false,
        default_missing_value = "true"
    )]
    pub upload_admission: bool,

    /// Maximum concurrent `POST /files` uploads per replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS",
        default_value_t = DEFAULT_MAX_CONCURRENT_UPLOADS
    )]
    pub max_concurrent_uploads: NonZeroUsize,

    /// In-flight upload bytes per replica, reserved from `Content-Length`
    /// (the write path's body limit when absent: 256 MiB, or 1 MiB for JSON).
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES",
        default_value_t = DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES
    )]
    pub max_in_flight_upload_bytes: NonZeroU64,

    /// How long an HTTP/1.x upload waits for admission before it's rejected.
    /// HTTP/2 uploads don't wait.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_UPLOAD_ADMISSION_WAIT_MS",
        default_value_t = DEFAULT_UPLOAD_ADMISSION_WAIT.as_millis() as u64
    )]
    pub upload_admission_wait_ms: u64,

    /// Maximum concurrent uploads per org on this replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS_PER_ORG",
        default_value_t = DEFAULT_MAX_CONCURRENT_UPLOADS_PER_ORG
    )]
    pub max_concurrent_uploads_per_org: NonZeroUsize,

    /// In-flight upload bytes per org on this replica, reserved as for the
    /// replica's budget.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG",
        default_value_t = DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG
    )]
    pub max_in_flight_upload_bytes_per_org: NonZeroU64,

    /// Maximum concurrent uploads per sandbox on this replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS_PER_SANDBOX",
        default_value_t = DEFAULT_MAX_CONCURRENT_UPLOADS_PER_SANDBOX
    )]
    pub max_concurrent_uploads_per_sandbox: NonZeroUsize,

    /// In-flight upload bytes per sandbox on this replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX",
        default_value_t = DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX
    )]
    pub max_in_flight_upload_bytes_per_sandbox: NonZeroU64,

    /// Sustained bytes per second each org's file transfers, uploads and
    /// downloads together, may move through this replica, or `off`. HTTP/1.x
    /// uploads over it are slowed; HTTP/2 uploads and downloads are refused
    /// with 429 while the org is over it. Applies only with
    /// `--upload-admission`.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_ORG_TRANSFER_BYTES_PER_SECOND",
        default_value_t = LimitOrOff(Some(DEFAULT_ORG_TRANSFER_RATE.bytes_per_second))
    )]
    pub org_transfer_bytes_per_second: LimitOrOff,

    /// Bytes an org may move at once before its transfer rate applies.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_ORG_TRANSFER_BURST_BYTES",
        default_value_t = DEFAULT_ORG_TRANSFER_RATE.burst_bytes
    )]
    pub org_transfer_burst_bytes: NonZeroU64,

    /// Slowest an admitted upload's body may arrive, in bytes per second of
    /// waiting on the client, before it's ended with 408, or `off`. Applies
    /// only with `--upload-admission`.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_UPLOAD_MIN_BODY_BYTES_PER_SECOND",
        default_value_t = LimitOrOff(Some(DEFAULT_UPLOAD_MIN_BODY_RATE.bytes_per_second))
    )]
    pub upload_min_body_bytes_per_second: LimitOrOff,

    /// The span of client waiting the minimum body rate is judged over.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_UPLOAD_MIN_BODY_RATE_WINDOW_MS",
        default_value_t = NonZeroU64::new(DEFAULT_UPLOAD_MIN_BODY_RATE.window.as_millis() as u64).unwrap()
    )]
    pub upload_min_body_rate_window_ms: NonZeroU64,

    /// How long after admission an HTTP/2 upload, which isn't held to the
    /// minimum body rate, may take to finish before it's ended with 408, or
    /// `off`. Applies only with `--upload-admission`.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_UPLOAD_HTTP2_MAX_DURATION_MS",
        default_value_t = LimitOrOff(NonZeroU64::new(DEFAULT_HTTP2_UPLOAD_MAX_DURATION.as_millis() as u64))
    )]
    pub upload_http2_max_duration_ms: LimitOrOff,
}

impl UploadAdmissionArgs {
    /// `None` unless `--upload-admission` is on.
    pub fn config(&self) -> Option<UploadAdmissionConfig> {
        self.upload_admission.then(|| UploadAdmissionConfig {
            max_concurrent_uploads: self.max_concurrent_uploads,
            max_in_flight_bytes: self.max_in_flight_upload_bytes,
            wait: Duration::from_millis(self.upload_admission_wait_ms),
            per_org: UploadShareConfig {
                max_concurrent_uploads: self.max_concurrent_uploads_per_org,
                max_in_flight_bytes: self.max_in_flight_upload_bytes_per_org,
            },
            per_sandbox: UploadShareConfig {
                max_concurrent_uploads: self.max_concurrent_uploads_per_sandbox,
                max_in_flight_bytes: self.max_in_flight_upload_bytes_per_sandbox,
            },
            org_transfer_rate: self
                .org_transfer_bytes_per_second
                .0
                .map(|bytes_per_second| ByteRateConfig {
                    bytes_per_second,
                    burst_bytes: self.org_transfer_burst_bytes,
                }),
            min_body_rate: self
                .upload_min_body_bytes_per_second
                .0
                .map(|bytes_per_second| MinBodyRateConfig {
                    bytes_per_second,
                    window: Duration::from_millis(self.upload_min_body_rate_window_ms.get()),
                }),
            http2_upload_max_duration: self
                .upload_http2_max_duration_ms
                .0
                .map(|ms| Duration::from_millis(ms.get())),
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

#[cfg(test)]
mod tests;
