#![forbid(unsafe_code)]

pub mod artifact;
mod btrfs_push;
mod codec_admission;
pub mod credentials;
pub mod crypto;
pub mod db;
pub mod executor;
pub mod run;
pub mod service;
pub mod staging;

pub use executor::{SnapshotUploadMode, StreamingPushBudget};

pub use run::{
    DEFAULT_CODEC_CONCURRENCY, DEFAULT_CODEC_PRIORITY_RESERVE, DEFAULT_PULL_CONCURRENCY,
    DEFAULT_PUSH_CONCURRENCY, DEFAULT_SERVER_DRAIN_TIMEOUT_SECS,
    DEFAULT_SERVICE_MUTEX_TIMEOUT_SECS, DEFAULT_TRANSFER_DRAIN_TIMEOUT_SECS,
    DEFAULT_TRANSFER_MAX_ATTEMPTS, DEFAULT_TRANSFER_TIMEOUT_SECS, ParsePullPriorityBandError,
    PullPriorityBand, RunVolmgrError, SERVICE_NAME, ValidatePullPriorityBandsError,
    VolmgrRunConfig, run,
};

pub use pplx_space_common_jobs::{JobPriority, PriorityBand};
pub use pplx_space_object_store::ObjectCredentials;
pub use pplx_space_volmgr_client::VOLMGR_SOCKET;
pub use pplx_space_volmgr_client::proto;

/// In-memory representation of a volume attachment row.
#[derive(Debug, Clone)]
pub struct VolumeAttachment {
    pub name: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub s3_region: String,
    pub read_only: bool,
    pub byok_key_id: Option<String>,
}
