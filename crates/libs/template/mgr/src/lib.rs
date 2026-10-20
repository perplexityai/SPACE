#![forbid(unsafe_code)]

pub mod background_jobs;
pub mod btrfs_via_nodemgr;
mod content_hash;
pub mod db;
pub(crate) mod eviction;
mod node_state_sync;
pub mod pinned_digests;
pub mod pool_manager;
pub mod prefetch;
mod rootfs_backing;
pub use rootfs_backing::RootfsBackingConfigError;
pub(crate) mod rootfs_cache;
pub(crate) mod rootfs_zstd;
mod run;
pub mod service;
pub mod volume;

// Re-export proto types and connect() from the client crate.
// This avoids cyclic dependencies: sbx-mgr can depend on the client crate
// without pulling in this full crate (which depends on sbx-mgr).
pub use pplx_space_template_mgr_client::{GRPC_REQUEST_TIMEOUT, SOCKET_PATH, connect, proto};

pub use run::{RunTemplateMgrError, SERVICE_NAME, TemplateMgrRunConfig, run};
