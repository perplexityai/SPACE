//! Node-local sandbox lifecycle service.

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::expect_used))]

pub(crate) mod btrfs_via_nodemgr;
pub(crate) mod byok;
pub mod config;
pub mod disk_usage;
mod firecracker_via_nodemgr;
mod guest_filesystem_flush;
mod inventory_budget;
mod lifecycle_metrics;
pub mod local_artifacts;
pub mod node_manager_transport;
mod node_state_sync;
pub(crate) mod retained_source;
pub(crate) mod rootfs_encryption;
pub(crate) mod route_activation_claims;
pub mod run;
mod runtime_reconciliation;
pub mod sandbox_eviction;
pub mod service;
mod sidecar;
mod sidecar_env;
pub(crate) mod snapshot_encryption;
pub mod snapshot_gc;
pub mod snapshot_restore;
pub mod snapshot_scheduler;
pub mod snapshot_upload;
pub mod snapshot_upload_jobs;
pub mod template_resolver;
mod transfer_directory;
mod vm_diagnostics;
pub mod watchdog;

pub use pplx_space_sbx_mgr_client::{
    SOCKET_PATH, SandboxManagerGrpcClient, connect, encode_optional_sandbox_id, proto,
};
pub(crate) use pplx_space_sbx_mgr_store::auto_lifecycle_delete_receipts;
pub use pplx_space_sbx_mgr_store::{auto_lifecycle, db, endpoint_proof, restore_operation, state};
pub use run::{RunSbxMgrError, SERVICE_NAME, SbxMgrRunConfig, run};
