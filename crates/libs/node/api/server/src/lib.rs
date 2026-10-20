//! The SPACE node API gateway: the per-node gRPC surface (WorkerApi,
//! WorkerApiInternal, BuilderApi, NodeInfo) and the startup, health, auth, and
//! drain wiring that fronts it.
//!
//! This crate is the whole service. The `space-node-api-svc` binary is a thin
//! `main` that parses flags, initializes telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the service.

pub mod builder;
mod client_stream;
pub mod clients;
pub mod drain;
pub mod node_ctl;
pub mod node_info;
mod restart_handoff;
mod run;
pub mod transfer;
pub mod uds_per_sandbox_limiter;
pub mod worker;

// `auth` (generic gRPC bearer plumbing) now lives in `pplx-space-common-svc`.
// Re-exported here so `crate::auth::…` and `node_api_server::auth` keep resolving.
pub use pplx_space_common_svc::auth;
pub use pplx_space_node_api_client::proto;
pub use pplx_space_node_mgr_client::{NetworkPoolStatusReader, NodeResourceStatusReader};
pub use run::{
    BindListenerError, DEFAULT_SHUTDOWN_GRACE_SECS, NodeApiRunConfig, NodeIdError, RunNodeApiError,
    SERVICE_NAME, run,
};
