//! The SPACE API service: the public API, the host-gated E2B compatibility
//! API, and the admin API, together with the controllers, stores, and
//! substrate wiring behind them.
//!
//! This crate is the whole service. The `space-api-svc` binary is a thin
//! `main` that parses flags, initializes telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against
//! this library without copying any of the service.

pub mod api_surface;
pub mod auth;
pub mod client_classification;
pub mod client_concurrency;
pub mod cluster_admission;
pub mod command_stream;
pub mod controllers;
pub mod e2b;
pub mod egress_profiles;
pub mod external_console;
pub mod federated_identity;
pub mod fork_operation_executor;
mod fork_operation_store;
pub mod fork_operations;
pub mod inline_operations;
pub(crate) mod limiter_liveness;
pub mod middleware;
pub mod models;
pub mod native_sandbox_limits;
// Resolving a node-group candidate set produces a `NodeGroupPlacement`, which
// lives behind common-core's `kubernetes` feature.
mod credential_injection;
mod ingress_activation;
#[cfg(feature = "kubernetes")]
pub mod node_group_placement;
pub mod public_operation_driver;
pub mod public_operations;
pub mod public_sandbox_operations;
mod public_sandbox_projection;
pub mod public_sessions;
mod public_store;
pub mod public_template_operations;
pub mod reconcile_queue;
pub mod reconcile_trigger;
pub(crate) mod redis_otel;
pub mod retry_deferral;
pub mod routes;
mod run;
pub mod template_build_jobs_store;
mod template_inventory_store;
mod template_releases_store;
pub mod tls_interception_exemptions;
pub mod user_role;
mod wake_trace;

pub use run::{
    ADMIN_SERVICE_NAME, ApiExtensions, ApiRunConfig, ApiSurface, DEFAULT_SHUTDOWN_GRACE_SECS,
    PUBLIC_SERVICE_NAME, RunApiError, run, run_with,
};

#[cfg(test)]
mod test_support;
