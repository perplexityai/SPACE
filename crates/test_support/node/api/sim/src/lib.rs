//! In-process simulator for SPACE `space-node-api-svc`.
//!
//! Designed for local KinD clusters: implements the worker-role gRPC surface
//! (`WorkerApi`, `WorkerApiInternal`, `NodeInfo`) on top of in-memory state
//! plus a shared filesystem directory for cross-node suspend/restore. No
//! Firecracker, sandbox-manager, node-manager, or template-manager required.

#![forbid(unsafe_code)]

pub mod fs;
pub mod harness;
pub mod node_info;
pub mod processes;
pub mod state;
pub mod templates;
pub mod worker;

pub use harness::{SimWorkerHarness, SimWorkerHarnessBuilder, SimWorkerHarnessError};
pub use node_info::SimNodeInfoService;
pub use processes::{SimProcessError, SimProcessSpec, SimProcessTable};
pub use state::{SandboxRecord, SimState, SimStateError, Snapshot};
pub use worker::{SimWorkerApiInternalService, SimWorkerApiService};
