//! Execution and runtime specification types.
//!
//! These types describe what a sandbox needs in order to run: the runtime
//! technology and startup policy, compute, storage and disk I/O requests,
//! the memory balloon, the host CPU identity a VM snapshot is portable
//! across, and the immutable execution environment and its metadata.
//!
//! `execution` and `environment` refer to each other (execution metadata names
//! its environment reference, and an environment is built from execution
//! identities), so they share a crate with the runtime, compute and host CPU
//! types they are built from. It is its own crate so that code can name these
//! types without depending on `pplx-space-common-core`, which re-exports each
//! module from `types`.

use pplx_space_byte_size as size;
use pplx_space_content_digest::{Digest, DigestParseError};
use pplx_space_cpu_arch::Arch;
use pplx_space_k8s_ids::{LabelValue, LabelValueParseError};

pub mod balloon;
pub mod compute;
pub mod disk_io;
pub mod environment;
pub mod execution;
pub mod host_cpu;
pub mod runtime;

pub use balloon::*;
pub use compute::*;
pub use disk_io::*;
pub use environment::*;
pub use execution::*;
pub use host_cpu::*;
pub use runtime::{
    EffectiveStartup, RUNTIME_CLOUD_HYPERVISOR, RUNTIME_CLOUD_HYPERVISOR_ALIAS,
    RUNTIME_FIRECRACKER, RUNTIME_FIRECRACKER_ALIAS, RUNTIME_GVISOR, RUNTIME_GVISOR_ALIAS,
    RuntimeCapabilities, RuntimeDeploymentId, RuntimeDeploymentIdParseError, RuntimeKind,
    RuntimeKindParseError, StartupPolicy, UnsupportedStartupPolicyError,
};
