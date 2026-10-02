//! `space-rtmgr-svc`: the node-local runtime and kernel manager.
//!
//! It serves its RPCs over the node's Unix socket. `ListExecutionArtifacts`
//! lists the runtime releases and guest kernels on the node, and
//! `GetStatus` reports whether the paths the runtime manager relies on are
//! protected from other accounts. `ListAvailableKernels`, `InstallKernel`,
//! and `UninstallKernel` list, install, and remove guest kernels a kernel
//! source publishes for a runtime line (Firecracker's by default), each
//! install or uninstall tracked as a
//! durable operation; mutations stay disabled unless the service is given a
//! kernel install root. It owns no selection: node-manager keeps resolving
//! launches from its own inventory, so this service being down or restarting
//! does not affect any launch.

mod convert;
mod installs;
mod protection;
mod run;
mod service;

pub use installs::{
    BoxError, ByteChunks, ContentMd5, ContentMd5ParseError, FIRECRACKER_CI_KERNEL_SOURCE,
    InstallLimits, KernelSource, KernelVersion, KernelVersionParseError, OpenInstallerError,
    PrepareStagingError, ReleaseLine, ReleaseLineParseError, RuntimeLine, SourceError,
    SourceKernel, StagingError, StoreError, VerifiableKernel,
};
pub use run::{
    DEFAULT_INSTALL_LIMITS, DEFAULT_LIST_ADMISSION_TIMEOUT, DEFAULT_SCAN_LIMITS,
    KernelSourceConfig, RunRuntimeManagerError, RuntimeManagerInstallConfig,
    RuntimeManagerRunConfig, SERVICE_NAME, run,
};
pub use service::RuntimeManagerService;
