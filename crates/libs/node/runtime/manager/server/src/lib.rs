//! Node-local diagnostic listing and a durable artifact install/delete API.
//!
//! Runtime-manager writes only private staging and operation state. Node-manager
//! verifies and publishes bytes into a separate root-owned managed store.
//! Execution never discovers that store: its image-provided roots, aliases,
//! default pair and restore behavior remain independent of this service.

mod convert;
mod installs;
mod protection;
mod run;
mod service;

#[cfg(feature = "github")]
pub use installs::GitHubReleaseSourceError;
pub use installs::{
    BoxError, ByteChunks, ContentMd5, ContentMd5ParseError, FIRECRACKER_CI_KERNEL_SOURCE,
    FIRECRACKER_RELEASE_SOURCE, InstallLimits, KernelSource, KernelVersion,
    KernelVersionParseError, OpenInstallerError, PrepareStagingError, ReleaseChunks, ReleaseLine,
    ReleaseLineParseError, ReleaseSource, ReleaseSourceError, ReleaseUrl, ReleaseUrlParseError,
    RuntimeLine, SourceError, SourceKernel, StagingError, StoreError, VerifiableKernel,
    VerifiableRelease,
};
pub use run::{
    DEFAULT_INSTALL_LIMITS, DEFAULT_LIST_ADMISSION_TIMEOUT, DEFAULT_SCAN_LIMITS,
    KernelSourceConfig, ReleaseSourceConfig, RunRuntimeManagerError, RuntimeManagerInstallConfig,
    RuntimeManagerReleaseConfig, RuntimeManagerRunConfig, SERVICE_NAME, run,
};
pub use service::RuntimeManagerService;
