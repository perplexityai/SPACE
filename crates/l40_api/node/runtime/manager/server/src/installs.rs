//! What the runtime manager installs and removes: guest kernels and runtime
//! releases.
//!
//! - [`source`]: the published-kernel vocabulary (release lines, kernel
//!   versions, MD5s) and the [`KernelSource`] trait.
//! - `s3`: the S3 kernel source, behind the `s3` feature.
//! - [`release_source`]: the release vocabulary and the [`ReleaseSource`]
//!   trait; `github`, Firecracker's GitHub releases, behind the `github`
//!   feature.
//! - [`operations`]: the [`Installer`] callers use, and the operation view.
//! - [`executor`]: the shared durable install/delete pipeline.
//! - `kernel` and `release`: artifact preparation adapters.
//! - [`publisher`]: the pins downloads are checked against, and node-manager,
//!   which publishes staged artifacts into the node's root-owned store.
//! - [`staging`]: private staging and hashing.
//! - [`store`]: durable mutation intent and per-job results.

mod boot_artifact;
mod executor;
#[cfg(feature = "github")]
mod github;
#[cfg(feature = "github")]
pub(crate) mod https_kernel;
mod kernel;
mod operations;
mod publisher;
mod release;
mod release_source;
#[cfg(feature = "s3")]
mod s3;
pub(crate) mod source;
mod staging;
mod store;
#[cfg(test)]
pub(crate) mod test_support;
mod transfer;

pub(crate) use executor::ExecutorParts;
#[cfg(feature = "github")]
pub use github::GitHubReleaseSourceError;
#[cfg(feature = "github")]
pub(crate) use github::{GitHubReleaseSource, GitHubRuntime};
pub(crate) use operations::{
    CancelOperationError, InstallOperation, Installer, LookupOperationError, Operation,
    OperationState, SubmitOperationError,
};
pub use operations::{InstallLimits, OpenInstallerError};
pub(crate) use publisher::{DirectoryCatalog, NodeManagerPublisher, PinSource};
pub use release::ReleaseConfigurationError;
pub(crate) use release::{ReleaseContext, ReleaseSources};
pub use release_source::{
    CLOUD_HYPERVISOR_RELEASE_SOURCE, FIRECRACKER_RELEASE_SOURCE, ReleaseArchiveMember,
    ReleaseChunks, ReleasePayload, ReleaseSource, ReleaseSourceError, ReleaseUrl,
    ReleaseUrlParseError, VerifiableRelease,
};
#[cfg(feature = "s3")]
pub(crate) use s3::S3KernelSource;
pub(crate) use source::host_arch;
pub use source::{
    BoxError, ByteChunks, ContentMd5, ContentMd5ParseError, FIRECRACKER_CI_KERNEL_SOURCE,
    KernelLocation, KernelLocationError, KernelSource, KernelVersion, KernelVersionParseError,
    ReleaseLine, ReleaseLineParseError, RuntimeLine, SourceError, SourceKernel, VerifiableKernel,
};
pub use staging::{PrepareStagingError, StagingError};
pub(crate) use store::JobOutcome;
pub use store::StoreError;

/// `error` and every error that caused it, as one line. Operation errors and
/// RPC statuses carry this, since their readers see only the text.
pub(crate) fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    text
}
