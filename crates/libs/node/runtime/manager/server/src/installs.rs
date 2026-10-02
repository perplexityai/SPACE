//! Guest kernels: what the kernel source publishes, and installing and
//! removing them.
//!
//! - [`source`]: the published-kernel vocabulary (release lines, kernel
//!   versions, MD5s) and the [`KernelSource`] trait.
//! - `s3`: the S3 source, behind the `s3` feature.
//! - [`operations`]: the [`Installer`] callers use, and the operation view.
//! - [`executor`]: the job that downloads, verifies, publishes, and removes.
//! - [`seed`]: recording the kernels the node image pre-installed.
//! - [`staging`]: private staging, hashing, and the no-replace rename.
//! - [`store`]: install records and per-job results.

mod executor;
mod operations;
#[cfg(feature = "s3")]
mod s3;
mod seed;
mod source;
mod staging;
mod store;
#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use operations::{
    CancelOperationError, InstallKernelError, Installer, KernelOperation, LookupOperationError,
    Operation, OperationState, UninstallKernelError,
};
pub use operations::{InstallLimits, OpenInstallerError};
#[cfg(feature = "s3")]
pub(crate) use s3::S3KernelSource;
pub(crate) use seed::{SeedError, SeedReport};
pub(crate) use source::host_arch;
pub use source::{
    BoxError, ByteChunks, ContentMd5, ContentMd5ParseError, FIRECRACKER_CI_KERNEL_SOURCE,
    KernelSource, KernelVersion, KernelVersionParseError, ReleaseLine, ReleaseLineParseError,
    RuntimeLine, SourceError, SourceKernel, VerifiableKernel,
};
pub use staging::{PrepareStagingError, StagingError};
pub use store::StoreError;
pub(crate) use store::{JobOutcome, KernelInstall};

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
